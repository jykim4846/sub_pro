from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from .db import (
    connect,
    enrich_notice_from_detail,
    enrich_notice_from_result,
    stub_notice_ids,
    upsert_demand_agency,
    upsert_bid_result,
    upsert_contract,
    upsert_notice,
    upsert_procurement_plan,
)


PPS_ENDPOINTS = {
    "notices": {
        "goods": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoThng",
        "construction": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoCnstwk",
        "service": "http://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServc",
    },
    "results": {
        "goods": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusThng",
        "construction": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusCnstwk",
        "service": "http://apis.data.go.kr/1230000/as/ScsbidInfoService/getScsbidListSttusServc",
    },
    "contracts": {
        "goods": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListThng",
        "construction": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListCnstwk",
        "service": "http://apis.data.go.kr/1230000/ao/CntrctInfoService/getCntrctInfoListServc",
    },
    "plans": {
        "goods": "http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListThng",
        "construction": "http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListCnstwk",
        "service": "http://apis.data.go.kr/1230000/ao/OrderPlanSttusService/getOrderPlanSttusListServc",
    },
}

PPS_USER_INFO_BASE_URL = "http://apis.data.go.kr/1230000/ao/UsrInfoService02"
PPS_USER_INFO_DEMAND_AGENCY_CANDIDATES = (
    # The exact demand-agency operation path is not exposed in the portal HTML.
    # These candidates follow the service's published naming pattern and are
    # probed until one returns a valid API payload instead of a 404.
    f"{PPS_USER_INFO_BASE_URL}/getDminsttInfo02",
    f"{PPS_USER_INFO_BASE_URL}/getDmndInsttInfo02",
    f"{PPS_USER_INFO_BASE_URL}/getDminsttInfoList02",
    f"{PPS_USER_INFO_BASE_URL}/getDmndInsttInfoList02",
    f"{PPS_USER_INFO_BASE_URL}/getDemandInsttInfo02",
)


class ApiError(RuntimeError):
    pass


@dataclass
class CollectResult:
    source: str
    category: str
    pages_fetched: int
    items_seen: int
    items_upserted: int


@dataclass
class BackfillWindowResult:
    source: str
    category: str
    start: str
    end: str
    pages_fetched: int
    items_seen: int
    items_upserted: int


@dataclass
class BackfillResult:
    category: str
    months: int
    windows: list[BackfillWindowResult]
    total_pages_fetched: int
    total_items_seen: int
    total_items_upserted: int


@dataclass
class EnrichStubResult:
    category: str
    attempted: int
    matched: int
    enriched: int
    skipped_invalid_id: int


@dataclass
class UserInfoSyncResult:
    pages_fetched: int
    items_seen: int
    items_upserted: int


RETRYABLE_HTTP_STATUS = frozenset({429, 500, 502, 503, 504})


class PublicDataPortalClient:
    def __init__(
        self,
        service_key: str,
        timeout: float = 30.0,
        opener: Callable[[str], str] | None = None,
        max_retries: int = 8,
        base_backoff_sec: float = 2.5,
        per_call_sleep_sec: float = 0.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.service_key = service_key
        self.timeout = timeout
        self.opener = opener or self._default_open
        self.max_retries = max_retries
        self.base_backoff_sec = base_backoff_sec
        self.per_call_sleep_sec = per_call_sleep_sec
        self.sleep = sleep

    def fetch_items(
        self,
        endpoint: str,
        query: dict[str, Any],
        max_pages: int = 1,
    ) -> tuple[list[dict[str, Any]], int]:
        page_no = int(query.get("pageNo", 1))
        all_items: list[dict[str, Any]] = []
        pages_fetched = 0

        while pages_fetched < max_pages:
            query["pageNo"] = page_no
            payload = self._request(endpoint, query)
            items, total_count = self._extract_items(payload)
            all_items.extend(items)
            pages_fetched += 1

            per_page = int(query.get("numOfRows", 100))
            if total_count is not None and page_no * per_page >= total_count:
                break
            if not items:
                break
            page_no += 1

        return all_items, pages_fetched

    def _request(self, endpoint: str, query: dict[str, Any]) -> Any:
        merged = dict(query)
        merged["serviceKey"] = self.service_key
        query_string = urllib.parse.urlencode(merged, doseq=True)
        url = f"{endpoint}?{query_string}"

        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                raw = self.opener(url)
                if self.per_call_sleep_sec > 0 and attempt == 0:
                    self.sleep(self.per_call_sleep_sec)
                return self._parse_payload(raw)
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code not in RETRYABLE_HTTP_STATUS:
                    raise
                self.sleep(self.base_backoff_sec * (2 ** attempt))
            except urllib.error.URLError as exc:
                last_exc = exc
                self.sleep(self.base_backoff_sec * (2 ** attempt))

        assert last_exc is not None
        raise last_exc

    def _default_open(self, url: str) -> str:
        with urllib.request.urlopen(url, timeout=self.timeout) as response:
            return response.read().decode("utf-8")

    @staticmethod
    def _parse_payload(raw: str) -> Any:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            try:
                root = ET.fromstring(raw)
            except ET.ParseError as exc:
                raise ApiError(f"Unable to parse API payload: {exc}") from exc
            return _xml_to_dict(root)

    @staticmethod
    def _extract_items(payload: Any) -> tuple[list[dict[str, Any]], int | None]:
        response = payload.get("response", payload) if isinstance(payload, dict) else payload
        body = response.get("body", response) if isinstance(response, dict) else {}
        header = response.get("header", {}) if isinstance(response, dict) else {}
        result_code = _first_non_empty(header, ["resultCode", "returnReasonCode"])
        if result_code and str(result_code) not in {"00", "0", "INFO-000"}:
            message = _first_non_empty(header, ["resultMsg", "returnAuthMsg", "returnReasonCode"])
            raise ApiError(f"API returned error {result_code}: {message}")

        items_value = body.get("items", []) if isinstance(body, dict) else []
        if isinstance(items_value, dict):
            item = items_value.get("item", [])
            items = item if isinstance(item, list) else [item]
        elif isinstance(items_value, list):
            items = items_value
        else:
            items = []

        total_count = body.get("totalCount") if isinstance(body, dict) else None
        return [item for item in items if isinstance(item, dict)], _to_int(total_count)


class PPSCollector:
    def __init__(self, client: PublicDataPortalClient, db_path: str) -> None:
        self.client = client
        self.db_path = db_path

    def collect(
        self,
        source: str,
        category: str,
        query: dict[str, Any],
        max_pages: int = 1,
        endpoint_override: str | None = None,
    ) -> CollectResult:
        endpoint = endpoint_override or PPS_ENDPOINTS[source][category]
        items, pages_fetched = self.client.fetch_items(endpoint, query, max_pages=max_pages)

        upserted = 0
        with connect(self.db_path) as conn:
            for item in items:
                if source == "notices":
                    if self._ingest_notice(conn, category, item):
                        upserted += 1
                elif source == "results":
                    if self._ingest_result(conn, category, item):
                        upserted += 1
                elif source == "contracts":
                    if self._ingest_contract(conn, category, item):
                        upserted += 1
                elif source == "plans":
                    if self._ingest_plan(conn, category, item):
                        upserted += 1
                else:
                    raise ValueError(f"Unsupported source: {source}")

        return CollectResult(
            source=source,
            category=category,
            pages_fetched=pages_fetched,
            items_seen=len(items),
            items_upserted=upserted,
        )

    def collect_between(
        self,
        category: str,
        sources: list[str],
        start: datetime,
        end: datetime | None = None,
        page_size: int = 100,
        max_pages_per_window: int = 20,
        inqry_div: str = "1",
    ) -> BackfillResult:
        windows: list[BackfillWindowResult] = []
        total_pages_fetched = 0
        total_items_seen = 0
        total_items_upserted = 0

        month_pairs = months_between(start=start, end=end)
        for start_text, end_text in month_pairs:
            for source in sources:
                query = build_collect_query(
                    source=source,
                    start=start_text,
                    end=end_text,
                    page_size=page_size,
                    inqry_div=inqry_div,
                )
                result = self.collect(
                    source=source,
                    category=category,
                    query=query,
                    max_pages=max_pages_per_window,
                )
                windows.append(
                    BackfillWindowResult(
                        source=source,
                        category=category,
                        start=start_text,
                        end=end_text,
                        pages_fetched=result.pages_fetched,
                        items_seen=result.items_seen,
                        items_upserted=result.items_upserted,
                    )
                )
                total_pages_fetched += result.pages_fetched
                total_items_seen += result.items_seen
                total_items_upserted += result.items_upserted

        return BackfillResult(
            category=category,
            months=len(month_pairs),
            windows=windows,
            total_pages_fetched=total_pages_fetched,
            total_items_seen=total_items_seen,
            total_items_upserted=total_items_upserted,
        )

    def backfill_recent_months(
        self,
        category: str,
        sources: list[str],
        months: int = 36,
        page_size: int = 100,
        max_pages_per_window: int = 20,
        end: datetime | None = None,
        inqry_div: str = "1",
    ) -> BackfillResult:
        windows: list[BackfillWindowResult] = []
        total_pages_fetched = 0
        total_items_seen = 0
        total_items_upserted = 0

        for start_text, end_text in month_windows(months=months, end=end):
            for source in sources:
                query = build_collect_query(
                    source=source,
                    start=start_text,
                    end=end_text,
                    page_size=page_size,
                    inqry_div=inqry_div,
                )
                result = self.collect(
                    source=source,
                    category=category,
                    query=query,
                    max_pages=max_pages_per_window,
                )
                windows.append(
                    BackfillWindowResult(
                        source=source,
                        category=category,
                        start=start_text,
                        end=end_text,
                        pages_fetched=result.pages_fetched,
                        items_seen=result.items_seen,
                        items_upserted=result.items_upserted,
                    )
                )
                total_pages_fetched += result.pages_fetched
                total_items_seen += result.items_seen
                total_items_upserted += result.items_upserted

        return BackfillResult(
            category=category,
            months=months,
            windows=windows,
            total_pages_fetched=total_pages_fetched,
            total_items_seen=total_items_seen,
            total_items_upserted=total_items_upserted,
        )

    def enrich_stub_notices(
        self,
        category: str,
        batch_limit: int | None = None,
        log: Callable[[str], None] | None = None,
    ) -> "EnrichStubResult":
        endpoint = PPS_ENDPOINTS["notices"][category]
        with connect(self.db_path) as conn:
            notice_ids = stub_notice_ids(conn, category=category, limit=batch_limit)

        attempted = 0
        matched = 0
        enriched = 0
        skipped_empty = 0

        for notice_id in notice_ids:
            attempted += 1
            bid_ntce_no = _split_notice_number(notice_id)
            if not bid_ntce_no:
                skipped_empty += 1
                continue

            query: dict[str, Any] = {
                "pageNo": 1,
                "numOfRows": 5,
                "inqryDiv": "2",
                "type": "json",
                "bidNtceNo": bid_ntce_no,
            }
            try:
                items, _ = self.client.fetch_items(endpoint, query, max_pages=1)
            except ApiError as exc:
                if log is not None:
                    log(f"[enrich-stubs] {notice_id}: api error {exc}")
                continue
            except urllib.error.HTTPError as exc:
                if log is not None:
                    log(
                        f"[enrich-stubs] {notice_id}: http {exc.code} after retries; "
                        f"cooling down 60s before continuing"
                    )
                time.sleep(60)
                continue
            except urllib.error.URLError as exc:
                if log is not None:
                    log(f"[enrich-stubs] {notice_id}: network error {exc}; skipping")
                continue

            detail = _pick_detail_for_notice(items, notice_id) if items else None
            if detail is None:
                continue
            matched += 1

            with connect(self.db_path) as conn:
                enrich_notice_from_detail(
                    conn=conn,
                    notice_id=notice_id,
                    agency_name=_first_non_empty(
                        detail, ["dminsttNm", "ntceInsttNm", "dmndInsttNm", "orderInsttNm"], ""
                    ),
                    agency_code=str(_first_non_empty(
                        detail, ["dminsttCd", "ntceInsttCd", "dmndInsttCd", "orderInsttCd"], ""
                    ) or ""),
                    category=category,
                    contract_method=_first_non_empty(
                        detail, ["cntrctCnclsMthdNm", "cntrctMthdNm", "bidMethdNm"], ""
                    ),
                    region=_first_non_empty(
                        detail, ["prtcptPsblRgnNm", "rgstTyNm"], ""
                    ),
                    base_amount=_to_float(
                        _first_non_empty(detail, ["presmptPrce", "asignBdgtAmt", "bsisAmt"], 0)
                    ),
                    estimated_amount=_to_float(
                        _first_non_empty(detail, ["asignBdgtAmt", "presmptPrce"], None)
                    ),
                    floor_rate=_to_float(
                        _first_non_empty(detail, ["sucsfbidLwltRate", "lwstLmtRt"], None)
                    ),
                    opened_at=_first_non_empty(
                        detail, ["bidNtceDt", "opengDt", "ntceDt"]
                    ),
                )
            enriched += 1
            if log is not None and enriched % 100 == 0:
                log(f"[enrich-stubs] {category}: enriched {enriched}/{attempted}")

        return EnrichStubResult(
            category=category,
            attempted=attempted,
            matched=matched,
            enriched=enriched,
            skipped_invalid_id=skipped_empty,
        )

    def sync_demand_agencies(
        self,
        endpoint: str,
        query: dict[str, Any],
        max_pages: int = 1,
    ) -> UserInfoSyncResult:
        items, pages_fetched = self.client.fetch_items(endpoint, query, max_pages=max_pages)
        upserted = 0
        with connect(self.db_path) as conn:
            for item in items:
                if self._ingest_demand_agency(conn, item):
                    upserted += 1
        return UserInfoSyncResult(
            pages_fetched=pages_fetched,
            items_seen=len(items),
            items_upserted=upserted,
        )

    def resolve_demand_agency_endpoint(
        self,
        endpoint: str | None = None,
        candidates: tuple[str, ...] = PPS_USER_INFO_DEMAND_AGENCY_CANDIDATES,
    ) -> str:
        if endpoint:
            return endpoint
        probe_query = {
            "pageNo": 1,
            "numOfRows": 1,
            "type": "json",
            "inqryDiv": "1",
        }
        last_error: Exception | None = None
        for candidate in candidates:
            try:
                self.client.fetch_items(candidate, dict(probe_query), max_pages=1)
                return candidate
            except urllib.error.HTTPError as exc:
                last_error = exc
                if exc.code == 404:
                    continue
                return candidate
            except ApiError as exc:
                # API-level validation errors still mean the endpoint exists.
                return candidate
            except urllib.error.URLError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise ApiError("Unable to resolve demand-agency endpoint")

    @staticmethod
    def _ingest_notice(conn, category: str, item: dict[str, Any]) -> bool:
        notice_id = _notice_id(item)
        if not notice_id:
            return False

        upsert_notice(
            conn=conn,
            notice_id=notice_id,
            agency_name=_first_non_empty(item, ["dminsttNm", "ntceInsttNm", "dmndInsttNm", "orderInsttNm"], ""),
            agency_code=str(_first_non_empty(item, ["dminsttCd", "ntceInsttCd", "dmndInsttCd", "orderInsttCd"], "")),
            category=category,
            contract_method=_first_non_empty(item, ["cntrctMthdNm", "bidMethdNm", "cntrctCnclsMthdNm"], ""),
            region=_first_non_empty(item, ["prtcptPsblRgnNm", "rgstTyNm", "dmndInsttOfclEmailAdrs"], ""),
            base_amount=_to_float(_first_non_empty(item, ["presmptPrce", "asignBdgtAmt", "bsisAmount"], 0)),
            estimated_amount=_to_float(_first_non_empty(item, ["asignBdgtAmt", "presmptPrce"], None)),
            floor_rate=_to_float(_first_non_empty(item, ["sucsfbidLwltRate", "lwstLmtRt"], None)),
            opened_at=_first_non_empty(item, ["bidNtceDt", "ntceDt", "opengDt"]),
        )
        return True

    @staticmethod
    def _ingest_result(conn, category: str, item: dict[str, Any]) -> bool:
        notice_id = _notice_id(item)
        if not notice_id:
            return False

        award_amount = _to_float(_first_non_empty(item, ["sucsfbidAmt", "bidwinnrAmt", "dcsnAmt"], 0)) or 0.0
        bid_rate = _to_float(_first_non_empty(item, ["sucsfbidRate", "bidwinnrRate", "bidprcRt"], 0)) or 0.0

        agency_from_result = _first_non_empty(item, ["dminsttNm", "ntceInsttNm", "dmndInsttNm"], "") or ""
        agency_code_from_result = str(_first_non_empty(item, ["dminsttCd", "ntceInsttCd", "dmndInsttCd"], "") or "")
        opened_at_from_result = _first_non_empty(item, ["rlOpengDt", "fnlSucsfDate", "rgstDt"])
        derived_base_amount = award_amount / (bid_rate / 100.0) if award_amount > 0 and bid_rate > 0 else None

        enrich_notice_from_result(
            conn=conn,
            notice_id=notice_id,
            category=category,
            agency_name=agency_from_result,
            agency_code=agency_code_from_result,
            base_amount=derived_base_amount,
            opened_at=opened_at_from_result,
        )

        upsert_bid_result(
            conn=conn,
            notice_id=notice_id,
            award_amount=award_amount,
            bid_rate=bid_rate,
            bidder_count=_to_int(_first_non_empty(item, ["prtcptCnum", "bidprtcptCnt", "opengRankCount"], 0)) or 0,
            winning_company=_first_non_empty(item, ["sucsfbidprsnCmpyNm", "bidwinnrNm", "sucsfbidNm"], ""),
            result_status=_first_non_empty(item, ["opengRsltDivNm", "bidRsltNm"], "awarded"),
            category=category,
            winner_biz_no=str(_first_non_empty(item, ["bidwinnrBizno", "sucsfbidprsnBizno", "bidprtcptBizno"], "") or ""),
        )
        return True

    @staticmethod
    def _ingest_contract(conn, category: str, item: dict[str, Any]) -> bool:
        contract_id = _first_non_empty(item, ["untyCntrctNo", "cntrctNo", "cntrctRefNo"])
        notice_id = _notice_id(item)
        if not contract_id or not notice_id:
            return False

        amount = _to_float(_first_non_empty(item, ["cntrctAmt", "lastCntrctAmt"], 0))
        upsert_contract(
            conn=conn,
            contract_id=contract_id,
            notice_id=notice_id,
            contract_amount=amount,
            contract_date=_first_non_empty(item, ["cntrctDate", "cntrctCnclsDate"]),
            changed_amount=_to_float(_first_non_empty(item, ["lastCntrctAmt", "chgCntrctAmt"], amount)),
            category=category,
        )
        return True

    @staticmethod
    def _ingest_plan(conn, category: str, item: dict[str, Any]) -> bool:
        plan_id = _first_non_empty(item, ["orderPlanUntyNo", "orderPlanNo"])
        if not plan_id:
            return False

        upsert_procurement_plan(
            conn=conn,
            plan_id=plan_id,
            agency_name=_first_non_empty(item, ["orderInsttNm", "dminsttNm", "dmndInsttNm"], ""),
            agency_code=str(_first_non_empty(item, ["orderInsttCd", "dminsttCd", "dmndInsttCd"], "") or ""),
            category=category,
            budget_amount=_to_float(_first_non_empty(item, ["asignBdgtAmt", "presmptPrce", "budgetAmount"], 0)),
            planned_quarter=_first_non_empty(item, ["orderBgnYm", "orderEndYm", "orderTmnlYm"], ""),
            contract_method=_first_non_empty(item, ["cntrctMthdNm", "orderMthdNm"], ""),
        )
        return True

    @staticmethod
    def _ingest_demand_agency(conn, item: dict[str, Any]) -> bool:
        agency_code = str(_first_non_empty(
            item,
            [
                "dminsttCd", "dmndInsttCd", "ntceInsttCd",
                "orgCd", "insttCd", "userInsttCd",
            ],
            "",
        ) or "")
        if not agency_code:
            return False
        agency_name = _first_non_empty(
            item,
            [
                "dminsttNm", "dmndInsttNm", "ntceInsttNm",
                "orgNm", "insttNm", "userInsttNm",
            ],
            "",
        ) or ""
        upsert_demand_agency(
            conn=conn,
            agency_code=agency_code,
            agency_name=agency_name,
            top_agency_code=str(_first_non_empty(
                item,
                [
                    "topInsttCd", "upperInsttCd", "hghrInsttCd", "topDminsttCd",
                    "toplvlInsttCd",
                ],
                "",
            ) or ""),
            top_agency_name=_first_non_empty(
                item,
                [
                    "topInsttNm", "upperInsttNm", "hghrInsttNm", "topDminsttNm",
                    "toplvlInsttNm",
                ],
                "",
            ) or "",
            jurisdiction_type=_first_non_empty(
                item,
                [
                    "jurirnoDivNm", "psitnDivNm", "sptDvsNm", "psitnNm",
                    "jrsdctnDivNm",
                ],
                "",
            ) or "",
            address=_first_non_empty(
                item,
                ["insttAddr", "dminsttAddr", "dmndInsttAddr", "orgAddr", "adrs"],
                "",
            ) or "",
            road_address=_first_non_empty(
                item,
                ["insttRoadNmAddr", "roadNmAddr", "rnAddr", "adrs"],
                "",
            ) or "",
            postal_code=str(_first_non_empty(item, ["zipNo", "postNo", "zip"], "") or ""),
            source="user-api",
            raw_json=json.dumps(item, ensure_ascii=False, separators=(",", ":")),
        )
        return True


def build_collect_query(
    source: str,
    start: str,
    end: str,
    page_size: int,
    inqry_div: str = "1",
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "pageNo": 1,
        "numOfRows": page_size,
        "inqryDiv": inqry_div,
        "type": "json",
    }
    if source in {"notices", "results", "contracts"}:
        query["inqryBgnDt"] = start
        query["inqryEndDt"] = end
    elif source == "plans":
        query["inqryBgnDt"] = start
        query["inqryEndDt"] = end
        query["orderBgnYm"] = start[:6]
        query["orderEndYm"] = end[:6]
    else:
        raise ValueError(f"Unsupported source: {source}")
    return query


def months_between(start: datetime, end: datetime | None = None) -> list[tuple[str, str]]:
    anchor_end = end or datetime.now()
    if start > anchor_end:
        return []

    windows: list[tuple[str, str]] = []
    year, month = start.year, start.month
    while (year, month) <= (anchor_end.year, anchor_end.month):
        last_day = monthrange(year, month)[1]
        if year == start.year and month == start.month:
            start_text = start.strftime("%Y%m%d%H%M")
        else:
            start_text = f"{year:04d}{month:02d}010000"
        if year == anchor_end.year and month == anchor_end.month:
            end_text = anchor_end.strftime("%Y%m%d%H%M")
        else:
            end_text = f"{year:04d}{month:02d}{last_day:02d}2359"
        windows.append((start_text, end_text))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return windows


def month_windows(months: int, end: datetime | None = None) -> list[tuple[str, str]]:
    if months <= 0:
        return []

    anchor = end or datetime.now()
    year = anchor.year
    month = anchor.month
    windows: list[tuple[str, str]] = []

    for _ in range(months):
        last_day = monthrange(year, month)[1]
        start = f"{year:04d}{month:02d}010000"
        if year == anchor.year and month == anchor.month:
            end_text = anchor.strftime("%Y%m%d%H%M")
        else:
            end_text = f"{year:04d}{month:02d}{last_day:02d}2359"
        windows.append((start, end_text))

        month -= 1
        if month == 0:
            month = 12
            year -= 1

    windows.reverse()
    return windows


def service_key_from_env() -> str | None:
    direct = os.getenv("DATA_GO_KR_SERVICE_KEY")
    if direct:
        return direct

    for path in (".env", ".env.local"):
        key = _service_key_from_dotenv(path)
        if key:
            return key
    return None


def _service_key_from_dotenv(path: str) -> str | None:
    if not os.path.exists(path):
        return None

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != "DATA_GO_KR_SERVICE_KEY":
                continue
            cleaned = value.strip().strip('"').strip("'")
            return cleaned or None
    return None


def _xml_to_dict(element: ET.Element) -> dict[str, Any]:
    children = list(element)
    if not children:
        return {element.tag: (element.text or "").strip()}

    grouped: dict[str, list[Any]] = {}
    for child in children:
        child_dict = _xml_to_dict(child)
        key, value = next(iter(child_dict.items()))
        grouped.setdefault(key, []).append(value)

    normalized: dict[str, Any] = {}
    for key, values in grouped.items():
        normalized[key] = values[0] if len(values) == 1 else values
    return {element.tag: normalized}


def _first_non_empty(item: dict[str, Any], keys: list[str], default: Any = None) -> Any:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _notice_id(item: dict[str, Any]) -> str:
    number = _first_non_empty(item, ["bidNtceNo", "ntceNo"])
    order = _first_non_empty(item, ["bidNtceOrd", "ntceOrd"], "")
    if not number:
        return ""
    return f"{number}-{str(order).zfill(3)}" if order not in {"", None} and "-" not in str(number) else str(number)


def _split_notice_number(notice_id: str) -> str:
    """Return the bidNtceNo portion of a stored notice_id.

    We store either ``bidNtceNo`` or ``bidNtceNo-bidNtceOrd``. The G2B notices
    API accepts ``bidNtceNo`` alone, so the order suffix has to be stripped
    before we pass it back as a query parameter.
    """
    if not notice_id:
        return ""
    if "-" not in notice_id:
        return notice_id
    head, _sep, _tail = notice_id.rpartition("-")
    return head or notice_id


def _pick_detail_for_notice(items: list[dict[str, Any]], notice_id: str) -> dict[str, Any] | None:
    for item in items:
        if _notice_id(item) == notice_id:
            return item
    bid_ntce_no = _split_notice_number(notice_id)
    for item in items:
        if str(item.get("bidNtceNo") or "") == bid_ntce_no:
            return item
    return items[0] if items else None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value
    try:
        return int(float(str(value).replace(",", "").strip()))
    except ValueError:
        return None
