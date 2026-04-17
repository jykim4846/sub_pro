from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from .db import connect, upsert_bid_result, upsert_contract, upsert_notice, upsert_procurement_plan


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


class PublicDataPortalClient:
    def __init__(
        self,
        service_key: str,
        timeout: float = 30.0,
        opener: Callable[[str], str] | None = None,
    ) -> None:
        self.service_key = service_key
        self.timeout = timeout
        self.opener = opener or self._default_open

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
        raw = self.opener(url)
        return self._parse_payload(raw)

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

    @staticmethod
    def _ingest_notice(conn, category: str, item: dict[str, Any]) -> bool:
        notice_id = _notice_id(item)
        if not notice_id:
            return False

        upsert_notice(
            conn=conn,
            notice_id=notice_id,
            agency_name=_first_non_empty(item, ["dminsttNm", "ntceInsttNm", "dmndInsttNm", "orderInsttNm"], ""),
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

        upsert_bid_result(
            conn=conn,
            notice_id=notice_id,
            award_amount=_to_float(_first_non_empty(item, ["sucsfbidAmt", "bidwinnrAmt", "dcsnAmt"], 0)),
            bid_rate=_to_float(_first_non_empty(item, ["sucsfbidRate", "bidwinnrRate", "bidprcRt"], 0)),
            bidder_count=_to_int(_first_non_empty(item, ["prtcptCnum", "bidprtcptCnt", "opengRankCount"], 0)) or 0,
            winning_company=_first_non_empty(item, ["sucsfbidprsnCmpyNm", "bidwinnrNm", "sucsfbidNm"], ""),
            result_status=_first_non_empty(item, ["opengRsltDivNm", "bidRsltNm"], "awarded"),
            category=category,
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
            category=category,
            budget_amount=_to_float(_first_non_empty(item, ["asignBdgtAmt", "presmptPrce", "budgetAmount"], 0)),
            planned_quarter=_first_non_empty(item, ["orderBgnYm", "orderEndYm", "orderTmnlYm"], ""),
            contract_method=_first_non_empty(item, ["cntrctMthdNm", "orderMthdNm"], ""),
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
