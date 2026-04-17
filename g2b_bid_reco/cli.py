from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from .agency_analysis import AgencyRangeAnalyzer
from .api import PPSCollector, PublicDataPortalClient, build_collect_query, service_key_from_env
from .backtest import build_backtest_report, run_batch_backtest
from .db import (
    connect,
    get_actual_award,
    get_latest_opened_at,
    get_notice_snapshot,
    init_db,
    insert_case,
    load_historical_cases,
    load_historical_cases_for_notice,
)
from .models import AgencyRangeRequest, BidRecommendationRequest
from .notice_prediction import NoticePredictor
from .recommender import BidRecommender
from .sample_data import SAMPLE_CASES


DEFAULT_DB_PATH = "data/bids.db"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="G2B bid recommender CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="Initialize SQLite database")
    init_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)

    sample_parser = subparsers.add_parser("load-sample", help="Load sample procurement data")
    sample_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)

    recommend_parser = subparsers.add_parser("recommend", help="Recommend bid rate and amount")
    recommend_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    recommend_parser.add_argument("--agency", required=True)
    recommend_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    recommend_parser.add_argument("--method", required=True)
    recommend_parser.add_argument("--region", required=True)
    recommend_parser.add_argument("--base-amount", type=float, required=True)
    recommend_parser.add_argument("--floor-rate", type=float)
    recommend_parser.add_argument("--bidder-count", type=int)

    agency_range_parser = subparsers.add_parser("agency-range", help="Analyze a predictive bid range for a specific agency")
    agency_range_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    agency_range_parser.add_argument("--agency", required=True)
    agency_range_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    agency_range_parser.add_argument("--method", required=True)
    agency_range_parser.add_argument("--region")
    agency_range_parser.add_argument("--base-amount", type=float)
    agency_range_parser.add_argument("--floor-rate", type=float)

    predict_notice_parser = subparsers.add_parser("predict-notice", help="Predict a bid range for a stored notice using prior similar cases")
    predict_notice_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    predict_notice_parser.add_argument("--notice-id", required=True)

    backtest_parser = subparsers.add_parser(
        "backtest-notice",
        help="Run predict-notice on an already awarded notice and compare against the actual award to measure the gap.",
    )
    backtest_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    backtest_parser.add_argument("--notice-id", required=True)

    batch_backtest_parser = subparsers.add_parser(
        "backtest-batch",
        help="Sample N awarded notices and report hit rate / mean gap of the prediction model.",
    )
    batch_backtest_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    batch_backtest_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    batch_backtest_parser.add_argument("--sample-size", type=int, default=50)
    batch_backtest_parser.add_argument("--worst-case-keep", type=int, default=5)
    batch_backtest_parser.add_argument("--verbose", action="store_true")

    demo_parser = subparsers.add_parser("demo", help="Initialize temp DB, load sample data, and print a demo recommendation")
    demo_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)

    collect_parser = subparsers.add_parser("collect", help="Collect procurement data from official OpenAPI")
    collect_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    collect_parser.add_argument("--source", required=True, choices=["notices", "results", "contracts", "plans"])
    collect_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    collect_parser.add_argument("--start", required=True, help="YYYYMMDDHHMM")
    collect_parser.add_argument("--end", required=True, help="YYYYMMDDHHMM")
    collect_parser.add_argument("--service-key")
    collect_parser.add_argument("--page-size", type=int, default=100)
    collect_parser.add_argument("--max-pages", type=int, default=1)
    collect_parser.add_argument("--inqry-div", default="1")
    collect_parser.add_argument("--endpoint")

    enrich_parser = subparsers.add_parser(
        "enrich-stubs",
        help="Backfill missing agency/method/base_amount on notice rows that only have a linked bid result.",
    )
    enrich_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    enrich_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    enrich_parser.add_argument("--service-key")
    enrich_parser.add_argument("--batch-limit", type=int, default=None)
    enrich_parser.add_argument("--verbose", action="store_true")
    enrich_parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.15,
        help="Fixed sleep between notices-detail calls (seconds) to stay under rate limits.",
    )

    recent_parser = subparsers.add_parser(
        "collect-recent",
        help="Incrementally collect data since the last `opened_at` stored in the DB (or a user-specified date).",
    )
    recent_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    recent_parser.add_argument("--category", required=True, choices=["goods", "service", "construction"])
    recent_parser.add_argument("--service-key")
    recent_parser.add_argument("--sources", default="notices,results,contracts")
    recent_parser.add_argument(
        "--since",
        help="Start date YYYYMMDD or YYYYMMDDHHMM. If omitted, uses the latest opened_at in DB.",
    )
    recent_parser.add_argument(
        "--fallback-days",
        type=int,
        default=30,
        help="How many days to look back if the DB has no previous data for the category.",
    )
    recent_parser.add_argument("--page-size", type=int, default=100)
    recent_parser.add_argument("--max-pages-per-window", type=int, default=20)
    recent_parser.add_argument("--inqry-div", default="1")
    recent_parser.add_argument("--sleep-sec", type=float, default=0.3)

    backfill_parser = subparsers.add_parser("backfill-recent-3y", help="Backfill the most recent 36 months in monthly windows")
    backfill_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    backfill_parser.add_argument("--category", default="service", choices=["goods", "service", "construction"])
    backfill_parser.add_argument("--service-key")
    backfill_parser.add_argument("--sources", default="notices,results,contracts,plans")
    backfill_parser.add_argument("--months", type=int, default=36)
    backfill_parser.add_argument("--page-size", type=int, default=100)
    backfill_parser.add_argument("--max-pages-per-window", type=int, default=20)
    backfill_parser.add_argument("--inqry-div", default="1")
    backfill_parser.add_argument("--end")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "init-db":
        init_db(args.db_path)
        print(f"Initialized database at {args.db_path}")
        return 0

    if args.command == "load-sample":
        _load_sample(args.db_path)
        print(f"Loaded {len(SAMPLE_CASES)} sample cases into {args.db_path}")
        return 0

    if args.command == "recommend":
        recommendation = _recommend_from_args(args)
        print(json.dumps(asdict(recommendation), ensure_ascii=False, indent=2))
        return 0

    if args.command == "agency-range":
        report = _agency_range_from_args(args)
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
        return 0

    if args.command == "predict-notice":
        report = _predict_notice_from_args(args)
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
        return 0

    if args.command == "backtest-notice":
        report = _backtest_notice_from_args(args)
        print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
        return 0

    if args.command == "backtest-batch":
        db_path = Path(args.db_path)
        if not db_path.exists():
            raise SystemExit(f"Database not found: {db_path}. Run init-db and collect or load sample data first.")
        log = (lambda line: print(line)) if args.verbose else None
        summary = run_batch_backtest(
            db_path=args.db_path,
            category=args.category,
            sample_size=args.sample_size,
            worst_case_keep=args.worst_case_keep,
            log=log,
        )
        print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
        return 0

    if args.command == "demo":
        _load_sample(args.db_path)
        request = BidRecommendationRequest(
            agency_name="한국출판문화산업진흥원",
            category="service",
            contract_method="적격심사",
            region="seoul",
            base_amount=240_000_000,
            floor_rate=87.745,
            bidder_count=34,
        )
        cases = load_historical_cases(args.db_path)
        recommendation = BidRecommender(cases).recommend(request)
        print(json.dumps(asdict(recommendation), ensure_ascii=False, indent=2))
        return 0

    if args.command == "collect":
        service_key = args.service_key or service_key_from_env()
        if not service_key:
            raise SystemExit("Missing service key. Pass --service-key or set DATA_GO_KR_SERVICE_KEY.")
        init_db(args.db_path)
        client = PublicDataPortalClient(service_key=service_key)
        collector = PPSCollector(client=client, db_path=args.db_path)
        query = build_collect_query(
            source=args.source,
            start=args.start,
            end=args.end,
            page_size=args.page_size,
            inqry_div=args.inqry_div,
        )
        result = collector.collect(
            source=args.source,
            category=args.category,
            query=query,
            max_pages=args.max_pages,
            endpoint_override=args.endpoint,
        )
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0

    if args.command == "enrich-stubs":
        service_key = args.service_key or service_key_from_env()
        if not service_key:
            raise SystemExit("Missing service key. Pass --service-key or set DATA_GO_KR_SERVICE_KEY.")
        init_db(args.db_path)
        client = PublicDataPortalClient(
            service_key=service_key,
            per_call_sleep_sec=max(0.0, args.sleep_sec),
        )
        collector = PPSCollector(client=client, db_path=args.db_path)
        log = (lambda line: print(line)) if args.verbose else None
        result = collector.enrich_stub_notices(
            category=args.category,
            batch_limit=args.batch_limit,
            log=log,
        )
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0

    if args.command == "collect-recent":
        service_key = args.service_key or service_key_from_env()
        if not service_key:
            raise SystemExit("Missing service key. Pass --service-key or set DATA_GO_KR_SERVICE_KEY.")
        init_db(args.db_path)
        client = PublicDataPortalClient(
            service_key=service_key,
            per_call_sleep_sec=max(0.0, args.sleep_sec),
        )
        collector = PPSCollector(client=client, db_path=args.db_path)
        sources = [source.strip() for source in args.sources.split(",") if source.strip()]
        start_dt = _resolve_collect_recent_start(args)
        result = collector.collect_between(
            category=args.category,
            sources=sources,
            start=start_dt,
            page_size=args.page_size,
            max_pages_per_window=args.max_pages_per_window,
            inqry_div=args.inqry_div,
        )
        payload = {"since": start_dt.strftime("%Y-%m-%d %H:%M"), **asdict(result)}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "backfill-recent-3y":
        service_key = args.service_key or service_key_from_env()
        if not service_key:
            raise SystemExit("Missing service key. Pass --service-key or set DATA_GO_KR_SERVICE_KEY.")
        init_db(args.db_path)
        client = PublicDataPortalClient(service_key=service_key)
        collector = PPSCollector(client=client, db_path=args.db_path)
        end = datetime.strptime(args.end, "%Y%m%d%H%M") if args.end else None
        sources = [source.strip() for source in args.sources.split(",") if source.strip()]
        result = collector.backfill_recent_months(
            category=args.category,
            sources=sources,
            months=args.months,
            page_size=args.page_size,
            max_pages_per_window=args.max_pages_per_window,
            end=end,
            inqry_div=args.inqry_div,
        )
        print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
        return 0

    parser.error("Unknown command")
    return 2


def _load_sample(db_path: str) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        for case in SAMPLE_CASES:
            insert_case(conn, case)


def _recommend_from_args(args: argparse.Namespace):
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}. Run init-db and load-sample or ingest real data first.")

    request = BidRecommendationRequest(
        agency_name=args.agency,
        category=args.category,
        contract_method=args.method,
        region=args.region,
        base_amount=args.base_amount,
        floor_rate=args.floor_rate,
        bidder_count=args.bidder_count,
    )
    cases = load_historical_cases(args.db_path)
    return BidRecommender(cases).recommend(request)


def _agency_range_from_args(args: argparse.Namespace):
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}. Run init-db and collect or load sample data first.")

    request = AgencyRangeRequest(
        agency_name=args.agency,
        category=args.category,
        contract_method=args.method,
        region=args.region,
        base_amount=args.base_amount,
        floor_rate=args.floor_rate,
    )
    cases = load_historical_cases(args.db_path)
    return AgencyRangeAnalyzer(cases).analyze(request)


def _resolve_collect_recent_start(args: argparse.Namespace) -> datetime:
    if args.since:
        text = args.since.strip()
        for fmt in ("%Y%m%d%H%M", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        raise SystemExit(f"Unparseable --since value: {args.since}")

    latest = get_latest_opened_at(args.db_path, args.category)
    if latest:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y%m%d%H%M"):
            try:
                return datetime.strptime(latest, fmt)
            except ValueError:
                continue

    return datetime.now() - timedelta(days=max(1, args.fallback_days))


def _predict_notice_from_args(args: argparse.Namespace):
    return _predict_notice(args.db_path, args.notice_id)


def _predict_notice(db_path_text: str, notice_id: str):
    db_path = Path(db_path_text)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}. Run init-db and collect or load sample data first.")

    notice = get_notice_snapshot(db_path_text, notice_id)
    if notice is None:
        raise SystemExit(f"Notice not found: {notice_id}")
    if not notice.agency_name or not notice.contract_method or notice.base_amount <= 0:
        raise SystemExit(f"Notice is missing required fields for prediction: {notice_id}")

    cases = load_historical_cases_for_notice(db_path_text, notice.notice_id, notice.opened_at)
    analyzer = AgencyRangeAnalyzer(cases)
    return NoticePredictor(analyzer).predict(notice)


def _backtest_notice_from_args(args: argparse.Namespace):
    prediction = _predict_notice(args.db_path, args.notice_id)
    actual = get_actual_award(args.db_path, args.notice_id)
    if actual is None:
        raise SystemExit(
            f"No recorded award found for notice {args.notice_id}. Backtesting requires a linked bid result."
        )
    if actual.award_amount <= 0 or actual.bid_rate <= 0:
        raise SystemExit(
            f"Recorded award for {args.notice_id} is missing award_amount or bid_rate; cannot backtest."
        )
    return build_backtest_report(prediction, actual)


if __name__ == "__main__":
    raise SystemExit(main())
