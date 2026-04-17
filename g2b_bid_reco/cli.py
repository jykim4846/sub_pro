from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from .agency_analysis import AgencyRangeAnalyzer
from .api import PPSCollector, PublicDataPortalClient, build_collect_query, service_key_from_env
from .db import connect, get_notice_snapshot, init_db, insert_case, load_historical_cases, load_historical_cases_for_notice
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


def _predict_notice_from_args(args: argparse.Namespace):
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}. Run init-db and collect or load sample data first.")

    notice = get_notice_snapshot(args.db_path, args.notice_id)
    if notice is None:
        raise SystemExit(f"Notice not found: {args.notice_id}")
    if not notice.agency_name or not notice.contract_method or notice.base_amount <= 0:
        raise SystemExit(f"Notice is missing required fields for prediction: {args.notice_id}")

    cases = load_historical_cases_for_notice(args.db_path, notice.notice_id, notice.opened_at)
    analyzer = AgencyRangeAnalyzer(cases)
    return NoticePredictor(analyzer).predict(notice)


if __name__ == "__main__":
    raise SystemExit(main())
