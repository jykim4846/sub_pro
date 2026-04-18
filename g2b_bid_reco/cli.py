from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from .agency_analysis import AgencyRangeAnalyzer
from .api import (
    PPSCollector,
    PPS_USER_INFO_BASE_URL,
    PPS_USER_INFO_DEMAND_AGENCY_CANDIDATES,
    PublicDataPortalClient,
    build_collect_query,
    service_key_from_env,
)
from .backtest import build_backtest_report, run_batch_backtest
from .csv_import import import_contract_history_csvs
from .db import (
    bump_automation_daily_stats,
    connect,
    finish_automation_run,
    get_actual_award,
    get_latest_opened_at,
    get_notice_snapshot,
    init_db,
    insert_case,
    load_cases_for_agencies,
    load_historical_cases,
    load_historical_cases_for_notice,
    load_pending_notices_for_prediction,
    replace_auto_mock_bid_batch,
    resolve_adaptive_agencies,
    seed_demand_agencies_from_notices,
    start_automation_run,
    top_winners_for_scope,
    update_automation_run,
)
from .models import AgencyRangeRequest, BidRecommendationRequest
from .notice_prediction import NoticePredictor
from .recommender import BidRecommender
from .sample_data import SAMPLE_CASES
from .simulation import CompetitorSpec, run_simulation


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

    import_contract_csv_parser = subparsers.add_parser(
        "import-contract-csv",
        help="Import one or more downloaded G2B contract-history CSV files into SQLite.",
    )
    import_contract_csv_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    import_contract_csv_parser.add_argument(
        "paths",
        nargs="+",
        help="CSV file(s), directories, or glob patterns to import.",
    )

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

    auto_bid_parser = subparsers.add_parser(
        "auto-bid-pending",
        help="Generate and save automatic mock-bid portfolios for pending notices.",
    )
    auto_bid_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    auto_bid_parser.add_argument("--category", choices=["goods", "service", "construction"])
    auto_bid_parser.add_argument("--agency")
    auto_bid_parser.add_argument("--since-days", type=int)
    auto_bid_parser.add_argument("--limit", type=int, default=0)
    auto_bid_parser.add_argument("--num-customers", type=int, default=5)
    auto_bid_parser.add_argument("--top-k", type=int, default=10)
    auto_bid_parser.add_argument("--target-win-probability", type=float, default=0.75)
    auto_bid_parser.add_argument("--dry-run", action="store_true")

    sync_agency_parser = subparsers.add_parser(
        "sync-demand-agencies",
        help="Sync demand-agency master data from the Nara Market user information service.",
    )
    sync_agency_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    sync_agency_parser.add_argument("--service-key")
    sync_agency_parser.add_argument(
        "--endpoint",
        help=(
            "User-info operation URL under the confirmed base "
            f"{PPS_USER_INFO_BASE_URL}. If omitted, auto-probes known demand-agency candidates "
            "or uses G2B_USER_INFO_ENDPOINT when set."
        ),
    )
    sync_agency_parser.add_argument("--page-size", type=int, default=100)
    sync_agency_parser.add_argument("--max-pages", type=int, default=20)
    sync_agency_parser.add_argument("--inqry-div", default="1")
    sync_agency_parser.add_argument("--since")
    sync_agency_parser.add_argument("--until")
    sync_agency_parser.add_argument("--print-candidates", action="store_true")

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

    if args.command == "import-contract-csv":
        init_db(args.db_path)
        result = import_contract_history_csvs(args.db_path, args.paths)
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
        run_id = f"collect-{args.category}-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        start_automation_run(
            args.db_path,
            run_id=run_id,
            kind=f"collect_recent:{args.category}",
            message="starting",
        )
        client = PublicDataPortalClient(
            service_key=service_key,
            per_call_sleep_sec=max(0.0, args.sleep_sec),
        )
        collector = PPSCollector(client=client, db_path=args.db_path)
        sources = [source.strip() for source in args.sources.split(",") if source.strip()]
        start_dt = _resolve_collect_recent_start(args)
        try:
            result = collector.collect_between(
                category=args.category,
                sources=sources,
                start=start_dt,
                page_size=args.page_size,
                max_pages_per_window=args.max_pages_per_window,
                inqry_div=args.inqry_div,
            )
            bump_automation_daily_stats(
                args.db_path,
                collect_api_calls=result.total_pages_fetched,
            )
            finish_automation_run(
                args.db_path,
                run_id=run_id,
                status="completed",
                processed_items=result.total_items_seen,
                success_items=result.total_items_upserted,
                failed_items=max(0, result.total_items_seen - result.total_items_upserted),
                message=f"pages={result.total_pages_fetched}",
            )
            payload = {"since": start_dt.strftime("%Y-%m-%d %H:%M"), **asdict(result)}
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0
        except Exception as exc:
            finish_automation_run(
                args.db_path,
                run_id=run_id,
                status="failed",
                message=str(exc),
            )
            raise

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

    if args.command == "auto-bid-pending":
        payload = _auto_bid_pending(args)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "sync-demand-agencies":
        if args.print_candidates:
            print(json.dumps({"candidates": list(PPS_USER_INFO_DEMAND_AGENCY_CANDIDATES)}, ensure_ascii=False, indent=2))
            return 0
        service_key = args.service_key or service_key_from_env()
        if not service_key:
            raise SystemExit("Missing service key. Pass --service-key or set DATA_GO_KR_SERVICE_KEY.")
        init_db(args.db_path)
        run_id = f"sync-demand-agencies-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
        start_automation_run(
            args.db_path,
            run_id=run_id,
            kind="sync_demand_agencies",
            message="starting",
        )
        client = PublicDataPortalClient(service_key=service_key)
        collector = PPSCollector(client=client, db_path=args.db_path)
        endpoint = collector.resolve_demand_agency_endpoint(
            endpoint=args.endpoint or os.environ.get("G2B_USER_INFO_ENDPOINT")
        )
        with connect(args.db_path) as conn:
            seeded = seed_demand_agencies_from_notices(conn)
        query = {
            "pageNo": 1,
            "numOfRows": args.page_size,
            "inqryDiv": args.inqry_div,
            "type": "json",
        }
        if args.since:
            query["inqryBgnDt"] = _parse_cli_datetime_text(args.since).strftime("%Y%m%d%H%M")
        if args.until:
            query["inqryEndDt"] = _parse_cli_datetime_text(args.until).strftime("%Y%m%d%H%M")
        try:
            result = collector.sync_demand_agencies(
                endpoint=endpoint,
                query=query,
                max_pages=args.max_pages,
            )
            bump_automation_daily_stats(
                args.db_path,
                agency_api_calls=result.pages_fetched,
            )
            finish_automation_run(
                args.db_path,
                run_id=run_id,
                status="completed",
                processed_items=result.items_seen,
                success_items=result.items_upserted,
                failed_items=max(0, result.items_seen - result.items_upserted),
                message=f"pages={result.pages_fetched}",
            )
            payload = {"seeded_from_notices": seeded, **asdict(result), "endpoint": endpoint}
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 0
        except Exception as exc:
            finish_automation_run(
                args.db_path,
                run_id=run_id,
                status="failed",
                message=str(exc),
            )
            raise

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


def _parse_cli_datetime_text(text: str) -> datetime:
    value = text.strip()
    for fmt in ("%Y%m%d%H%M", "%Y%m%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise SystemExit(f"Unparseable datetime value: {text}")


def _resolve_collect_recent_start(args: argparse.Namespace) -> datetime:
    if args.since:
        return _parse_cli_datetime_text(args.since)

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


def _load_cases_adaptive(
    db_path: str,
    notice_id: str,
    agency_name: str,
    category: str,
    contract_method: str,
    opened_at: str | None,
) -> tuple[list, str | None]:
    agency_list, parent_used = resolve_adaptive_agencies(
        db_path,
        agency_name,
        category=category,
        contract_method=contract_method,
    )
    if parent_used:
        cases = load_cases_for_agencies(
            db_path,
            agency_list,
            category=category,
            contract_method=contract_method,
            cutoff_opened_at=opened_at,
            exclude_notice_id=notice_id,
        )
        peer = load_historical_cases_for_notice(
            db_path,
            notice_id,
            opened_at,
            category=category,
            contract_method=contract_method,
        )
        seen = {case.notice_id for case in cases}
        for case in peer:
            if case.notice_id not in seen:
                cases.append(case)
        return cases, parent_used
    return (
        load_historical_cases_for_notice(
            db_path,
            notice_id,
            opened_at,
            category=category,
            contract_method=contract_method,
        ),
        None,
    )


def _auto_bid_pending(args: argparse.Namespace) -> dict:
    db_path = args.db_path
    init_db(db_path)
    notices = load_pending_notices_for_prediction(
        db_path=db_path,
        category=args.category,
        agency_name=(args.agency or "").strip() or None,
        since_days=args.since_days if args.since_days and args.since_days > 0 else None,
        limit=args.limit if args.limit and args.limit > 0 else None,
    )
    simulation_id = f"auto-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    start_automation_run(
        db_path,
        run_id=simulation_id,
        kind="auto_bid_pending",
        total_items=len(notices),
        message="starting",
    )
    reports: list[dict] = []
    batch_rows: list[dict] = []
    failures = 0
    saved = 0
    try:
        for idx, notice in enumerate(notices, start=1):
            try:
                cases, parent_used = _load_cases_adaptive(
                    db_path,
                    notice.notice_id,
                    notice.agency_name,
                    notice.category,
                    notice.contract_method,
                    notice.opened_at,
                )
                analyzer = AgencyRangeAnalyzer(cases, target_win_probability=args.target_win_probability)
                prediction = NoticePredictor(analyzer).predict(notice)
                analysis = prediction.analysis
                winners = top_winners_for_scope(
                    db_path,
                    notice.agency_name,
                    notice.category,
                    notice.contract_method,
                    limit=max(1, args.top_k),
                    base_amount=notice.base_amount,
                )
                report = run_simulation(
                    notice_id=notice.notice_id,
                    base_amount=notice.base_amount,
                    floor_rate=notice.floor_rate,
                    predicted_rate=analysis.blended_rate,
                    lower_rate=analysis.lower_rate,
                    upper_rate=analysis.upper_rate,
                    predicted_amount=analysis.recommended_amount,
                    competitors=[
                        CompetitorSpec(
                            biz_no=row["biz_no"],
                            company_name=row["company_name"],
                            historical_rates=row["rates"],
                            wins=row["wins"],
                        )
                        for row in winners
                    ],
                    historical_cases=cases,
                    n_customers=max(1, args.num_customers),
                )
                reports.append(
                    {
                        "notice_id": notice.notice_id,
                        "agency_name": notice.agency_name,
                        "portfolio_win_rate": round(report.our_win_rate, 4),
                        "best_customer_idx": report.best_customer_idx,
                        "market_drift": report.market_drift,
                        "uncertainty_score": report.uncertainty_score,
                        "parent_used": parent_used,
                    }
                )
                for customer in report.customers:
                    batch_rows.append(
                        {
                            "notice_id": notice.notice_id,
                            "bid_amount": customer.amount,
                            "bid_rate": customer.rate,
                            "predicted_amount": analysis.recommended_amount,
                            "predicted_rate": analysis.blended_rate,
                            "note": (
                                "auto:trend-aware-quantile"
                                f";portfolio_win_rate={report.our_win_rate:.3f}"
                                f";role={customer.role}"
                                f";uncertainty={report.uncertainty_score:.3f}"
                            ),
                            "customer_idx": customer.idx,
                        }
                    )
            except Exception:
                failures += 1
                reports.append(
                    {
                        "notice_id": notice.notice_id,
                        "agency_name": notice.agency_name,
                        "error": "auto-bid failed",
                    }
                )
            if idx == len(notices) or idx % 10 == 0:
                update_automation_run(
                    db_path,
                    run_id=simulation_id,
                    processed_items=idx,
                    success_items=idx - failures,
                    failed_items=failures,
                    message=f"processing {idx}/{len(notices)}",
                )
        if batch_rows and not args.dry_run:
            saved = replace_auto_mock_bid_batch(db_path, simulation_id, batch_rows)
            bump_automation_daily_stats(
                db_path,
                auto_bid_runs=1,
                auto_bid_notices=len(notices),
                auto_bid_customer_bids=saved,
            )
        finish_automation_run(
            db_path,
            run_id=simulation_id,
            status="completed",
            processed_items=len(notices),
            success_items=len(notices) - failures,
            failed_items=failures,
            message=f"saved {saved} customer bids",
        )
    except Exception as exc:
        finish_automation_run(
            db_path,
            run_id=simulation_id,
            status="failed",
            processed_items=len(reports),
            success_items=len(reports) - failures,
            failed_items=failures or 1,
            message=str(exc),
        )
        raise
    return {
        "simulation_id": simulation_id,
        "dry_run": bool(args.dry_run),
        "notices_seen": len(notices),
        "customer_bids_saved": saved,
        "reports": reports,
    }


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
