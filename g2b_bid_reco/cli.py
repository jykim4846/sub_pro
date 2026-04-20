from __future__ import annotations

import argparse
import bisect
import json
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from time import perf_counter

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
    AGENCY_SHRINKAGE_K,
    bump_automation_daily_stats,
    connect,
    create_run_task,
    finish_automation_run,
    finish_run_task,
    get_actual_award,
    get_latest_opened_at,
    get_notice_snapshot,
    heartbeat_run_task,
    init_db,
    insert_case,
    list_run_tasks,
    load_cases_for_agencies,
    load_historical_cases,
    load_historical_cases_for_notice,
    load_pending_notices_for_prediction,
    load_strategy_table_for_scope,
    replace_auto_mock_bid_batch,
    refresh_mock_bid_evaluations,
    resolve_adaptive_agencies,
    save_mock_bid_batch,
    seed_demand_agencies_from_notices,
    start_automation_run,
    start_run_task,
    summarize_run_tasks,
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
    sync_agency_parser.add_argument(
        "--since-days",
        type=int,
        default=7,
        help="Default incremental window (days) when --since/--until are not provided. "
             "The user-info API returns 0 items without a window and errors on ranges "
             "wider than a few months, so a bounded default is required.",
    )
    sync_agency_parser.add_argument("--print-candidates", action="store_true")

    eval_parser = subparsers.add_parser(
        "evaluate-mock-bids",
        help="Materialize mock-bid verdicts into a dedicated evaluation table.",
    )
    eval_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    eval_parser.add_argument("--today-results-only", action="store_true")
    eval_parser.add_argument("--simulation-id")

    strategy_parser = subparsers.add_parser(
        "build-strategy-tables",
        help="Populate strategy_tables via monte carlo over historical bid_results (MODES.md Path B).",
    )
    strategy_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    strategy_parser.add_argument("--n-max", type=int, default=10)
    strategy_parser.add_argument("--n-trials", type=int, default=2000)
    strategy_parser.add_argument("--min-samples", type=int, default=30)
    strategy_parser.add_argument("--seed", type=int, default=42)
    strategy_parser.add_argument(
        "--model",
        choices=["v1", "v2"],
        default="v2",
        help="v1=coverage (historical), v2=within-notice 평균가 auction (default, MODES.md §9)",
    )
    strategy_parser.add_argument(
        "--max-bidders",
        type=int,
        default=50,
        help="v2 only: cap on competitor count per trial",
    )

    cleanup_parser = subparsers.add_parser(
        "cleanup-floor-rates",
        help="Fill NULL/0/outlier floor_rate with the (category, contract_method) modal value.",
    )
    cleanup_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    cleanup_parser.add_argument(
        "--contract-methods", nargs="+", default=["전자입찰"],
        help="Target contract methods. Default: 전자입찰 only (낙찰하한제 적용 scope).",
    )
    cleanup_parser.add_argument(
        "--min-modal-n", type=int, default=30,
        help="Minimum sample size required to trust the scope's modal value.",
    )
    cleanup_parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute target counts but do not write back.",
    )

    update_strategy_parser = subparsers.add_parser(
        "update-strategy-tables",
        help="Blend observed win-rates into strategy_tables via EMA (MODES.md Path C).",
    )
    update_strategy_parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    update_strategy_parser.add_argument(
        "--alpha", type=float, default=0.1,
        help="EMA weight on observed rate (default 0.1 — conservative).",
    )
    update_strategy_parser.add_argument(
        "--min-decided", type=int, default=20,
        help="Minimum decided evaluations required to update a (scope, N) row.",
    )
    update_strategy_parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute diffs but do not write back to strategy_tables.",
    )

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
        now_dt = datetime.now()
        since_dt = _parse_cli_datetime_text(args.since) if args.since else now_dt - timedelta(days=max(1, args.since_days))
        until_dt = _parse_cli_datetime_text(args.until) if args.until else now_dt
        query["inqryBgnDt"] = since_dt.strftime("%Y%m%d%H%M")
        query["inqryEndDt"] = until_dt.strftime("%Y%m%d%H%M")
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

    if args.command == "evaluate-mock-bids":
        init_db(args.db_path)
        payload = refresh_mock_bid_evaluations(
            args.db_path,
            today_results_only=bool(args.today_results_only),
            simulation_id=(args.simulation_id or "").strip() or None,
        )
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    if args.command == "build-strategy-tables":
        from .strategy_mc import build_strategy_tables, build_strategy_tables_v2
        init_db(args.db_path)
        if args.model == "v1":
            summary = build_strategy_tables(
                args.db_path,
                n_range=range(1, args.n_max + 1),
                n_trials=args.n_trials,
                min_samples=args.min_samples,
                seed=args.seed,
            )
        else:
            summary = build_strategy_tables_v2(
                args.db_path,
                n_range=range(1, args.n_max + 1),
                n_trials=args.n_trials,
                min_samples=args.min_samples,
                max_bidders=args.max_bidders,
                seed=args.seed,
            )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "update-strategy-tables":
        from .strategy_update import ema_update_strategy_tables
        init_db(args.db_path)
        summary = ema_update_strategy_tables(
            args.db_path,
            alpha=float(args.alpha),
            min_decided=int(args.min_decided),
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "cleanup-floor-rates":
        from .data_cleanup import cleanup_floor_rates
        init_db(args.db_path)
        summary = cleanup_floor_rates(
            args.db_path,
            contract_methods=args.contract_methods,
            min_modal_n=int(args.min_modal_n),
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(summary, ensure_ascii=False, indent=2))
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


def _auto_bid_scope_worker(payload: dict) -> dict:
    """Process a single (category, contract_method) scope.

    Runs in a worker process: opens its own sqlite connection, preloads the
    scope-wide pool once, then computes prediction + simulation for every
    notice in the bucket. Returns reports/batch_rows back to the parent.
    """
    db_path = payload["db_path"]
    cat = payload["category"]
    method = payload["contract_method"]
    group = payload["notices"]
    top_k = max(1, int(payload["top_k"]))
    num_customers = max(1, int(payload["num_customers"]))
    target_win_prob = float(payload["target_win_probability"])
    progress_run_id = payload.get("progress_run_id")
    progress_total_items = int(payload.get("progress_total_items") or 0)
    progress_initial_processed = int(payload.get("progress_initial_processed") or 0)
    progress_every = max(1, int(payload.get("progress_every") or 0))
    task_id = payload.get("task_id")
    task_heartbeat_every = max(1, int(payload.get("task_heartbeat_every") or 30))
    if task_id:
        try:
            start_run_task(
                db_path,
                task_id=task_id,
                message=f"starting scope={cat}/{method} n={len(group)}",
            )
        except Exception:
            pass  # non-fatal: worker keeps going, status stays queued
    timings = {
        "preload_s": 0.0,
        "case_prep_s": 0.0,
        "predict_s": 0.0,
        "simulate_s": 0.0,
    }

    preload_started = perf_counter()
    scope_cases = load_historical_cases_for_notice(
        db_path,
        notice_id="",
        cutoff_opened_at=None,
        category=cat,
        contract_method=method,
    )
    scope_cases.sort(key=lambda c: (c.opened_at or ""))
    scope_opened = [c.opened_at or "" for c in scope_cases]
    winners = top_winners_for_scope(
        db_path,
        "",
        cat,
        method,
        limit=top_k,
        base_amount=None,
    )
    comps_cached = [
        CompetitorSpec(
            biz_no=row["biz_no"],
            company_name=row["company_name"],
            historical_rates=row["rates"],
            wins=row["wins"],
        )
        for row in winners
    ]
    timings["preload_s"] = perf_counter() - preload_started

    # Approved parent-agency index + strategy_tables (per-N quantiles) loaded
    # once per scope. Parent pool narrows shrinkage peers to siblings;
    # strategy rows drive portfolio quantile selection (MODES.md §5-step2).
    agency_to_parent: dict[str, str] = {}
    parent_to_children: dict[str, set[str]] = {}
    with connect(db_path) as _map_conn:
        for row in _map_conn.execute(
            "SELECT agency_name, parent_name FROM agency_parent_mapping "
            "WHERE status='approved' AND parent_name != ''"
        ):
            agency_to_parent[row["agency_name"]] = row["parent_name"]
            parent_to_children.setdefault(row["parent_name"], set()).add(
                row["agency_name"]
            )
        strategy_rows = load_strategy_table_for_scope(_map_conn, cat, method)
    if strategy_rows:
        n_values = sorted(strategy_rows.keys())
        source_tag = "strategy_v2"
    else:
        n_values = [num_customers]
        # Preserve the legacy note prefix so existing consumers/tests that key
        # off "auto:trend-aware-quantile" keep working when no strategy table
        # is available for this scope.
        source_tag = "trend-aware-quantile"

    group = sorted(group, key=lambda item: ((item.opened_at or ""), item.notice_id))
    prior_cases: list = []
    prior_valid_rates_opened_asc: list[float] = []
    scope_cursor = 0

    out_reports: list[dict] = []
    out_batch: list[dict] = []
    failures = 0
    local_processed = 0
    for notice in group:
        try:
            case_started = perf_counter()
            cutoff = notice.opened_at or ""
            cutoff_idx = bisect.bisect_left(scope_opened, cutoff) if cutoff else len(scope_cases)
            while scope_cursor < cutoff_idx:
                case = scope_cases[scope_cursor]
                prior_cases.append(case)
                if 0 < case.bid_rate <= 110:
                    prior_valid_rates_opened_asc.append(case.bid_rate)
                scope_cursor += 1
            cases = prior_cases
            timings["case_prep_s"] += perf_counter() - case_started

            predict_started = perf_counter()
            parent_name = agency_to_parent.get(notice.agency_name)
            parent_pool: frozenset[str] | None = None
            if parent_name:
                children = parent_to_children.get(parent_name, set())
                pool = set(children)
                pool.add(parent_name)
                pool.discard(notice.agency_name)
                parent_pool = frozenset(pool) if pool else None
            analyzer = AgencyRangeAnalyzer(
                cases,
                target_win_probability=target_win_prob,
                prior_strength=AGENCY_SHRINKAGE_K if parent_name else 4.0,
                parent_pool_agencies=parent_pool,
                parent_name=parent_name,
            )
            prediction = NoticePredictor(analyzer).predict(notice)
            analysis = prediction.analysis
            timings["predict_s"] += perf_counter() - predict_started

            simulate_started = perf_counter()
            for n in n_values:
                override_q = strategy_rows.get(n) if strategy_rows else None
                report = run_simulation(
                    notice_id=notice.notice_id,
                    base_amount=notice.base_amount,
                    floor_rate=notice.floor_rate,
                    predicted_rate=analysis.blended_rate,
                    lower_rate=analysis.lower_rate,
                    upper_rate=analysis.upper_rate,
                    predicted_amount=analysis.recommended_amount,
                    competitors=comps_cached,
                    historical_cases=cases,
                    n_customers=n,
                    historical_rates_opened_asc=prior_valid_rates_opened_asc,
                    override_quantiles=override_q,
                )
                out_reports.append(
                    {
                        "notice_id": notice.notice_id,
                        "agency_name": notice.agency_name,
                        "n_customers": n,
                        "source": source_tag,
                        "portfolio_win_rate": round(report.our_win_rate, 4),
                        "best_customer_idx": report.best_customer_idx,
                        "market_drift": report.market_drift,
                        "uncertainty_score": report.uncertainty_score,
                        "parent_used": parent_name or "",
                    }
                )
                for customer in report.customers:
                    out_batch.append(
                        {
                            "notice_id": notice.notice_id,
                            "bid_amount": customer.amount,
                            "bid_rate": customer.rate,
                            "predicted_amount": analysis.recommended_amount,
                            "predicted_rate": analysis.blended_rate,
                            "note": (
                                f"auto:{source_tag}"
                                f";portfolio_win_rate={report.our_win_rate:.3f}"
                                f";role={customer.role}"
                                f";uncertainty={report.uncertainty_score:.3f}"
                                f";n={n}"
                            ),
                            "customer_idx": customer.idx,
                            "n_customers": n,
                        }
                    )
            timings["simulate_s"] += perf_counter() - simulate_started
        except Exception as exc:
            failures += 1
            out_reports.append(
                {
                    "notice_id": notice.notice_id,
                    "agency_name": notice.agency_name,
                    "error": f"auto-bid failed: {type(exc).__name__}: {exc}",
                }
            )
        local_processed += 1
        if task_id and local_processed % task_heartbeat_every == 0:
            try:
                heartbeat_run_task(
                    db_path,
                    task_id=task_id,
                    processed_items=local_processed,
                    success_items=local_processed - failures,
                    failed_items=failures,
                    message=f"processing {local_processed}/{len(group)}",
                )
            except Exception:
                pass
        if progress_run_id and progress_every and local_processed % progress_every == 0:
            progressed = progress_initial_processed + local_processed
            update_automation_run(
                db_path,
                run_id=progress_run_id,
                processed_items=progressed,
                success_items=progressed - failures,
                failed_items=failures,
                message=f"processing {progressed}/{progress_total_items} (1x, heartbeat)",
            )
    if task_id:
        try:
            if len(group) == 0:
                final_status = "completed"
            elif failures == 0:
                final_status = "completed"
            elif local_processed - failures > 0:
                final_status = "partial"
            else:
                final_status = "failed"
            finish_run_task(
                db_path,
                task_id=task_id,
                status=final_status,
                message=(
                    f"done processed={local_processed} success={local_processed - failures} "
                    f"failed={failures} preload={timings['preload_s']:.2f}s "
                    f"predict={timings['predict_s']:.2f}s simulate={timings['simulate_s']:.2f}s"
                ),
                processed_items=local_processed,
                success_items=local_processed - failures,
                failed_items=failures,
            )
        except Exception:
            pass
    return {
        "reports": out_reports,
        "batch_rows": out_batch,
        "failures": failures,
        "scope_key": (cat, method),
        "processed": len(group),
        "timings": timings,
    }


def _auto_bid_pending(args: argparse.Namespace) -> dict:
    db_path = args.db_path
    init_db(db_path)
    target_limit = int(args.limit) if args.limit and args.limit > 0 else 0
    fetch_limit = max(target_limit * 5, 2500) if target_limit > 0 else 1_000_000
    notices = load_pending_notices_for_prediction(
        db_path=db_path,
        category=args.category,
        agency_name=(args.agency or "").strip() or None,
        since_days=args.since_days if args.since_days and args.since_days > 0 else None,
        limit=fetch_limit,
    )
    if target_limit > 0:
        notices = notices[:target_limit]
    total_notices = len(notices)
    notice_ids = [n.notice_id for n in notices if getattr(n, "notice_id", "")]
    resumed_notice_ids: set[str] = set()
    if notice_ids:
        with connect(db_path) as _resume_conn:
            chunk_size = 800
            for start in range(0, len(notice_ids), chunk_size):
                chunk_ids = notice_ids[start : start + chunk_size]
                placeholders = ",".join("?" for _ in chunk_ids)
                resumed_rows = _resume_conn.execute(
                    f"""
                    SELECT DISTINCT m.notice_id
                    FROM mock_bids m
                    WHERE m.note LIKE 'auto:%'
                      AND m.notice_id IN ({placeholders})
                      AND NOT EXISTS (
                          SELECT 1
                          FROM bid_results r
                          WHERE r.notice_id = m.notice_id
                            AND r.award_amount > 0
                            AND r.bid_rate > 0
                      )
                    """,
                    chunk_ids,
                ).fetchall()
                resumed_notice_ids.update(
                    str(row[0]) for row in resumed_rows if row and row[0]
                )
        if resumed_notice_ids:
            notices = [n for n in notices if n.notice_id not in resumed_notice_ids]

    # Group by scope so we hit SQLite once per (category, contract_method)
    # instead of once per notice. This is the main performance win.
    scope_buckets: dict[tuple[str, str], list] = defaultdict(list)
    skipped_no_scope: list = []
    for n in notices:
        if not n.category or not n.contract_method:
            skipped_no_scope.append(n)
            continue
        scope_buckets[(n.category, n.contract_method)].append(n)

    with connect(db_path) as _parent_conn:
        approved_parents = {
            row[0]: row[1]
            for row in _parent_conn.execute(
                "SELECT agency_name, parent_name FROM agency_parent_mapping "
                "WHERE status='approved' AND parent_name != ''"
            )
        }

    simulation_id = f"auto-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    resumed_count = len(resumed_notice_ids)
    start_automation_run(
        db_path,
        run_id=simulation_id,
        kind="auto_bid_pending",
        total_items=total_notices,
        resumed_items=resumed_count,
        message=f"starting (resumed {resumed_count})" if resumed_count else "starting",
    )
    reports: list[dict] = []
    batch_rows: list[dict] = []
    failures = 0
    saved = 0
    processed = resumed_count
    FLUSH_EVERY = 2000  # customer bids per flush
    timing_totals = {
        "preload_s": 0.0,
        "case_prep_s": 0.0,
        "predict_s": 0.0,
        "simulate_s": 0.0,
    }
    started_at = perf_counter()

    # Split oversized scopes into sub-chunks so the worker pool stays balanced.
    env_chunk_size = os.environ.get("AUTO_BID_CHUNK_SIZE")
    try:
        chunk_size = int(env_chunk_size) if env_chunk_size else 250
    except ValueError:
        chunk_size = 250
    CHUNK_SIZE = max(25, chunk_size)
    tasks: list[dict] = []
    task_seq = 0
    task_heartbeat_every = max(10, min(30, CHUNK_SIZE // 4))
    for (cat, method), group in scope_buckets.items():
        for start in range(0, len(group), CHUNK_SIZE):
            task_seq += 1
            chunk_notices = group[start : start + CHUNK_SIZE]
            task_id = f"{simulation_id}:t{task_seq:05d}"
            try:
                create_run_task(
                    db_path,
                    task_id=task_id,
                    run_id=simulation_id,
                    kind="auto_bid_pending",
                    category=cat,
                    contract_method=method,
                    task_seq=task_seq,
                    total_items=len(chunk_notices),
                    resumed_items=0,
                )
            except Exception:
                pass  # best-effort; worker will still run
            tasks.append({
                "db_path": db_path,
                "category": cat,
                "contract_method": method,
                "notices": chunk_notices,
                "top_k": args.top_k,
                "num_customers": args.num_customers,
                "target_win_probability": args.target_win_probability,
                "approved_parents": approved_parents,
                "task_id": task_id,
                "task_heartbeat_every": task_heartbeat_every,
            })

    env_workers = os.environ.get("AUTO_BID_WORKERS")
    default_workers = max(2, min(8, (os.cpu_count() or 4)))
    try:
        max_workers = int(env_workers) if env_workers else default_workers
    except ValueError:
        max_workers = default_workers
    max_workers = max(1, min(max_workers, len(tasks) or 1))
    if max_workers == 1 and tasks:
        running_offset = processed
        progress_every = max(10, min(50, CHUNK_SIZE // 2))
        for task in tasks:
            task["progress_run_id"] = simulation_id
            task["progress_total_items"] = total_notices
            task["progress_initial_processed"] = running_offset
            task["progress_every"] = progress_every
            running_offset += len(task["notices"])

    try:
        if max_workers == 1 or len(tasks) <= 1:
            results_iter = (_auto_bid_scope_worker(task) for task in tasks)
        else:
            try:
                pool = ProcessPoolExecutor(max_workers=max_workers)
                futures = [pool.submit(_auto_bid_scope_worker, task) for task in tasks]
                results_iter = (fut.result() for fut in as_completed(futures))
            except PermissionError:
                max_workers = 1
                results_iter = (_auto_bid_scope_worker(task) for task in tasks)
        try:
            for result in results_iter:
                reports.extend(result["reports"])
                batch_rows.extend(result["batch_rows"])
                failures += result["failures"]
                processed += result["processed"]
                for key, value in (result.get("timings") or {}).items():
                    timing_totals[key] = timing_totals.get(key, 0.0) + float(value or 0.0)
                if not args.dry_run and len(batch_rows) >= FLUSH_EVERY:
                    saved += replace_auto_mock_bid_batch(db_path, simulation_id, batch_rows)
                    batch_rows = []
                elapsed = max(perf_counter() - started_at, 0.001)
                rate = processed / elapsed
                update_automation_run(
                    db_path,
                    run_id=simulation_id,
                    processed_items=processed,
                    success_items=processed - failures,
                    failed_items=failures,
                    message=(
                        f"processing {processed}/{total_notices} ({max_workers}x, {rate:.1f}/s, resumed={resumed_count}) "
                        f"| preload={timing_totals['preload_s']:.1f}s "
                        f"cases={timing_totals['case_prep_s']:.1f}s "
                        f"predict={timing_totals['predict_s']:.1f}s "
                        f"simulate={timing_totals['simulate_s']:.1f}s"
                    ),
                )
        finally:
            if max_workers > 1 and len(tasks) > 1:
                # cancel_futures=False is the default; kwarg was Python 3.9+ only
                # and broke 3.8. as_completed already drained all futures above.
                pool.shutdown(wait=False)
        for notice in skipped_no_scope:
            processed += 1
            failures += 1
            reports.append(
                {
                    "notice_id": notice.notice_id,
                    "agency_name": notice.agency_name,
                    "error": "missing category or contract_method",
                }
            )
        if batch_rows and not args.dry_run:
            saved += replace_auto_mock_bid_batch(db_path, simulation_id, batch_rows)
            batch_rows = []
        if saved and not args.dry_run:
            bump_automation_daily_stats(
                db_path,
                auto_bid_runs=1,
                auto_bid_notices=processed - failures,
                auto_bid_customer_bids=saved,
            )
        # Decide final run status based on aggregated success/failure ratio.
        success = max(0, processed - failures)
        if processed == 0:
            final_status = "completed" if total_notices == 0 else "failed"
        elif failures == 0:
            final_status = "completed"
        elif success > 0:
            final_status = "partial"
        else:
            final_status = "failed"
        finish_automation_run(
            db_path,
            run_id=simulation_id,
            status=final_status,
            processed_items=processed,
            success_items=success,
            failed_items=failures,
            message=(
                f"{final_status}: saved {saved} customer bids "
                f"| resumed={resumed_count} "
                f"| preload={timing_totals['preload_s']:.1f}s "
                f"cases={timing_totals['case_prep_s']:.1f}s "
                f"predict={timing_totals['predict_s']:.1f}s "
                f"simulate={timing_totals['simulate_s']:.1f}s"
            ),
        )
    except Exception as exc:
        success = max(0, processed - failures)
        # Keep resumed/heartbeat progress on failure so reruns and monitoring
        # see the true amount of already-usable work.
        error_status = "partial" if success > 0 else "failed"
        finish_automation_run(
            db_path,
            run_id=simulation_id,
            status=error_status,
            processed_items=processed,
            success_items=success,
            failed_items=failures or 1,
            message=str(exc),
        )
        raise
    return {
        "simulation_id": simulation_id,
        "dry_run": bool(args.dry_run),
        "notices_seen": total_notices,
        "notices_resumed": resumed_count,
        "notices_computed": len(notices),
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
