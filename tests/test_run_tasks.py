import sqlite3
import tempfile
import unittest
from pathlib import Path

from g2b_bid_reco.db import (
    connect,
    create_run_task,
    fail_stale_run_tasks,
    fail_stale_automation_runs,
    finish_automation_run,
    finish_run_task,
    get_latest_automation_run,
    heartbeat_run_task,
    init_db,
    list_run_tasks,
    start_automation_run,
    start_run_task,
    summarize_run_tasks,
)


class RunTaskLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "bids.db"
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _active_run(self, run_id: str, kind: str = "auto_bid_pending") -> None:
        start_automation_run(
            self.db_path,
            run_id=run_id,
            kind=kind,
            total_items=500,
            resumed_items=50,
            message="starting (resumed 50)",
        )

    def test_create_start_heartbeat_finish(self) -> None:
        self._active_run("run-1")
        create_run_task(
            self.db_path,
            task_id="run-1:t00001",
            run_id="run-1",
            kind="auto_bid_pending",
            category="goods",
            contract_method="제한경쟁",
            task_seq=1,
            total_items=200,
        )
        tasks = list_run_tasks(self.db_path, "run-1")
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["status"], "queued")

        start_run_task(self.db_path, task_id="run-1:t00001", message="starting")
        heartbeat_run_task(
            self.db_path,
            task_id="run-1:t00001",
            processed_items=120,
            success_items=120,
            failed_items=0,
            message="hb",
        )
        finish_run_task(
            self.db_path,
            task_id="run-1:t00001",
            status="completed",
            processed_items=200,
            success_items=200,
            failed_items=0,
            message="done",
        )
        tasks = list_run_tasks(self.db_path, "run-1")
        self.assertEqual(tasks[0]["status"], "completed")
        self.assertEqual(tasks[0]["processed_items"], 200)
        self.assertEqual(tasks[0]["success_items"], 200)
        self.assertIsNotNone(tasks[0]["started_at"])
        self.assertIsNotNone(tasks[0]["finished_at"])

    def test_summarize_run_tasks(self) -> None:
        self._active_run("run-2")
        create_run_task(
            self.db_path, task_id="run-2:t1", run_id="run-2",
            kind="auto_bid_pending", category="goods", contract_method="A",
            task_seq=1, total_items=100,
        )
        create_run_task(
            self.db_path, task_id="run-2:t2", run_id="run-2",
            kind="auto_bid_pending", category="service", contract_method="B",
            task_seq=2, total_items=80,
        )
        start_run_task(self.db_path, task_id="run-2:t1")
        finish_run_task(
            self.db_path, task_id="run-2:t1", status="completed",
            processed_items=100, success_items=100, failed_items=0,
        )
        start_run_task(self.db_path, task_id="run-2:t2")
        finish_run_task(
            self.db_path, task_id="run-2:t2", status="partial",
            processed_items=60, success_items=55, failed_items=5,
        )
        summary = summarize_run_tasks(self.db_path, "run-2")
        self.assertEqual(summary["task_count"], 2)
        self.assertEqual(summary["completed_tasks"], 1)
        self.assertEqual(summary["partial_tasks"], 1)
        self.assertEqual(summary["total_items"], 180)
        self.assertEqual(summary["processed_items"], 160)
        self.assertEqual(summary["success_items"], 155)
        self.assertEqual(summary["failed_items"], 5)

    def test_fail_stale_run_tasks_partial_vs_failed(self) -> None:
        self._active_run("run-3")
        create_run_task(
            self.db_path, task_id="run-3:a", run_id="run-3",
            kind="auto_bid_pending", category="goods", contract_method="A",
            task_seq=1, total_items=50,
        )
        create_run_task(
            self.db_path, task_id="run-3:b", run_id="run-3",
            kind="auto_bid_pending", category="goods", contract_method="A",
            task_seq=2, total_items=50,
        )
        start_run_task(self.db_path, task_id="run-3:a")
        heartbeat_run_task(
            self.db_path, task_id="run-3:a",
            processed_items=20, success_items=20, failed_items=0,
        )
        start_run_task(self.db_path, task_id="run-3:b")
        # Force both to look stale by backdating updated_at.
        with connect(self.db_path) as conn:
            conn.execute(
                "UPDATE automation_run_tasks SET updated_at = datetime('now','-60 minutes') "
                "WHERE run_id = ?",
                ("run-3",),
            )
        updated = fail_stale_run_tasks(
            self.db_path, kind="auto_bid_pending", stale_after_minutes=5,
        )
        self.assertEqual(updated, 2)
        tasks = {t["task_id"]: t for t in list_run_tasks(self.db_path, "run-3")}
        self.assertEqual(tasks["run-3:a"]["status"], "partial")  # had successes
        self.assertEqual(tasks["run-3:b"]["status"], "failed")   # no successes

    def test_start_automation_run_records_resumed_items(self) -> None:
        start_automation_run(
            self.db_path,
            run_id="run-4",
            kind="auto_bid_pending",
            total_items=300,
            resumed_items=120,
            message="starting (resumed 120)",
        )
        run = get_latest_automation_run(self.db_path, "auto_bid_pending")
        self.assertIsNotNone(run)
        self.assertEqual(int(run["total_items"]), 300)
        self.assertEqual(int(run["resumed_items"]), 120)
        self.assertEqual(int(run["processed_items"]), 120)
        self.assertEqual(int(run["success_items"]), 120)

    def test_fail_stale_automation_runs_marks_partial_when_progress_exists(self) -> None:
        start_automation_run(
            self.db_path,
            run_id="run-5",
            kind="auto_bid_pending",
            total_items=300,
            resumed_items=120,
            message="starting",
        )
        with connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE automation_runs
                SET updated_at = datetime('now','-60 minutes')
                WHERE run_id = 'run-5'
                """
            )
        updated = fail_stale_automation_runs(
            self.db_path,
            kind="auto_bid_pending",
            stale_after_minutes=5,
        )
        self.assertEqual(updated, 1)
        run = get_latest_automation_run(self.db_path, "auto_bid_pending")
        self.assertIsNotNone(run)
        self.assertEqual(run["status"], "partial")

    def test_get_latest_automation_run_prefers_recent_completed_over_stale_running(self) -> None:
        start_automation_run(
            self.db_path,
            run_id="run-stale",
            kind="auto_bid_pending",
            total_items=100,
            resumed_items=20,
            message="stale running",
        )
        with connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE automation_runs
                SET updated_at = datetime('now','-60 minutes')
                WHERE run_id = 'run-stale'
                """
            )
        updated = fail_stale_automation_runs(
            self.db_path,
            kind="auto_bid_pending",
            stale_after_minutes=5,
        )
        self.assertEqual(updated, 1)
        start_automation_run(
            self.db_path,
            run_id="run-fresh",
            kind="auto_bid_pending",
            total_items=100,
            resumed_items=20,
            message="fresh",
        )
        finish_automation_run(
            self.db_path,
            run_id="run-fresh",
            status="completed",
            processed_items=100,
            success_items=100,
            failed_items=0,
            message="done",
        )
        run = get_latest_automation_run(self.db_path, "auto_bid_pending")
        self.assertIsNotNone(run)
        self.assertEqual(run["run_id"], "run-fresh")


if __name__ == "__main__":
    unittest.main()
