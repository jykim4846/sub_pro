import tempfile
import unittest
from pathlib import Path
from typing import Dict, List

from g2b_bid_reco.csv_import import import_contract_history_csvs
from g2b_bid_reco.db import connect, init_db


class ContractCsvImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.db_path = self.root / "bids.db"
        self.csv_dir = self.root / "csv"
        self.csv_dir.mkdir()
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_import_contract_csv_populates_notice_result_and_contract(self) -> None:
        csv_path = self.csv_dir / "history-2025.csv"
        self._write_contract_csv(
            csv_path,
            [
                {
                    "조달업무구분": "일반용역",
                    "입찰공고번호": "R25BK00000001",
                    "입찰공고차수": "002",
                    "공고기관": "공고기관A",
                    "공고기관코드": "ORG001",
                    "수요기관": "수요기관A",
                    "수요기관코드": "DEM001",
                    "공고게시일자": "20251020",
                    "표준계약방법": "일반경쟁",
                    "수요기관법정동명": "서울특별시",
                    "입찰추정가격": "1000000",
                    "계약번호": "R25TA00000001",
                    "계약금액": "875000",
                    "계약시점대표업체명": "낙찰사A",
                    "기준일자": "20251023",
                },
                {
                    "조달업무구분": "알수없음",
                    "입찰공고번호": "R25BK00000002",
                    "입찰공고차수": "000",
                    "수요기관": "무시기관",
                    "수요기관코드": "DEM999",
                    "공고게시일자": "20251020",
                    "표준계약방법": "제한경쟁",
                    "입찰추정가격": "2000000",
                    "계약번호": "R25TA00000002",
                    "계약금액": "1800000",
                    "기준일자": "20251024",
                },
            ],
        )

        result = import_contract_history_csvs(self.db_path, [csv_path])

        self.assertEqual(result.files_seen, 1)
        self.assertEqual(result.rows_seen, 2)
        self.assertEqual(result.notices_upserted, 1)
        self.assertEqual(result.results_upserted, 1)
        self.assertEqual(result.contracts_upserted, 1)
        self.assertEqual(result.skipped_unknown_category, 1)
        self.assertEqual(result.skipped_missing_notice_id, 0)

        with connect(self.db_path) as conn:
            notice = conn.execute(
                "SELECT * FROM bid_notices WHERE notice_id = ?",
                ("R25BK00000001-002",),
            ).fetchone()
            bid_result = conn.execute(
                "SELECT * FROM bid_results WHERE notice_id = ?",
                ("R25BK00000001-002",),
            ).fetchone()
            contract = conn.execute(
                "SELECT * FROM contracts WHERE contract_id = ?",
                ("R25TA00000001",),
            ).fetchone()

        self.assertEqual(notice["agency_name"], "수요기관A")
        self.assertEqual(notice["agency_code"], "DEM001")
        self.assertEqual(notice["category"], "service")
        self.assertEqual(notice["contract_method"], "일반경쟁")
        self.assertEqual(notice["region"], "서울특별시")
        self.assertEqual(notice["base_amount"], 1000000.0)
        self.assertEqual(notice["opened_at"], "2025-10-20")
        self.assertEqual(bid_result["winning_company"], "낙찰사A")
        self.assertEqual(bid_result["award_amount"], 875000.0)
        self.assertAlmostEqual(bid_result["bid_rate"], 87.5, places=6)
        self.assertEqual(bid_result["bidder_count"], 0)
        self.assertEqual(contract["notice_id"], "R25BK00000001-002")
        self.assertEqual(contract["contract_amount"], 875000.0)
        self.assertEqual(contract["contract_date"], "2025-10-23")

    def test_import_accepts_directory_and_upserts_without_duplication(self) -> None:
        first_csv = self.csv_dir / "history-2024.csv"
        second_csv = self.csv_dir / "history-2025.csv"
        row = {
            "조달업무구분": "물품(내자)",
            "입찰공고번호": "R24BK00000011",
            "입찰공고차수": "000",
            "공고기관": "공고기관B",
            "공고기관코드": "ORG002",
            "수요기관": "",
            "수요기관코드": "",
            "공고게시일자": "20241118",
            "표준계약방법": "수의계약",
            "수요기관법정동명": "",
            "입찰추정가격": "3000000",
            "계약번호": "R24TA00000011",
            "계약금액": "2900000",
            "계약시점대표업체명": "낙찰사B",
            "기준일자": "20241120",
        }
        self._write_contract_csv(first_csv, [row])
        self._write_contract_csv(second_csv, [row])

        result = import_contract_history_csvs(self.db_path, [self.csv_dir])

        self.assertEqual(result.files_seen, 2)
        self.assertEqual(result.rows_seen, 2)
        self.assertEqual(result.notices_upserted, 2)
        self.assertEqual(result.results_upserted, 2)
        self.assertEqual(result.contracts_upserted, 2)

        with connect(self.db_path) as conn:
            counts = conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM bid_notices) AS notices,
                    (SELECT COUNT(*) FROM bid_results) AS results,
                    (SELECT COUNT(*) FROM contracts) AS contracts
                """
            ).fetchone()
            notice = conn.execute(
                "SELECT * FROM bid_notices WHERE notice_id = ?",
                ("R24BK00000011-000",),
            ).fetchone()

        self.assertEqual(counts["notices"], 1)
        self.assertEqual(counts["results"], 1)
        self.assertEqual(counts["contracts"], 1)
        self.assertEqual(notice["agency_name"], "공고기관B")
        self.assertEqual(notice["agency_code"], "ORG002")
        self.assertEqual(notice["category"], "goods")

    def _write_contract_csv(self, path: Path, rows: List[Dict[str, str]]) -> None:
        headers = [
            "조달업무구분",
            "입찰공고번호",
            "입찰공고차수",
            "공고기관",
            "공고기관코드",
            "수요기관",
            "수요기관코드",
            "공고게시일자",
            "표준계약방법",
            "수요기관법정동명",
            "입찰추정가격",
            "계약번호",
            "계약금액",
            "계약시점대표업체명",
            "기준일자",
        ]
        preamble = [
            "검색조건 : 프롬프트 1: 공고게시일자(From)",
            "2025-01-01",
            "프롬프트 2: 공고게시일자(To)",
            "2025-12-31",
        ]

        with path.open("w", encoding="utf-16", newline="") as handle:
            for line in preamble:
                handle.write(f"{line}\n")
            handle.write("\t".join(f'"{header}"' for header in headers) + "\n")
            for row in rows:
                values = [row.get(header, "") for header in headers]
                handle.write("\t".join(f'"{value}"' for value in values) + "\n")


if __name__ == "__main__":
    unittest.main()
