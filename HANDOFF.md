# AI Handoff

## Goal

사용자가 만들고 싶은 것은:

- 조달청/나라장터 공개 데이터를 수집한다.
- 특정 수요기관이 새 발주를 냈을 때,
- 그 기관의 과거 발주/낙찰 데이터를 근거로
- `어느 정도 금액(투찰률/투찰금액)으로 입찰하면 가능성이 높은지`
  추정하는 시스템을 만든다.

중요한 사용자 의도:

- `정답 금액`을 맞추려는 게 아니다.
- `가장 가능성이 높은 투찰 구간`을 데이터 기반으로 제시하려는 것이다.
- 기관명 자동 정규화를 섣불리 해서 다른 기관을 섞는 것은 원하지 않는다.
- 표본이 부족하면 우선 `기간을 늘려서` 모수를 확보하고 싶어 한다.
- 즉, fallback 1순위는 기관군 병합이 아니라 `같은 기관의 더 긴 기간 조회`다.

## What Has Been Built

현재 프로젝트는 Python + SQLite 기반 CLI MVP다.

주요 파일:

- `g2b_bid_reco/api.py`
  - 조달청 OpenAPI 호출
  - 월 단위 백필(`backfill-recent-3y`)
- `g2b_bid_reco/db.py`
  - SQLite 스키마
  - 공고/낙찰/계약/발주계획 upsert
- `g2b_bid_reco/recommender.py`
  - 기본 추천 엔진
- `g2b_bid_reco/agency_analysis.py`
  - 기관 단위 예측 범위 계산
- `g2b_bid_reco/notice_prediction.py`
  - 저장된 공고번호 기준 예측
- `g2b_bid_reco/cli.py`
  - CLI 엔트리포인트
- `tests/`
  - 회귀 테스트

지원 명령:

- `init-db`
- `load-sample`
- `collect`
- `backfill-recent-3y`
- `recommend`
- `agency-range`
- `predict-notice`

## Git / Remote

저장소는 이미 GitHub에 올라가 있음.

- remote: `git@github-personal:jykim4846/sub_pro.git`
- pushed branch: `main`

최근 push된 커밋:

- `1ecd52b` Clarify how to operate the bid prediction workflow
- `22d0e7f` Bootstrap a G2B bid recommendation MVP with live data backfill

## Data Ingestion Status

현재 로컬 DB: `data/bids.db`

집계 시점 기준 적재량:

- `bid_notices`: `46,208`
- `bid_results`: `21,707`
- `contracts`: `7,688`
- `procurement_plans`: `16,005`

카테고리별 공고 수:

- `construction`: `5,913`
- `goods`: `9,603`
- `service`: `3,982`

카테고리별 낙찰 연결 수:

- `construction`: `660`
- `goods`: `1,691`
- `service`: `140`

예측 가능한 `agency + category + contract_method` 조합 수:

- `182`

기준:

- `bid_notices`와 `bid_results`가 연결돼 있고
- `agency_name`, `contract_method`, `base_amount`가 존재하며
- 같은 조합의 낙찰 표본이 `3건 이상`

## Current Product Behavior

현재 예측은 다음 구조다.

1. 저장된 공고 또는 입력 조건에서
   - 기관명
   - category
   - contract_method
   - region
   - base_amount
   를 읽는다.
2. 같은 조건의 과거 사례를 찾는다.
3. 기관 자체 표본이 부족하면 peer 분포를 섞는다.
4. 추천 결과로
   - 중심 투찰률
   - 하한/상한 범위
   - 추천 금액
   - 신뢰도
   - 근거 사례
   를 반환한다.

## Important Recent Change Not Yet Committed

아래 변경은 로컬 파일에는 반영되어 있지만 아직 GitHub에 push되지 않았다.

수정 파일:

- `README.md`
- `g2b_bid_reco/cli.py`
- `g2b_bid_reco/csv_import.py`
- `tests/test_csv_import.py`

핵심 변경 내용:

- `import-contract-csv` CLI 명령 추가
- 나라장터 다운로드 CSV(`utf-16` + 메타 프리앰블 + 탭 구분)를 직접 적재하는 importer 추가
- 입력 경로는 파일 / 디렉터리 / glob 패턴 모두 지원
- `입찰공고번호 + 차수`로 `notice_id` 생성
- `수요기관 -> agency`, 없으면 `공고기관` fallback
- `조달업무구분`을 현재 앱 카테고리로 매핑
  - `공사 -> construction`
  - `일반용역`, `기술용역 -> service`
  - `물품(내자)`, `물품(외자) -> goods`
- `입찰추정가격`과 `계약금액`이 둘 다 있으면 `bid_rate = 계약금액 / 입찰추정가격 * 100`으로 계산해 `bid_results`까지 생성
- README에 사용법 추가

현재 테스트 상태:

- `python3 -m unittest discover -s tests -t .`
  - 통과

실데이터 smoke test:

```bash
python3 -m g2b_bid_reco.cli import-contract-csv \
  --db-path /tmp/g2b_contract_csv_smoke.db \
  "data/UI-ADOXFA-076R.입찰공고 및 계약내역.csv"
```

결과 요약:

- CSV 읽은 행 수: `258,499`
- importer 결과:
  - `notices_upserted`: `258,499`
  - `results_upserted`: `176,453`
  - `contracts_upserted`: `179,481`
- 최종 DB 실체 row 수:
  - `bid_notices`: `235,244`
  - `bid_results`: `155,958`
  - `contracts`: `179,481`

왜 `notices_upserted`보다 실제 `bid_notices` row 수가 적은가:

- 같은 `입찰공고번호-차수`에 여러 계약 행이 있어 upsert로 합쳐지기 때문

## Why This Change Was Added

사용자가 명시적으로 원하는 건:

- 표본이 적은 기관에 대해
- 기관군 병합을 먼저 하는 게 아니라
- `같은 기관의 데이터 기간을 더 길게 봐서`
  모수를 늘리는 것

그래서 현재 로직은:

- 최근 3년으로 먼저 계산
- 동일 기관 표본이 충분하지 않으면 최근 5년
- 그래도 부족하면 최근 7년

## Example: Korea Water Resources Corporation

사용자가 `수자원공사 정보 적재됐어?`라고 물었고,
실제로 일부 적재되어 있었다.

확인된 기관:

- `한국수자원공사 시화사업본부`
- `한국수자원공사 운문댐관리단`
- `한국수자원공사 한강권역부문 연천포천권지사`

예시 실행:

```bash
python3 -m g2b_bid_reco.cli agency-range \
  --db-path data/bids.db \
  --agency "한국수자원공사 시화사업본부" \
  --category goods \
  --method "전자시담" \
  --base-amount 2343000000
```

현재 결과 요약:

- `lookback_years_used`: `7`
- `agency_case_count`: `1`
- `peer_case_count`: `419`
- `confidence`: `medium`

즉:

- 같은 기관 자체 표본은 여전히 매우 적다.
- 대신 최근 7년까지 넓혀서 동일 계약방법 peer를 더 많이 반영하는 쪽으로 바뀌었다.

## Constraints / Product Decisions

### 1. 기관명 정규화는 매우 보수적으로 해야 함

사용자는 기관명 자동 병합이 위험하다고 보고 있다.

현재 방향:

- 원본 기관명 보존
- 검증 없는 fuzzy merge 금지
- 기관군 병합은 나중에 별도 계층으로 다룰 것

### 2. 기간 확장이 기관군 병합보다 우선

표본 부족 시 우선순위:

1. 같은 기관의 더 긴 기간
2. 그래도 부족하면 이후 기관군/peer fallback 검토

### 3. 예측은 금액 단일값보다 범위가 중요

사용자는 `무조건 된다`가 아니라
`가장 가능성이 높은 투찰 구간`을 원한다.

## Open Problems

1. `service` 낙찰 연결 수가 상대적으로 적다.
   - notice는 많은데 awarded_rows가 적음
   - 공고-낙찰 매핑 보강 여지가 큼

2. 기관군 fallback은 아직 본격 구현하지 않았다.
   - 사용자도 섣부른 병합은 원하지 않음

3. `goods`/`construction` 백필은 꽤 진행됐고 DB 수치도 커졌지만,
   CLI 백필 프로세스가 무출력 장시간 실행 패턴을 보였다.
   - 실제 적재는 진행됐음
   - 다만 retry/checkpoint/structured progress는 아직 없음

4. 최근 추가한 `3/5/7년 자동 확장` 로직은 아직 commit/push 안 됨.

## Suggested Next Steps For Another AI

우선순위는 이 순서가 맞다.

1. 로컬 변경분 커밋/푸시
   - 특히 `import-contract-csv` importer

2. 사용자가 연도별 CSV를 계속 내려받을 예정이므로,
   importer로 실제 `data/*.csv`를 `data/bids.db`에 적재

3. 적재 후 품질 점검
   - 예측 가능한 `agency + category + contract_method` 조합 수
   - `각 수요기관` 같은 저품질 기관명 비율
   - category별 usable awarded rows

4. 필요하면 저품질 기관명 제외 규칙 추가
   - 현재는 원본 보존 우선이라 자동 정규화는 넣지 않음

5. 그 다음에야 `service` 매핑률 추가 개선 또는 API 증분 수집 경로 보강 검토

## Current Local State Notes

- `data/2024.csv`, `data/2025.csv`, `data/2026.csv`, `data/UI-ADOXFA-076R...csv`, `reports/`는 로컬 데이터 산출물이다. 대용량이므로 기본적으로 Git에는 올리지 않는 편이 맞다.
- `.DS_Store`도 로컬 잡파일이므로 커밋하지 않는 편이 맞다.

## Release Assets For Another Machine

연도별 CSV는 GitHub Release asset으로 올렸다.

- release tag: `csv-history-20260417`
- URL: `https://github.com/jykim4846/sub_pro/releases/tag/csv-history-20260417`

포함 파일:

- `2024.csv`
- `2025.csv`
- `2026.csv`

다른 PC에서 내려받은 뒤 적재:

```bash
python3 -m g2b_bid_reco.cli import-contract-csv \
  --db-path data/bids.db \
  data
```

주의:

- CSV는 Git repo에는 포함되지 않는다.
- 다음 AI는 release asset을 먼저 내려받고 importer를 돌리는 흐름으로 시작하면 된다.

## Safe Commands

테스트:

```bash
python3 -m unittest discover -s tests -t .
```

공고 예측:

```bash
python3 -m g2b_bid_reco.cli predict-notice \
  --db-path data/bids.db \
  --notice-id R25BK000029-000
```

기관 예측:

```bash
python3 -m g2b_bid_reco.cli agency-range \
  --db-path data/bids.db \
  --agency "한국수자원공사 시화사업본부" \
  --category goods \
  --method "전자시담" \
  --base-amount 2343000000
```

## Current Working Tree

- 마지막 확인 시점 기준으로 `dashboard.py`를 제외한 주요 importer/handoff 변경은 원격 `main`에 반영돼 있었다.
- 후속 작업 전에는 `git status`로 실제 로컬 상태를 다시 확인하는 편이 안전하다.
