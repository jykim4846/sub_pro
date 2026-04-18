# G2B Bid Recommender MVP

나라장터 공개 데이터를 수집해서, `새 발주가 올라왔을 때 어느 정도 금액으로 입찰하는 게 유리한지`를 데이터 기준으로 추정하는 프로젝트입니다.

현재는 다음 범위를 지원합니다.

- 조달청 OpenAPI 수집
- SQLite 적재
- 최근 3년 월별 백필
- 기관/계약방법/금액대 기반 추천
- 저장된 공고번호 기준 예측

## What It Does

- `collect`: 특정 기간의 공고/낙찰/계약/발주계획 수집
- `import-contract-csv`: 내려받은 입찰공고/계약내역 CSV를 대량 적재
- `backfill-recent-3y`: 최근 36개월을 월 단위로 백필
- `enrich-stubs`: 낙찰만 연결돼 있고 공고 메타가 비어 있는 stub 행을 `bidNtceNo` 개별 조회로 보강
- `auto-bid-pending`: 진행 중 공고에 대해 자동 모의 입찰 포트폴리오 생성/저장
- `sync-demand-agencies`: 나라장터 사용자정보 서비스로 수요기관 마스터 동기화
- `recommend`: 기관/조건을 직접 넣어 추천 투찰률 계산
- `agency-range`: 기관 단위 예측 범위 계산
- `predict-notice`: 저장된 공고번호 기준으로 과거 유사 사례만 써서 예측
- `backtest-notice`: 이미 낙찰된 공고 하나를 예측 → 실제 낙찰가와 gap 출력
- `backtest-batch`: N건 샘플링 → hit rate, 평균 gap, worst case 집계

## Setup

```bash
cp .env.example .env
```

`.env`에 공공데이터포털 인증키를 입력합니다.

```env
DATA_GO_KR_SERVICE_KEY=발급받은키
```

DB 초기화:

```bash
python3 -m g2b_bid_reco.cli init-db --db-path data/bids.db
```

## Common Commands

샘플 데이터 로드:

```bash
python3 -m g2b_bid_reco.cli load-sample --db-path data/bids.db
```

단건 수집:

```bash
python3 -m g2b_bid_reco.cli collect \
  --db-path data/bids.db \
  --source notices \
  --category service \
  --start 202601010000 \
  --end 202601312359
```

다운로드한 계약내역 CSV 대량 적재:

```bash
python3 -m g2b_bid_reco.cli import-contract-csv \
  --db-path data/bids.db \
  "data/UI-ADOXFA-076R.입찰공고 및 계약내역.csv"
```

여러 연도 파일을 한 번에 넣을 수도 있습니다.

```bash
python3 -m g2b_bid_reco.cli import-contract-csv \
  --db-path data/bids.db \
  data
```

- `utf-16` + 메타 프리앰블 + 탭 구분 형식의 나라장터 다운로드 CSV를 처리합니다.
- `입찰공고번호 + 차수`로 `notice_id`를 만들고, `agency_name`/`agency_code`/`contract_method`/`base_amount`/`opened_at`/`contract_amount`를 적재합니다.
- `계약금액 / 입찰추정가격`으로 `bid_rate`를 계산해 `bid_results`도 함께 채웁니다.

증분 수집 (DB의 마지막 `opened_at` 이후만 받기):

```bash
python3 -m g2b_bid_reco.cli collect-recent \
  --db-path data/bids.db \
  --category service \
  --sources notices,results,contracts
```

- `--since 20260401` 처럼 직접 지정하면 그 시각 이후만 수집
- DB가 비어 있으면 `--fallback-days 30`(기본) 만큼 과거를 커버
- `scripts/daily-api-collect.sh`는 증분 수집 뒤 자동 모의 입찰 포트폴리오까지 함께 갱신
- `scripts/daily-api-collect.sh`는 증분 수집 뒤 수요기관 마스터 동기화와 자동 모의 입찰 포트폴리오 갱신까지 함께 수행
- cron/LaunchAgent로 하루 1회 돌리면 사실상 자동 최신 유지

자동 모의 입찰 포트폴리오 생성:

```bash
python3 -m g2b_bid_reco.cli auto-bid-pending \
  --db-path data/bids.db \
  --category service \
  --num-customers 5 \
  --top-k 10
```

- 기본값은 `모든 진행 중 공고`를 읽어 고객별 분산 투찰 포트폴리오를 생성합니다.
- 기간 제한이 필요하면 `--since-days N`, 건수 제한이 필요하면 `--limit N`을 추가합니다.
- 기존 pending 자동 입찰(`note LIKE 'auto:%'`)은 같은 공고 기준으로 교체 저장됩니다.
- 실제 낙찰 결과가 수집되면 `mock_bids` 평가는 자동 반영됩니다.

수요기관 마스터 동기화:

```bash
G2B_USER_INFO_ENDPOINT='http://apis.data.go.kr/1230000/ao/UsrInfoService02/<operation>' \
python3 -m g2b_bid_reco.cli sync-demand-agencies \
  --db-path data/bids.db
```

- `bid_notices.agency_code`와 연결되는 `demand_agencies` 테이블을 유지합니다.
- 기본 동작은 알려진 수요기관 조회 후보 operation path를 자동 탐지합니다.
- 필요하면 `--endpoint` 또는 `G2B_USER_INFO_ENDPOINT`로 강제 지정할 수 있습니다.
- 후보 목록은 `python3 -m g2b_bid_reco.cli sync-demand-agencies --print-candidates` 로 확인할 수 있습니다.

일일 자동화에서 수요기관 동기화까지 포함:

```bash
bash scripts/daily-api-collect.sh
```

- 기본값으로 `SYNC_DEMAND_AGENCIES=1` 이라 수요기관 마스터 동기화가 먼저 실행됩니다.
- exact operation URL을 알고 있으면 `G2B_USER_INFO_ENDPOINT` 로 강제 지정할 수 있습니다.
- 기관 동기화 기간을 제한하려면 `DEMAND_AGENCY_SINCE`, `DEMAND_AGENCY_UNTIL` 환경변수를 사용합니다.

최근 3년 백필:

```bash
python3 -m g2b_bid_reco.cli backfill-recent-3y \
  --db-path data/bids.db \
  --category service \
  --sources notices,results,contracts,plans \
  --months 36 \
  --page-size 100 \
  --max-pages-per-window 20
```

여러 카테고리를 순차 보강 (goods + construction 기본):

```bash
bash scripts/enrich-all-stubs.sh
# 또는 특정 카테고리만
bash scripts/enrich-all-stubs.sh service
# 커스텀 DB / 슬립 시간
DB_PATH=data/bids.db SLEEP_SEC=1.5 bash scripts/enrich-all-stubs.sh
```

- 각 카테고리 시작/종료 시점에 stub/usable 건수 출력
- 중간에 중단해도 `enrich-stubs`가 stub 상태인 행만 다시 집어 resume 가능

Stub notice 보강 (낙찰만 적재되고 공고 메타가 비어 있는 경우):

```bash
python3 -m g2b_bid_reco.cli enrich-stubs \
  --db-path data/bids.db \
  --category service \
  --batch-limit 200 \
  --verbose
```

`backfill-recent-3y`로 results만 들어오고 notices는 백필 기간을 벗어나 있던 공고는
`agency_name`/`contract_method`/`base_amount`가 비어 있어 예측 대상에서 빠집니다.
이 명령은 그런 stub 행을 모아 `bidNtceNo`로 notices API를 개별 조회해 메타 데이터만 보강합니다.
`--batch-limit`을 생략하면 남은 stub을 전부 처리하므로 서비스 호출량에 유의하세요.

기관 조건 기반 추천:

```bash
python3 -m g2b_bid_reco.cli recommend \
  --db-path data/bids.db \
  --agency "한국출판문화산업진흥원" \
  --category service \
  --method "적격심사" \
  --region seoul \
  --base-amount 240000000 \
  --floor-rate 87.745
```

기관별 예측 범위:

```bash
python3 -m g2b_bid_reco.cli agency-range \
  --db-path data/bids.db \
  --agency "한국출판문화산업진흥원" \
  --category service \
  --method "적격심사" \
  --region seoul \
  --base-amount 275000000
```

저장된 공고번호 기준 예측:

```bash
python3 -m g2b_bid_reco.cli predict-notice \
  --db-path data/bids.db \
  --notice-id R25BK000029-000
```

단일 낙찰 공고 백테스트 (예측 vs 실제):

```bash
python3 -m g2b_bid_reco.cli backtest-notice \
  --db-path data/bids.db \
  --notice-id R25BK000029-000
```

배치 백테스트 (N건 샘플링 → hit rate 집계):

```bash
python3 -m g2b_bid_reco.cli backtest-batch \
  --db-path data/bids.db \
  --category service \
  --sample-size 100 \
  --worst-case-keep 5
```

테스트:

```bash
python3 -m unittest discover -s tests -t .
```

## Dashboard

기관 하나를 골라 그 기관의 과거 공고마다 `predict-notice`를 돌려 예측 vs 실제 낙찰가를 시각화합니다.

설치 (한 번만):

```bash
pip install -e ".[dashboard]"
```

실행:

```bash
streamlit run dashboard.py
```

- 사이드바에서 DB 경로 / 카테고리 / 최소 공고 수 선택
- 기관 드롭다운에서 대상 기관 선택
- 꺾은선 차트: 예산(base) / 실제 낙찰가 / 예측 투찰가
- 마커: ⭐ = "낙찰 가능", ✕ = "낙찰 불가"
- 하단 테이블에 공고별 세부 비교

낙찰 가능 여부 정의:

- `predicted_amount ≤ actual_amount` 그리고
- 하한율이 주어지면 `predicted_rate ≥ floor_rate`

## DB Snapshot 공유

`data/*.db`는 Git에서 제외되므로 다른 PC에서 repo만 clone해서는 예측이 동작하지 않습니다.
갱신 시점의 DB 스냅샷을 GitHub Release 자산으로 업로드하고 다른 PC에서 바로 내려받는 흐름입니다.

스냅샷 발행 (GH 인증이 있는 작업 PC에서):

```bash
bash scripts/publish-db-snapshot.sh
```

- 기존 `db-snapshot` 릴리스를 제거하고 동일 태그로 재발행합니다.
- 자산 URL이 항상 `https://github.com/<owner>/<repo>/releases/download/db-snapshot/bids.db.gz`이므로 다른 PC에서는 URL 고정입니다.

다른 PC에서 복원:

```bash
bash scripts/pull-db-snapshot.sh
```

- 기존 `data/bids.db`는 `data/bids.db.bak`으로 백업된 뒤 덮어씁니다.
- `gh auth login`을 먼저 수행해야 합니다 (repo가 private인 경우 필수).

## Recommended Workflow

1. `init-db`로 DB 생성
2. `collect` 또는 `backfill-recent-3y`로 원천 데이터 적재
3. `predict-notice`로 새 공고 기준 예측
4. 표본이 적은 기관은 `agency-range`로 신뢰도와 peer 반영 범위 확인

## Project Layout

- `g2b_bid_reco/api.py`: 조달청 OpenAPI 호출과 월별 백필
- `g2b_bid_reco/db.py`: SQLite 스키마와 적재 로직
- `g2b_bid_reco/recommender.py`: 기본 추천 엔진
- `g2b_bid_reco/agency_analysis.py`: 기관 단위 예측 범위 분석
- `g2b_bid_reco/notice_prediction.py`: 저장된 공고 기준 예측
- `g2b_bid_reco/cli.py`: CLI 진입점
- `tests/`: 회귀 테스트

## Notes

- 응답 필드명은 서비스별 차이가 있어서 현재는 별칭 기반 정규화를 사용합니다.
- 예측은 `정답 금액`이 아니라 `가능성 높은 투찰률/금액 범위`를 반환합니다.
- 기관 표본이 부족하면 예측 기간을 `최근 3년 -> 5년 -> 7년` 순서로 자동 확장합니다.
- `.env`, `.omx`, SQLite DB 파일은 Git에 포함하지 않습니다.
