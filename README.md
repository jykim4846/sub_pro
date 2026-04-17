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
- `backfill-recent-3y`: 최근 36개월을 월 단위로 백필
- `recommend`: 기관/조건을 직접 넣어 추천 투찰률 계산
- `agency-range`: 기관 단위 예측 범위 계산
- `predict-notice`: 저장된 공고번호 기준으로 과거 유사 사례만 써서 예측

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

테스트:

```bash
python3 -m unittest discover -s tests -t .
```

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
