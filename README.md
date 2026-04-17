# G2B Bid Recommender MVP

나라장터 공개 데이터를 기준으로 `낙찰 가능성이 높은 투찰 금액`을 추천하기 위한 MVP 프로젝트입니다.

현재 버전은 다음에 초점을 둡니다.

- SQLite 기반 로컬 데이터 저장소
- 발주계획, 공고, 낙찰, 계약을 저장할 수 있는 기본 스키마
- 과거 사례를 기반으로 한 추천 엔진
- 조달청 OpenAPI 수집기 및 적재 CLI
- 샘플 데이터 적재 및 추천 실행용 CLI
- 기본 회귀 테스트

## Quick Start

```bash
python3 -m g2b_bid_reco.cli init-db --db-path data/bids.db
python3 -m g2b_bid_reco.cli load-sample --db-path data/bids.db
python3 -m g2b_bid_reco.cli recommend \
  --db-path data/bids.db \
  --agency "한국출판문화산업진흥원" \
  --category service \
  --method "적격심사" \
  --region seoul \
  --base-amount 240000000 \
  --floor-rate 87.745
```

데모 실행:

```bash
python3 -m g2b_bid_reco.cli demo
```

기관의 새 공고 기준 예측:

```bash
python3 -m g2b_bid_reco.cli predict-notice \
  --db-path data/bids.db \
  --notice-id R25BK01247014-000
```

실데이터 수집:

```bash
cp .env.example .env
# .env 파일에 DATA_GO_KR_SERVICE_KEY=... 입력

export DATA_GO_KR_SERVICE_KEY="발급받은키"

python3 -m g2b_bid_reco.cli collect \
  --db-path data/bids.db \
  --source notices \
  --category service \
  --start 202601010000 \
  --end 202601312359

python3 -m g2b_bid_reco.cli collect \
  --db-path data/bids.db \
  --source results \
  --category service \
  --start 202601010000 \
  --end 202601312359
```

`.env` 파일이 있으면 CLI가 자동으로 읽습니다.

최근 3년 월별 백필:

```bash
python3 -m g2b_bid_reco.cli backfill-recent-3y \
  --db-path data/bids.db \
  --category service \
  --sources notices,results,contracts,plans \
  --months 36 \
  --page-size 100 \
  --max-pages-per-window 20
```

테스트 실행:

```bash
python3 -m unittest discover -s tests -t .
```

## Project Layout

- `g2b_bid_reco/db.py`: SQLite 스키마와 저장소 로직
- `g2b_bid_reco/api.py`: 조달청 OpenAPI 클라이언트와 정규화/적재 로직
- `g2b_bid_reco/recommender.py`: 추천 엔진
- `g2b_bid_reco/agency_analysis.py`: 기관별 예측범위 분석
- `g2b_bid_reco/notice_prediction.py`: 새 공고 기준 예측
- `g2b_bid_reco/sample_data.py`: 샘플 적재용 사례 데이터
- `g2b_bid_reco/cli.py`: 명령행 인터페이스
- `tests/test_recommender.py`: 핵심 추천 테스트

## Current Scope

- 물품/일반용역 중심의 추천 MVP
- 규칙 기반 + 유사 사례 가중치 방식
- OpenAPI 수집은 지원하지만 필드 정규화는 보수적으로 구현됨

## OpenAPI Notes

공식 데이터셋 기준으로 아래 엔드포인트 계열을 사용합니다.

- 입찰공고: `BidPublicInfoService`
- 낙찰: `ScsbidInfoService`
- 계약: `CntrctInfoService`
- 발주계획: `OrderPlanSttusService`

이 프로젝트는 JSON 응답을 기본으로 사용합니다. 서비스별 응답 필드명이 완전히 동일하지 않아서, 현재는 여러 후보 필드를 순서대로 읽는 별칭 방식으로 정규화합니다.

## Next Steps

1. 서비스별 상세 응답 필드 샘플 확보 후 정규화 규칙 보강
2. 공고-낙찰-계약 자동 매핑 보강
3. 기관/계약방법별 세분화 모델 추가
4. 웹 대시보드 또는 API 서버 추가
