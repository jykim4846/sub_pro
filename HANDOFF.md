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

- `.gitignore`
- `README.md`
- `g2b_bid_reco/models.py`
- `g2b_bid_reco/agency_analysis.py`
- `g2b_bid_reco/notice_prediction.py`
- `tests/test_agency_analysis.py`
- `tests/test_notice_prediction.py`

핵심 변경 내용:

- `AgencyRangeRequest`에 `reference_date` 추가
- `AgencyRangeReport`에 `lookback_years_used` 추가
- 기관 표본이 부족하면 예측 기간을 자동 확장:
  - `3년 -> 5년 -> 7년`
- `predict-notice`는 현재 공고의 `opened_at`을 기준일로 넘겨서
  과거 사례만 사용하고,
  최근 3년에서 부족하면 5년, 그래도 부족하면 7년으로 넓힘
- README에도 이 동작을 설명함
- 테스트 추가/수정 완료

현재 테스트 상태:

- `python3 -m unittest discover -s tests -t .`
  - 통과

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
   - 특히 `3/5/7년 자동 확장` 로직

2. `service` 공고-낙찰 매핑률 개선
   - 이게 예측 가능한 기관 수를 더 늘릴 가능성이 큼

3. 결과 설명성 강화
   - 왜 3년이 아니라 5년/7년으로 확장됐는지
   - 동일 기관 사례가 몇 건이고 peer가 몇 건인지
   - 더 명확히 보여주기

4. 필요하면 이후 기관군 fallback 설계
   - 단, 사용자 의도상 기간 확장 다음 단계여야 함

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

현재 Git working tree에는 미커밋 변경이 있다.

- `.gitignore`
- `README.md`
- `g2b_bid_reco/models.py`
- `g2b_bid_reco/agency_analysis.py`
- `g2b_bid_reco/notice_prediction.py`
- `tests/test_agency_analysis.py`
- `tests/test_notice_prediction.py`

이 변경은 의도된 작업 중 상태다. 실수로 되돌리지 말 것.
