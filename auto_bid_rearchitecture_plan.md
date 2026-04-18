# Auto Bid 구조개편 설계안

## 목표
현재 시스템에서 아래 4가지만 우선 구조개편할 수 있도록, 다른 AI가 바로 구현 작업으로 옮길 수 있는 수준의 설계안을 정리한다.

- `1. auto_bid_pending 작업 단위 재설계`
- `3. resume / partial / heartbeat 정식화`
- `4. scope 전처리 캐시`
- `5. 대시보드 정합성 통일`

## 범위
- 포함
  - `auto_bid_pending` 실행 구조 개선
  - 상태 추적 체계 정리
  - 캐시 기반 성능 개선
  - 대시보드 상태 표시 일관화
- 제외
  - Postgres 전환
  - Kubernetes 도입
  - 예측 모델 수식 변경
  - 분산 입찰 전략 변경

## 핵심 원칙
- 예측값 산출 로직은 바꾸지 않는다.
- 분산 입찰 전략 로직은 바꾸지 않는다.
- 성능 개선은 “같은 결과를 더 안정적으로, 더 잘게 나눠 처리”하는 방향으로만 한다.
- 이미 저장된 `auto:%` 결과는 자산으로 취급하고, 재실행 시 재활용 가능해야 한다.
- 상태 표시는 `공고 단위 결과`와 `배치 단위 실행 상태`를 분리한다.

## 1. 작업 단위 재설계

### 현재 문제
- `auto_bid_pending`이 큰 단일 run으로 동작한다.
- 내부적으로 scope/chunk 개념은 있으나 DB 레벨 작업 단위로 노출되지 않는다.
- 중간 실패/재개/부분 반영을 안정적으로 다루기 어렵다.

### 목표 구조
- `run -> task -> notice result` 3단계 구조로 분리한다.

### 테이블 설계

#### 기존 유지
- `automation_runs`
  - 의미: 사용자 또는 스케줄러가 시작한 상위 배치 실행

#### 신규 추가
- `automation_run_tasks`
  - 의미: 실제 처리 단위
  - 1 task = `(category, contract_method)` scope의 chunk

#### 선택사항
- `automation_run_task_notices`
  - 필요 시 task에 포함된 notice 목록 기록
  - 초기 구현에서는 생략 가능

### `automation_run_tasks` 스키마 제안
- `task_id TEXT PRIMARY KEY`
- `run_id TEXT NOT NULL`
- `kind TEXT NOT NULL`
  - 예: `auto_bid_scope_chunk`
- `category TEXT NOT NULL`
- `contract_method TEXT NOT NULL`
- `task_seq INTEGER NOT NULL`
- `total_items INTEGER NOT NULL DEFAULT 0`
- `processed_items INTEGER NOT NULL DEFAULT 0`
- `success_items INTEGER NOT NULL DEFAULT 0`
- `failed_items INTEGER NOT NULL DEFAULT 0`
- `status TEXT NOT NULL DEFAULT 'queued'`
  - 허용값:
    - `queued`
    - `running`
    - `completed`
    - `partial`
    - `failed`
    - `cancelled`
- `resumed_items INTEGER NOT NULL DEFAULT 0`
- `message TEXT NOT NULL DEFAULT ''`
- `started_at TEXT`
- `updated_at TEXT`
- `finished_at TEXT`

### 작업 생성 규칙
- `load_pending_notices_for_prediction()` 결과를 기준으로 전체 pending 공고를 가져온다.
- 이미 `auto:%` 결과가 있고 아직 결과 확정되지 않은 공고는 `resumed`로 분류한다.
- 나머지만 실제 계산 대상으로 남긴다.
- 계산 대상 notices를 `(category, contract_method)`로 묶는다.
- 각 scope를 `chunk_size` 단위로 잘라 task를 생성한다.
- 상위 run에는 총 대상 수와 resumed 수를 기록한다.
- 각 task는 자기 chunk의 notice 목록만 책임진다.

### 실행 규칙
- 단일 프로세스에서도 task 단위 루프로 실행한다.
- 나중에 멀티워커로 가더라도 task를 DB 기준으로 선점해 처리할 수 있어야 한다.
- 각 task 완료 시 상위 run 집계를 갱신한다.

## 3. resume / partial / heartbeat 정식화

### 현재 문제
- 이미 계산된 공고를 부분적으로 이어받지만 상태 체계가 불완전하다.
- stale `running` row가 남기 쉽다.
- “실제로 반영된 공고”와 “배치 완료”가 혼재된다.

### 상태 정의
- `completed`
  - task/run의 목표 대상이 전부 처리 완료
- `partial`
  - 일부는 처리/반영됐지만 전체 완료 전 중단
- `failed`
  - 실질적으로 실패했고 반영분이 없거나 매우 적음
- `running`
  - heartbeat가 최근 갱신됨
- `stalled`
  - DB에는 저장하지 않아도 됨
  - UI 계산 상태로만 표현 가능

### resume 규칙
아래 3개 조건을 모두 만족하면 `resumed`로 인정한다.

- `mock_bids.note LIKE 'auto:%'`
- `bid_results`가 아직 확정되지 않은 notice
- 현재 pending 집합에 속하는 notice

### run 시작 시 규칙
- `total_items = 전체 pending 대상`
- `processed_items = resumed_items`
- `success_items = resumed_items`
- `failed_items = 0`
- `message = starting (resumed N)`

### heartbeat 규칙

#### task 기준
- `10~30 notice`마다 또는
- `30초`마다
- 둘 중 먼저 도달 시 DB 갱신

#### 갱신 항목
- `processed_items`
- `success_items`
- `failed_items`
- `message`
- `updated_at`

#### 추가 원칙
- 단일 worker일 때도 task 내부 heartbeat 필수
- chunk 완료까지 기다리지 말고 중간 heartbeat 허용

### partial 규칙
- 프로세스 종료, 예외, 사용자 중단 시:
  - `success_items > 0` 이면 `partial`
  - 아니면 `failed`
- 상위 run도 동일 기준으로 마감한다.
- stale run 정리 시에도 같은 규칙을 적용한다.

### 메시지 포맷 제안

#### run
`processing 134091/265106 (resumed=133841, active_task=goods/제한경쟁#12)`

#### task
`processing 150/250 | predict=3.1s simulate=41.8s`

## 4. scope 전처리 캐시

### 현재 병목
- 큰 scope에서 과거 사례를 반복 가공한다.
- 특히 simulation 쪽 시장분포 준비 비용이 크다.

### 원칙
- 예측 및 전략 공식은 유지한다.
- 입력 데이터 준비만 캐시한다.

### 캐시 대상
- scope key: `(category, contract_method)`
- 캐시 값:
  - `scope_cases_sorted_by_opened_at`
  - `scope_opened_at_list`
  - `scope_bid_rates_opened_asc`
  - `competitors_top_k_base`
  - 필요 시 `agency_case_count_map`

### 권장 구조
- 프로세스 메모리 캐시 사용
- task worker 내부에서 1회 생성 후 chunk 전체 재사용
- DB 영속 캐시는 2차 단계에서 고려

### 새 dataclass 제안
- `ScopePreparedData`
  - `scope_key`
  - `cases`
  - `opened_at_values`
  - `valid_bid_rates_opened_asc`
  - `competitors`
  - `agency_case_counts`
  - `prepared_at`

### 헬퍼 함수 제안
- `prepare_scope_data(db_path, category, contract_method, top_k) -> ScopePreparedData`
- `slice_scope_prefix(prepared, cutoff_opened_at) -> PrefixView`

### `PrefixView` 구성
- `cases`
- `rates_opened_asc`
- `agency_case_counts`

### 구현 원칙
- task마다 notice별로 `cases` 전체 복사를 최소화한다.
- 가능하면 prefix index 기반으로 view만 만들고 재사용한다.
- simulation에는 이미 `historical_rates_opened_asc` 전달 가능한 구조를 유지한다.

### 경쟁사 캐시
- `top_winners_for_scope()` 결과는 scope 단위 1회만 계산한다.
- 현재 전략이 `base_amount` 무시 scope 경쟁사 구성을 유지한다면 그대로 캐시 재사용한다.
- 나중에 `base_amount`별 competitor가 필요해지면 별도 계층 캐시를 만든다.

### 측정 지표
- task 메시지에 아래 항목 포함:
  - `preload_s`
  - `case_prep_s`
  - `predict_s`
  - `simulate_s`
- 목표는 `simulate_s` 및 총 elapsed 감소 확인이다.

## 5. 대시보드 정합성 통일

### 현재 문제
- 서로 다른 영역이 서로 다른 기준으로 상태를 보여준다.
- `running`인데 “실행 중인 배치 없음” 같은 불일치가 발생한다.
- `속도`는 세션 샘플 기준, `성공`은 DB 기준이라 어긋난다.

### 원칙
- 모든 상태 표시는 동일한 `run/task source of truth`에서 계산한다.
- `속도`, `성공`, `실행 여부` 모두 같은 시점, 같은 run 기준으로 본다.
- 세션 샘플은 보조 지표일 뿐이며 stale이면 0 처리한다.

### 대시보드가 보여야 하는 상태 계층
- 상위 run 상태
- 현재 active task 상태
- resumed 반영분
- 새로 계산된 반영분
- 최근 속도

### 표시 항목 재정의

#### 처리
- `processed_items / total_items`
- resumed 포함

#### 성공
- `success_items`
- resumed 포함

#### 신규 계산
- `success_items - resumed_items`

#### resumed
- 시작 시점에 이어받은 공고 수

#### 최근 5분 처리량
- stale가 아니고 `updated_at`이 최근인 경우만 계산
- 아니면 `0`

#### 평균 속도
- `(processed_items - resumed_items) / 실제 계산 경과시간`
- resumed를 빼는 쪽이 해석상 더 적절하다

### UI 기준 통일
- “실행 중인 배치 없음”
  - `latest run.status == running` 이고 `updated_at`이 최근이면 절대 표시 금지
- “멈춤 의심”
  - `running`이지만 heartbeat 임계 초과
- “부분 완료”
  - `status == partial`
- “실패”
  - `status == failed`

### 권장 UI 구조

#### 상위 run 카드
- 상태
- 전체/성공/실패
- resumed
- 신규 계산
- 최근 5분 처리량
- 평균 속도

#### 현재 active task 카드
- scope
- chunk 번호
- task 처리량
- predict/simulate 타이밍

#### 하단 표
- 최근 task 목록
- 상태별 필터 가능

### 세션 샘플 처리 규칙
- `run_id` 바뀌면 샘플 초기화
- `updated_at`이 갱신되지 않으면 속도 `0`
- `running`이 아니면 속도 `0`
- stale 상태면 최근 5분 처리량도 `0`

## 구현 순서
1. `automation_run_tasks` 테이블 추가
2. `auto_bid_pending`을 `run + tasks` 구조로 분리
3. resumed / partial / heartbeat 규칙 적용
4. scope 전처리 캐시 모듈화
5. dashboard 상태 계산 로직 단일화
6. stale run/task 정리 로직 추가
7. 테스트 보강

## 필수 테스트
- resumed notice가 run 시작 시 `success_items`에 반영됨
- resumed notice는 다시 계산하지 않음
- 일부 task 성공 후 중단되면 `partial`
- stale running task/run 정리 시 상태가 올바름
- scope cache 사용 전후 결과 동일
- simulation optimization 전후 결과 동일
- dashboard 상태 계산이 아래 케이스별로 일관적
  - `running`
  - `partial`
  - `failed`
  - `stalled`

## 다른 AI에게 줄 구현 지침
- 예측 알고리즘 수식 변경 금지
- 분산 입찰 배치 로직 변경 금지
- `AgencyRangeAnalyzer`, `NoticePredictor`, `run_simulation`의 의미를 바꾸지 말 것
- 허용되는 변경:
  - 작업 분할
  - 상태 추적
  - 캐시
  - heartbeat
  - UI 정합성
  - 동일 결과 보장 하의 성능 최적화
- `mock_bids`는 기존 데이터 자산으로 취급하고 재실행 시 `resume` 가능해야 한다.
- `replace_auto_mock_bid_batch()`와 resume 규칙의 상호작용을 반드시 테스트할 것

## 완료 기준
- 큰 run이 중간에 멈춰도 이미 계산된 공고는 즉시 `success`로 반영됨
- 재시작 시 기존 auto 결과를 이어받고 남은 공고만 계산
- 대시보드에서 `running / partial / stalled / failed`가 일관되게 보임
- `predict`와 `simulate` 타이밍이 task 단위로 보임
- 이전 대비 첫 chunk 완료 시간이 줄고 진행률이 더 자주 갱신됨
