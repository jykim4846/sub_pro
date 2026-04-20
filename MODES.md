# Backtest vs Auto-bid — 두 모드의 목적과 설계

> 이 문서는 예측 엔진을 "어떤 목적으로, 어떤 단위로" 돌리는지 기준을 정한다.
> 두 모드는 **목적이 다르고**, 따라서 **단위·산출물·학습 루프가 모두 다르다**.
> 혼동해서 한쪽 로직을 다른 쪽에 끌고 들어가면 안 된다.

---

## 0. TL;DR

| 항목 | **Backtest** | **Auto-bid** |
|---|---|---|
| 목적 | 예측 정확도 + 전략 유효성 검증 | 진행 중 공고에 고객군을 어떻게 배치할지 전략 수립 |
| 대상 | 이미 끝난 공고 (실낙찰 존재) | 진행 중인 공고 (결과 미정) |
| 단위 | Notice 당 **예측 1건** | Notice 당 **N별 포트폴리오 묶음** (N=1,2,3,…) |
| 고객 수 개념 | 없음 | 핵심 축 — N마다 독립 전략 |
| 산출물 | hit_rate, rate_gap 등 지표 | per-N 분포 전략 + 실측 win_rate |
| 코드 경로 | `backtest.py`, `notice_prediction.py` | `cli.py` (auto_bid), `simulation.py`, `dashboard.py` |

---

## 1. Backtest 모드

### 목적
- 우리의 예측가가 실제 낙찰가에 얼마나 근접하는가
- 그 예측을 기반으로 짠 전략(quantile 선택, shrinkage 강도, trend 조정 등)이
  - 낙찰률을 높이고
  - 동시에 고객 수익을 희생하지 않는가
  를 **검증**한다

### 단위
- **Notice 당 예측값 1개** (AgencyRangeReport 내 단일 target_rate)
- "고객 N명" 개념 자체가 없음 — 이미 끝난 경기에 포지션 분산은 무의미

### 평가 지표
- `hit_rate` — 예측 범위 내 실낙찰 포함 여부
- `rate_gap` — 예측 target vs 실낙찰 bid_rate 차이
- `confidence_breakdown` — 신뢰도 구간별 정확도
- 현재 `backtest.py:22-95` `run_batch_batch_backtest()` 가 category 단위 집계 수행

### 불변식 (Invariants)
- ❌ Backtest 경로에서 `simulation.generate_customer_bids()` 호출 금지
- ❌ `num_customers` 파라미터 등장 금지
- ❌ `mock_bids` 테이블에 backtest가 직접 쓰지 않음
- ✅ 필요시 `notice_prediction_cache` 에 단일 예측 결과만 저장

---

## 2. Auto-bid 모드

### 목적 (정정됨)
**"최적 N을 찾는 게 아니다."** N은 시장이 결정한다 — 우리가 통제 못 함.
대신:

> **가능한 모든 N 값에 대해, 그 N일 때의 최적 분포 전략을 미리 갖춰둔다.**

즉 N=1이면 어디 한 점, N=2면 두 점, N=3이면 세 점… 각각 **서로 다른 최적 분포**가 존재할 수 있다고 전제하고 전략 테이블을 구축한다.

### 단위
- Notice 당 **N별 포트폴리오 묶음**
- N 범위는 상한을 고정하지 않음 — 시장 관찰치에 따라 조정 (초기 1~10, 필요시 확장)
- 한 공고 1건 → mock_bids 합계 `Σ N = 1+2+…+Nmax` 행

### 표기 규칙 (스키마)
mock_bids 에 다음 컬럼으로 포트폴리오 식별:
- `n_customers: int` — 이 포트폴리오가 가정한 N (신규 컬럼)
- `customer_idx: int` — 1..N 내 위치 (기존 컬럼 재활용)
- `role: TEXT` — attack / core / explore 등 (현재 `note` 필드에 인코딩, 별도 컬럼으로 승격 고려)
- `simulation_id: TEXT` — 동일 run의 포트폴리오 묶음 식별자 (기존)

같은 notice 안에서 `(simulation_id, n_customers)` 조합이 **하나의 포트폴리오**.

### 평가 지표 (집계 키)
- `(n_customers, role, quantile_bucket)` 단위 `win_rate`
- `(scope, n_customers)` 단위 `win_rate` — 기관+카테고리+계약방식별
- 동일 notice 에서 N별 win_rate 비교 — "N=1 전략 vs N=5 전략" 같은 질문에 답

### 불변식 (Invariants)
- ✅ 모든 auto-bid mock_bid 에 `n_customers` 태그 필수
- ✅ 평가 집계 시 항상 N으로 group by
- ❌ `backtest.py` import 금지 (역방향 의존 차단)

---

## 3. Per-N 전략 테이블 — 핵심 개념

Auto-bid 의 본질은 다음 테이블을 **스코프별로** 유지·갱신하는 것:

```
Scope = (agency, category, contract_method, region)

┌─────┬──────────────────────────────────────┐
│  N  │ 최적 quantile 분포                    │
├─────┼──────────────────────────────────────┤
│  1  │ [0.50]                                │
│  2  │ [0.35, 0.65]                          │
│  3  │ [0.25, 0.50, 0.75]                    │
│  4  │ [0.20, 0.40, 0.60, 0.80]              │
│  …  │ …                                     │
│ 10  │ [0.10, 0.20, …, 0.90, 0.95]           │
└─────┴──────────────────────────────────────┘
```

- 각 행은 **독립적** — N=2의 최적이 [0.35, 0.65]라 해도 N=3이 여기에 한 점 추가된 형태여야 한다는 제약 없음
- 스코프마다 테이블이 다를 수 있음 (기관별 낙찰 분포 차이 반영)
- 표본 부족 스코프는 상위(parent) 스코프 테이블로 shrinkage

### 초기값 산출 (Path B — 데이터 탐색)
- 해당 스코프의 과거 `bid_results` 를 실측 분포로 보고
- 각 N 에 대해 "가상 경쟁자 N-1 명이 실낙찰 분포에서 샘플링됐을 때, 우리 N 명이 어느 quantile 조합에 있어야 최소 1명 낙찰 확률이 최대인가"를 몬테카를로로 추정
- 결과를 per-scope × per-N 분포로 저장 (`strategy_tables` 신규 테이블)

### 온라인 업데이트 (Path C — 운영 피드백)
- auto-bid 평가 결과 유입 시:
  - `(scope, n_customers, quantile_position)` 단위 win_rate 업데이트
  - 주기적으로 (예: 주간 배치) 분포를 미세 조정
  - 조정 폭은 보수적 (EMA α=0.1 수준으로 시작)

---

## 4. 데이터 모델 변경사항

### 신규 컬럼
```sql
ALTER TABLE mock_bids ADD COLUMN n_customers INTEGER DEFAULT 0;
ALTER TABLE mock_bid_evaluations ADD COLUMN n_customers INTEGER DEFAULT 0;
```

### 기존 데이터 처리 (보존 방침)
- 기존 auto-bid mock_bids → `n_customers=5` 로 백필 (`cli.py` 기본값이 5였으므로)
- 기존 평가 결과도 동일 백필

### 신규 테이블 (초안)
```sql
CREATE TABLE strategy_tables (
    scope_key TEXT NOT NULL,           -- (agency, category, contract_method, region) 해시/조합키
    n_customers INTEGER NOT NULL,
    quantiles_json TEXT NOT NULL,      -- [0.35, 0.65] 식 배열
    source TEXT NOT NULL,              -- 'montecarlo' / 'online'
    sample_size INTEGER,
    win_rate_estimate REAL,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (scope_key, n_customers)
);
```

- `source='montecarlo'` 으로 Path B 초기 채움
- `source='online'` 으로 Path C 가 덮어쓰기

---

## 5. 학습 루프 흐름 (B + C 병행)

```
┌──────────────────────────────────────────────┐
│ 1. 과거 bid_results 로 몬테카를로 → 초기 테이블 │  ← Path B (1회/주기적)
└──────────────────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│ 2. 진행 중 공고 1건 들어옴                     │
│    scope 로 strategy_tables 조회              │
│    N=1..Nmax 각각 분포 읽어 mock_bids 생성     │  ← Runtime
└──────────────────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│ 3. 공고 낙찰 결과 유입                         │
│    (n_customers, role, quantile) 단위 판정   │  ← Auto eval
└──────────────────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────┐
│ 4. 주간 배치: 누적 결과로 strategy_tables 갱신 │  ← Path C
└──────────────────────────────────────────────┘
                       ↓
                     (2 로 루프)
```

---

## 6. 구현 단계 제안 (참고)

1. **스키마 준비** — mock_bids / mock_bid_evaluations 에 `n_customers` 추가, 기존 5-bid 는 백필
2. **Path B 초기 탐색** — 몬테카를로로 scope × N → quantile 분포 산출, `strategy_tables` 구축
3. **Auto-bid worker 리팩토링** — notice 당 N=1..Nmax 전부 생성하도록 확장
4. **평가/집계 확장** — N 기준 group by 추가, 대시보드에 N별 win_rate 패널
5. **Path C 업데이트 배치** — 주간 `strategy_tables` 갱신 잡
6. **Floor rate 백필** — 별도 트랙 (아래 7.)

---

## 7. Floor rate (하한율) 백필 — 별도 이슈

현재 DB 상태:
- 전체 공고 135,429 중 floor_rate NULL 72,623 (53.6%), 0 14,632 (10.8%), 유효 48,174 (35.6%)
- 결과 있는 공고 57,707 중 **50,275 (87%)가 하한율 결측**

영향:
- `simulation.py:234` `floor_guard` 가 0이 되어 하한 제약 무력화
- 디스쿼리 판정이 사실상 동작 안 함

백필 소스는 아직 확정 안 됨 — 조달청 OpenAPI 공고 상세 엔드포인트에 포함되는지 확인 필요.

---

## 8. 열린 질문

- Nmax 상한 — 실측 참여자 수 분포 관찰 후 결정
- `strategy_tables` 의 scope_key 정규화 방식 (해시 vs 복합키)
- Path B 몬테카를로 반복 수 / 수렴 기준
- 기존 `_quantile_plan()` 휴리스틱을 Path B 의 웜스타트로 쓸지, 완전 교체할지

---

## 9. Auto-bid 모델 버전 히스토리

Auto-bid 의 "승/패 판정" 방식을 버전으로 구분해 기록한다. 새 모델이 들어오면
여기에 버전을 추가.

### v1 — 과거 커버리지 모델 (초기 구현, 2026-04-20)

**승/패 룰:**
> 과거에 실제 낙찰된 레이트 리스트 중에서, 우리 N 고객 포지션에 하나라도
> 그 낙찰 레이트보다 **아래**가 있었으면 "승"으로 집계.

`simulation.py:309-316` `bisect.bisect_left(customer_rates, competitor_rate) - 1`
가 그대로 이 룰. 여기서 "competitor_rate" 는 **하나의 경쟁자**가 아니라
**과거 한 공고의 실낙찰가** 전체 분포에서 뽑은 샘플.

**해석:** 이건 실제 경매 시뮬레이션이 아니라 **"우리 포지션이 과거 낙찰가
분포를 얼마나 커버하는가"** 를 측정. 같은 공고 내 경쟁자 수 M, 기준가격,
디스쿼리 판정은 전혀 모델링 안 됨.

**Path B v1 실측 결과 (2026-04-20, 12 scopes):**

| N | win_rate 범위 |
|---|---|
| 1 | 0.75~0.78 |
| 2 | 0.92~0.94 |
| 3 | 0.95~0.96 |
| 5 | 0.94~0.96 |
| 10 | 0.94~0.96 |

N=3 이후 완전 plateau. 이건 **coverage 의 한계** — 과거 분포를 3명이 덮어서
더 늘려도 안 늘어남. 실제 경매와는 괴리.

**위치:** `strategy_mc.py` 의 `estimate_win_rate()`, `optimize_quantiles()`,
`build_strategy_tables()`. CLI는 `--model v1` 옵션.

### v2 — 평균가 기반 within-notice 경매 모델 (2026-04-20 추가)

한국 조달청 공공입찰의 주류(> 85%)가 쓰는 **복수예가/낙찰하한제** 를 직접
모델링. 같은 공고 안에서 N 고객 + M 경쟁자가 동시에 입찰하는 시나리오.

#### 실제 규칙

1. 발주처가 기초금액 근처로 랜덤 **15개 예정가격 후보** 생성
2. 입찰자들이 추첨한 **4개 평균 = 예정가격**
3. **기준가격 = 예정가격 × 하한율 (보통 87.745%)**
4. 모든 입찰자의 레이트 중:
   - 기준가격 **초과** → **실격**
   - 기준가격 **이하** 중 **가장 높은 투찰 = 낙찰**
5. 기준가격은 사전에 알 수 없음 → "적당히 하한율 근처에 걸쳐놓기" 가 핵심.
   고객 여러 명이 서로 다른 quantile 에 분산하면 누군가가 기준 바로 아래
   걸릴 확률이 올라감.

#### MC 단순화

- **T** (이 공고의 기준가격%) ← 해당 scope 의 historical `bid_results.bid_rate`
  분포에서 샘플. 과거 낙찰가 ≈ "기준 바로 아래" 이므로 T 의 proxy.
- **M** (경쟁자 수) ← scope 의 historical `bid_results.bidder_count` 분포에서
  샘플. MC 안정성 + 계산량 제어 위해 **max_bidders=50** 로 캡.
- **경쟁자 rate** ← historical bid_rate 분포에서 M 개 부트스트랩
- **우리 rate** ← `[_quantile(sorted_hist, q) for q in our_quantiles]`
- **승/패:** 모든 bidders (우리 N + 저들 M) 중 rate ≤ T 인 것만 유효,
  그중 최대 rate 보유자가 낙찰. 우리 중 하나가 낙찰자면 승.
- **타이 브레이킹:** `best_ours > best_them` 일 때만 승 (동률 = 패로 간주)

#### 계약방식별 처리

| 계약방식 | DB 비중 | 적용 |
|---|---|---|
| 전자입찰 | 85.1% | ✅ v2 공통 모델 |
| 전자시담 | 7.7% | ✅ v2 공통 모델 |
| 직찰 | 3.1% | ✅ v2 공통 모델 |
| 제한경쟁 | 1.5% | ✅ v2 공통 모델 |
| 일반경쟁 | 0.5% | ✅ v2 공통 모델 |
| 지명경쟁 | 0.1% | ✅ v2 공통 모델 |
| 우편/직찰 기타 | 0.7% | ✅ v2 공통 모델 |
| **수의계약** | 1.0% | ❌ **스킵** — 경쟁 없음(직접 협상), "분산 전략" 성립 안 함 |
| 적격심사 | 0.0% | ⚠️ 극소수라 일단 v2 공통 적용 (가격+기술점수 모델은 추후) |

#### 저장 규약

`strategy_tables.source = 'montecarlo_v2'` 로 태깅. v1 row 와 같은 PK
(agency, category, contract_method, region, n_customers) 에 upsert 되므로
**v1 결과를 덮어씀**. 병행하고 싶으면 source 칼럼으로 분리 조회.

**위치:** `strategy_mc.py` 의 `simulate_win_rate_v2()`,
`optimize_quantiles_v2()`, `build_strategy_tables_v2()`. CLI는
`--model v2` (기본값).

---

_마지막 갱신: 2026-04-20_
