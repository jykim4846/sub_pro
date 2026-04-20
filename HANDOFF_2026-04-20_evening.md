# 2026-04-20 저녁 핸드오프 — 집 PC로 이어가기

> 이 파일은 **다음 세션(집 PC)** 이 흐름 끊기지 않고 이어갈 수 있도록 쓴다.
> 출근 PC에서 이번 세션 작업을 마치면서 중단 지점과 다음 할 일을 정리.
> **맨 위부터 순서대로** 읽으면 된다.

---

## 0. 집 PC 최초 셋업 체크리스트

```bash
# 1) 프로젝트 받기 (이미 clone돼 있으면 pull만)
cd ~/Desktop
git clone <origin-url> sub_pro   # 기존이면 생략
cd sub_pro
git pull

# 2) 이번 세션에서 추가된 커밋 확인 (아래 §1 참고)
git log --oneline -12

# 3) Turso CLI + 인증
brew tap libsql/sqld
brew install tursodatabase/tap/turso
turso auth login   # 같은 계정 (jykim4846@gmail.com 연결 GitHub)

# 4) .env 복사 — 회사 PC의 .env 내용을 파일로 옮김
cat > .env <<'EOF'
DATA_GO_KR_SERVICE_KEY=<회사 PC .env 의 값 그대로>
TURSO_DATABASE_URL=libsql://g2bdb-jykim4846.aws-ap-northeast-1.turso.io
TURSO_AUTH_TOKEN=<회사 PC .env 의 값 그대로>
EOF

# 5) 최신 DB 스냅샷 pull
bash scripts/turso-pull.sh   # data/bids.db 로 받아짐 (약 48MB)

# 6) 동작 확인
python3 -m pytest tests/   # 68 passed 나와야 함
sqlite3 data/bids.db "SELECT COUNT(*) FROM strategy_tables;"  # 100 나와야 함

# 7) (선택) 집 PC 에서도 daily batch 돌리려면 launchd 등록
PROJECT_DIR=$HOME/Desktop/sub_pro
sed "s#__PROJECT_DIR__#${PROJECT_DIR}#g" \
    scripts/launchd-daily-collect.plist.template \
    > ~/Library/LaunchAgents/com.sub_pro.daily-collect.plist
launchctl load ~/Library/LaunchAgents/com.sub_pro.daily-collect.plist
# 회사 PC 는 10:00 등록돼 있음. 집 PC 도 같은 시각이면 충돌 — 다른 시간으로
# 바꾸거나 로드 안 하는 쪽 권장.
```

**주의:** 두 PC에서 `turso-push.sh` 를 동시에 실행하면 한쪽이 덮어씀 (last
push wins). `TURSO_SYNC.md` 의 "⚠️ 동시 작업 금지" 섹션 숙지.

---

## 1. 이번 세션에서 쌓은 커밋 (8개, 전부 local `main`)

```
51fa21c  Add v2 within-notice 평균가 auction MC (default model)
97125c1  Add build-strategy-tables CLI for Path B monte carlo
f8b10d2  Add monte-carlo initializer for strategy_tables (Path B)
d80aeae  Add strategy_tables for per-N auto-bid portfolio strategies
1324c50  Push enriched DB to Turso at end of enrich-all-stubs
82624e4  Move daily batch schedule from 07:00 to 10:00 KST
a3a9adb  Wire Turso pull/push into daily batch
6b11bb8  Add Turso cloud sync for multi-PC DB sharing
2ea3361  Split backtest vs auto-bid modes, add n_customers schema
```

**아직 origin 에 push 안 했음.** 집 PC 에서 받으려면 회사 PC 에서 먼저
`git push origin main` 하거나, 또는 집 PC 에서 pull 뒤 회사 로컬의 커밋을
전송하는 방법을 합의해야 함.

(주의: 커밋 코어 로직은 Turso DB 로도 전파되지 않는다. Turso 는 DB 데이터만.
**git push 는 반드시 필요**.)

---

## 2. 지금까지 설계/구현된 것 — 세션 흐름 요약

### 2-1. 두 모드 구분 (`MODES.md`)
- **Backtest** = 과거 공고 1건당 예측 1개. "N명 고객" 개념 없음.
- **Auto-bid** = 진행 중 공고에 **N별 포트폴리오 묶음** 생성. N 은 시장이
  결정하므로 가능한 모든 N 값에 대해 **최적 분포 전략을 미리 갖춰둔다**.
  중요 전제.

### 2-2. 스키마
- `mock_bids.n_customers`, `mock_bid_evaluations.n_customers` 추가 (기존
  5-bid 데이터는 `MAX(customer_idx) per simulation_id` 로 백필)
- 신규 테이블 `strategy_tables (agency, category, contract_method, region,
  n_customers, quantiles_json, source, sample_size, win_rate_estimate)`

### 2-3. Path B 몬테카를로 — `g2b_bid_reco/strategy_mc.py`
- **v1 (coverage):** 과거 낙찰가 분포를 우리 포지션이 커버하는가. 실제
  경매 아님. N=3 이후 플래토 (당연).
- **v2 (within-notice 평균가, 기본값):** 한 공고 내 N 고객 + M 경쟁자
  동시 입찰 시뮬. 복수예가/낙찰하한제 룰. **수의계약은 스킵**. 자세한 건
  `MODES.md §9`.

### 2-4. 실DB 실행 결과 (v2, `strategy_tables` 에 저장됨)
```
scope 10개, N=1..10, 총 100 rows
construction/전자입찰: N=1 4.6%  → N=10 29.8%  (어려움, 입찰자 많음)
goods/전자시담:       N=1 43.1% → N=10 87.7%  (쉬움)
```
N 에 따라 **단조 증가** — v1 의 플래토 문제 해결됨. 사용자 의도 부합.

### 2-5. Turso 동기화
- DB 는 `libsql://g2bdb-jykim4846.aws-ap-northeast-1.turso.io` 에 있음
- Group-level 토큰이라 push (destroy+recreate) 후에도 유효
- `scripts/turso-{push,pull}.sh` 에 안전장치 (`data/backups/`)
- `daily-api-collect.sh` 와 `enrich-all-stubs.sh` 끝에 auto-push 훅

### 2-6. Launchd
- 회사 PC 에 `com.sub_pro.daily-collect` 등록됨 (10:00 KST 매일)
- Pull → collect/evaluate/auto-bid/snapshot → Push 의 전체 루프

---

## 3. 다음 세션에서 할 일 (우선순위 순)

### A. Auto-bid worker 를 strategy_tables 참조하도록 개조 (MODES.md C3)
지금은 `_quantile_plan()` 휴리스틱이 N 별 분포를 계산. 대신 DB의
`strategy_tables` 에서 `(scope, N)` 조회해 `quantiles_json` 을 쓰도록 교체.

수정 대상:
- `g2b_bid_reco/cli.py:624~` `_auto_bid_scope_worker` / `generate_customer_bids` 호출 부
- `g2b_bid_reco/simulation.py` `_quantile_plan` 을 strategy_tables lookup 으로 분기

포인트:
- Scope (agency, category, contract_method, region) 매치되는 row 없으면
  휴리스틱으로 fallback
- N=1..Nmax (Nmax 는 strategy_tables 에 있는 최대값) 전부 생성
- 각 mock_bid 에 `n_customers` 컬럼 반드시 채우기

### B. 대시보드 N별 win_rate 패널 (MODES.md C4)
`dashboard.py` 에 새 탭/섹션 추가 — strategy_tables 조회해서 scope×N
히트맵 또는 바 차트. `mock_bid_evaluations` 에 데이터 쌓이면 실측 win_rate 를
strategy_tables 의 추정치와 비교하는 패널도.

### C. Path C — 주간 업데이트 배치 (MODES.md C5)
`mock_bid_evaluations` 가 충분히 쌓이면 `(scope, n_customers, role)` 실측
win_rate 로 strategy_tables 를 EMA 업데이트. 이건 A 가 동작해서 평가
데이터가 쌓인 뒤에 의미 있음. 주단위 cron 또는 daily batch 부록.

### D. 보류 중
- **Stub enrichment 재개**: 2차 실행이 stub count 안 줄이고 멈춘 상태에서
  종료했음 (SLEEP_SEC=0.3 이 rate-limit 유발 의심). 필요하면 `SLEEP_SEC=1.0`
  으로 돌리되 `--verbose --batch-limit 100` 단계별 확인부터.
- **Floor rate 백필 후속**: enrich 끝나면 floor_rate 분포 재측정. 현재도
  48,731 건 valid 해서 MC 돌리는 데는 충분.
- **적격심사/전자시담** 세부 룰이 v2 공통과 미묘하게 다를 수 있으므로,
  운영 결과로 검증 후 별도 모델 분기 고려.
- **origin 에 git push 아직 안 함** — 합의 후 진행.

### E. 아직 열려 있는 설계 질문
- Nmax 최종값 (지금은 10, 필요시 확장)
- 적격심사 별도 모델 필요 여부
- Path C 업데이트 EMA α (초안 0.1)
- 대시보드에서 "전략 전환 감지" 시각화 방향

---

## 4. 집 Claude Code 가 읽어야 할 파일 (순서)

1. **`MODES.md`** — 두 모드 구분 + v1/v2 model 히스토리. 전체 맥락.
2. **`HANDOFF_2026-04-20_evening.md`** (이 파일) — 세션 연결점.
3. **`HANDOVER_2026-04-20.md`** — 이전 세션(새벽/아침)의 운영 상태.
4. **`TURSO_SYNC.md`** — sync 워크플로우.
5. 필요 시 `g2b_bid_reco/strategy_mc.py` 본체.

---

## 5. 자주 쓸 명령

```bash
# 현재 strategy_tables 내용 스폿체크
sqlite3 data/bids.db "SELECT category, contract_method, n_customers,
  printf('%.3f', win_rate_estimate) AS wr, sample_size
  FROM strategy_tables WHERE source='montecarlo_v2' AND n_customers IN (1,3,5,10)
  ORDER BY sample_size DESC, category, contract_method, n_customers;"

# v2 재실행 (DB 스냅샷 새로 떨어지면)
python3 -m g2b_bid_reco.cli build-strategy-tables --db-path data/bids.db \
    --model v2 --n-max 10 --n-trials 2000 --min-samples 30

# 테스트
python3 -m pytest tests/ -v

# 대시보드
streamlit run dashboard.py

# 수동 daily batch 트리거
launchctl kickstart gui/$(id -u)/com.sub_pro.daily-collect
# 또는 직접:
bash scripts/daily-api-collect.sh
```

---

## 6. 메모리 상태 (Claude Code auto-memory)

- `~/.claude-personal/projects/-Users-jongyeon-kim-Desktop-sub-pro/memory/`
- 주요 엔트리:
  - `two_modes_backtest_vs_autobid.md` — 두 모드 혼동 방지 (project)
  - `claude_config_dir.md` — CLAUDE_CONFIG_DIR 환경변수 경고 (reference)
  - `feedback_preannounce_user_actions.md` — 사용자 액션 일괄 공지 (feedback)

집 PC 에서도 같은 Claude 계정이면 동일 메모리가 읽힘 (자동). 경로가
`~/.claude-personal` 로 다른지 체크만.

---

_마지막 갱신: 2026-04-20 저녁 — 회사 PC 에서 퇴근 전 핸드오프_
