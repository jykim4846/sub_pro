# Instructions for Claude — sub_pro (FROZEN)

> 이 프로젝트는 **2026-04-22 부로 동결**되었습니다. 신규 개발은 모두
> [`g2b-bid-optimizer`](https://github.com/jykim4846/g2b-bid-optimizer) 로 이전.
> 이 파일은 리포 루트에 있고 Claude Code 가 자동 로드하므로, 사용자가
> 어떤 작업을 요청해도 **이 지시를 먼저 따릅니다**.

## 사용자가 이 리포에서 무언가를 시키면 — 자동 대응 루틴

### 1. "확인해봐" / "세팅해봐" / "돌려봐" / pull 받은 직후

즉시 아래 스크립트 실행:

```bash
cd ~/Desktop/sub_pro
bash scripts/freeze-sub-pro.sh
```

이 스크립트가 자동으로:
- launchd daily-collect 중지 (살아 있으면)
- streamlit / CLI 잔여 프로세스 점검
- Turso DB 상태 보고
- 신규 리포 clone 여부 확인
- 다음에 사용자가 할 일 요약 출력

출력 내용을 사용자에게 그대로 전달하세요.

### 2. 신규 리포가 없으면 clone 유도

`~/Desktop/g2b-bid-optimizer/.git` 이 없으면:

```bash
cd ~/Desktop
git clone https://github.com/jykim4846/g2b-bid-optimizer.git
```

사용자 확인 없이 바로 실행해도 OK (공개 github 받기만).

### 3. 신규 리포 셋업 안내

clone 이후에는 반드시 **신규 리포로 이동해서 그쪽의 `HOME_PC_SETUP.md`**
를 따르도록 안내:

```bash
cd ~/Desktop/g2b-bid-optimizer
# HOME_PC_SETUP.md 대로 hub 셋업 진행
```

## 절대 하지 말 것

- sub_pro 의 `scripts/daily-api-collect.sh` / `enrich-all-stubs.sh` /
  `turso-push.sh` / `turso-pull.sh` **수동 실행 금지**
- sub_pro 의 `data/bids.db` **수정 금지** (백업/읽기 전용)
- sub_pro 의 코드 **수정 금지** — 모든 신규 기능은 g2b-bid-optimizer 에서
- Turso `g2bdb` DB **삭제하지 말 것** — 2~3주 병행 비교 기간 필요
- 이 sub_pro 디렉터리 **삭제하지 말 것**

## 허용되는 작업

- Streamlit dashboard 읽기용 실행: `streamlit run dashboard.py`
- sqlite3 로 `data/bids.db` 읽기 전용 조회
- 코드 **읽기** 해서 g2b-bid-optimizer 로 로직 참고/이식

## 사용자가 구 기능을 요청하면 응답 패턴

| 사용자 요청 | 응답 |
|---|---|
| "auto-bid 돌려줘" | g2b-bid-optimizer Airflow DAG 로 안내 |
| "데이터 수집해줘" | 신규 리포 daily_collection 으로 안내 |
| "Model F 튜닝" | `g2b-bid-optimizer/docs/model-f-tuning.md` |
| "대시보드 고쳐줘" | sub_pro 의 streamlit 은 동결. 신규 리포 frontend(Next.js) 로 안내 |
| "strategy_tables 갱신" | g2b-bid-optimizer weekly_retrain DAG |
| "백테스트 돌려줘" | `g2b-bid-optimizer/scripts/run_counterfactual.py` |
| "공고 분석 API" | `g2b-bid-optimizer/backend` |

## 참조 문서 (이 리포 내부)

- `MIGRATION_TO_G2B_BID_OPTIMIZER.md` — 왜, 언제, 어떻게 이전했는지 전체 기록
- `scripts/freeze-sub-pro.sh` — 자동 정리 스크립트
- `README.md` — 상단에 동결 배너
- 구 운영 기록: `HANDOVER_2026-04-20.md`, `HANDOFF_2026-04-20_evening.md`,
  `HANDOFF_2026-04-21.md`, `MODES.md`, `TURSO_SYNC.md` — **역사 기록일 뿐,
  지금 따르지 말 것**

## 혼동 방지

Claude Code 의 `auto_memory` 에도 이 프로젝트 역사가 쌓여 있지만 모두
**~2026-04-21 시점** 의 스냅샷입니다. 현재 진실 소스는:

1. **이 CLAUDE.md** (가장 최신 지시)
2. `MIGRATION_TO_G2B_BID_OPTIMIZER.md`
3. 신규 리포의 모든 문서 (`~/Desktop/g2b-bid-optimizer/docs/*`)

위 3개와 memory 가 충돌하면 **위 3개를 따르세요**.
