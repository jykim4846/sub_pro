# ⚠️ 이 프로젝트는 동결(Frozen)되었습니다

**2026-04-22 부로 `sub_pro` 는 새 프로젝트 [`g2b-bid-optimizer`](https://github.com/jykim4846/g2b-bid-optimizer) 로 대체되었습니다.**

- 신규 개발: `~/Desktop/g2b-bid-optimizer` 에서만 진행
- 이 디렉터리는 **백업/참고용으로 유지**. 코드/배치 수정 금지.
- 3~4주 후 안정성 확인되면 archive 예정.

---

## Claude 가 이 프로젝트를 pull 받아 작업하게 되면 할 일

sub_pro 를 "집 PC에서 그대로 쓰려는 시도"가 있을 때 Claude 는 다음을 **자동으로** 수행해야 합니다:

### 1. 동결 상태 먼저 알림

사용자에게 이 MIGRATION 문서 링크를 보여주고, 새 프로젝트(`g2b-bid-optimizer`) 로의 이전 상태를 설명.

### 2. sub_pro 의 자동화 중지

launchd daily 배치가 살아 있으면 **반드시 끄기**. 새 프로젝트와 중복 수집되어 API 쿼터 낭비.

```bash
# 상태 확인
launchctl list | grep sub_pro

# 살아 있으면 내리기
if [ -x scripts/disable-launchd.sh ]; then
    bash scripts/disable-launchd.sh
fi
rm -f ~/Library/LaunchAgents/com.sub_pro.daily-collect.plist

# 재확인
launchctl list | grep sub_pro   # 출력 없어야 정상
```

### 3. Turso push/pull 자동화 중지

`daily-api-collect.sh` 와 `enrich-all-stubs.sh` 가 끝날 때 `turso-push.sh` 를 호출. launchd 를 껐으니 자동 호출은 사라지지만, **사용자가 수동으로 이 스크립트들을 실행하지 않도록** 안내:

```bash
# 실행하면 안 되는 스크립트 (신규 작업용 Neon 에 반영 안 됨)
#   scripts/daily-api-collect.sh
#   scripts/enrich-all-stubs.sh
#   scripts/turso-push.sh
#   scripts/turso-pull.sh
```

이것들은 그대로 두지만 **건드리지 말 것**. 대신 신규 작업은 `g2b-bid-optimizer` 의 Airflow DAG 로.

### 4. Streamlit 대시보드 참고용 실행은 OK

읽기 전용 대시보드는 기존 데이터를 보는 용도로 계속 사용 가능:

```bash
cd ~/Desktop/sub_pro
streamlit run dashboard.py
# localhost:8501 — 과거 mock_bid_evaluations / strategy_tables 등 확인
```

단, **데이터 수정은 하지 말 것.** 신규 mock_bid 생성, strategy_tables 재계산 등은 모두 `g2b-bid-optimizer` 에서.

### 5. 사용자를 `g2b-bid-optimizer` 로 안내

사용자가 "새 수집 돌려줘", "Model F 갱신", "auto-bid" 같은 요청을 하면, sub_pro 가 아닌 **`~/Desktop/g2b-bid-optimizer` 에서 작업**하도록 안내.

- "집 PC 에서 처음이면" → `git clone https://github.com/jykim4846/g2b-bid-optimizer.git` 후 해당 리포의 `HOME_PC_SETUP.md` 참조.
- "이미 clone 돼 있으면" → `cd ~/Desktop/g2b-bid-optimizer && git pull`.

---

## 두 프로젝트 동거 기간 운영 원칙

| 항목 | sub_pro (여기) | g2b-bid-optimizer (신규) |
|---|---|---|
| launchd daily | ❌ 중지 | ❌ (Airflow 로 이전) |
| Airflow | 없음 | ✅ daily_collection / weekly_retrain |
| 데이터 DB | Turso `g2bdb` (읽기 전용) | Neon Postgres (주력) |
| 코드 수정 | ❌ 금지 | ✅ 여기서만 |
| Streamlit 대시보드 | ✅ 참고용 OK | — (Next.js 로 대체 예정) |
| Model F 생성 | ❌ | ✅ Airflow weekly_retrain |
| 법적 설계 | `docs/legal-design.md` (기반) | 동일 문서 이관됨 |

---

## 이관 현황 체크리스트

| 자산 | 이관 상태 |
|---|---|
| `bid_notices` / `bid_results` / `contracts` | ✅ Neon 에 seed 완료 (135k/57k/27k) |
| `mock_bid_evaluations` | ⏸️ 보존, 신규는 Airflow eval job 으로 대체 |
| `strategy_tables` | ⏸️ 보존, Model F 로 교체 |
| `agency_parent_mapping` | 🔜 Phase 2 feature engineering 에 이식 예정 |
| `sub_pro/data/bids.db` | ✅ 백업으로 유지 (`data/backups/` 에 최신 스냅샷) |
| Turso `g2bdb` | ⏸️ 살려둠 (추후 archive) |

---

## 언제 sub_pro 를 archive 할지

다음 조건을 모두 만족하면:

- [ ] `g2b-bid-optimizer` Airflow 가 2주 이상 에러 없이 돌았다
- [ ] Neon 데이터량이 sub_pro 대비 10% 이상 많아졌다
- [ ] Model F counterfactual 백테스트가 한 번 이상 성공적으로 돌았다
- [ ] Streamlit 대시보드가 더 이상 필요 없어졌다

```bash
# Turso DB 삭제
turso db destroy g2bdb

# sub_pro 디렉터리 tar 백업 후 삭제
tar -czf ~/sub_pro_archive_$(date +%Y%m%d).tar.gz ~/Desktop/sub_pro
rm -rf ~/Desktop/sub_pro

# 로컬 LaunchAgents 잔해 확인
ls ~/Library/LaunchAgents/*sub_pro* 2>/dev/null   # 없어야
```

---

## 관련 문서

- **신규 프로젝트 repo**: https://github.com/jykim4846/g2b-bid-optimizer
- **집 PC 셋업 가이드**: 신규 repo 의 `HOME_PC_SETUP.md`
- **PC 역할 구분**: 신규 repo 의 `docs/PC_ROLES.md`
- **왜 새로 설계했는가**: 신규 repo 의 `docs/MIGRATION_FROM_SUB_PRO.md`

---

_작성일: 2026-04-22_
