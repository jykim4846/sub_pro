#!/usr/bin/env bash
# 집 PC 에서 sub_pro 를 pull 받은 뒤 Claude 가 "확인해봐" 요청에 실행할 스크립트.
#
# 기능:
#   1. sub_pro launchd daily-collect 가 살아 있으면 중지
#   2. Streamlit/API 잔여 프로세스 점검
#   3. Turso DB 가 살아 있으면 정보 출력 (삭제는 안 함)
#   4. 신규 리포(g2b-bid-optimizer) clone 필요 여부 안내
#   5. 모든 결과를 사람 친화 요약으로 출력
#
# idempotent — 여러 번 돌려도 문제 없음.

set -uo pipefail

say() { printf "\n\033[1;34m▸ %s\033[0m\n" "$*"; }
ok()  { printf "  \033[32m✓\033[0m %s\n" "$*"; }
warn(){ printf "  \033[33m!\033[0m %s\n" "$*"; }
err() { printf "  \033[31m✗\033[0m %s\n" "$*"; }

say "Step 1/5 — sub_pro launchd 자동 배치 점검"
if launchctl list 2>/dev/null | grep -q sub_pro; then
    warn "sub_pro launchd 가 살아 있음 → 중지 시도"
    if [ -x scripts/disable-launchd.sh ]; then
        bash scripts/disable-launchd.sh || true
    fi
    rm -f "$HOME/Library/LaunchAgents/com.sub_pro.daily-collect.plist"
    if launchctl list 2>/dev/null | grep -q sub_pro; then
        err "여전히 활성 — 수동으로 'launchctl bootout gui/\$(id -u)/com.sub_pro.daily-collect' 실행 필요"
    else
        ok "launchd 중지 완료"
    fi
else
    ok "launchd 에 sub_pro 항목 없음 (이미 정리됨)"
fi
rm -f "$HOME/Library/LaunchAgents/com.sub_pro.daily-collect.plist" 2>/dev/null

say "Step 2/5 — 잔여 streamlit / python 프로세스 점검"
if pgrep -fa "streamlit run dashboard.py" >/dev/null 2>&1; then
    warn "Streamlit dashboard 가 떠 있음 (읽기 전용이므로 killing 은 선택)"
    pgrep -fal "streamlit run dashboard.py" | head -3
else
    ok "streamlit 미실행"
fi
if pgrep -fa "g2b_bid_reco.cli" >/dev/null 2>&1; then
    warn "sub_pro CLI 실행 중 프로세스 있음:"
    pgrep -fal "g2b_bid_reco.cli" | head -5
else
    ok "sub_pro CLI 실행 중 아님"
fi

say "Step 3/5 — Turso DB 상태"
if command -v turso >/dev/null 2>&1; then
    if turso auth status >/dev/null 2>&1; then
        if turso db list 2>/dev/null | grep -q g2bdb; then
            warn "Turso DB 'g2bdb' 살아 있음 — 당분간 유지 (신규 Neon 안정 확인 후 삭제)"
            turso db show --url g2bdb 2>/dev/null | sed 's/^/    /'
        else
            ok "Turso DB 'g2bdb' 없음"
        fi
    else
        warn "turso 로그인 필요 (turso auth login) — 필수 아님"
    fi
else
    ok "turso CLI 미설치 — 이 PC 에서는 Turso 사용 이력 없음"
fi

say "Step 4/5 — 신규 리포 확인 (g2b-bid-optimizer)"
if [ -d "$HOME/Desktop/g2b-bid-optimizer/.git" ]; then
    ok "신규 리포 이미 clone 되어 있음: $HOME/Desktop/g2b-bid-optimizer"
    (cd "$HOME/Desktop/g2b-bid-optimizer" && git fetch --quiet && LATEST=$(git log origin/main --oneline -1) && echo "    origin/main: $LATEST")
    if [ -f "$HOME/Desktop/g2b-bid-optimizer/HOME_PC_SETUP.md" ]; then
        ok "HOME_PC_SETUP.md 존재 — Claude 가 이 가이드 따라 hub 셋업 가능"
    fi
else
    warn "신규 리포 미존재 — clone 필요"
    printf "    다음 명령으로 받으세요:\n"
    printf "      cd ~/Desktop && git clone https://github.com/jykim4846/g2b-bid-optimizer.git\n"
fi

say "Step 5/5 — 요약"
cat <<EOF

이 sub_pro 리포는 동결 상태입니다. 앞으로의 작업은 모두
  ~/Desktop/g2b-bid-optimizer
에서 진행하세요. Claude 에게 이렇게 말하면 됩니다:

  "g2b-bid-optimizer 의 HOME_PC_SETUP.md 대로 hub 셋업해줘"

그러면 Claude 가:
  - sub_pro launchd 중지 확인 (이 스크립트가 이미 했음)
  - .env 값 3개 요청 (DATA_GO_KR_SERVICE_KEY / DATABASE_URL / AIRFLOW_FERNET_KEY)
  - make install + make up-hub 실행
  - Airflow DAG active 검증
  - Backend /health 확인

자세한 흐름은 아래 문서 참조:
  sub_pro      : MIGRATION_TO_G2B_BID_OPTIMIZER.md
  신규 리포     : HOME_PC_SETUP.md

EOF
