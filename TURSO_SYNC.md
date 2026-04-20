# Turso DB Sync Guide

두 대 PC에서 `data/bids.db` 를 공유하는 절차. Turso 무료 티어 (9GB) 사용.

## 사전 준비 (양 PC 공통)

```bash
brew tap libsql/sqld
brew install tursodatabase/tap/turso
turso auth login     # 기존 계정이면 login, 신규면 signup
```

프로젝트 루트 `.env` 에 아래 두 줄 있어야 함 (PC1에서 발급한 값을 PC2에 복사):

```
TURSO_DATABASE_URL=libsql://g2bdb-<user>.aws-ap-northeast-1.turso.io
TURSO_AUTH_TOKEN=<group token>
```

`.env` 는 `.gitignore` 에 포함되어 있음.

## 일상 워크플로우

### PC1 작업 시작 → 작업 → 종료

```bash
bash scripts/turso-pull.sh   # 최신 상태 받아오기
# ... 작업 (daily batch, dashboard, CLI 등) ...
bash scripts/turso-push.sh   # 결과 업로드
```

### PC2 에서 작업할 때

PC1이 push 끝낸 뒤에만 pull + 작업 시작.

```bash
bash scripts/turso-pull.sh
# ... 작업 ...
bash scripts/turso-push.sh
```

## ⚠️ 동시 작업 금지

현재 sync 모델은 **"last push wins"** — 전체 DB 덮어쓰기 방식.

**같은 시간에 양 PC에서 write 하면 한쪽 작업이 통째로 사라집니다.**

규칙:
- PC1에서 작업 끝나고 push 완료 → PC2에서 pull 후 시작
- launchd daily batch (PC1 매일 07:00) 직전/직후에는 PC2에서 push 금지

## 안전장치

- `turso-pull.sh` 실행 시 기존 `data/bids.db` 는 `data/backups/bids.pre-pull.*.db` 로 복사
- `turso-push.sh` 실행 시도 마찬가지로 `bids.pre-push.*.db` 로 복사
- 문제 발생 시 `cp data/backups/bids.pre-pull.<timestamp>.db data/bids.db` 로 복원

## 스냅샷 일관성

`turso-push.sh` 는 SQLite online backup API (`.backup` 커맨드) 로 스냅샷을
뜬 뒤 업로드하므로, 동시에 다른 프로세스(예: daily batch, enrich-stubs)가
DB를 쓰고 있어도 **업로드되는 스냅샷은 내부적으로 일관**. 단 "그 시점 직후
들어온 쓰기"는 포함 안 되니 write-heavy 작업 끝난 뒤 push 권장.

## 토큰 관리

- `.env` 의 토큰은 **group-level token** (DB 재생성되어도 유효)
- 만료 없음 (`--expiration none`)
- 유출됐다면:
  ```bash
  turso group tokens invalidate default
  turso group tokens create default --expiration none
  ```
  후 `.env` 에 새 토큰 복사

## 제약 & 향후

- **Python 3.9 + Apple Silicon 제약으로 libsql embedded replica 미사용**.
  투명한 백그라운드 sync (Turso 공식 추천 패턴)는 Python 3.10+ 로 업그레이드 후 고려.
- 지금은 수동 push/pull 로 운영. Daily batch 종료 hook 으로 `turso-push.sh` 자동 호출 가능.
