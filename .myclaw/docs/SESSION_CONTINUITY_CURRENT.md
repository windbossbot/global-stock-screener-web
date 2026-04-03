# SESSION_CONTINUITY_CURRENT

Updated: 2026-04-03

## 1. Current Status

- `2026-04-03` 에 공유 스크립트 `C:\Users\KGWPC\workspace\myclaw\scripts\bootstrap_myclaw_nest.py` 로 프로젝트 로컬 `.myclaw` nest 를 생성했다.
- 루트 `AGENTS.md` 가 이제 `.myclaw` 기반 current truth 읽기 순서를 가리킨다.
- 루트 문서는 `README.md`, `DEPLOY_WEB.md` 중심으로 정리했고, 오래된 복구/범위 문서는 제거 대상이다.
- 앱 코드는 Google 제거, CSV 저장, 재무 필터, 우선순위 정렬 기준으로 갱신했다.

## 2. What The Next Session Should Read First

1. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\START_HERE_CURRENT.md`
2. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_TRUTH_CURRENT.md`
3. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state\current_workline_report.md`
4. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\CONTEXT_ENGINEERING_CURRENT.md`
5. 관련 코드 파일 또는 루트 참고 문서

## 3. Confirmed Facts To Carry Forward

1. `app.py` 에는 배당 전용, 배당 주기, `K-ETF` 배당 메타, `pykrx` 기반 흐름이 들어 있다.
2. `app.py` 에는 `ROE`, `EV/EBITDA`, `PER`, `PER/ROE` 구현이 들어 있다.
3. Google Sheets 관련 코드는 제거되었고, 결과는 CSV로 저장된다.
4. Git 메타데이터는 이 폴더 기준 하나로 복구/통일하는 방향이 맞다.

## 4. Non-Destructive Connection Direction

1. 앱 기능 변경은 프로젝트 루트 코드 파일에서 진행한다.
2. current truth, continuity, state, 작업 요약은 `.myclaw` 아래에서만 유지한다.
3. 루트 참고 문서는 최소화하고, 충돌 시 `.myclaw` 기준을 우선한다.
4. 원격 푸시 전에는 이 폴더 Git 기준을 정상화한다.

## 5. Biggest Remaining Risk

1. Render free tier 특성상 cold start 는 남는다.
2. 캐시/가상환경 잔재는 커밋 대상에서 계속 제외해야 한다.
3. Render 쪽 실제 서비스 루트가 repo root 기준으로 다시 읽히는지 배포 후 확인이 필요하다.
