# SESSION_CONTINUITY_CURRENT

Updated: 2026-04-03

## 1. Current Status

- `2026-04-03` 에 공유 스크립트 `C:\Users\KGWPC\workspace\myclaw\scripts\bootstrap_myclaw_nest.py` 로 프로젝트 로컬 `.myclaw` nest 를 생성했다.
- 루트 `AGENTS.md` 가 이제 `.myclaw` 기반 current truth 읽기 순서를 가리킨다.
- 루트 문서는 `README.md` 중심으로 정리했고, 오래된 복구/범위/배포 문서는 제거했다.
- 앱 코드는 Google 제거, CSV 저장, 재무 필터, 우선순위 정렬, HTTP 세션 재사용, 로컬 스냅샷 캐시 기준으로 갱신했다.
- 운영 기준은 외부 무료 호스팅이 아니라 로컬 실행으로 확정했다.
- 로컬 실행 스크립트는 숨김 background worker 기반의 `run_web.ps1`, `run_web_keepalive.ps1`, `stop_web.ps1` 기준으로 정리했다.
- 사용자 업로드 CSV는 루트가 아니라 `user_uploads` 아래로 옮겨 관리한다.

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
4. 재무/배당 스냅샷은 `_cache` 아래 로컬 CSV 캐시를 우선 사용한다.
5. Git 메타데이터는 이 폴더 기준 하나로 복구/통일하는 방향이 맞다.
6. `US 월배당` 검색은 전용 메타 소스가 약해 탐색 효율이 낮지만, 현재는 시장별 선필터 조정으로 미국 종목이 초기에 통째로 탈락하지 않도록 수정했다.

## 4. Non-Destructive Connection Direction

1. 앱 기능 변경은 프로젝트 루트 코드 파일에서 진행한다.
2. current truth, continuity, state, 작업 요약은 `.myclaw` 아래에서만 유지한다.
3. 루트 참고 문서는 최소화하고, 충돌 시 `.myclaw` 기준을 우선한다.
4. 원격 푸시 전에는 이 폴더 Git 기준을 정상화한다.

## 5. Biggest Remaining Risk

1. 캐시/가상환경 잔재는 커밋 대상에서 계속 제외해야 한다.
2. 장시간 스캔 중 외부 데이터 소스 응답 저하나 429는 계속 대비해야 한다.
