# START_HERE_CURRENT

Updated: 2026-04-03

## Fast Read Order

1. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_TRUTH_CURRENT.md`
2. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\SESSION_CONTINUITY_CURRENT.md`
3. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state\current_workline_report.md`
4. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\CONTEXT_ENGINEERING_CURRENT.md`
5. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\SUPPORT_MEANS_POLICY_CURRENT.md`
6. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\MYCLAW_PROJECT_PROFILE_CURRENT.json`

## Project Snapshot

- 프로젝트 루트 앱은 `app.py` 중심의 Streamlit 기반 KR/US 스크리너입니다.
- 현재 코드에는 배당 전용/배당 주기 처리, `pykrx`, `Yahoo`, `K-ETF` 기반 흐름, `PER / ROE / PER/ROE / EV/EBITDA` 재무 필터가 있습니다.
- Google Sheets 업로드는 제거되었고, 결과는 `_cache/exports` 에 CSV로 저장됩니다.
- `Yahoo` 호출은 세션 재사용과 로컬 스냅샷 캐시를 우선 사용하도록 정리했습니다.
- 현재 운영 기준은 로컬 실행 전용이며, 루트 참고 문서는 `README.md` 중심으로 유지합니다.
- 로컬 실행 스크립트는 `run_web.ps1`, `run_web_keepalive.ps1`, `stop_web.ps1` 기준으로 사용합니다.
- Git 기준은 이 폴더 하나로 복구/통일하는 것이 원칙입니다.

## Pull Next Only If Needed

1. `C:\Users\KGWPC\workspace\dividend-screener-v3\README.md`
2. `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_BOOTSTRAP_CURRENT.md`
3. `C:\Users\KGWPC\workspace\myclaw\docs\PROJECT_NEST_BOOTSTRAP_CURRENT.md`

## Rule

Use this project-local nest as the default operations home for this project.
Do not treat `C:\Users\KGWPC\workspace\myclaw` as the active work root unless the task explicitly switches into engine admin maintenance.
Do not spread new operations docs into the project root unless the project explicitly needs repo-level ops files.
When `.myclaw` docs and older root docs conflict, prefer `.myclaw`.
App feature cleanup such as Google removal, valuation filters, and speed/stability tuning should stay aligned with `app.py`.
