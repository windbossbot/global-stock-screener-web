# PROJECT_TRUTH_CURRENT

Updated: 2026-04-03

## 1. Project Identity

- `dividend-screener-v3` 는 `app.py` 중심의 Streamlit 웹 스크리너 프로젝트다.
- 이번 adoption 작업의 목적은 이 프로젝트를 공유 엔진 `myclaw` 에 직접 섞지 않고, 로컬 `.myclaw` 아래에 current truth 와 state 를 고정하는 것이다.

## 2. Current Code Facts Confirmed During Adoption

1. `app.py` 와 `requirements.txt` 에서 `Google Sheets` 관련 코드와 패키지는 제거되었다.
2. `app.py` 에는 `dividend_only`, `dividend_cycles`, `K-ETF` 배당 메타, `pykrx` 기반 배당 보강 경로가 들어 있다.
3. `app.py` 에는 `PER`, `ROE`, `PER/ROE`, `EV/EBITDA` 재무 필터와 우선순위 정렬 로직이 들어 있다.
4. 결과 저장은 `_cache/exports/latest_screener.csv` 와 타임스탬프 CSV를 기준으로 한다.
5. Git 기준은 이 폴더의 정상 `.git` 디렉터리 하나이며, 캐시/가상환경은 추적 대상이 아니다.
6. `Yahoo` 재무/배당 조회는 HTTP 세션 재사용과 로컬 CSV 스냅샷 캐시를 함께 사용한다.
7. 현재 운영 모드는 외부 무료 호스팅이 아니라 로컬 실행 전용이다.

## 3. Main Paths

1. project root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3`
2. project-local nest root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw`
3. local docs root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs`
4. local state root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state`
5. local runs root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\runs`
6. local smoke root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\smoke`
7. local output root:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\output`
9. shared engine root:
   - `C:\Users\KGWPC\workspace\myclaw`

## 4. Main Code And Reference Files

1. runtime app:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\app.py`
2. runtime config:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\requirements.txt`
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.streamlit\config.toml`
3. existing root reference docs:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\README.md`

## 5. Docs Ownership

1. default current truth:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs`
2. default live state:
   - `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state`
3. root docs are still useful as reference, but they are no longer the default truth root
4. `README.md` 는 현재 로컬 실행 경로와 CSV/재무 필터 기준으로 갱신되었다
5. 오래된 복구/범위 문서는 제거하고 current truth 만 유지한다
6. 공유 엔진 `C:\Users\KGWPC\workspace\myclaw` 는 재사용 script/template/helper 원천이지, 이 프로젝트의 기본 truth 루트가 아니다

## 6. Practical Rule

1. use this nest as the local operations home first
2. escalate into the shared engine docs only when changing the engine itself or when a reusable core helper is needed
3. app 기능 수정은 프로젝트 루트 코드 파일에서 하고, continuity 와 current truth 유지 관리는 `.myclaw` 에서 한다
4. avoid creating duplicate current docs in both the project root and this nest without a clear ownership split
