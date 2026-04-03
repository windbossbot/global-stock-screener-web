기본적으로 이 프로젝트의 작업 루트는 `C:\Users\KGWPC\workspace\dividend-screener-v3` 이고, 운영 current truth/state 홈은 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw` 입니다.

핵심 규칙:
- 먼저 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\START_HERE_CURRENT.md`, `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\PROJECT_TRUTH_CURRENT.md`, `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\CONTEXT_ENGINEERING_CURRENT.md`, `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\SESSION_CONTINUITY_CURRENT.md` 를 읽고 진행합니다.
- 필요한 경우에만 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state\current_workline_report.md` 와 관련 state json 을 추가로 읽습니다.
- 새 current docs 는 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs`, 새 live state 는 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\state` 에만 기록합니다.
- 일반 작업에서는 프로젝트 루트에 운영 문서를 흩뿌리지 말고 `.myclaw` 안에서 관리합니다.
- 이 프로젝트의 앱 본체는 `app.py` 중심의 Streamlit 스크리너입니다. 앱 기능 수정은 루트 코드 파일에서 하고, 운영 current truth 갱신은 `.myclaw` 안에서만 합니다.
- 공유 엔진 `C:\Users\KGWPC\workspace\myclaw` 는 admin-only 유지보수 표면입니다. 일반 프로젝트 작업에서는 기본 참조/쓰기 루트로 쓰지 않습니다.
- 공유 엔진은 재사용 script/template/helper 가 실제로 필요할 때만 제한적으로 호출합니다.
- 엔진 자체를 바꾸는 작업이 아니라면 `C:\Users\KGWPC\workspace\myclaw` 아래 파일을 수정하지 않습니다.
- 루트의 참고 문서는 기본적으로 `README.md` 만 유지합니다. 그 외 오래된 복구/범위/배포 문서는 필요 시 정리하거나 삭제합니다.
- Git 기준은 이 폴더의 정상 `.git` 디렉터리 하나로 통일합니다. worktree 잔재나 다른 복제본을 현재 truth 로 쓰지 않습니다.
- context engineering 기본 규칙은 `write / select / compress / isolate` 이고, 현재 단계에 꼭 필요한 최소 context package만 유지합니다.
- 스킬, 스크립트, 에이전트, 자동화, 템플릿 등을 실제로 사용했을 때는 가능하면 `[judge_request.py 스크립트]`, `[myclaw-admin-mode 스킬]`, `[worker 에이전트]` 같은 대괄호 태그로 짧게 표시합니다.
- 이 프로젝트 초반 약 5~10개의 사용자 질문 구간에서는 보조수단 추천을 조금 더 적극적으로 합니다. 다만 짧고 비차단적으로 추천합니다.
- 여러 프로젝트에 재사용될 만한 보조수단이 보이면, 공유 엔진 `C:\Users\KGWPC\workspace\myclaw` 로 올릴 수 있다고 가볍게 추천합니다. 실제 공유 엔진 수정은 엔진 관리자모드일 때만 합니다.
- 사용자가 로컬 요청문 파일 경로를 주고 읽으라고 하면, 그 파일을 먼저 읽고 그 안의 요청문을 현재 bounded order의 입력으로 삼습니다. 다만 이것은 로컬 파일 prompt-library 관례이지 플랫폼 차원의 자동 import 보장은 아닙니다.

bootstrap 이후 기본 진행:
1. 최소 bootstrap 저장 상태를 유지합니다.
2. 사용자의 목표와 범위를 짧게 확인합니다.
3. 이후 참조, 상태 갱신, 진행 보고는 이 프로젝트의 `.myclaw` 기준으로 이어갑니다.
