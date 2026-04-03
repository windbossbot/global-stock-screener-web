> 세션/운영 current truth 시작점은 `C:\Users\KGWPC\workspace\dividend-screener-v3\.myclaw\docs\START_HERE_CURRENT.md` 입니다.

# Global Stock Screener (Local)

KR/US 주식 스크리너 로컬 앱입니다. 현재 운영 기준은 `로컬 실행 전용`입니다.

## 핵심 기능
- 시장 모드: `국장(KR) / 미장(US) / 통합(KR+US)`
- 조건 필터: 가격 범위, ETF 모드, 배당 여부/주기, 일/주/월 이동평균, 근접도, 기본 조건
- 재무 필터: `PER`, `ROE`, `PER/ROE`, `EV/EBITDA`
- 점수화: 추세 + 배당 + 재무 + 섹터 강도 + 유동성 우선순위
- 결과 저장: 서버 CSV 자동 저장 + CSV 다운로드
- 진단 패널: 단계별 시간/커버율/보강 건수

## 로컬 실행
```powershell
cd C:\Users\KGWPC\workspace\dividend-screener-v3
powershell -ExecutionPolicy Bypass -File .\run_web.ps1
```

유지 실행(프로세스 재기동):
```powershell
powershell -ExecutionPolicy Bypass -File .\run_web_keepalive.ps1
```

중지:
```powershell
powershell -ExecutionPolicy Bypass -File .\stop_web.ps1
```

기본 접속 주소:
- `http://127.0.0.1:8501`

실행 동작:
- 이미 앱이 떠 있으면 브라우저만 다시 엽니다.
- `requirements.txt` 가 바뀐 경우에만 패키지를 다시 설치합니다.
- 기본 실행은 `숨김 background worker` 로 떠서 CMD/PowerShell 창이 남지 않습니다.
- `stop_web.ps1` 는 앱 Python 프로세스와 숨김 worker 를 함께 종료합니다.
- 처음 기동되면 브라우저를 자동으로 엽니다.

업로드/참고 파일:
- 사용자가 넣은 CSV/참고 자료는 `user_uploads/` 아래로 모아둡니다.
- 루트에 떨어진 임시 업로드 파일은 이 폴더로 정리하는 기준을 사용합니다.

## 운영 기준
- 외부 무료 호스팅은 cold start, 휘발성 파일시스템(ephemeral filesystem), 장시간 조건검색 시 자원 제약 때문에 현재 프로젝트 기준으로 사용하지 않습니다.
- 결과 저장은 `_cache/exports/latest_screener.csv` 와 타임스탬프 CSV 기준입니다.
- `KRX Open API` 키는 `_cache/krx_api_key.txt` 또는 앱 사이드바 설정에서 관리합니다.
- `US 월배당`은 전용 메타 소스가 아직 약해 `KR`보다 탐색 효율이 낮지만, 현재는 선필터에서 미국 종목이 통째로 탈락하지 않도록 조정했습니다.

## KRX Open API (선택)
- 앱 사이드바 `KRX Open API 설정`에서 키 저장/테스트 가능
- 발급은 KRX Open API 사이트에서 진행:
  - https://openapi.krx.co.kr

## 주의
- 무료 데이터 소스 특성상 응답 지연/차단이 발생할 수 있습니다.
- KRX 실데이터 소스가 실패하면 fallback 목록으로 동작할 수 있습니다.
- Google Sheets 업로드는 제거했습니다.
- 투자 판단은 본인 책임입니다.
