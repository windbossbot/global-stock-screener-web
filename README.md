# Global Stock Screener (Web)

KR/US 주식 스크리너 웹 앱입니다. 로컬 실행과 웹 공유(Render/Streamlit Cloud) 모두 지원합니다.

## 핵심 기능
- 시장 모드: `국장(KR) / 미장(US) / 통합(KR+US)`
- 조건 필터: 가격 범위, ETF 모드, 일/주/월 이동평균, 근접도, 기본 조건
- 점수화: 추세 + 배당 + 섹터 강도
- 결과 저장: CSV 다운로드
- 진단 패널: 단계별 시간/커버율/보강 건수

## 로컬 실행
```powershell
cd C:\Users\KGWPC\workspace\Jarvis\global_stock_screener_web
powershell -ExecutionPolicy Bypass -File .\run_web.ps1
```

유지 실행(프로세스 재기동):
```powershell
powershell -ExecutionPolicy Bypass -File .\run_web_keepalive.ps1
```

## 웹 공유 (무료)
### 1) Render
- 이 폴더에는 `render.yaml`이 포함되어 있습니다.
- Render에서 GitHub repo 연결 후 Blueprint 배포하면 됩니다.
- 시작 명령: `streamlit run app.py --server.port $PORT --server.address 0.0.0.0`

### 2) Streamlit Community Cloud
1. GitHub repo 연결
2. 앱 경로를 `global_stock_screener_web/app.py`로 지정
3. 배포

## KRX Open API (선택)
- 앱 사이드바 `KRX Open API 설정`에서 키 저장/테스트 가능
- 발급은 KRX Open API 사이트에서 진행:
  - https://openapi.krx.co.kr

## 주의
- 무료 데이터 소스 특성상 응답 지연/차단이 발생할 수 있습니다.
- KRX 실데이터 소스가 실패하면 fallback 목록으로 동작할 수 있습니다.
- 투자 판단은 본인 책임입니다.
