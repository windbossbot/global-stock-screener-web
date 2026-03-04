# Web Deployment Quick Guide

## Render (Recommended for always-on)
1. Push this project to GitHub.
2. In Render, create a new Blueprint and connect the repo.
3. Render will read `global_stock_screener_web/render.yaml`.
4. Deploy and open the generated URL.

Notes:
- Free plan may sleep when idle.
- First boot can take a few minutes.

## Streamlit Community Cloud
1. Connect GitHub repo.
2. Set app file path to `global_stock_screener_web/app.py`.
3. Deploy.

Notes:
- Best for lightweight sharing.
- Session can reset when idle/redeploy.

## Environment
- Python dependencies are in `requirements.txt`.
- Streamlit config is in `.streamlit/config.toml`.
- Optional KRX key is entered in-app (sidebar).

## Troubleshooting
- If app shows no KR symbols:
  - Check source diagnostics panel.
  - Run with fallback sample list until KRX source is available.
- If scan is too slow:
  - Use `시장 모드: 국장(KR)` or limit `최대 스캔 수`.
  - Keep `조회 기간` at `2y` unless long MA filters are required.
