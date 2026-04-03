import concurrent.futures
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import threading
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import streamlit as st
from urllib3.util.retry import Retry
# yfinance cache dir should be set before import when possible.
try:
    _pre_yf_cache = (Path(__file__).resolve().parent / "_cache" / "yf_cache")
    _pre_yf_cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("YF_CACHE_DIR", str(_pre_yf_cache))
except Exception:
    pass
import yfinance as yf
import yfinance.cache as yf_cache
try:
    from pykrx import stock as pykrx_stock
except Exception:
    pykrx_stock = None
try:
    import FinanceDataReader as fdr
except Exception:
    fdr = None

US_LISTING_URLS = [
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt",
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt",
]
USDKRW_APIS = [
    "https://open.er-api.com/v6/latest/USD",
    "https://api.exchangerate.host/latest?base=USD&symbols=KRW",
]

FALLBACK_US_TICKERS = [
    ("AAPL", "Apple Inc."),
    ("MSFT", "Microsoft Corp."),
    ("NVDA", "NVIDIA Corp."),
    ("AMZN", "Amazon.com Inc."),
    ("GOOGL", "Alphabet Inc."),
    ("META", "Meta Platforms Inc."),
    ("TSLA", "Tesla Inc."),
    ("BRK-B", "Berkshire Hathaway Inc."),
    ("JPM", "JPMorgan Chase & Co."),
    ("V", "Visa Inc."),
    ("QQQ", "Invesco QQQ Trust"),
    ("SPY", "SPDR S&P 500 ETF"),
    ("DIA", "SPDR Dow Jones Industrial Average ETF"),
    ("IWM", "iShares Russell 2000 ETF"),
    ("TLT", "iShares 20+ Year Treasury Bond ETF"),
]
FALLBACK_KR_TICKERS = [
    ("005930", "삼성전자", ".KS"),
    ("000660", "SK하이닉스", ".KS"),
    ("207940", "삼성바이오로직스", ".KS"),
    ("373220", "LG에너지솔루션", ".KS"),
    ("005380", "현대차", ".KS"),
    ("035420", "NAVER", ".KS"),
    ("035720", "카카오", ".KS"),
    ("068270", "셀트리온", ".KS"),
    ("051910", "LG화학", ".KS"),
    ("006400", "삼성SDI", ".KS"),
    ("096770", "SK이노베이션", ".KS"),
    ("105560", "KB금융", ".KS"),
    ("055550", "신한지주", ".KS"),
    ("323410", "카카오뱅크", ".KS"),
    ("028260", "삼성물산", ".KS"),
]
CACHE_DIR = Path(__file__).resolve().parent / "_cache"
US_CACHE_PATH = CACHE_DIR / "us_universe.csv"
KR_CACHE_PATH = CACHE_DIR / "kr_universe.csv"
KR_ETF_CACHE_PATH = CACHE_DIR / "kr_etf_universe.csv"
KR_ETF_DIVIDEND_META_PATH = CACHE_DIR / "kr_etf_dividend_meta.csv"
SECTOR_CACHE_PATH = CACHE_DIR / "sector_cache.csv"
SECTOR_SEED_PATH = CACHE_DIR / "sector_seed.csv"
KRX_KEY_PATH = CACHE_DIR / "krx_api_key.txt"
KR_SEED_PATH = CACHE_DIR / "kr_universe_seed.csv"
KR_SECTOR_MASTER_PATH = CACHE_DIR / "kr_sector_master.csv"
FILTERS_PATH = CACHE_DIR / "saved_filters.json"
RESULT_EXPORT_DIR = CACHE_DIR / "exports"
RESULT_EXPORT_LATEST_PATH = RESULT_EXPORT_DIR / "latest_screener.csv"
VALUATION_CACHE_PATH = CACHE_DIR / "valuation_cache.csv"
QUOTE_CACHE_PATH = CACHE_DIR / "quote_snapshot_cache.csv"
KRX_OPENAPI_ENDPOINTS = [
    ("KOSPI", "https://data.krx.co.kr/svc/apis/sto/stk_bydd_trd"),
    ("KOSDAQ", "https://data.krx.co.kr/svc/apis/sto/ksq_bydd_trd"),
    ("KONEX", "https://data.krx.co.kr/svc/apis/sto/knx_bydd_trd"),
]
KETF_LINEUP_URL = "https://www.k-etf.com/api/v0/lineup/etf/in/market_temp?code=XKRX&lang=ko"
KETF_DIVIDEND_RANK_URL = "https://www.k-etf.com/api/v0/performance/etf/orderby/dividend?market=XKRX&limit={limit}&lang=ko"
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0"}
_http_session_lock = threading.Lock()
_http_session: Optional[requests.Session] = None
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    yf.set_tz_cache_location(str(CACHE_DIR / "yf_tz_cache"))
    yf_cache_dir = CACHE_DIR / "yf_cache"
    yf_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ["YF_CACHE_DIR"] = str(yf_cache_dir)
    yf_cache.set_cache_location(str(yf_cache_dir))
except Exception:
    pass


def get_http_session() -> requests.Session:
    global _http_session
    with _http_session_lock:
        if _http_session is None:
            session = requests.Session()
            retry = Retry(
                total=2,
                connect=2,
                read=2,
                status=2,
                backoff_factor=0.4,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset(["GET", "POST"]),
                respect_retry_after_header=True,
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(HTTP_HEADERS)
            _http_session = session
    return _http_session


def http_get(url: str, **kwargs) -> requests.Response:
    return get_http_session().get(url, **kwargs)


def http_post(url: str, **kwargs) -> requests.Response:
    return get_http_session().post(url, **kwargs)


def _read_pipe_table(text: str) -> pd.DataFrame:
    lines = [line for line in text.splitlines() if line and not line.startswith("File Creation Time")]
    if not lines:
        return pd.DataFrame()
    return pd.read_csv(pd.io.common.StringIO("\n".join(lines)), sep="|", dtype=str)


def fallback_us_universe() -> pd.DataFrame:
    rows = []
    for t, n in FALLBACK_US_TICKERS:
        rows.append({"market": "US", "ticker": t, "yf_ticker": t, "name": n, "currency": "USD", "_source": "fallback_us_sample"})
    return pd.DataFrame(rows)


def fallback_kr_universe() -> pd.DataFrame:
    rows = []
    for t, n, sfx in FALLBACK_KR_TICKERS:
        rows.append({"market": "KR", "ticker": t, "yf_ticker": f"{t}{sfx}", "name": n, "currency": "KRW", "_source": "fallback_kr_sample"})
    return pd.DataFrame(rows)


def kr_universe_from_seed(limit: int = 3000) -> pd.DataFrame:
    seed = get_sector_seed_map()
    if seed is None or seed.empty:
        return pd.DataFrame()
    kr = seed[seed["market"].astype(str).str.upper().eq("KR")].copy()
    if kr.empty:
        return pd.DataFrame()
    kr["ticker"] = kr["ticker"].astype(str).str.zfill(6)
    kr["name"] = kr.get("name", kr.get("sector", "")).fillna("").astype(str)
    kr["yf_ticker"] = kr["ticker"] + ".KS"
    kr["market"] = "KR"
    kr["currency"] = "KRW"
    kr["_source"] = "seed_kr"
    out = kr[["market", "ticker", "yf_ticker", "name", "currency", "_source"]].drop_duplicates(subset=["ticker"])
    if limit > 0:
        out = out.head(int(limit))
    return out.reset_index(drop=True)


def save_universe_cache(df: pd.DataFrame, path: Path) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")
    except Exception:
        pass


def load_snapshot_cache(path: Path, ttl_hours: int, value_cols: List[str]) -> pd.DataFrame:
    empty = pd.DataFrame(columns=["yf_ticker"] + list(value_cols))
    try:
        if not _is_cache_fresh(path, ttl_hours):
            return empty
        cached = pd.read_csv(path)
        if cached is None or cached.empty or "yf_ticker" not in cached.columns:
            return empty
        out = cached.copy()
        out["yf_ticker"] = out["yf_ticker"].astype(str).str.strip()
        out = out[out["yf_ticker"].ne("")]
        for col in value_cols:
            if col not in out.columns:
                out[col] = pd.Series(dtype=float)
            out[col] = pd.to_numeric(out[col], errors="coerce")
        return out[["yf_ticker"] + list(value_cols)].drop_duplicates(subset=["yf_ticker"], keep="last")
    except Exception:
        return empty


def save_snapshot_cache(path: Path, fresh_df: pd.DataFrame, value_cols: List[str], keep_hours: int = 72) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        fresh = fresh_df.copy()
        if fresh.empty or "yf_ticker" not in fresh.columns:
            return
        fresh["yf_ticker"] = fresh["yf_ticker"].astype(str).str.strip()
        fresh = fresh[fresh["yf_ticker"].ne("")]
        for col in value_cols:
            if col not in fresh.columns:
                fresh[col] = pd.Series(dtype=float)
            fresh[col] = pd.to_numeric(fresh[col], errors="coerce")
        base = load_snapshot_cache(path, keep_hours, value_cols)
        frames = [fresh[["yf_ticker"] + list(value_cols)]]
        if not base.empty:
            frames.insert(0, base)
        merged = pd.concat(frames, ignore_index=True)
        merged = merged.drop_duplicates(subset=["yf_ticker"], keep="last")
        save_universe_cache(merged, path)
    except Exception:
        pass


def load_saved_filters() -> Dict[str, Dict]:
    try:
        if FILTERS_PATH.exists():
            raw = json.loads(FILTERS_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                out: Dict[str, Dict] = {}
                for k, v in raw.items():
                    if not isinstance(k, str) or not isinstance(v, dict):
                        continue
                    out[k] = {
                        "market_mode": str(v.get("market_mode", "통합(KR+US)")),
                        "selected_conditions": [str(x) for x in v.get("selected_conditions", []) if str(x).strip()],
                        "tf_rule_map": dict(v.get("tf_rule_map", {})),
                        "near_tf_label": str(v.get("near_tf_label", "사용 안함")),
                        "near_periods": [int(x) for x in v.get("near_periods", []) if str(x).isdigit()],
                        "near_pct": float(v.get("near_pct", 3.0)),
                        "min_price": float(v.get("min_price", 0.0)),
                        "max_price": float(v.get("max_price", 0.0)),
                        "etf_mode": str(v.get("etf_mode", "ETF 포함")),
                        "dividend_only": bool(v.get("dividend_only", False)),
                        "dividend_cycles": [str(x) for x in v.get("dividend_cycles", []) if str(x).strip()],
                        "per_max": float(v.get("per_max", 0.0)),
                        "roe_min": float(v.get("roe_min", 0.0)),
                        "per_roe_max": float(v.get("per_roe_max", 0.0)),
                        "ev_to_ebitda_max": float(v.get("ev_to_ebitda_max", 0.0)),
                    }
                return out
    except Exception:
        pass
    return {}


def save_saved_filters(data: Dict[str, Dict]) -> bool:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        FILTERS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception:
        return False


def save_result_csv(df: pd.DataFrame) -> Tuple[bool, str]:
    try:
        if df is None or df.empty:
            return False, "저장할 결과가 없습니다."
        RESULT_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_path = RESULT_EXPORT_DIR / f"screener_{stamp}.csv"
        df.to_csv(snapshot_path, index=False, encoding="utf-8-sig")
        df.to_csv(RESULT_EXPORT_LATEST_PATH, index=False, encoding="utf-8-sig")
        return True, str(snapshot_path)
    except Exception as e:
        return False, str(e)


def load_universe_cache(path: Path) -> pd.DataFrame:
    try:
        if path.exists():
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()


def _is_cache_fresh(path: Path, max_age_hours: int) -> bool:
    try:
        if not path.exists():
            return False
        age_sec = time.time() - path.stat().st_mtime
        return age_sec <= max_age_hours * 3600
    except Exception:
        return False


def _classify_dividend_cycle_from_summary(count: Optional[float], begin: Optional[int], last: Optional[int]) -> Optional[str]:
    try:
        cnt = int(float(count or 0))
        if cnt <= 0:
            return None
        if cnt >= 10:
            return "월배당"
        if cnt >= 4:
            return "분기배당"
        if cnt == 2:
            return "반기배당"
        if cnt == 1:
            return "연배당"
        if begin and last:
            b = datetime.strptime(str(int(begin)), "%Y%m%d").date()
            l = datetime.strptime(str(int(last)), "%Y%m%d").date()
            span_days = max(1, (l - b).days)
            avg_gap = span_days / max(1, cnt - 1) if cnt > 1 else span_days
            if avg_gap <= 45:
                return "월배당"
            if avg_gap <= 130:
                return "분기배당"
            if avg_gap <= 230:
                return "반기배당"
        return "연배당"
    except Exception:
        return None


def get_kr_etf_universe_ketf(force_refresh: bool = False) -> pd.DataFrame:
    if not force_refresh:
        cached = load_universe_cache(KR_ETF_CACHE_PATH)
        if not cached.empty and _is_cache_fresh(KR_ETF_CACHE_PATH, 24):
            return cached
    try:
        r = http_get(KETF_LINEUP_URL, timeout=20, headers=HTTP_HEADERS)
        r.raise_for_status()
        js = r.json()
        rows: List[Dict] = []
        if isinstance(js, list):
            for it in js:
                code = str((it or {}).get("code", "")).strip().upper()
                name = str((it or {}).get("name", "")).strip()
                if not code or len(code) < 6:
                    continue
                ticker = code[-6:]
                rows.append(
                    {
                        "market": "KR",
                        "ticker": ticker,
                        "yf_ticker": f"{ticker}.KS",
                        "name": name,
                        "currency": "KRW",
                        "_source": "k_etf",
                        "is_etf_hint": True,
                    }
                )
        out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        if not out.empty:
            save_universe_cache(out, KR_ETF_CACHE_PATH)
        return out
    except Exception:
        return load_universe_cache(KR_ETF_CACHE_PATH)


def get_kr_etf_dividend_meta(force_refresh: bool = False, limit: int = 2000) -> pd.DataFrame:
    if not force_refresh:
        cached = load_universe_cache(KR_ETF_DIVIDEND_META_PATH)
        if not cached.empty and _is_cache_fresh(KR_ETF_DIVIDEND_META_PATH, 24):
            return cached
    try:
        url = KETF_DIVIDEND_RANK_URL.format(limit=max(500, int(limit)))
        r = http_get(url, timeout=20, headers=HTTP_HEADERS)
        r.raise_for_status()
        js = r.json()
        rows: List[Dict] = []
        if isinstance(js, list):
            for it in js:
                code = str((it or {}).get("code", "")).strip().upper()
                meta = (it or {}).get("dict") or {}
                if not code or not isinstance(meta, dict):
                    continue
                ticker = code[-6:]
                div_cycle = _classify_dividend_cycle_from_summary(meta.get("count"), meta.get("begin"), meta.get("last"))
                rows.append(
                    {
                        "ticker": ticker,
                        "dividend_cycle": div_cycle,
                        "dividend_event_count": int(float(meta.get("count", 0) or 0)),
                        "dividend_sum": float(meta.get("sum", 0.0) or 0.0),
                        "dividend_begin": meta.get("begin"),
                        "dividend_last": meta.get("last"),
                        "is_dividend": bool(float(meta.get("sum", 0.0) or 0.0) > 0),
                        "_source": "k_etf_dividend_rank",
                    }
                )
        out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        if not out.empty:
            save_universe_cache(out, KR_ETF_DIVIDEND_META_PATH)
        return out
    except Exception:
        return load_universe_cache(KR_ETF_DIVIDEND_META_PATH)


def merge_kr_etf_universe(base_df: pd.DataFrame) -> pd.DataFrame:
    etf_df = get_kr_etf_universe_ketf()
    if etf_df.empty:
        return base_df if isinstance(base_df, pd.DataFrame) else pd.DataFrame()
    meta_df = get_kr_etf_dividend_meta()
    merged_etf = etf_df.copy()
    if not meta_df.empty:
        merged_etf = merged_etf.merge(meta_df, on="ticker", how="left")
    if base_df is None or base_df.empty:
        return merged_etf.drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    out = base_df.copy()
    if "is_etf_hint" not in out.columns:
        out["is_etf_hint"] = pd.NA
    if "dividend_cycle" not in out.columns:
        out["dividend_cycle"] = None
    if "is_dividend" not in out.columns:
        out["is_dividend"] = pd.NA
    out = pd.concat([out, merged_etf], ignore_index=True)
    out = out.drop_duplicates(subset=["ticker"], keep="last").reset_index(drop=True)
    return out


def _finalize_kr_universe(df: pd.DataFrame) -> pd.DataFrame:
    out = merge_kr_etf_universe(df)
    if out is None:
        return pd.DataFrame()
    if not out.empty:
        save_universe_cache(out, KR_CACHE_PATH)
    return out.reset_index(drop=True)


def load_sector_cache() -> pd.DataFrame:
    try:
        if SECTOR_CACHE_PATH.exists():
            df = pd.read_csv(SECTOR_CACHE_PATH)
            need_cols = {"ticker", "market", "sector", "updated_at"}
            if need_cols.issubset(set(df.columns)):
                df["market"] = df["market"].astype(str).str.upper().str.strip()
                df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
                df["sector"] = df["sector"].fillna("Unknown").astype(str).str.strip()
                df["ticker_norm"] = df.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
                return df[df["sector"].ne("Unknown")]
    except Exception:
        pass
    return pd.DataFrame(columns=["ticker", "market", "sector", "updated_at"])


def save_sector_cache(df: pd.DataFrame) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        clean = df.copy()
        clean["market"] = clean["market"].astype(str).str.upper().str.strip()
        clean["ticker"] = clean["ticker"].astype(str).str.upper().str.strip()
        clean["sector"] = clean["sector"].fillna("Unknown").astype(str).str.strip()
        clean["ticker_norm"] = clean.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
        clean = clean[clean["sector"].ne("Unknown")]
        clean.to_csv(SECTOR_CACHE_PATH, index=False, encoding="utf-8-sig")
    except Exception:
        pass


def load_krx_api_key() -> str:
    try:
        if KRX_KEY_PATH.exists():
            return KRX_KEY_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return ""


def load_kr_seed_universe() -> pd.DataFrame:
    try:
        if not KR_SEED_PATH.exists():
            return pd.DataFrame()
        df = pd.read_csv(KR_SEED_PATH, dtype=str)
        if df is None or df.empty:
            return pd.DataFrame()
        code_col = None
        for c in ["ticker", "종목코드", "Code", "code", "ISU_SRT_CD"]:
            if c in df.columns:
                code_col = c
                break
        if not code_col:
            return pd.DataFrame()
        name_col = None
        for c in ["name", "회사명", "Name", "ISU_ABBRV"]:
            if c in df.columns:
                name_col = c
                break
        market_col = None
        for c in ["market", "시장", "Market", "MKT_NM"]:
            if c in df.columns:
                market_col = c
                break
        rows: List[Dict] = []
        for _, r in df.iterrows():
            tk = str(r.get(code_col, "")).strip()
            tk = "".join(ch for ch in tk if ch.isdigit()).zfill(6)
            if tk == "000000":
                continue
            mkt_txt = str(r.get(market_col, "")).upper() if market_col else ""
            sfx = ".KQ" if ("KOSDAQ" in mkt_txt or "KONEX" in mkt_txt or "KSQ" in mkt_txt or "KNX" in mkt_txt) else ".KS"
            nm = str(r.get(name_col, "")).strip() if name_col else ""
            rows.append(
                {
                    "market": "KR",
                    "ticker": tk,
                    "yf_ticker": f"{tk}{sfx}",
                    "name": nm,
                    "currency": "KRW",
                    "_source": "seed_csv_kr",
                }
            )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def save_kr_seed_universe(uploaded_file) -> Tuple[bool, str]:
    try:
        df = pd.read_csv(uploaded_file, dtype=str)
        if df is None or df.empty:
            return False, "CSV가 비어 있습니다."
        code_exists = any(c in df.columns for c in ["ticker", "종목코드", "Code", "code", "ISU_SRT_CD"])
        if not code_exists:
            return False, "필수 컬럼(종목코드/ticker/Code)이 없습니다."
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(KR_SEED_PATH, index=False, encoding="utf-8-sig")
        return True, f"KR CSV 저장 완료 ({len(df):,}행)"
    except Exception as e:
        return False, f"CSV 저장 실패: {e}"


def save_krx_api_key(key: str) -> bool:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        KRX_KEY_PATH.write_text((key or "").strip(), encoding="utf-8")
        return True
    except Exception:
        return False


def mask_key(key: str) -> str:
    if not key:
        return "-"
    k = key.strip()
    if len(k) <= 8:
        return "*" * len(k)
    return f"{k[:4]}{'*' * (len(k)-8)}{k[-4:]}"


def test_krx_api_key(key: str) -> Tuple[bool, str]:
    k = (key or "").strip()
    if len(k) < 10:
        return False, "키 길이가 너무 짧습니다. 발급 키를 다시 확인하세요."
    base = datetime.now()
    for d in range(0, 7):
        bas_dd = (base - timedelta(days=d)).strftime("%Y%m%d")
        ok_any = False
        for market_name, url in KRX_OPENAPI_ENDPOINTS:
            try:
                r = http_get(
                    url,
                    headers={"AUTH_KEY": k, "User-Agent": "Mozilla/5.0"},
                    params={"basDd": bas_dd},
                    timeout=5,
                )
                if r.status_code == 200:
                    js = r.json()
                    rows = js.get("OutBlock_1") or js.get("OutBlock1") or js.get("output")
                    if isinstance(rows, list) and len(rows) > 0:
                        return True, f"실데이터 확인 OK ({market_name}, {bas_dd}, {len(rows):,}행)"
                    ok_any = True
            except Exception:
                continue
        if ok_any:
            # endpoint is alive but this day can be non-trading day
            continue
    return False, "키 테스트 실패: 실데이터 응답 없음 (권한/서비스신청/키값/일자 확인 필요)"


def _krx_openapi_list_by_date(api_key: str, bas_dd: str) -> pd.DataFrame:
    headers = {"AUTH_KEY": api_key.strip(), "User-Agent": "Mozilla/5.0"}
    params = {"basDd": bas_dd}
    frames: List[pd.DataFrame] = []
    for market_name, url in KRX_OPENAPI_ENDPOINTS:
        try:
            r = http_get(url, headers=headers, params=params, timeout=12)
            if r.status_code != 200:
                continue
            js = r.json()
            rows = js.get("OutBlock_1") or js.get("OutBlock1") or js.get("output") or []
            if not rows:
                continue
            df = pd.json_normalize(rows)
            if df is not None and not df.empty:
                df["_krx_market_hint"] = market_name
                frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def get_kr_universe_from_openapi(api_key: str) -> pd.DataFrame:
    key = (api_key or "").strip()
    if len(key) < 10:
        return pd.DataFrame()
    base = datetime.now()
    raw = pd.DataFrame()
    used_date = ""
    for d in range(0, 10):
        bas_dd = (base - timedelta(days=d)).strftime("%Y%m%d")
        raw = _krx_openapi_list_by_date(key, bas_dd)
        if not raw.empty:
            used_date = bas_dd
            break
    if raw.empty:
        return pd.DataFrame()

    # Flexible column mapping (KRX schema names can vary by service version).
    col_ticker = None
    for c in ["ISU_SRT_CD", "ISU_CD", "isuSrtCd", "isuCd"]:
        if c in raw.columns:
            col_ticker = c
            break
    col_name = None
    for c in ["ISU_ABBRV", "ISU_NM", "isuAbbrv", "isuNm"]:
        if c in raw.columns:
            col_name = c
            break
    col_market = None
    for c in ["MKT_NM", "MKT_ID", "mktNm", "mktId"]:
        if c in raw.columns:
            col_market = c
            break
    if not col_ticker:
        return pd.DataFrame()

    rows: List[Dict] = []
    for _, r in raw.iterrows():
        tk = str(r.get(col_ticker, "")).strip().upper()
        if not tk:
            continue
        tk = "".join(ch for ch in tk if ch.isdigit()).zfill(6)
        if tk == "000000":
            continue
        mkt_txt = str(r.get(col_market, "")).upper()
        if not mkt_txt:
            mkt_txt = str(r.get("_krx_market_hint", "")).upper()
        if "KOSDAQ" in mkt_txt or "KSQ" in mkt_txt:
            sfx = ".KQ"
        elif "KONEX" in mkt_txt or "KNX" in mkt_txt:
            sfx = ".KQ"
        else:
            sfx = ".KS"
        nm = str(r.get(col_name, "")).strip()
        rows.append({
            "market": "KR",
            "ticker": tk,
            "yf_ticker": f"{tk}{sfx}",
            "name": nm,
            "currency": "KRW",
            "_source": f"krx_openapi_{used_date}",
        })
    return pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)


def get_kr_universe_from_krx_finder() -> pd.DataFrame:
    endpoints = [
        "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
        "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
    }
    payload = {"bld": "dbms/comm/finder/finder_stkisu"}
    for url in endpoints:
        try:
            r = http_post(url, data=payload, headers=headers, timeout=12)
            if r.status_code != 200:
                continue
            js = r.json()
            rows = js.get("block1") or js.get("Block1") or []
            if not isinstance(rows, list) or len(rows) == 0:
                continue
            out_rows: List[Dict] = []
            for it in rows:
                tk = str(it.get("short_code", "")).strip()
                tk = "".join(ch for ch in tk if ch.isdigit()).zfill(6)
                if tk == "000000":
                    continue
                mkt_code = str(it.get("marketCode", "")).upper().strip()
                mkt_name = str(it.get("marketName", "")).upper().strip()
                if mkt_code in {"KSQ", "KNX"} or ("KOSDAQ" in mkt_name) or ("KONEX" in mkt_name) or ("코스닥" in mkt_name) or ("코넥스" in mkt_name):
                    sfx = ".KQ"
                elif mkt_code in {"STK"} or ("KOSPI" in mkt_name) or ("유가증권" in mkt_name) or ("코스피" in mkt_name):
                    sfx = ".KS"
                else:
                    continue
                nm = str(it.get("codeName", "")).strip()
                out_rows.append(
                    {
                        "market": "KR",
                        "ticker": tk,
                        "yf_ticker": f"{tk}{sfx}",
                        "name": nm,
                        "currency": "KRW",
                        "_source": "krx_finder",
                    }
                )
            if out_rows:
                return pd.DataFrame(out_rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
        except Exception:
            continue
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_kr_sector_map_krx() -> pd.DataFrame:
    # Pull KR sector labels directly from KRX market stats endpoint.
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.krx.co.kr/contents/MDC/MDI/outerLoader/index.cmd",
    }
    try:
        date_meta = http_get(
            "http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld",
            headers=headers,
            timeout=10,
        )
        date_meta.raise_for_status()
        j = date_meta.json()
        trd_dd = (((j or {}).get("result") or {}).get("output") or [{}])[0].get("max_work_dt")
        if not trd_dd:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    mkt_map = [("STK", ".KS"), ("KSQ", ".KQ"), ("KNX", ".KQ")]
    rows: List[Dict] = []
    for mkt_id, suffix in mkt_map:
        payload = {
            "bld": "dbms/MDC/STAT/standard/MDCSTAT01501",
            "mktId": mkt_id,
            "trdDd": trd_dd,
            "share": "1",
            "money": "1",
            "csvxls_isNo": "false",
        }
        try:
            r = http_post(
                "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
                headers=headers,
                data=payload,
                timeout=12,
            )
            if r.status_code != 200:
                continue
            js = r.json()
            block = js.get("OutBlock_1") or js.get("output") or []
            if not isinstance(block, list) or len(block) == 0:
                continue
            for it in block:
                tk = str(it.get("ISU_SRT_CD", "")).strip()
                tk = "".join(ch for ch in tk if ch.isdigit()).zfill(6)
                if tk == "000000":
                    continue
                sec = str(it.get("SECT_TP_NM", "") or it.get("IDX_IND_NM", "")).strip()
                if not sec:
                    continue
                rows.append(
                    {
                        "market": "KR",
                        "ticker": tk,
                        "ticker_norm": tk,
                        "sector": sec,
                        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "yf_ticker": f"{tk}{suffix}",
                    }
                )
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows).drop_duplicates(subset=["market", "ticker_norm"], keep="last")
    return out[["market", "ticker", "ticker_norm", "sector", "updated_at"]].reset_index(drop=True)


@st.cache_data(ttl=86400)
def get_kr_sector_map_master() -> pd.DataFrame:
    # Stable KR sector fallback from FinanceData stock_master dataset.
    url = "https://github.com/FinanceData/stock_master/raw/master/stock_master.csv.gz"
    try:
        df = pd.read_csv(url, dtype={"Symbol": str})
        if df is None or df.empty:
            return pd.DataFrame()
        if "Symbol" not in df.columns:
            return pd.DataFrame()
        sec_col = "Sector" if "Sector" in df.columns else None
        if not sec_col:
            return pd.DataFrame()
        if "Listing" in df.columns:
            try:
                listed = df["Listing"].astype(str).str.lower().isin(["true", "1", "y", "yes"])
                df = df[listed]
            except Exception:
                pass
        out = pd.DataFrame(
            {
                "market": "KR",
                "ticker": df["Symbol"].astype(str).str.zfill(6),
                "ticker_norm": df["Symbol"].astype(str).str.zfill(6),
                "sector": df[sec_col].fillna("").astype(str).str.strip(),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        out = out[out["sector"] != ""]
        out = out.drop_duplicates(subset=["market", "ticker_norm"], keep="last").reset_index(drop=True)
        try:
            out.to_csv(KR_SECTOR_MASTER_PATH, index=False, encoding="utf-8-sig")
        except Exception:
            pass
        return out
    except Exception:
        pass
    try:
        if KR_SECTOR_MASTER_PATH.exists():
            cached = pd.read_csv(KR_SECTOR_MASTER_PATH, dtype=str)
            need = {"market", "ticker", "ticker_norm", "sector", "updated_at"}
            if need.issubset(set(cached.columns)) and not cached.empty:
                return cached
    except Exception:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_kr_sector_map_kind() -> pd.DataFrame:
    urls = [
        "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
        "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
    ]
    for url in urls:
        try:
            tables = pd.read_html(url, dtype=str)
            if not tables:
                continue
            df = tables[0].copy()
            code_col = "종목코드" if "종목코드" in df.columns else None
            sec_col = "업종" if "업종" in df.columns else None
            if not code_col or not sec_col:
                continue
            out = pd.DataFrame(
                {
                    "market": "KR",
                    "ticker": df[code_col].astype(str).str.zfill(6),
                    "ticker_norm": df[code_col].astype(str).str.zfill(6),
                    "sector": df[sec_col].fillna("").astype(str).str.strip(),
                    "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            out = out[out["sector"] != ""]
            out = out.drop_duplicates(subset=["market", "ticker_norm"], keep="last").reset_index(drop=True)
            if not out.empty:
                return out
        except Exception:
            continue
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_kr_universe_from_kind() -> pd.DataFrame:
    urls = [
        ("KOSPI", "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType=stockMkt"),
        ("KOSDAQ", "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType=kosdaqMkt"),
        ("KONEX", "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&marketType=konexMkt"),
    ]
    rows: List[Dict] = []
    for market_name, url in urls:
        try:
            tables = pd.read_html(url, dtype=str)
            if not tables:
                continue
            df = tables[0].copy()
            code_col = "종목코드" if "종목코드" in df.columns else None
            name_col = "회사명" if "회사명" in df.columns else None
            if not code_col:
                continue
            for _, r in df.iterrows():
                tk = str(r.get(code_col, "")).strip()
                tk = "".join(ch for ch in tk if ch.isdigit()).zfill(6)
                if tk == "000000":
                    continue
                nm = str(r.get(name_col, "")).strip() if name_col else ""
                sfx = ".KS" if market_name == "KOSPI" else ".KQ"
                rows.append(
                    {
                        "market": "KR",
                        "ticker": tk,
                        "yf_ticker": f"{tk}{sfx}",
                        "name": nm,
                        "currency": "KRW",
                        "_source": "kind_krx",
                    }
                )
        except Exception:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)


@st.cache_data(ttl=86400)
def get_sector_seed_map() -> pd.DataFrame:
    # One-day sector seed cache from file/FDR listings (cheap and stable).
    try:
        if SECTOR_SEED_PATH.exists():
            cached = pd.read_csv(SECTOR_SEED_PATH)
            need = {"market", "ticker", "sector"}
            if need.issubset(set(cached.columns)) and not cached.empty:
                cached["market"] = cached["market"].astype(str).str.upper().str.strip()
                cached["ticker"] = cached["ticker"].astype(str).str.upper().str.strip()
                if "ticker_norm" not in cached.columns:
                    cached["ticker_norm"] = cached.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
                return cached
    except Exception:
        pass

    if fdr is None:
        return pd.DataFrame(columns=["market", "ticker", "sector", "updated_at"])
    rows: List[Dict] = []
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        kr = fdr.StockListing("KRX")
        if kr is not None and not kr.empty:
            for _, r in kr.iterrows():
                tk = str(r.get("Code", "")).strip()
                sec = str(r.get("Sector", "") or r.get("Industry", "")).strip()
                if tk and sec:
                    rows.append({"market": "KR", "ticker": tk, "sector": sec, "updated_at": now_s})
    except Exception:
        pass
    for m in ["NASDAQ", "NYSE", "AMEX"]:
        try:
            us = fdr.StockListing(m)
            if us is None or us.empty:
                continue
            for _, r in us.iterrows():
                tk = str(r.get("Symbol", "")).strip().upper()
                sec = str(r.get("Sector", "") or r.get("Industry", "")).strip()
                if tk and sec:
                    rows.append({"market": "US", "ticker": tk, "sector": sec, "updated_at": now_s})
        except Exception:
            continue
    out = pd.DataFrame(rows).drop_duplicates(subset=["market", "ticker"], keep="last")
    if not out.empty:
        out["market"] = out["market"].astype(str).str.upper().str.strip()
        out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
        out["ticker_norm"] = out.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
    if out.empty:
        return out
    try:
        out.to_csv(SECTOR_SEED_PATH, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    return out


def probe_endpoint(url: str, timeout_sec: int = 8) -> str:
    try:
        r = http_get(url, timeout=timeout_sec)
        return f"HTTP {r.status_code}"
    except Exception as e:
        return f"ERR: {e}"


@st.cache_data(ttl=3600)
def get_us_universe() -> pd.DataFrame:
    rows: List[Dict] = []
    # Primary: Nasdaq Trader symbol directory
    try:
        for url in US_LISTING_URLS:
            resp = http_get(url, timeout=20)
            resp.raise_for_status()
            df = _read_pipe_table(resp.text)
            if df.empty:
                continue

            symbol_col = "Symbol" if "Symbol" in df.columns else ("ACT Symbol" if "ACT Symbol" in df.columns else None)
            if not symbol_col:
                continue
            name_col = "Security Name" if "Security Name" in df.columns else "Security Name "

            for _, row in df.iterrows():
                sym = str(row.get(symbol_col, "")).strip()
                name = str(row.get(name_col, "")).strip()
                if not sym or sym in {"Symbol", "File Creation Time"}:
                    continue
                if "$" in sym or "." in sym:
                    continue
                rows.append({"market": "US", "ticker": sym, "yf_ticker": sym, "name": name, "currency": "USD", "_source": "nasdaqtrader"})
    except Exception:
        rows = []

    out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True) if rows else pd.DataFrame()
    if not out.empty and len(out) > 500:
        save_universe_cache(out, US_CACHE_PATH)
        return out

    # Secondary fallback: FinanceDataReader listings
    if fdr is not None:
        try:
            frows: List[Dict] = []
            for market in ["NASDAQ", "NYSE", "AMEX"]:
                listed = fdr.StockListing(market)
                if listed is None or listed.empty:
                    continue
                for _, r in listed.iterrows():
                    sym = str(r.get("Symbol", "")).strip().upper()
                    name = str(r.get("Name", "")).strip()
                    if not sym:
                        continue
                    if "$" in sym or "." in sym:
                        continue
                    frows.append({"market": "US", "ticker": sym, "yf_ticker": sym, "name": name, "currency": "USD", "_source": "fdr_us"})
            fout = pd.DataFrame(frows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
            if not fout.empty:
                save_universe_cache(fout, US_CACHE_PATH)
                return fout
        except Exception:
            pass

    cached = load_universe_cache(US_CACHE_PATH)
    if not cached.empty:
        if "_source" not in cached.columns:
            cached["_source"] = "cache_us"
        else:
            cached["_source"] = "cache_us"
        return cached

    # Last fallback: small static sample
    return fallback_us_universe()


def _safe_kr_tickers(market: str) -> List[str]:
    if pykrx_stock is None:
        return []
    try:
        out = pykrx_stock.get_market_ticker_list(market=market)
        if out:
            return out
    except Exception:
        pass

    base = datetime.now()
    for d in range(0, 15):
        dt = (base - timedelta(days=d)).strftime("%Y%m%d")
        try:
            out = pykrx_stock.get_market_ticker_list(date=dt, market=market)
            if out:
                return out
        except Exception:
            continue
    return []


def _nearest_krx_date() -> Optional[str]:
    if pykrx_stock is None:
        return None
    base = datetime.now()
    for d in range(0, 15):
        dt = (base - timedelta(days=d)).strftime("%Y%m%d")
        try:
            out = pykrx_stock.get_market_ticker_list(date=dt, market="KOSPI")
            if out:
                return dt
        except Exception:
            continue
    return None


def enrich_kr_fundamentals(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # Fill KR dividend/value metrics from pykrx in bulk.
    if df.empty:
        return df, {"kr_fund_div_filled": 0.0, "kr_per_filled": 0.0, "kr_roe_filled": 0.0, "kr_per_roe_filled": 0.0}
    out = df.copy()
    kr_mask = out["market"].astype(str).str.upper().eq("KR")
    if kr_mask.sum() == 0:
        return out, {"kr_fund_div_filled": 0.0, "kr_per_filled": 0.0, "kr_roe_filled": 0.0, "kr_per_roe_filled": 0.0}
    if pykrx_stock is None:
        return out, {"kr_fund_div_filled": 0.0, "kr_per_filled": 0.0, "kr_roe_filled": 0.0, "kr_per_roe_filled": 0.0}

    before_div = int(pd.to_numeric(out.loc[kr_mask, "dividend_yield_pct"], errors="coerce").notna().sum())
    before_per = int(pd.to_numeric(out.loc[kr_mask, "per"], errors="coerce").notna().sum()) if "per" in out.columns else 0
    before_roe = int(pd.to_numeric(out.loc[kr_mask, "roe_pct"], errors="coerce").notna().sum()) if "roe_pct" in out.columns else 0
    before_per_roe = int(pd.to_numeric(out.loc[kr_mask, "per_roe_ratio"], errors="coerce").notna().sum()) if "per_roe_ratio" in out.columns else 0

    date = _nearest_krx_date()
    if not date:
        return out, {"kr_fund_div_filled": 0.0, "kr_per_filled": 0.0, "kr_roe_filled": 0.0, "kr_per_roe_filled": 0.0}
    fund = pd.DataFrame()
    try:
        fdf = pykrx_stock.get_market_fundamental_by_ticker(date, market="ALL")
        if fdf is not None and not fdf.empty:
            fdf = fdf.reset_index().rename(columns={fdf.columns[0]: "ticker"})
            fdf["ticker"] = fdf["ticker"].astype(str).str.zfill(6)
            keep_cols = ["ticker"]
            rename_map = {}
            for src, dst in [("DIV", "div_fill"), ("PER", "per_fill"), ("PBR", "pbr_fill"), ("EPS", "eps_fill"), ("BPS", "bps_fill")]:
                if src in fdf.columns:
                    keep_cols.append(src)
                    rename_map[src] = dst
            if len(keep_cols) > 1:
                fund = fdf[keep_cols].rename(columns=rename_map)
    except Exception:
        pass

    merged = out.loc[kr_mask, ["ticker"]].copy()
    merged["dividend_yield_pct"] = pd.to_numeric(out.loc[kr_mask, "dividend_yield_pct"], errors="coerce")
    merged["per"] = pd.to_numeric(out.loc[kr_mask, "per"], errors="coerce") if "per" in out.columns else pd.Series(index=merged.index, dtype=float)
    merged["roe_pct"] = pd.to_numeric(out.loc[kr_mask, "roe_pct"], errors="coerce") if "roe_pct" in out.columns else pd.Series(index=merged.index, dtype=float)
    merged["per_roe_ratio"] = pd.to_numeric(out.loc[kr_mask, "per_roe_ratio"], errors="coerce") if "per_roe_ratio" in out.columns else pd.Series(index=merged.index, dtype=float)
    merged["ticker"] = merged["ticker"].astype(str).str.zfill(6)
    if not fund.empty:
        merged = merged.merge(fund, on="ticker", how="left")
        div_fill = _numeric_series(merged, "div_fill")
        merged["dividend_yield_pct"] = pd.to_numeric(merged["dividend_yield_pct"], errors="coerce").where(
            pd.to_numeric(merged["dividend_yield_pct"], errors="coerce").notna(),
            div_fill,
        )
        per_fill = _numeric_series(merged, "per_fill")
        pbr_fill = _numeric_series(merged, "pbr_fill")
        eps_fill = _numeric_series(merged, "eps_fill")
        bps_fill = _numeric_series(merged, "bps_fill")
        merged["per"] = pd.to_numeric(merged["per"], errors="coerce").where(
            pd.to_numeric(merged["per"], errors="coerce").notna(),
            per_fill,
        )
        roe_from_eps = ((eps_fill / bps_fill) * 100.0).where(eps_fill.notna() & bps_fill.notna() & (bps_fill != 0))
        roe_from_ratio = ((pbr_fill / per_fill) * 100.0).where(pbr_fill.notna() & per_fill.notna() & (per_fill != 0))
        roe_fill = roe_from_eps.where(roe_from_eps.notna(), roe_from_ratio)
        merged["roe_pct"] = pd.to_numeric(merged["roe_pct"], errors="coerce").where(
            pd.to_numeric(merged["roe_pct"], errors="coerce").notna(),
            roe_fill,
        )

    out.loc[kr_mask, "dividend_yield_pct"] = merged["dividend_yield_pct"].values
    out.loc[kr_mask, "per"] = merged["per"].values
    out.loc[kr_mask, "roe_pct"] = merged["roe_pct"].values
    per_series = pd.to_numeric(merged["per"], errors="coerce")
    roe_series = pd.to_numeric(merged["roe_pct"], errors="coerce")
    ratio_fill = (per_series / roe_series).where(per_series.notna() & roe_series.notna() & (roe_series != 0))
    merged["per_roe_ratio"] = pd.to_numeric(merged["per_roe_ratio"], errors="coerce").where(
        pd.to_numeric(merged["per_roe_ratio"], errors="coerce").notna(),
        ratio_fill,
    )
    out.loc[kr_mask, "per_roe_ratio"] = merged["per_roe_ratio"].values

    after_div = int(pd.to_numeric(out.loc[kr_mask, "dividend_yield_pct"], errors="coerce").notna().sum())
    after_per = int(pd.to_numeric(out.loc[kr_mask, "per"], errors="coerce").notna().sum())
    after_roe = int(pd.to_numeric(out.loc[kr_mask, "roe_pct"], errors="coerce").notna().sum())
    after_per_roe = int(pd.to_numeric(out.loc[kr_mask, "per_roe_ratio"], errors="coerce").notna().sum())
    return out, {
        "kr_fund_div_filled": float(max(0, after_div - before_div)),
        "kr_per_filled": float(max(0, after_per - before_per)),
        "kr_roe_filled": float(max(0, after_roe - before_roe)),
        "kr_per_roe_filled": float(max(0, after_per_roe - before_per_roe)),
    }


def check_krx_connection() -> Tuple[bool, str]:
    try:
        key = load_krx_api_key()
        if key:
            odf = get_kr_universe_from_openapi(key)
            if not odf.empty:
                return True, f"KRX OpenAPI 로딩 OK ({len(odf):,}개)"
            k_ok, k_msg = test_krx_api_key(key)
            if not k_ok:
                return False, k_msg
        k1 = _safe_kr_tickers("KOSPI")
        k2 = _safe_kr_tickers("KOSDAQ")
        total = len(k1) + len(k2)
        if total > 0:
            return True, f"KRX 로딩 OK (KOSPI+KOSDAQ {total:,}개)"
        finder_df = get_kr_universe_from_krx_finder()
        if not finder_df.empty:
            return True, f"KRX Finder 로딩 OK ({len(finder_df):,}개)"
        kind_df = get_kr_universe_from_kind()
        if not kind_df.empty:
            return True, f"KIND 상장목록 로딩 OK ({len(kind_df):,}개)"
        seed_csv = load_kr_seed_universe()
        if not seed_csv.empty:
            return True, f"KR CSV 시드 로딩 OK ({len(seed_csv):,}개)"
        seed_df = kr_universe_from_seed(limit=3000)
        if not seed_df.empty:
            return True, f"KRX 직접 티커는 비었지만 seed 복구 가능 ({len(seed_df):,}개)"
        return False, "KRX 응답은 있으나 티커가 비어 있습니다."
    except Exception as e:
        return False, f"KRX 체크 실패: {e}"


@st.cache_data(ttl=120)
def get_kr_universe(key_hint: str = "") -> pd.DataFrame:
    key = (key_hint or load_krx_api_key()).strip()
    if key:
        openapi_df = get_kr_universe_from_openapi(key)
        if not openapi_df.empty:
            return _finalize_kr_universe(openapi_df)

    rows: List[Dict] = []
    for market, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ"), ("KONEX", ".KQ")]:
        tickers = _safe_kr_tickers(market)
        for t in tickers:
            try:
                n = pykrx_stock.get_market_ticker_name(t)
            except Exception:
                n = ""
            rows.append({"market": "KR", "ticker": t, "yf_ticker": f"{t}{suffix}", "name": n, "currency": "KRW", "_source": "pykrx"})
    out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
    if not out.empty:
        return _finalize_kr_universe(out)
    finder_df = get_kr_universe_from_krx_finder()
    if not finder_df.empty:
        return _finalize_kr_universe(finder_df)
    # Fallback path for KRX 403 / pykrx upstream instability.
    if fdr is not None:
        try:
            listed = fdr.StockListing("KRX")
            if listed is not None and not listed.empty:
                frows = []
                for _, r in listed.iterrows():
                    sym = str(r.get("Code", "")).strip()
                    name = str(r.get("Name", "")).strip()
                    market = str(r.get("Market", "")).upper()
                    if not sym or len(sym) < 6:
                        continue
                    if market == "KOSPI":
                        suffix = ".KS"
                    elif market in {"KOSDAQ", "KONEX"}:
                        suffix = ".KQ"
                    else:
                        continue
                    frows.append({
                        "market": "KR",
                        "ticker": sym,
                        "yf_ticker": f"{sym}{suffix}",
                        "name": name,
                        "currency": "KRW",
                        "_source": "fdr_krx",
                    })
                fallback = pd.DataFrame(frows).drop_duplicates(subset=["ticker"]).reset_index(drop=True)
                if not fallback.empty:
                    return _finalize_kr_universe(fallback)
        except Exception:
            pass
    kind_df = get_kr_universe_from_kind()
    if not kind_df.empty:
        return _finalize_kr_universe(kind_df)
    seed_csv = load_kr_seed_universe()
    if not seed_csv.empty:
        return _finalize_kr_universe(seed_csv)

    cached = load_universe_cache(KR_CACHE_PATH)
    if not cached.empty:
        if "_source" not in cached.columns:
            cached["_source"] = "cache_kr"
        else:
            cached["_source"] = "cache_kr"
        return merge_kr_etf_universe(cached)
    seeded = kr_universe_from_seed(limit=3000)
    if not seeded.empty:
        return _finalize_kr_universe(seeded)
    # Last fallback: small KR sample so KR-only mode remains usable.
    return _finalize_kr_universe(fallback_kr_universe())


def diagnose_kr_sources(key_hint: str = "") -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    key = (key_hint or load_krx_api_key()).strip()
    checks = [
        ("krx_openapi", lambda: get_kr_universe_from_openapi(key) if key else pd.DataFrame()),
        ("pykrx_kospi", lambda: pd.DataFrame({"ticker": _safe_kr_tickers("KOSPI")})),
        ("pykrx_kosdaq", lambda: pd.DataFrame({"ticker": _safe_kr_tickers("KOSDAQ")})),
        ("krx_finder", get_kr_universe_from_krx_finder),
        ("kind_krx", get_kr_universe_from_kind),
        ("seed_csv_kr", load_kr_seed_universe),
        ("cache_kr", lambda: load_universe_cache(KR_CACHE_PATH)),
    ]
    for name, fn in checks:
        try:
            df = fn()
            n = 0 if df is None else int(len(df))
            out.append({"소스": name, "결과": f"OK ({n:,})"})
        except Exception as e:
            out.append({"소스": name, "결과": f"ERR: {str(e)[:140]}"})
    return out


@st.cache_data(ttl=120)
def get_usdkrw() -> Optional[float]:
    for url in USDKRW_APIS:
        try:
            r = http_get(url, timeout=10)
            r.raise_for_status()
            d = r.json()
            if "rates" in d and "KRW" in d["rates"]:
                return float(d["rates"]["KRW"])
        except Exception:
            continue
    return None


def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return bool(v)
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y"}


def _normalize_dividend_yield_pct(v, annual_dividend_rate=None, price=None) -> Optional[float]:
    n = _safe_float(v)
    if n is None or n < 0:
        return None
    rate = _safe_float(annual_dividend_rate)
    px = _safe_float(price)
    if rate is not None and px is not None and px > 0:
        expected_pct = (rate / px) * 100.0
        raw_gap = abs(n - expected_pct)
        scaled_gap = abs((n * 100.0) - expected_pct)
        return n if raw_gap <= scaled_gap else (n * 100.0)
    # Upstream sources are inconsistent: some return ratios (0.004),
    # others already return percentage values (0.4, 4.0).
    if n <= 0.2:
        return n * 100.0
    return n


def _normalize_ratio_pct(v) -> Optional[float]:
    n = _safe_float(v)
    if n is None:
        return None
    if -1.0 <= n <= 1.0:
        return n * 100.0
    return n


def _extract_raw_value(v):
    if isinstance(v, dict):
        for key in ["raw", "fmt", "longFmt"]:
            if key in v and v.get(key) not in [None, ""]:
                return v.get(key)
        return None
    return v


def _numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    series = df.get(col)
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    return pd.Series(index=df.index, dtype=float)


def _truthy_flag_series(df: pd.DataFrame, col: str) -> pd.Series:
    series = df.get(col)
    if not isinstance(series, pd.Series):
        return pd.Series(False, index=df.index)
    text = series.astype("string").fillna("").str.strip().str.lower()
    return text.isin(["1", "true", "t", "yes", "y"])


def _compute_per_roe_ratio(per, roe_pct) -> Optional[float]:
    per_num = _safe_float(per)
    roe_num = _safe_float(roe_pct)
    if per_num is None or roe_num is None or roe_num == 0:
        return None
    return per_num / roe_num


def _to_eok(v) -> Optional[float]:
    n = _safe_float(v)
    if n is None:
        return None
    return round(n / 100_000_000.0, 2)


def _norm_ticker(market: str, ticker: str) -> str:
    m = str(market or "").upper().strip()
    t = str(ticker or "").upper().strip()
    if m == "KR":
        digits = "".join(ch for ch in t if ch.isdigit())
        if digits:
            return digits.zfill(6)
    return t


def _sma(series: pd.Series, n: int) -> Optional[float]:
    if len(series) < n:
        return None
    return float(series.tail(n).mean())


def _tf_close(daily_close: pd.Series, tf: str) -> pd.Series:
    if tf == "D":
        return daily_close
    if tf == "W":
        return daily_close.resample("W-FRI").last().dropna()
    return daily_close.resample("ME").last().dropna()


def _compute_tf_ma(daily_close: pd.Series, tf: str, periods: List[int]) -> Dict[str, Optional[float]]:
    src = _tf_close(daily_close, tf)
    out = {}
    for p in periods:
        out[f"{tf}_ma{p}"] = _sma(src, p)
    return out


def _slope_up(series: pd.Series, n: int) -> Optional[bool]:
    if len(series) < n + 5:
        return None
    now = float(series.tail(n).mean())
    prev = float(series.iloc[-(n + 5):-5].mean())
    return now > prev


def _classify_dividend_cycle(divs: pd.Series) -> Optional[str]:
    if divs is None or divs.empty:
        return None
    try:
        events = divs[divs > 0].dropna()
        if events.empty:
            return None
        idx = pd.Series(pd.to_datetime(events.index, errors="coerce")).dropna().drop_duplicates().sort_values()
        if len(idx) <= 1:
            return "연배당"
        gaps = idx.diff().dt.days.dropna()
        if gaps.empty:
            return "연배당"
        med = float(gaps.median())
        if med <= 45:
            return "월배당"
        if med <= 130:
            return "분기배당"
        if med <= 230:
            return "반기배당"
        return "연배당"
    except Exception:
        return None


def _build_metrics_from_history(item: Dict, hist: pd.DataFrame, fx: Optional[float], info: Optional[Dict] = None) -> Optional[Dict]:
    info = info or {}
    if hist.empty or "Close" not in hist.columns:
        return None
    close = hist["Close"].dropna()
    if close.empty:
        return None

    vol = hist["Volume"].dropna() if "Volume" in hist.columns else pd.Series(dtype=float)
    price = float(close.iloc[-1])

    d_ma = _compute_tf_ma(close, "D", [5, 20, 60, 120])
    w_ma = _compute_tf_ma(close, "W", [20, 60, 120])
    m_ma = _compute_tf_ma(close, "M", [20, 60, 120])

    ma60_up = _slope_up(close, 60)

    vol20 = float(vol.tail(20).mean()) if len(vol) >= 20 else None
    vol_last = float(vol.iloc[-1]) if len(vol) >= 1 else None

    info_div_rate = _safe_float(info.get("dividendRate"))
    if info_div_rate is None:
        info_div_rate = _safe_float(info.get("trailingAnnualDividendRate"))
    info_price = _safe_float(info.get("currentPrice"))
    if info_price is None:
        info_price = _safe_float(info.get("regularMarketPrice"))
    div_cycle = str(item.get("dividend_cycle", "")).strip() or None
    divs = pd.Series(dtype=float)
    if "Dividends" in hist.columns:
        try:
            divs = pd.to_numeric(hist["Dividends"], errors="coerce")
            hist_div_cycle = _classify_dividend_cycle(divs)
            if hist_div_cycle:
                div_cycle = hist_div_cycle
        except Exception:
            divs = pd.Series(dtype=float)
    dy = _normalize_dividend_yield_pct(
        info.get("dividendYield"),
        annual_dividend_rate=info_div_rate,
        price=info_price if info_price is not None else price,
    )
    # Fast-mode fallback: compute dividend yield from downloaded price actions.
    if dy is None and "Dividends" in hist.columns:
        try:
            non_null_divs = divs.dropna()
            if not non_null_divs.empty:
                recent = non_null_divs.tail(min(len(non_null_divs), 252))
                annual_div = float(recent.sum())
                if price > 0 and annual_div > 0:
                    dy = (annual_div / price) * 100.0
        except Exception:
            pass
    if div_cycle is None and str(item.get("market", "")).upper() == "KR" and dy is not None and dy > 0:
        div_cycle = "연배당"

    traded_value = None
    if len(close) >= 20 and len(vol) >= 20:
        dv = (hist["Close"] * hist["Volume"]).dropna().tail(20)
        if not dv.empty:
            traded_value = float(dv.mean())

    if item["currency"] == "USD" and fx and traded_value is not None:
        traded_value_krw = traded_value * fx
    elif item["currency"] == "KRW":
        traded_value_krw = traded_value
    else:
        traded_value_krw = None

    listing_days = int((datetime.now().date() - close.index[0].date()).days)

    name_text = f"{item['name']} {str(info.get('shortName', ''))} {str(info.get('longName', ''))}".upper()
    quote_type = str(info.get("quoteType", "")).upper()
    ticker = str(item["ticker"]).upper()
    etf_hint = _as_bool(item.get("is_etf_hint", False))

    is_spac = (
        ("SPAC" in name_text)
        or ("SPAC" in quote_type)
        or ("스팩" in item["name"])
        or ("기업인수목적" in item["name"])
    )
    is_etn = ("ETN" in name_text) or ("ETN" in quote_type) or ("EXCHANGE TRADED NOTE" in name_text)
    is_etf = etf_hint or ("ETF" in name_text) or ("ETF" in quote_type) or (quote_type == "ETF")
    is_leveraged_etf = is_etf and any(token in name_text for token in ["2X", "3X", "ULTRA", "LEVERAGED", "레버리지"])
    is_inverse_etf = is_etf and any(token in name_text for token in ["INVERSE", "SHORT", "BEAR", "인버스"])
    is_single_stock_etf = is_etf and any(token in name_text for token in ["SINGLE STOCK", "SINGLE-STOCK"])
    is_warrant_rights = (
        ticker.endswith(("W", "R", "RT", "WS"))
        or ("WARRANT" in name_text)
        or ("RIGHT" in name_text)
        or ("워런트" in name_text)
        or ("신주인수권" in name_text)
        or ("리츠인수권" in name_text)
    )
    is_preferred_like = is_warrant_rights or ("PREFERRED" in name_text) or (" 우" in name_text) or ("우B" in name_text)
    is_untradeable = (info.get("tradeable") is False) or (vol_last == 0)
    is_low_price = (item["market"] == "KR" and price < 1000) or (item["market"] == "US" and price < 1)
    is_low_liquidity = traded_value_krw is not None and traded_value_krw < 500_000_000
    is_new_listing = listing_days < 30

    d20, d60, d120, d5 = d_ma.get("D_ma20"), d_ma.get("D_ma60"), d_ma.get("D_ma120"), d_ma.get("D_ma5")

    price_above_120 = (d120 is not None) and (price > d120)
    pullback_20 = (d20 is not None) and (d120 is not None) and (price > d120) and (abs(price - d20) / d20 <= 0.03)
    strong_trend = all(v is not None for v in [d5, d20, d60, d120]) and (d5 > d20 > d60 > d120)
    general_trend = all(v is not None for v in [d20, d60, d120]) and (d20 > d60 > d120)
    down_trend = (d120 is not None) and (price < d120)
    vol_surge = (vol_last is not None) and (vol20 is not None) and (vol_last >= vol20 * 1.8)

    box_breakout = False
    if len(close) >= 21:
        prev20_high = float(close.iloc[-21:-1].max())
        box_breakout = price > prev20_high
    high_lookback = close.tail(min(len(close), 252))
    is_new_high_52w = False
    if not high_lookback.empty:
        max_52w = float(high_lookback.max())
        if max_52w > 0:
            is_new_high_52w = price >= (max_52w * 0.999)

    price_krw = price * fx if item["currency"] == "USD" and fx else (price if item["currency"] == "KRW" else None)
    per = _safe_float(info.get("trailingPE"))
    if per is None:
        per = _safe_float(info.get("forwardPE"))
    roe_pct = _normalize_ratio_pct(info.get("returnOnEquity"))
    ev_to_ebitda = _safe_float(info.get("enterpriseToEbitda"))
    per_roe_ratio = _compute_per_roe_ratio(per, roe_pct)

    return {
        "market": item["market"],
        "ticker": item["ticker"],
        "yf_ticker": item.get("yf_ticker", item["ticker"]),
        "name": item["name"],
        "currency": item["currency"],
        "price": round(price, 4),
        "price_krw": round(price_krw, 2) if price_krw is not None else None,
        "dividend_yield_pct": round(dy, 4) if dy is not None else None,
        "dividend_cycle": div_cycle,
        "is_dividend": _as_bool(item.get("is_dividend", False)) or bool((dy is not None) and (dy > 0)) or (div_cycle is not None),
        "avg_traded_value_krw_20": round(traded_value_krw, 2) if traded_value_krw is not None else None,
        "per": round(per, 4) if per is not None else None,
        "roe_pct": round(roe_pct, 4) if roe_pct is not None else None,
        "per_roe_ratio": round(per_roe_ratio, 4) if per_roe_ratio is not None else None,
        "ev_to_ebitda": round(ev_to_ebitda, 4) if ev_to_ebitda is not None else None,
        "listing_days": listing_days,
        **d_ma,
        **w_ma,
        **m_ma,
        "price_above_120": price_above_120,
        "ma60_up": ma60_up,
        "pullback_20": pullback_20,
        "vol_surge": vol_surge,
        "box_breakout": box_breakout,
        "strong_trend": strong_trend,
        "general_trend": general_trend,
        "down_trend": down_trend,
        "cond_5_over_20": (d5 is not None and d20 is not None and d5 > d20),
        "cond_60_over_120": (d60 is not None and d120 is not None and d60 > d120),
        "is_spac": is_spac,
        "is_etn": is_etn,
        "is_etf": is_etf,
        "is_leveraged_etf": is_leveraged_etf,
        "is_inverse_etf": is_inverse_etf,
        "is_single_stock_etf": is_single_stock_etf,
        "is_warrant_rights": is_warrant_rights,
        "is_preferred_like": is_preferred_like,
        "is_untradeable": is_untradeable,
        "is_low_price": is_low_price,
        "is_low_liquidity": is_low_liquidity,
        "is_new_listing": is_new_listing,
        "is_new_high_52w": is_new_high_52w,
    }


def _extract_history_from_download(data: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if data is None or data.empty:
        return pd.DataFrame()
    try:
        if isinstance(data.columns, pd.MultiIndex):
            level0 = set(data.columns.get_level_values(0))
            level1 = set(data.columns.get_level_values(1))
            if ticker in level0:
                out = data[ticker].copy()
            elif ticker in level1:
                out = data.xs(ticker, axis=1, level=1, drop_level=True).copy()
            else:
                return pd.DataFrame()
        else:
            out = data.copy()
        if "Close" not in out.columns:
            return pd.DataFrame()
        return out.dropna(how="all")
    except Exception:
        return pd.DataFrame()


def _download_batch_with_deadline(
    tickers: List[str], history_period: str, current_threads: int, timeout_sec: int
) -> Tuple[pd.DataFrame, bool]:
    result: Dict[str, object] = {"data": pd.DataFrame(), "done": False}

    def _worker() -> None:
        try:
            data = yf.download(
                tickers=tickers,
                period=history_period,
                interval="1d",
                auto_adjust=False,
                actions=True,
                group_by="ticker",
                threads=current_threads,
                progress=False,
                timeout=10,
            )
            result["data"] = data if data is not None else pd.DataFrame()
        except Exception:
            result["data"] = pd.DataFrame()
        finally:
            result["done"] = True

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(max(1, int(timeout_sec)))
    if not bool(result.get("done")):
        return pd.DataFrame(), True
    data = result.get("data")
    if isinstance(data, pd.DataFrame):
        return data, False
    return pd.DataFrame(), False


def _period_to_days(period: str) -> int:
    p = str(period).strip().lower()
    if p.endswith("y"):
        try:
            return int(p[:-1]) * 365
        except Exception:
            return 5 * 365
    if p.endswith("mo"):
        try:
            return int(p[:-2]) * 30
        except Exception:
            return 365
    if p.endswith("d"):
        try:
            return int(p[:-1])
        except Exception:
            return 365
    return 5 * 365


def _fetch_kr_history_pykrx(ticker: str, history_period: str) -> pd.DataFrame:
    if pykrx_stock is None:
        return pd.DataFrame()
    tk = "".join(ch for ch in str(ticker) if ch.isdigit()).zfill(6)
    if tk == "000000":
        return pd.DataFrame()
    end_dt = datetime.now().date()
    start_dt = end_dt - timedelta(days=_period_to_days(history_period) + 35)
    try:
        ohlcv = pykrx_stock.get_market_ohlcv_by_date(
            start_dt.strftime("%Y%m%d"),
            end_dt.strftime("%Y%m%d"),
            tk,
        )
    except Exception:
        return pd.DataFrame()
    if ohlcv is None or ohlcv.empty:
        return pd.DataFrame()
    out = ohlcv.copy()
    rename_map = {
        "종가": "Close",
        "거래량": "Volume",
        "시가": "Open",
        "고가": "High",
        "저가": "Low",
        "거래대금": "Value",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})
    if "Close" not in out.columns:
        return pd.DataFrame()
    if "Volume" not in out.columns:
        out["Volume"] = pd.Series([0] * len(out), index=out.index, dtype=float)
    if not isinstance(out.index, pd.DatetimeIndex):
        try:
            out.index = pd.to_datetime(out.index)
        except Exception:
            pass
    return out.dropna(how="all")


def _compute_metrics(item: Dict, fx: Optional[float], fetch_info: bool = True, history_period: str = "5y") -> Optional[Dict]:
    if str(item.get("market", "")).upper() == "KR":
        hist = _fetch_kr_history_pykrx(item.get("ticker", ""), history_period)
        if hist.empty and item.get("yf_ticker"):
            try:
                hist = yf.Ticker(str(item.get("yf_ticker"))).history(period=history_period, interval="1d", auto_adjust=False, actions=True)
            except Exception:
                hist = pd.DataFrame()
        return _build_metrics_from_history(item, hist, fx, info={})

    tk = yf.Ticker(item["yf_ticker"])
    hist = pd.DataFrame()
    for i in range(3):
        try:
            hist = tk.history(period=history_period, interval="1d", auto_adjust=False, actions=True)
            if not hist.empty:
                break
        except Exception as e:
            if "429" in str(e):
                time.sleep(1.2 * (i + 1))
                continue
            return None
        time.sleep(0.15)

    info = {}
    if fetch_info:
        for i in range(2):
            try:
                info = tk.info or {}
                break
            except Exception as e:
                if "429" in str(e):
                    time.sleep(1.0 * (i + 1))
                    continue
                info = {}
                break
    return _build_metrics_from_history(item, hist, fx, info=info)


def _fetch_sector_single(yf_ticker: str, market: str, ticker: str) -> Tuple[str, str, str]:
    sector = "Unknown"
    for i in range(2):
        try:
            u = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{yf_ticker}?modules=assetProfile"
            r = http_get(u, timeout=8, headers=HTTP_HEADERS)
            r.raise_for_status()
            d = r.json()
            result = (((d or {}).get("quoteSummary") or {}).get("result") or [{}])[0]
            ap = result.get("assetProfile") or {}
            sector = str(ap.get("sector") or ap.get("industry") or "").strip()
            if sector:
                break
        except Exception:
            pass
        time.sleep(0.4 * (i + 1))
    return market, ticker, (sector if sector else "Unknown")


@st.cache_data(ttl=21600, show_spinner=False)
def fetch_valuation_snapshot(yf_ticker: str) -> Dict[str, Optional[float]]:
    endpoints = [
        "https://query2.finance.yahoo.com/v10/finance/quoteSummary",
        "https://query1.finance.yahoo.com/v10/finance/quoteSummary",
    ]
    params = {"modules": "financialData,defaultKeyStatistics,summaryDetail"}
    headers = HTTP_HEADERS
    for base in endpoints:
        for retry in range(2):
            try:
                r = http_get(f"{base}/{yf_ticker}", params=params, timeout=8, headers=headers)
                r.raise_for_status()
                d = r.json()
                result = (((d or {}).get("quoteSummary") or {}).get("result") or [{}])[0]
                financial_data = result.get("financialData") or {}
                key_stats = result.get("defaultKeyStatistics") or {}
                summary_detail = result.get("summaryDetail") or {}
                per = _safe_float(_extract_raw_value(summary_detail.get("trailingPE")))
                if per is None:
                    per = _safe_float(_extract_raw_value(key_stats.get("trailingPE")))
                if per is None:
                    per = _safe_float(_extract_raw_value(financial_data.get("forwardPE")))
                roe_pct = _normalize_ratio_pct(_extract_raw_value(financial_data.get("returnOnEquity")))
                if roe_pct is None:
                    roe_pct = _normalize_ratio_pct(_extract_raw_value(key_stats.get("returnOnEquity")))
                ev_to_ebitda = _safe_float(_extract_raw_value(key_stats.get("enterpriseToEbitda")))
                if ev_to_ebitda is None:
                    ev_to_ebitda = _safe_float(_extract_raw_value(financial_data.get("enterpriseToEbitda")))
                return {
                    "per": per,
                    "roe_pct": roe_pct,
                    "per_roe_ratio": _compute_per_roe_ratio(per, roe_pct),
                    "ev_to_ebitda": ev_to_ebitda,
                }
            except Exception:
                time.sleep(0.35 * (retry + 1))
                continue
    return {"per": None, "roe_pct": None, "per_roe_ratio": None, "ev_to_ebitda": None}


def enrich_valuation_metrics(
    df: pd.DataFrame,
    workers: int = 4,
    timeout_sec: int = 45,
    need_per: bool = False,
    need_roe: bool = False,
    need_ev: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    if df.empty:
        return df, {"valuation_targets": 0.0, "valuation_timeout": 0.0, "valuation_cache_hits": 0.0, "valuation_per_filled": 0.0, "valuation_roe_filled": 0.0, "valuation_ev_filled": 0.0}
    if not any([need_per, need_roe, need_ev]):
        out = df.copy()
        per_series = _numeric_series(out, "per")
        roe_series = _numeric_series(out, "roe_pct")
        out["per_roe_ratio"] = (per_series / roe_series).where(per_series.notna() & roe_series.notna() & (roe_series != 0))
        return out, {"valuation_targets": 0.0, "valuation_timeout": 0.0, "valuation_cache_hits": 0.0, "valuation_per_filled": 0.0, "valuation_roe_filled": 0.0, "valuation_ev_filled": 0.0}

    out = df.copy()
    before_per = int(_numeric_series(out, "per").notna().sum())
    before_roe = int(_numeric_series(out, "roe_pct").notna().sum())
    before_ev = int(_numeric_series(out, "ev_to_ebitda").notna().sum())
    cache_cols = ["per", "roe_pct", "per_roe_ratio", "ev_to_ebitda"]
    cache_hits = 0

    cache_df = load_snapshot_cache(VALUATION_CACHE_PATH, 24, cache_cols)
    if not cache_df.empty:
        cache_key = cache_df.set_index("yf_ticker")
        for col in cache_cols:
            fill = out["yf_ticker"].map(cache_key[col]) if col in cache_key.columns else pd.Series(index=out.index, dtype=float)
            prev_count = int(_numeric_series(out, col).notna().sum())
            out[col] = _numeric_series(out, col).where(_numeric_series(out, col).notna(), pd.to_numeric(fill, errors="coerce"))
            cache_hits += max(0, int(_numeric_series(out, col).notna().sum()) - prev_count)

    need_mask = pd.Series(False, index=out.index)
    if need_per:
        need_mask = need_mask | _numeric_series(out, "per").isna()
    if need_roe:
        need_mask = need_mask | _numeric_series(out, "roe_pct").isna()
    if need_ev:
        need_mask = need_mask | _numeric_series(out, "ev_to_ebitda").isna()

    targets = out.loc[need_mask, "yf_ticker"].dropna().astype(str).unique().tolist()
    if targets:
        value_map: Dict[str, Dict[str, Optional[float]]] = {}
        max_workers = max(1, min(int(workers), 6))
        timeout_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(fetch_valuation_snapshot, yf_ticker): yf_ticker for yf_ticker in targets}
            try:
                for future in concurrent.futures.as_completed(futures, timeout=timeout_sec):
                    yf_ticker = futures[future]
                    try:
                        value_map[yf_ticker] = future.result()
                    except Exception:
                        value_map[yf_ticker] = {"per": None, "roe_pct": None, "per_roe_ratio": None, "ev_to_ebitda": None}
            except concurrent.futures.TimeoutError:
                pass
            pending = [future for future in futures if not future.done()]
            timeout_count = len(pending)
            for future in pending:
                future.cancel()
            for future, yf_ticker in futures.items():
                if yf_ticker in value_map:
                    continue
                if future.done():
                    try:
                        value_map[yf_ticker] = future.result()
                    except Exception:
                        value_map[yf_ticker] = {"per": None, "roe_pct": None, "per_roe_ratio": None, "ev_to_ebitda": None}

        if value_map:
            fresh_cache_rows = []
            for yf_ticker, payload in value_map.items():
                fresh_cache_rows.append(
                    {
                        "yf_ticker": yf_ticker,
                        "per": payload.get("per"),
                        "roe_pct": payload.get("roe_pct"),
                        "per_roe_ratio": payload.get("per_roe_ratio"),
                        "ev_to_ebitda": payload.get("ev_to_ebitda"),
                    }
                )
            if fresh_cache_rows:
                save_snapshot_cache(VALUATION_CACHE_PATH, pd.DataFrame(fresh_cache_rows), cache_cols, keep_hours=72)
            if need_per:
                per_fill = out["yf_ticker"].map({k: v.get("per") for k, v in value_map.items()})
                out["per"] = _numeric_series(out, "per").where(_numeric_series(out, "per").notna(), per_fill)
            if need_roe:
                roe_fill = out["yf_ticker"].map({k: v.get("roe_pct") for k, v in value_map.items()})
                out["roe_pct"] = _numeric_series(out, "roe_pct").where(_numeric_series(out, "roe_pct").notna(), roe_fill)
            if need_ev:
                ev_fill = out["yf_ticker"].map({k: v.get("ev_to_ebitda") for k, v in value_map.items()})
                out["ev_to_ebitda"] = _numeric_series(out, "ev_to_ebitda").where(_numeric_series(out, "ev_to_ebitda").notna(), ev_fill)
        per_series = _numeric_series(out, "per")
        roe_series = _numeric_series(out, "roe_pct")
        out["per_roe_ratio"] = (per_series / roe_series).where(per_series.notna() & roe_series.notna() & (roe_series != 0))
        return out, {
            "valuation_targets": float(len(targets)),
            "valuation_timeout": float(timeout_count),
            "valuation_cache_hits": float(cache_hits),
            "valuation_per_filled": float(max(0, int(_numeric_series(out, "per").notna().sum()) - before_per)),
            "valuation_roe_filled": float(max(0, int(_numeric_series(out, "roe_pct").notna().sum()) - before_roe)),
            "valuation_ev_filled": float(max(0, int(_numeric_series(out, "ev_to_ebitda").notna().sum()) - before_ev)),
        }

    per_series = _numeric_series(out, "per")
    roe_series = _numeric_series(out, "roe_pct")
    out["per_roe_ratio"] = (per_series / roe_series).where(per_series.notna() & roe_series.notna() & (roe_series != 0))
    return out, {"valuation_targets": 0.0, "valuation_timeout": 0.0, "valuation_cache_hits": float(cache_hits), "valuation_per_filled": float(max(0, int(_numeric_series(out, "per").notna().sum()) - before_per)), "valuation_roe_filled": float(max(0, int(_numeric_series(out, "roe_pct").notna().sum()) - before_roe)), "valuation_ev_filled": float(max(0, int(_numeric_series(out, "ev_to_ebitda").notna().sum()) - before_ev))}


def enrich_dividend_from_quote(df: pd.DataFrame) -> pd.DataFrame:
    # Lightweight batch dividend fill for fast mode.
    if df.empty or "dividend_yield_pct" not in df.columns:
        return df
    out = df.copy()
    miss = out["dividend_yield_pct"].isna()
    if miss.sum() == 0:
        return out
    targets = out.loc[miss, "yf_ticker"].dropna().astype(str).unique().tolist()
    if not targets:
        return out
    dy_map: Dict[str, float] = {}
    chunk = 180
    for i in range(0, len(targets), chunk):
        syms = targets[i:i + chunk]
        try:
            url = "https://query1.finance.yahoo.com/v7/finance/quote"
            r = http_get(url, params={"symbols": ",".join(syms)}, timeout=12, headers=HTTP_HEADERS)
            r.raise_for_status()
            js = r.json()
            results = (((js or {}).get("quoteResponse") or {}).get("result") or [])
            for q in results:
                sym = str(q.get("symbol", "")).strip()
                dy = q.get("trailingAnnualDividendYield")
                if dy is None:
                    dy = q.get("dividendYield")
                v = _normalize_dividend_yield_pct(
                    dy,
                    annual_dividend_rate=q.get("trailingAnnualDividendRate"),
                    price=q.get("regularMarketPrice"),
                )
                if sym and v is not None:
                    dy_map[sym] = v
        except Exception:
            continue
    if dy_map:
        fill = out["yf_ticker"].map(dy_map)
        out["dividend_yield_pct"] = out["dividend_yield_pct"].where(out["dividend_yield_pct"].notna(), fill)
    return out


def enrich_quote_snapshot(
    df: pd.DataFrame, fx: Optional[float], limit: int = 2000
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # Fill lightweight quote fields (dividend only) in batch for final candidates.
    if df.empty:
        return df, {"quote_targets": 0.0, "quote_div_filled": 0.0}
    out = df.copy()
    targets = out["yf_ticker"].dropna().astype(str).unique().tolist()
    if limit > 0:
        targets = targets[: int(limit)]
    if not targets:
        return out, {"quote_targets": 0.0, "quote_div_filled": 0.0}

    before_div = int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum())
    cache_hits = 0
    quote_cache = load_snapshot_cache(QUOTE_CACHE_PATH, 12, ["dividend_yield_pct"])
    if not quote_cache.empty:
        quote_key = quote_cache.set_index("yf_ticker")
        fill_cached = out["yf_ticker"].map(quote_key["dividend_yield_pct"])
        prev_div = int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum())
        out["dividend_yield_pct"] = pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").where(
            pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna(),
            pd.to_numeric(fill_cached, errors="coerce"),
        )
        cache_hits = max(0, int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum()) - prev_div)
        remaining_mask = pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").isna()
        targets = out.loc[remaining_mask, "yf_ticker"].dropna().astype(str).unique().tolist()
        if limit > 0:
            targets = targets[: int(limit)]
    if not targets:
        return out, {"quote_targets": 0.0, "quote_cache_hits": float(cache_hits), "quote_div_filled": float(max(0, int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum()) - before_div))}

    dy_map: Dict[str, float] = {}
    endpoints = [
        "https://query1.finance.yahoo.com/v7/finance/quote",
        "https://query2.finance.yahoo.com/v7/finance/quote",
    ]
    chunk = 120
    for i in range(0, len(targets), chunk):
        syms = targets[i:i + chunk]
        got = False
        for ep in endpoints:
            for retry in range(2):
                try:
                    r = http_get(
                        ep,
                        params={"symbols": ",".join(syms)},
                        timeout=12,
                        headers=HTTP_HEADERS,
                    )
                    r.raise_for_status()
                    js = r.json()
                    results = (((js or {}).get("quoteResponse") or {}).get("result") or [])
                    if not results:
                        continue
                    for q in results:
                        sym = str(q.get("symbol", "")).strip()
                        if not sym:
                            continue
                        dy = q.get("trailingAnnualDividendYield")
                        if dy is None:
                            dy = q.get("dividendYield")
                        vdy = _normalize_dividend_yield_pct(
                            dy,
                            annual_dividend_rate=q.get("trailingAnnualDividendRate"),
                            price=q.get("regularMarketPrice"),
                        )
                        if vdy is None:
                            dps = _safe_float(q.get("trailingAnnualDividendRate"))
                            px = _safe_float(q.get("regularMarketPrice"))
                            if dps is not None and px is not None and px > 0:
                                vdy = (dps / px) * 100.0
                        if vdy is not None:
                            dy_map[sym] = vdy
                    got = True
                    break
                except Exception as e:
                    if "10013" in str(e):
                        break
                    time.sleep(0.5 * (retry + 1))
                    continue
            if got:
                break

    # Fallback: per-symbol yfinance for residual missing fields (limited for speed).
    fallback_used = 0
    if len(dy_map) == 0:
        probe = out[out["yf_ticker"].isin(targets)][["yf_ticker", "price", "currency"]].drop_duplicates()
        probe = probe.head(350)
        for _, r in probe.iterrows():
            sym = str(r.get("yf_ticker", "")).strip()
            if not sym:
                continue
            need_div = sym not in dy_map
            if not need_div:
                continue
            try:
                tk = yf.Ticker(sym)
                if need_div:
                    divs = getattr(tk, "dividends", pd.Series(dtype=float))
                    if isinstance(divs, pd.Series) and not divs.empty:
                        recent = divs[divs.index >= (datetime.now() - timedelta(days=370))]
                        if not recent.empty:
                            annual = float(recent.sum())
                            px = _safe_float(r.get("price"))
                            if px is not None and px > 0:
                                dy_map[sym] = (annual / px) * 100.0
                fallback_used += 1
            except Exception:
                continue

    if dy_map:
        save_snapshot_cache(
            QUOTE_CACHE_PATH,
            pd.DataFrame([{"yf_ticker": k, "dividend_yield_pct": v} for k, v in dy_map.items()]),
            ["dividend_yield_pct"],
            keep_hours=36,
        )
    if dy_map:
        fill_dy = out["yf_ticker"].map(dy_map)
        out["dividend_yield_pct"] = pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").where(
            pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna(),
            pd.to_numeric(fill_dy, errors="coerce"),
        )

    after_div = int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum())
    stats = {
        "quote_targets": float(len(targets)),
        "quote_cache_hits": float(cache_hits),
        "quote_div_filled": float(max(0, after_div - before_div)),
        "quote_fallback_used": float(fallback_used),
    }
    return out, stats


def adjust_history_period(
    history_period: str, tf_rule_map: Dict[str, str], near_tf_label: str, near_periods: List[int]
) -> str:
    # Auto-extend only when long MA rules require it.
    need_w120 = tf_rule_map.get("W_120") in [">", "<"] or (near_tf_label == "주봉" and 120 in near_periods)
    need_m60 = tf_rule_map.get("M_60") in [">", "<"] or (near_tf_label == "월봉" and 60 in near_periods)
    need_m120 = tf_rule_map.get("M_120") in [">", "<"] or (near_tf_label == "월봉" and 120 in near_periods)
    if need_m120 or need_m60:
        return "10y"
    if need_w120 and history_period in {"2y"}:
        return "3y"
    return history_period


def enrich_sector_strength(
    df: pd.DataFrame,
    sector_workers: int = 4,
    sector_fetch_limit: int = 1500,
    sector_max_seconds: int = 180,
    sector_pick_mode: str = "상위",
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    if df.empty:
        return df, {"sector_cached": 0.0, "sector_fetched": 0.0, "sector_covered": 0.0}
    out = df.copy()
    out["market"] = out["market"].astype(str).str.upper().str.strip()
    out["ticker"] = out["ticker"].astype(str).str.upper().str.strip()
    out["ticker_norm"] = out.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
    cache = load_sector_cache()
    if not cache.empty and "ticker_norm" not in cache.columns:
        cache["ticker_norm"] = cache.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
    cache_key = cache.drop_duplicates(subset=["market", "ticker_norm"], keep="last")
    out = out.merge(cache_key[["market", "ticker_norm", "sector"]], on=["market", "ticker_norm"], how="left")
    out["sector"] = out["sector"].fillna("Unknown")
    us_mask = out["market"].eq("US")

    # KR sector fill from KRX stats endpoint.
    kr_sector = get_kr_sector_map_krx()
    if kr_sector is not None and not kr_sector.empty:
        if "ticker_norm" not in kr_sector.columns:
            kr_sector["ticker_norm"] = kr_sector["ticker"].astype(str).str.zfill(6)
        out = out.merge(
            kr_sector[["market", "ticker_norm", "sector"]].rename(columns={"sector": "sector_kr"}),
            on=["market", "ticker_norm"],
            how="left",
        )
        out["sector"] = out["sector"].where(out["sector"] != "Unknown", out["sector_kr"])
        out["sector"] = out["sector"].fillna("Unknown")
        out = out.drop(columns=["sector_kr"], errors="ignore")
    kr_sector_master = get_kr_sector_map_master()
    if kr_sector_master is not None and not kr_sector_master.empty:
        out = out.merge(
            kr_sector_master[["market", "ticker_norm", "sector"]].rename(columns={"sector": "sector_kr_master"}),
            on=["market", "ticker_norm"],
            how="left",
        )
        out["sector"] = out["sector"].where(out["sector"] != "Unknown", out["sector_kr_master"])
        out["sector"] = out["sector"].fillna("Unknown")
        out = out.drop(columns=["sector_kr_master"], errors="ignore")
    kr_sector_kind = get_kr_sector_map_kind()
    if kr_sector_kind is not None and not kr_sector_kind.empty:
        out = out.merge(
            kr_sector_kind[["market", "ticker_norm", "sector"]].rename(columns={"sector": "sector_kr_kind"}),
            on=["market", "ticker_norm"],
            how="left",
        )
        out["sector"] = out["sector"].where(out["sector"] != "Unknown", out["sector_kr_kind"])
        out["sector"] = out["sector"].fillna("Unknown")
        out = out.drop(columns=["sector_kr_kind"], errors="ignore")
    # Fill from static seed map first (KRX/FDR), then call network for residual unknown.
    seed = get_sector_seed_map()
    if seed is not None and not seed.empty:
        if "ticker_norm" not in seed.columns:
            seed["ticker_norm"] = seed.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
        out = out.merge(
            seed[["market", "ticker_norm", "sector"]].rename(columns={"sector": "sector_seed"}),
            on=["market", "ticker_norm"],
            how="left",
        )
        out["sector"] = out["sector"].where(out["sector"] != "Unknown", out["sector_seed"])
        out["sector"] = out["sector"].fillna("Unknown")
        out = out.drop(columns=["sector_seed"], errors="ignore")

    # Fetch residual unknown sectors for both US and KR (scoring is still market-separated).
    missing = out[out["sector"].eq("Unknown")][["market", "ticker", "yf_ticker"]].drop_duplicates()
    if sector_fetch_limit > 0:
        take_n = min(int(sector_fetch_limit), len(missing))
        if sector_pick_mode == "랜덤":
            missing = missing.sample(n=take_n, random_state=42)
        else:
            # Top mode: keep original order from upstream scan.
            missing = missing.head(take_n)

    fetched_rows: List[Dict] = []
    timed_out = 0
    if not missing.empty:
        workers = max(1, min(int(sector_workers), 8))
        futures = []
        sector_bar = st.progress(0, text="섹터 정보 수집 중...")
        fetched_done = 0
        deadline = time.time() + max(30, int(sector_max_seconds))
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [
                ex.submit(_fetch_sector_single, r["yf_ticker"], r["market"], r["ticker"])
                for _, r in missing.iterrows()
            ]
            pending = set(futures)
            while pending and time.time() < deadline:
                done, pending = concurrent.futures.wait(
                    pending, timeout=1.0, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for f in done:
                    fetched_done += 1
                    try:
                        mkt, tk, sec = f.result()
                        fetched_rows.append(
                            {"market": mkt, "ticker": tk, "sector": sec, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                        )
                    except Exception:
                        continue
                sector_bar.progress(
                    int(fetched_done * 100 / max(1, len(futures))),
                    text=f"섹터 정보 수집 중... {fetched_done}/{len(futures)}",
                )
            if pending:
                timed_out = len(pending)
                for f in pending:
                    f.cancel()
        sector_bar.empty()

    if fetched_rows:
        fetched_df = pd.DataFrame(fetched_rows)
        fetched_df["market"] = fetched_df["market"].astype(str).str.upper().str.strip()
        fetched_df["ticker"] = fetched_df["ticker"].astype(str).str.upper().str.strip()
        fetched_df["ticker_norm"] = fetched_df.apply(lambda r: _norm_ticker(r["market"], r["ticker"]), axis=1)
        new_cache = pd.concat([cache, fetched_df], ignore_index=True)
        new_cache = new_cache.drop_duplicates(subset=["market", "ticker_norm"], keep="last")
        save_sector_cache(new_cache)
        out = out.merge(
            new_cache[["market", "ticker_norm", "sector"]].rename(columns={"sector": "sector_cache"}),
            on=["market", "ticker_norm"],
            how="left",
        )
        out["sector"] = out["sector"].where(out["sector"] != "Unknown", out["sector_cache"])
        out["sector"] = out["sector"].fillna("Unknown")
        out = out.drop(columns=["sector_cache"], errors="ignore")

    known = out[out["sector"].ne("Unknown")].copy()
    if not known.empty:
        grp = (
            known.groupby(["market", "sector"], as_index=False)
            .agg(
                sector_new_high_count=("is_new_high_52w", "sum"),
                sector_total=("ticker", "count"),
            )
        )
        grp["sector_new_high_ratio"] = grp["sector_new_high_count"] / grp["sector_total"].clip(lower=1)
        grp["sector_strength_score"] = (
            grp["sector_new_high_count"].rank(pct=True).fillna(0.0) * 100.0
        ).round(2)
        out = out.merge(
            grp[["market", "sector", "sector_new_high_count", "sector_new_high_ratio", "sector_strength_score"]],
            on=["market", "sector"],
            how="left",
        )
    else:
        out["sector_new_high_count"] = 0.0
        out["sector_new_high_ratio"] = 0.0
        out["sector_strength_score"] = 0.0

    out["sector_strength_score"] = out["sector_strength_score"].fillna(0.0)
    out = out.drop(columns=["ticker_norm"], errors="ignore")
    covered = float((out.loc[us_mask, "sector"] != "Unknown").mean() * 100.0) if us_mask.any() else 0.0
    covered_kr = float((out.loc[~us_mask, "sector"] != "Unknown").mean() * 100.0) if (~us_mask).any() else 0.0
    stats = {
        "sector_cached": float(len(cache_key)),
        "sector_seeded": float(0 if seed is None else len(seed)),
        "sector_fetched": float(len(fetched_rows)),
        "sector_covered": round(covered, 2),
        "sector_covered_kr": round(covered_kr, 2),
        "sector_timeout": float(timed_out),
        "sector_pick_mode": sector_pick_mode,
        "sector_pick_limit": float(sector_fetch_limit),
    }
    return out, stats


def _run_scan_batched(
    universe: pd.DataFrame, fx: Optional[float], workers: int, history_period: str = "2y"
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    items = universe.to_dict(orient="records")
    kr_items = [it for it in items if str(it.get("market", "")).upper() == "KR"]
    non_kr_items = [it for it in items if str(it.get("market", "")).upper() != "KR"]
    rows: List[Dict] = []
    none_count = 0
    err_count = 0
    t0 = time.time()
    batch_size = 140
    current_batch = batch_size
    current_threads = max(2, min(int(workers), 12))
    adaptive_down_count = 0
    stalled_batches = 0
    batch_timeout_sec = 18
    done = 0
    bar = st.progress(0, text="스캔 계산 중(배치 모드)...")

    # KR is fetched through pykrx per ticker to reduce yfinance dependency.
    if kr_items:
        kr_workers = max(2, min(int(workers), 10))
        with concurrent.futures.ThreadPoolExecutor(max_workers=kr_workers) as ex:
            futures = [ex.submit(_compute_metrics, it, fx, False, history_period) for it in kr_items]
            for idx, f in enumerate(concurrent.futures.as_completed(futures), start=1):
                try:
                    r = f.result()
                    if r:
                        rows.append(r)
                    else:
                        none_count += 1
                except Exception:
                    err_count += 1
                done += 1
                if idx % 20 == 0 or idx == len(futures):
                    bar.progress(
                        int(done * 100 / max(1, len(items))),
                        text=f"스캔 계산 중(KR 모드)... {done}/{len(items)}",
                    )

    i = 0
    while i < len(non_kr_items):
        batch_items = non_kr_items[i:i + current_batch]
        tickers = [it["yf_ticker"] for it in batch_items if it.get("yf_ticker")]
        if not tickers:
            none_count += len(batch_items)
            done += len(batch_items)
            i += len(batch_items)
            continue
        data = pd.DataFrame()
        batch_started_at = time.time()
        for retry in range(2):
            try:
                remain_sec = max(3, int(batch_timeout_sec - (time.time() - batch_started_at)))
                data, timed_out = _download_batch_with_deadline(
                    tickers=tickers,
                    history_period=history_period,
                    current_threads=current_threads,
                    timeout_sec=remain_sec,
                )
                if timed_out:
                    break
                if data is not None and not data.empty:
                    break
            except Exception:
                pass
            if (time.time() - batch_started_at) > batch_timeout_sec:
                break
            time.sleep(0.6 * (retry + 1))
        if data is None or data.empty:
            current_batch = max(80, int(current_batch * 0.7))
            current_threads = max(2, current_threads - 1)
            adaptive_down_count += 1
            stalled_batches += 1
            # If batch mode keeps stalling, switch to per-ticker history mode for current+remaining items.
            if stalled_batches >= 3 and i < len(non_kr_items):
                remain = non_kr_items[i:]
                if remain:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=current_threads) as ex:
                        futures = [ex.submit(_compute_metrics, it, fx, False, history_period) for it in remain]
                        for idx, f in enumerate(concurrent.futures.as_completed(futures), start=1):
                            try:
                                r = f.result()
                                if r:
                                    rows.append(r)
                                else:
                                    none_count += 1
                            except Exception:
                                err_count += 1
                            done += 1
                            if idx % 12 == 0 or idx == len(futures):
                                bar.progress(
                                    int(done * 100 / max(1, len(items))),
                                    text=f"스캔 계산 중(폴백 모드)... {done}/{len(items)}",
                                )
                i = len(non_kr_items)
                break
            err_count += len(batch_items)
            done += len(batch_items)
            bar.progress(int(done * 100 / max(1, len(items))), text=f"스캔 계산 중(배치 모드)... {done}/{len(items)}")
            time.sleep(0.8)
            i += len(batch_items)
            continue
        stalled_batches = 0
        batch_success = 0
        for it in batch_items:
            hist = _extract_history_from_download(data, it["yf_ticker"])
            r = _build_metrics_from_history(it, hist, fx, info={})
            if r:
                rows.append(r)
                batch_success += 1
            else:
                none_count += 1
            done += 1
        bar.progress(int(done * 100 / max(1, len(items))), text=f"스캔 계산 중(배치 모드)... {done}/{len(items)}")
        fail_ratio = 1.0 - (batch_success / max(1, len(batch_items)))
        if fail_ratio > 0.45:
            current_batch = max(80, int(current_batch * 0.7))
            current_threads = max(2, current_threads - 1)
            adaptive_down_count += 1
            time.sleep(min(2.0, 0.6 + fail_ratio))
        elif fail_ratio < 0.05 and current_batch < 450:
            current_batch = min(450, current_batch + 25)
        i += len(batch_items)
    bar.empty()
    stats = {
        "scan_input": float(len(items)),
        "scan_success": float(len(rows)),
        "scan_none": float(none_count),
        "scan_error": float(err_count),
        "scan_elapsed_sec": round(time.time() - t0, 2),
        "workers_used": float(current_threads),
        "fetch_info": 0.0,
        "scan_mode": 1.0,
        "history_period": history_period,
        "batch_size": float(current_batch),
        "adaptive_down_count": float(adaptive_down_count),
    }
    return pd.DataFrame(rows), stats


def run_scan(
    universe: pd.DataFrame, fx: Optional[float], workers: int, fetch_info: bool = True, history_period: str = "5y"
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    if not fetch_info:
        return _run_scan_batched(universe, fx, workers, history_period=history_period)
    items = universe.to_dict(orient="records")
    workers = max(2, min(int(workers), 12))
    rows: List[Dict] = []
    none_count = 0
    err_count = 0
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_compute_metrics, it, fx, fetch_info, history_period) for it in items]
        done = 0
        bar = st.progress(0, text="스캔 계산 중...")
        for f in concurrent.futures.as_completed(futures):
            done += 1
            if done % 20 == 0 or done == len(futures):
                bar.progress(int(done * 100 / max(1, len(futures))), text=f"스캔 계산 중... {done}/{len(futures)}")
            try:
                r = f.result()
                if r:
                    rows.append(r)
                else:
                    none_count += 1
            except Exception:
                err_count += 1
                continue
        bar.empty()
    stats = {
        "scan_input": float(len(items)),
        "scan_success": float(len(rows)),
        "scan_none": float(none_count),
        "scan_error": float(err_count),
        "scan_elapsed_sec": round(time.time() - t0, 2),
        "workers_used": float(workers),
        "fetch_info": float(1 if fetch_info else 0),
        "scan_mode": 0.0,
        "history_period": history_period,
    }
    return pd.DataFrame(rows), stats


def normalize_etf_mode(mode: str) -> str:
    txt = str(mode).strip()
    if txt == "전체":
        return "ETF 포함"
    if txt == "ETF 제외":
        return "ETF 미포함"
    if txt == "ETF만":
        return "ETF 만"
    if txt in {"ETF 포함", "ETF 미포함", "ETF 만"}:
        return txt
    return "ETF 포함"


def build_filter_context(
    *,
    min_price: float,
    max_price: float,
    etf_mode: str,
    dividend_only: bool,
    dividend_cycles: List[str],
    selected_conditions: List[str],
    tf_rule_map: Dict[str, str],
    near_tf_label: str,
    near_periods: List[int],
    near_pct: float,
) -> Dict[str, object]:
    tf_rules: List[Tuple[str, str, int]] = []
    for key in ["M_20", "M_60", "M_120", "W_20", "W_60", "W_120", "D_20", "D_60", "D_120"]:
        op = tf_rule_map.get(key, "패스")
        if op not in [">", "<"]:
            continue
        tf, period = key.split("_")
        tf_rules.append((tf, op, int(period)))
    near_tf = None
    if near_tf_label != "사용 안함" and near_periods:
        near_tf = {"일봉": "D", "주봉": "W", "월봉": "M"}.get(str(near_tf_label).strip())
    return {
        "min_price": float(min_price),
        "max_price": float(max_price),
        "etf_mode": normalize_etf_mode(etf_mode),
        "dividend_only": bool(dividend_only),
        "dividend_cycles": [str(x).strip() for x in dividend_cycles if str(x).strip()],
        "selected_conditions": [str(x).strip() for x in selected_conditions if str(x).strip()],
        "strict_metadata_prefilter": True,
        "tf_rules": tf_rules,
        "near_tf": near_tf,
        "near_periods": list(near_periods),
        "near_pct": float(near_pct),
    }


def apply_price_filter(df: pd.DataFrame, min_price: float, max_price: float) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    price_krw = out.get("price_krw")
    if isinstance(price_krw, pd.Series):
        price_basis = price_krw.where(price_krw.notna(), out.get("price"))
    else:
        price_basis = out.get("price")
    price_basis = pd.to_numeric(price_basis, errors="coerce")
    out = out[price_basis >= float(min_price)]
    if max_price > 0:
        next_basis = out.get("price")
        price_krw_next = out.get("price_krw")
        if isinstance(price_krw_next, pd.Series):
            next_basis = price_krw_next.where(price_krw_next.notna(), out.get("price"))
        next_basis = pd.to_numeric(next_basis, errors="coerce")
        out = out[next_basis <= float(max_price)]
    return out


def prefilter_universe_fast(
    universe: pd.DataFrame,
    etf_mode: str,
    dividend_only: bool = False,
    dividend_cycles: Optional[List[str]] = None,
    strict_metadata_prefilter: bool = True,
) -> pd.DataFrame:
    # Cheap string-based prefilter before network-heavy yfinance calls.
    if universe.empty:
        return universe

    def should_enforce_strict(match_count: int, known_count: int, current_size: int, market_label: str = "") -> bool:
        if not strict_metadata_prefilter:
            return False
        if match_count <= 0 or known_count <= 0 or current_size <= 0:
            return False
        coverage = known_count / max(1, current_size)
        market_norm = str(market_label).strip().upper()
        if market_norm == "US" and coverage < 0.18:
            return False
        if coverage >= 0.55 and match_count >= 3:
            return True
        if match_count >= 12 and coverage >= 0.08:
            return True
        if match_count >= 5 and coverage >= 0.25:
            return True
        if market_norm == "KR" and match_count >= 20:
            return True
        return False

    def build_market_keep_mask(frame: pd.DataFrame, known_mask: pd.Series, match_mask: pd.Series) -> pd.Series:
        if frame.empty:
            return pd.Series(dtype=bool)
        keep_mask = pd.Series(False, index=frame.index, dtype=bool)
        if "market" not in frame.columns:
            if should_enforce_strict(int(match_mask.sum()), int(known_mask.sum()), len(frame)):
                return match_mask.astype(bool)
            return ((~known_mask) | match_mask).astype(bool)
        market_series = frame["market"].fillna("").astype(str).str.upper()
        for market_label in market_series.unique():
            market_idx = market_series.eq(market_label)
            market_known = known_mask.loc[market_idx]
            market_match = match_mask.loc[market_idx]
            if should_enforce_strict(int(market_match.sum()), int(market_known.sum()), int(market_idx.sum()), market_label):
                keep_mask.loc[market_idx] = market_match.astype(bool)
            else:
                keep_mask.loc[market_idx] = ((~market_known) | market_match).astype(bool)
        return keep_mask

    out = universe.copy()
    etf_mode_norm = normalize_etf_mode(etf_mode)
    selected_cycles = [str(x).strip() for x in (dividend_cycles or []) if str(x).strip()]
    name_series = out["name"].fillna("").str.upper()
    etf_hint = pd.Series(False, index=out.index)
    if "is_etf_hint" in out.columns:
        etf_hint = _truthy_flag_series(out, "is_etf_hint")
    etf_like = name_series.str.contains("ETF", regex=False) | etf_hint
    if etf_mode_norm == "ETF 포함":
        pass
    elif etf_mode_norm == "ETF 만":
        out = out[etf_like]
    else:
        out = out[~etf_like]
    if dividend_only and "is_dividend" in out.columns:
        known_div = out["is_dividend"].notna()
        div_true = _truthy_flag_series(out, "is_dividend")
        out = out[build_market_keep_mask(out, known_div, div_true)]
    if selected_cycles and "dividend_cycle" in out.columns:
        cycle = out["dividend_cycle"].fillna("").astype(str).str.strip()
        known_cycle = cycle.ne("")
        cycle_match = cycle.isin(selected_cycles)
        out = out[build_market_keep_mask(out, known_cycle, cycle_match)]
    return out


def prioritize_scan_candidates(universe: pd.DataFrame, ctx: Dict[str, object]) -> pd.DataFrame:
    if universe.empty:
        return universe
    out = universe.copy()
    name_series = out["name"].fillna("").astype(str).str.upper()
    etf_hint = pd.Series(False, index=out.index)
    if "is_etf_hint" in out.columns:
        etf_hint = _truthy_flag_series(out, "is_etf_hint")
    etf_like = name_series.str.contains("ETF", regex=False) | etf_hint
    dividend_flag = pd.Series(False, index=out.index)
    if "is_dividend" in out.columns:
        dividend_flag = _truthy_flag_series(out, "is_dividend")
    cycle = out.get("dividend_cycle", pd.Series("", index=out.index)).fillna("").astype(str).str.strip()
    market_series = out.get("market", pd.Series("", index=out.index)).fillna("").astype(str).str.upper()
    us_like = market_series.eq("US")
    income_name_hint = name_series.str.contains(r"DIVIDEND|INCOME|YIELD|MONTHLY|REIT|TRUST|FUND|ETF", regex=True)
    selected_cycles = [str(x).strip() for x in ctx.get("dividend_cycles", []) if str(x).strip()]
    selected_conditions = [str(x).strip() for x in ctx.get("selected_conditions", []) if str(x).strip()]
    scan_priority = pd.Series(0.0, index=out.index, dtype=float)

    etf_mode = str(ctx.get("etf_mode", "ETF 포함"))
    if etf_mode == "ETF 만":
        scan_priority += etf_like.astype(float) * 5.0
    elif etf_mode == "ETF 미포함":
        scan_priority += (~etf_like).astype(float) * 3.0

    if bool(ctx.get("dividend_only", False)):
        scan_priority += dividend_flag.astype(float) * 6.0
        scan_priority += cycle.ne("").astype(float) * 3.0
        scan_priority += (us_like & income_name_hint & cycle.eq("")).astype(float) * 1.4

    if selected_cycles:
        cycle_bonus_map = {"월배당": 4.0, "분기배당": 3.0, "반기배당": 2.0, "연배당": 1.0}
        scan_priority += cycle.isin(selected_cycles).astype(float) * 10.0
        scan_priority += cycle.map(cycle_bonus_map).fillna(0.0)
        if "월배당" in selected_cycles:
            monthly_hint = name_series.str.contains(r"MONTHLY|DIVIDEND|INCOME|YIELD|REIT", regex=True)
            scan_priority += (us_like & cycle.eq("") & monthly_hint).astype(float) * 2.2

    if selected_conditions:
        if "강한 상승 구조 (5>20>60>120)" in selected_conditions:
            scan_priority += dividend_flag.astype(float) * 0.6
        if "일반 상승 구조 (20>60>120)" in selected_conditions:
            scan_priority += cycle.ne("").astype(float) * 0.4
        if "하락 구조 제외 (주가<120MA 제거)" in selected_conditions:
            scan_priority += etf_like.astype(float) * 0.2

    out["_scan_priority"] = scan_priority
    sort_cols = ["_scan_priority"]
    ascending = [False]
    if "market" in out.columns:
        sort_cols.append("market")
        ascending.append(True)
    if "ticker" in out.columns:
        sort_cols.append("ticker")
        ascending.append(True)
    out = out.sort_values(sort_cols, ascending=ascending, kind="stable").reset_index(drop=True)
    return out.drop(columns=["_scan_priority"], errors="ignore")


def apply_prefilter_pipeline(universe: pd.DataFrame, ctx: Dict[str, object]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    out = universe.copy()
    counts = {"pre_input": len(out)}
    strict_metadata_prefilter = bool(ctx.get("strict_metadata_prefilter", True))
    out = prefilter_universe_fast(
        out,
        str(ctx.get("etf_mode", "ETF 포함")),
        dividend_only=False,
        dividend_cycles=[],
        strict_metadata_prefilter=strict_metadata_prefilter,
    )
    counts["pre_etf"] = len(out)
    out = prefilter_universe_fast(
        out,
        "ETF 포함",
        dividend_only=bool(ctx.get("dividend_only", False)),
        dividend_cycles=[],
        strict_metadata_prefilter=strict_metadata_prefilter,
    )
    counts["pre_dividend_only"] = len(out)
    out = prefilter_universe_fast(
        out,
        "ETF 포함",
        dividend_only=False,
        dividend_cycles=list(ctx.get("dividend_cycles", [])),
        strict_metadata_prefilter=strict_metadata_prefilter,
    )
    counts["pre_dividend_cycle"] = len(out)
    if ("price" in out.columns) or ("price_krw" in out.columns):
        out = apply_price_filter(out, float(ctx.get("min_price", 0.0)), float(ctx.get("max_price", 0.0)))
    counts["pre_price"] = len(out)
    out = prioritize_scan_candidates(out, ctx)
    return out, counts


def apply_hard_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    # Missing boolean flags should not be treated as hard-fail.
    def ok(col: str) -> pd.Series:
        return ~pd.to_numeric(df.get(col, False), errors="coerce").fillna(0).astype(bool)

    return df[
        ok("is_spac")
        & ok("is_etn")
        & ok("is_leveraged_etf")
        & ok("is_inverse_etf")
        & ok("is_single_stock_etf")
        & ok("is_warrant_rights")
        & ok("is_low_liquidity")
        & ok("is_untradeable")
        & ok("is_preferred_like")
        & ok("is_low_price")
        & ok("is_new_listing")
    ]


def apply_etf_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    mode_norm = normalize_etf_mode(mode)
    if mode_norm == "ETF 만":
        return df[df["is_etf"] == True]
    if mode_norm == "ETF 미포함":
        return df[df["is_etf"] == False]
    return df


def apply_dividend_filter(df: pd.DataFrame, dividend_only: bool, dividend_cycles: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    dy = _numeric_series(out, "dividend_yield_pct")
    cycle = out.get("dividend_cycle")
    has_cycle = cycle.notna() if isinstance(cycle, pd.Series) else pd.Series(False, index=out.index)
    has_div_flag = pd.Series(False, index=out.index)
    if "is_dividend" in out.columns:
        has_div_flag = _truthy_flag_series(out, "is_dividend")
    has_div = (dy > 0).fillna(False) | has_cycle | has_div_flag
    if dividend_only:
        out = out[has_div]
    selected_cycles = [str(x).strip() for x in dividend_cycles if str(x).strip()]
    if selected_cycles and "dividend_cycle" in out.columns:
        out = out[out["dividend_cycle"].isin(selected_cycles)]
    return out


def apply_valuation_filters(
    df: pd.DataFrame,
    per_max: float = 0.0,
    roe_min: float = 0.0,
    per_roe_max: float = 0.0,
    ev_to_ebitda_max: float = 0.0,
) -> pd.DataFrame:
    out = df.copy()
    if per_max > 0:
        per = _numeric_series(out, "per")
        out = out[per.notna() & (per <= float(per_max))]
    if roe_min > 0:
        roe = _numeric_series(out, "roe_pct")
        out = out[roe.notna() & (roe >= float(roe_min))]
    if per_roe_max > 0:
        per_roe = _numeric_series(out, "per_roe_ratio")
        out = out[per_roe.notna() & (per_roe <= float(per_roe_max))]
    if ev_to_ebitda_max > 0:
        ev_to_ebitda = _numeric_series(out, "ev_to_ebitda")
        out = out[ev_to_ebitda.notna() & (ev_to_ebitda <= float(ev_to_ebitda_max))]
    return out


def apply_condition_list(df: pd.DataFrame, selected: List[str]) -> pd.DataFrame:
    out = df.copy()
    for cond in selected:
        if cond == "1) 주가 > 120MA":
            out = out[out["price_above_120"] == True]
        elif cond == "2) 60MA 우상향":
            out = out[out["ma60_up"] == True]
        elif cond == "3) 20MA 눌림목":
            out = out[out["pullback_20"] == True]
        elif cond == "4) 거래량 증가":
            out = out[out["vol_surge"] == True]
        elif cond == "5) 박스권 돌파":
            out = out[out["box_breakout"] == True]
        elif cond == "강한 상승 구조 (5>20>60>120)":
            out = out[out["strong_trend"] == True]
        elif cond == "일반 상승 구조 (20>60>120)":
            out = out[out["general_trend"] == True]
        elif cond == "하락 구조 제외 (주가<120MA 제거)":
            out = out[out["down_trend"] == False]
        elif cond == "단기 가속 (5MA>20MA)":
            out = out[out["cond_5_over_20"] == True]
        elif cond == "중장기 강세 (60MA>120MA)":
            out = out[out["cond_60_over_120"] == True]
    return out


def apply_tf_ma_rules(df: pd.DataFrame, rules: List[Tuple[str, str, int]]) -> pd.DataFrame:
    out = df.copy()
    for tf, op, p in rules:
        col = f"{tf}_ma{p}"
        if col not in out.columns:
            continue
        price_num = pd.to_numeric(out["price"], errors="coerce")
        ma_num = pd.to_numeric(out[col], errors="coerce")
        if op == ">":
            # If MA is missing, pass through (do not filter out).
            out = out[ma_num.isna() | (price_num > ma_num)]
        else:
            # If MA is missing, pass through (do not filter out).
            out = out[ma_num.isna() | (price_num < ma_num)]
    return out


def apply_ma_near_filter(df: pd.DataFrame, tf: str, periods: List[int], pct: float) -> pd.DataFrame:
    if not periods:
        return df
    out = df.copy()
    tol = pct / 100.0
    cond = pd.Series(False, index=out.index)
    price_num = pd.to_numeric(out["price"], errors="coerce")
    for p in periods:
        col = f"{tf}_ma{p}"
        if col not in out.columns:
            continue
        ma_num = pd.to_numeric(out[col], errors="coerce")
        part = ma_num.notna() & (ma_num != 0) & ((price_num - ma_num).abs() / ma_num <= tol)
        cond = cond | part
    return out[cond]


def enrich_score(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        if "score" not in out.columns:
            out["score"] = pd.Series(dtype=float)
        if "priority_bucket" not in out.columns:
            out["priority_bucket"] = pd.Series(dtype=float)
        if "dividend_yield_pct" not in out.columns:
            out["dividend_yield_pct"] = pd.Series(dtype=float)
        return out
    out["dividend_yield_pct"] = _numeric_series(out, "dividend_yield_pct").fillna(0.0)

    def market_rank(col: str, ascending: bool, neutral: float = 0.5) -> pd.Series:
        num = _numeric_series(out, col)
        if "market" in out.columns:
            ranked = num.groupby(out["market"]).rank(pct=True, ascending=ascending)
        else:
            ranked = num.rank(pct=True, ascending=ascending)
        return ranked.fillna(neutral)

    out["yield_rank"] = market_rank("dividend_yield_pct", ascending=True, neutral=0.0)
    cycle_bonus = out.get("dividend_cycle", pd.Series("", index=out.index)).fillna("").map(
        {"월배당": 1.0, "분기배당": 0.8, "반기배당": 0.55, "연배당": 0.35}
    ).fillna(0.0)
    has_dividend = ((out["dividend_yield_pct"] > 0).fillna(False) | cycle_bonus.gt(0)).astype(float)
    out["dividend_score"] = ((out["yield_rank"] * 0.7) + (cycle_bonus * 0.2) + (has_dividend * 0.1)).clip(0, 1)

    out["trend_score"] = (
        out["price_above_120"].fillna(False).astype(float) * 0.16
        + out["ma60_up"].fillna(False).astype(float) * 0.14
        + out["strong_trend"].fillna(False).astype(float) * 0.26
        + out["general_trend"].fillna(False).astype(float) * 0.10
        + out["pullback_20"].fillna(False).astype(float) * 0.12
        + out["vol_surge"].fillna(False).astype(float) * 0.08
        + out["box_breakout"].fillna(False).astype(float) * 0.14
    ).clip(0, 1)

    valuation_parts = pd.concat(
        [
            market_rank("per", ascending=False, neutral=0.45),
            market_rank("roe_pct", ascending=True, neutral=0.45),
            market_rank("per_roe_ratio", ascending=False, neutral=0.45),
            market_rank("ev_to_ebitda", ascending=False, neutral=0.45),
        ],
        axis=1,
    )
    out["valuation_score"] = valuation_parts.mean(axis=1).fillna(0.45).clip(0, 1)
    out["liquidity_score"] = market_rank("avg_traded_value_krw_20", ascending=True, neutral=0.35).clip(0, 1)
    if "sector_strength_score" in out.columns:
        sector_norm = (pd.to_numeric(out["sector_strength_score"], errors="coerce").fillna(0.0) / 100.0).clip(0, 1)
    else:
        sector_norm = pd.Series(0.0, index=out.index)
    out["priority_bucket"] = (
        out["price_above_120"].fillna(False).astype(int) * 2
        + out["strong_trend"].fillna(False).astype(int) * 2
        + out["ma60_up"].fillna(False).astype(int)
        + out["general_trend"].fillna(False).astype(int)
        + has_dividend.astype(int)
        + cycle_bonus.ge(0.8).astype(int)
        + out["valuation_score"].ge(0.65).astype(int)
    )
    out["search_priority"] = (
        out["priority_bucket"].astype(float)
        + out["dividend_score"].ge(0.72).astype(float) * 1.2
        + out["trend_score"].ge(0.55).astype(float) * 1.5
        + out["valuation_score"].ge(0.60).astype(float) * 0.9
        + cycle_bonus.ge(0.8).astype(float) * 0.8
        + out["liquidity_score"].ge(0.55).astype(float) * 0.4
    )
    out["score"] = (
        (out["trend_score"] * 0.42)
        + (out["dividend_score"] * 0.24)
        + (out["valuation_score"] * 0.19)
        + (sector_norm * 0.10)
        + (out["liquidity_score"] * 0.05)
    ) * 100
    out["score"] = out["score"].round(2)
    return out


def main() -> None:
    fx_preview = get_usdkrw()
    st.set_page_config(page_title="Global Stock Screener", layout="wide")
    st.markdown(
        """
        <style>
        .hero {padding:14px 18px;border-radius:14px;background:linear-gradient(120deg,#0f172a 0%,#1e293b 50%,#0b6b5c 100%);color:#f8fafc;margin-bottom:14px;}
        .hero h1{margin:0;font-size:1.6rem;}
        .hero p{margin:4px 0 0 0;opacity:.92;}
        div[data-testid="stButton"] button[kind="primary"]{
          background-color:#b91c1c !important;
          border-color:#7f1d1d !important;
          color:#fff !important;
        }
        div[data-testid="stButton"] button[kind="primary"]:hover{
          background-color:#991b1b !important;
        }
        </style>
        <div class='hero'>
          <h1>Global Stock Screener</h1>
          <p>일/주/월 이평 필터 + 조건조합 저장 + 배당/추세 점수화</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if "saved_filters" not in st.session_state:
        st.session_state["saved_filters"] = load_saved_filters()
    if "krx_api_key" not in st.session_state:
        st.session_state["krx_api_key"] = load_krx_api_key()
    if "active_filter" not in st.session_state:
        st.session_state["active_filter"] = None
    if "active_filter_name" not in st.session_state:
        st.session_state["active_filter_name"] = ""
    if "last_filtered" not in st.session_state:
        st.session_state["last_filtered"] = None
    if "last_summary" not in st.session_state:
        st.session_state["last_summary"] = ""
    if "pending_filter_name" not in st.session_state:
        st.session_state["pending_filter_name"] = ""
    defaults = {
        "ui_market_mode": "통합(KR+US)",
        "ui_etf_mode": "ETF 포함",
        "ui_dividend_only": False,
        "ui_dividend_cycles": [],
        "ui_min_price_text": "0",
        "ui_max_price_text": "0",
        "ui_selected_conditions": [],
        "ui_near_tf_label": "사용 안함",
        "ui_near_periods": [],
        "ui_near_pct": 3.0,
        "ui_per_max": 0.0,
        "ui_roe_min": 0.0,
        "ui_per_roe_max": 0.0,
        "ui_ev_to_ebitda_max": 0.0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    for rule_key in ["rule_d20", "rule_d60", "rule_d120", "rule_w20", "rule_w60", "rule_w120", "rule_m20", "rule_m60", "rule_m120"]:
        if rule_key not in st.session_state:
            st.session_state[rule_key] = "패스"

    pending_filter_name = str(st.session_state.get("pending_filter_name", "")).strip()
    if pending_filter_name:
        cfg = st.session_state.get("saved_filters", {}).get(pending_filter_name)
        if isinstance(cfg, dict):
            rule_map = cfg.get("tf_rule_map", {})
            st.session_state["ui_market_mode"] = str(cfg.get("market_mode", "통합(KR+US)"))
            st.session_state["ui_etf_mode"] = normalize_etf_mode(str(cfg.get("etf_mode", "ETF 포함")))
            st.session_state["ui_dividend_only"] = bool(cfg.get("dividend_only", False))
            st.session_state["ui_dividend_cycles"] = [str(x) for x in cfg.get("dividend_cycles", []) if str(x).strip()]
            st.session_state["ui_min_price_text"] = str(cfg.get("min_price", 0.0))
            st.session_state["ui_max_price_text"] = str(cfg.get("max_price", 0.0))
            st.session_state["ui_selected_conditions"] = [str(x) for x in cfg.get("selected_conditions", []) if str(x).strip()]
            st.session_state["ui_near_tf_label"] = str(cfg.get("near_tf_label", "사용 안함"))
            st.session_state["ui_near_periods"] = [int(x) for x in cfg.get("near_periods", []) if str(x).isdigit()]
            st.session_state["ui_near_pct"] = float(cfg.get("near_pct", 3.0))
            st.session_state["ui_per_max"] = float(cfg.get("per_max", 0.0))
            st.session_state["ui_roe_min"] = float(cfg.get("roe_min", 0.0))
            st.session_state["ui_per_roe_max"] = float(cfg.get("per_roe_max", 0.0))
            st.session_state["ui_ev_to_ebitda_max"] = float(cfg.get("ev_to_ebitda_max", 0.0))
            for map_key, state_key in [
                ("D_20", "rule_d20"),
                ("D_60", "rule_d60"),
                ("D_120", "rule_d120"),
                ("W_20", "rule_w20"),
                ("W_60", "rule_w60"),
                ("W_120", "rule_w120"),
                ("M_20", "rule_m20"),
                ("M_60", "rule_m60"),
                ("M_120", "rule_m120"),
            ]:
                st.session_state[state_key] = str(rule_map.get(map_key, "패스"))
            st.session_state["active_filter"] = cfg
            st.session_state["active_filter_name"] = pending_filter_name
        st.session_state["pending_filter_name"] = ""

    condition_options = [
        "1) 주가 > 120MA",
        "2) 60MA 우상향",
        "3) 20MA 눌림목",
        "4) 거래량 증가",
        "강한 상승 구조 (5>20>60>120)",
        "일반 상승 구조 (20>60>120)",
        "하락 구조 제외 (주가<120MA 제거)",
        "단기 가속 (5MA>20MA)",
        "중장기 강세 (60MA>120MA)",
    ]

    with st.sidebar:
        st.header("기본 옵션")
        st.subheader("필터 조합 저장")
        save_name = st.text_input("저장 이름")
        saved_keys = list(st.session_state["saved_filters"].keys())
        loaded = st.selectbox("저장된 필터", ["선택 안함"] + saved_keys, index=0)
        save_col, load_col, delete_col = st.columns(3)
        with save_col:
            save_clicked = st.button("저장")
        with load_col:
            load_clicked = loaded != "선택 안함" and st.button("불러오기")
        with delete_col:
            delete_clicked = loaded != "선택 안함" and st.button("삭제")
        if load_clicked:
            st.session_state["pending_filter_name"] = loaded
            st.rerun()
        if delete_clicked:
            st.session_state["saved_filters"].pop(loaded, None)
            save_saved_filters(st.session_state["saved_filters"])
            if st.session_state.get("active_filter_name") == loaded:
                st.session_state["active_filter"] = None
                st.session_state["active_filter_name"] = ""
            st.session_state["pending_filter_name"] = ""
            st.success(f"삭제 완료: {loaded}")
            st.rerun()

        st.divider()
        top_col1, top_col2 = st.columns([1.35, 1.35])
        with top_col1:
            market_mode = st.selectbox("시장 모드", ["국장(KR)", "미장(US)", "통합(KR+US)"], key="ui_market_mode")
        with top_col2:
            etf_mode = st.selectbox("ETF 모드", ["ETF 포함", "ETF 미포함", "ETF 만"], key="ui_etf_mode")
        dividend_only = st.checkbox("배당만", key="ui_dividend_only")
        dividend_cycles = st.multiselect("배당 주기", ["월배당", "분기배당", "반기배당", "연배당"], key="ui_dividend_cycles")
        if market_mode == "국장(KR)":
            markets = ["KR"]
        elif market_mode == "미장(US)":
            markets = ["US"]
        else:
            markets = ["KR", "US"]
        if market_mode == "통합(KR+US)":
            st.caption("권장: 국장/미장은 분리 스캔이 더 안정적입니다.")

        st.divider()
        st.subheader("가격 범위 (KRW)")
        pcol1, pcol_mid, pcol2 = st.columns([1, 0.2, 1])
        with pcol1:
            min_price_text = st.text_input("최소값", key="ui_min_price_text")
        with pcol_mid:
            st.markdown("<div style='text-align:center; margin-top:30px;'>~</div>", unsafe_allow_html=True)
        with pcol2:
            max_price_text = st.text_input("최대값 (0=무제한)", key="ui_max_price_text")

        min_price = 0.0
        max_price = 0.0
        try:
            min_price = float(str(min_price_text).strip() or "0")
            max_price = float(str(max_price_text).strip() or "0")
        except Exception:
            st.warning("가격 입력 형식이 올바르지 않습니다. 숫자만 입력하세요. 예: 10000 / 50000")
            min_price, max_price = 0.0, 0.0
        if fx_preview and fx_preview > 0:
            usd_min = min_price / fx_preview
            usd_max = max_price / fx_preview if max_price > 0 else 0
            if max_price > 0:
                st.caption(f"대략 USD 환산: ${usd_min:.2f} ~ ${usd_max:.2f}")
            else:
                st.caption(f"대략 USD 환산: ${usd_min:.2f} 이상")

        st.divider()
        st.subheader("일/주/월 이평 필터")
        st.caption("예전 구성처럼 위에서 아래로 빠르게 설정할 수 있게 유지하되, > / < 비교는 그대로 남겼습니다.")
        rule_options = ["패스", ">", "<"]
        tf_rule_map = {}
        st.markdown("일봉")
        d1, d2, d3 = st.columns(3)
        with d1:
            tf_rule_map["D_20"] = st.selectbox("20MA", rule_options, key="rule_d20")
        with d2:
            tf_rule_map["D_60"] = st.selectbox("60MA", rule_options, key="rule_d60")
        with d3:
            tf_rule_map["D_120"] = st.selectbox("120MA", rule_options, key="rule_d120")
        st.markdown("주봉")
        w1, w2, w3 = st.columns(3)
        with w1:
            tf_rule_map["W_20"] = st.selectbox("20WMA", rule_options, key="rule_w20")
        with w2:
            tf_rule_map["W_60"] = st.selectbox("60WMA", rule_options, key="rule_w60")
        with w3:
            tf_rule_map["W_120"] = st.selectbox("120WMA", rule_options, key="rule_w120")
        st.markdown("월봉")
        m1, m2, m3 = st.columns(3)
        with m1:
            tf_rule_map["M_20"] = st.selectbox("20MMA", rule_options, key="rule_m20")
        with m2:
            tf_rule_map["M_60"] = st.selectbox("60MMA", rule_options, key="rule_m60")
        with m3:
            tf_rule_map["M_120"] = st.selectbox("120MMA", rule_options, key="rule_m120")

        st.divider()
        st.subheader("기본 조건 필터")
        selected_conditions = st.multiselect("조건 선택", condition_options, key="ui_selected_conditions")

        st.divider()
        st.subheader("재무 조건 필터")
        v1, v2 = st.columns(2)
        with v1:
            per_max = st.number_input("PER 최대 (0=사용 안함)", min_value=0.0, step=1.0, key="ui_per_max")
            per_roe_max = st.number_input("PER/ROE 최대 (0=사용 안함)", min_value=0.0, step=0.1, format="%.2f", key="ui_per_roe_max")
        with v2:
            roe_min = st.number_input("ROE 최소 % (0=사용 안함)", min_value=0.0, step=1.0, key="ui_roe_min")
            ev_to_ebitda_max = st.number_input("EV/EBITDA 최대 (0=사용 안함)", min_value=0.0, step=0.5, format="%.2f", key="ui_ev_to_ebitda_max")
        st.caption("ROE, PER, PER/ROE, EV/EBITDA는 유지되고 있으며 기술적 필터 후 마지막 단계에서만 보강합니다.")

        st.divider()
        st.subheader("이평 근접도 필터")
        near_tf_label = st.selectbox("근접도 기준 봉", ["사용 안함", "일봉", "주봉", "월봉"], key="ui_near_tf_label")
        near_periods = st.multiselect("근접 체크 MA", [20, 60, 120], key="ui_near_periods")
        near_pct = st.slider("근접 허용 오차(%)", 0.5, 20.0, step=0.5, key="ui_near_pct")

        st.divider()
        with st.expander("고급 옵션", expanded=False):
            workers = st.slider("병렬 스레드 (권장 4~6)", 2, 12, 5, 1)
            speed_mode = st.toggle("고속 모드 (기본 ON)", value=True, help="ON: 속도 우선, 일부 상세 정보 호출 최소화")
            history_period = st.selectbox("조회 기간", ["2y", "3y", "5y"], index=0, help="고속 모드에서 짧을수록 빠름")
            sector_scoring = st.toggle("섹터 강도 점수화", value=True, help="섹터별 52주 신고가 개수 기반 가점")
            sector_pick_mode = st.selectbox("섹터 보강 대상", ["상위", "랜덤"], index=0)
            sector_pick_label = st.selectbox("섹터 보강 개수", ["상위 50", "상위 100", "상위 200", "상위 400", "전체"], index=1)
            sector_limit_map = {"상위 50": 50, "상위 100": 100, "상위 200": 200, "상위 400": 400, "전체": 0}
            sector_fetch_limit = sector_limit_map.get(sector_pick_label, 100)
            sector_workers = st.slider("섹터 수집 스레드", 1, 8, 3, 1)
            sector_max_seconds = st.slider("섹터 수집 최대시간(초)", 30, 1800, 120, 30)
            max_symbols = st.number_input("최대 스캔 수 (0=전체)", 0, 20000, 0, 100)

        st.divider()
        krx_status = "Y" if bool(st.session_state.get("krx_api_key", "").strip()) else "N"
        with st.expander(f"KRX Open API 설정 [KEY:{krx_status}]", expanded=False):
            key_input = st.text_input("KRX API 키", value=st.session_state.get("krx_api_key", ""), type="password")
            st.caption(f"저장된 키: {mask_key(st.session_state.get('krx_api_key', ''))}")
            k1, k2, k3 = st.columns(3)
            with k1:
                if st.button("키 저장"):
                    if save_krx_api_key(key_input):
                        st.session_state["krx_api_key"] = key_input.strip()
                        try:
                            get_kr_universe.clear()
                        except Exception:
                            pass
                        st.success("키 저장 완료")
                        st.rerun()
                    else:
                        st.error("키 저장 실패")
            with k2:
                if st.button("연결 테스트"):
                    ok, msg = test_krx_api_key(key_input)
                    st.session_state["krx_api_test"] = msg
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
            with k3:
                if st.button("키 삭제"):
                    save_krx_api_key("")
                    st.session_state["krx_api_key"] = ""
                    try:
                        get_kr_universe.clear()
                    except Exception:
                        pass
                    st.success("키 삭제 완료")
                    st.rerun()
            if st.button("KR 목록 캐시 새로고침"):
                try:
                    get_kr_universe.clear()
                except Exception:
                    pass
                st.success("KR 목록 캐시를 초기화했습니다.")
            if st.button("KR ETF 캐시 새로고침"):
                etf_universe = get_kr_etf_universe_ketf(force_refresh=True)
                etf_meta = get_kr_etf_dividend_meta(force_refresh=True)
                try:
                    get_kr_universe.clear()
                except Exception:
                    pass
                st.success(f"KR ETF 캐시 갱신 완료 (ETF {len(etf_universe):,}개 / 배당메타 {len(etf_meta):,}개)")
            if st.session_state.get("krx_api_test"):
                st.caption(f"마지막 테스트: {st.session_state['krx_api_test']}")
            st.caption("KR 종목 CSV 업로드 (종목코드/ticker 컬럼 필수)")
            up = st.file_uploader("KR 시드 CSV", type=["csv"], key="kr_seed_upload")
            if up is not None and st.button("KR CSV 저장"):
                ok, msg = save_kr_seed_universe(up)
                if ok:
                    try:
                        get_kr_universe.clear()
                    except Exception:
                        pass
                    st.success(msg)
                else:
                    st.error(msg)

        if save_clicked and save_name.strip():
            st.session_state["saved_filters"][save_name.strip()] = {
                "market_mode": market_mode,
                "selected_conditions": selected_conditions,
                "tf_rule_map": tf_rule_map,
                "near_tf_label": near_tf_label,
                "near_periods": near_periods,
                "near_pct": near_pct,
                "min_price": min_price,
                "max_price": max_price,
                "etf_mode": etf_mode,
                "dividend_only": dividend_only,
                "dividend_cycles": dividend_cycles,
                "per_max": per_max,
                "roe_min": roe_min,
                "per_roe_max": per_roe_max,
                "ev_to_ebitda_max": ev_to_ebitda_max,
            }
            if save_saved_filters(st.session_state["saved_filters"]):
                st.session_state["active_filter"] = st.session_state["saved_filters"][save_name.strip()]
                st.session_state["active_filter_name"] = save_name.strip()
                st.success(f"저장 완료: {save_name.strip()}")
                st.rerun()
            else:
                st.warning("세션에는 저장됐지만 파일 저장에는 실패했습니다.")

    active_cfg = st.session_state.get("active_filter")
    if active_cfg:
        st.info(f"현재 적용 중인 저장 필터: {st.session_state.get('active_filter_name', '')}")

    if not markets:
        st.warning("시장을 1개 이상 선택하세요.")
        return

    source_errors: List[str] = []
    us_df = pd.DataFrame()
    kr_df = pd.DataFrame()

    if "US" in markets:
        try:
            us_df = get_us_universe()
        except Exception as e:
            source_errors.append(f"US 목록 로딩 실패: {e}")
            us_df = fallback_us_universe()
            st.warning("US 목록 소스 연결 실패로 fallback 목록(샘플 티커) 사용 중입니다.")

    if "KR" in markets:
        try:
            kr_df = get_kr_universe(st.session_state.get("krx_api_key", ""))
        except Exception as e:
            source_errors.append(f"KR 목록 로딩 실패: {e}")
            kr_df = pd.DataFrame()

    if "KR" in markets and kr_df.empty:
        st.warning("KRX 목록을 가져오지 못했습니다. 네트워크/거래소 응답 상태를 'KRX 연결 체크'로 먼저 확인하세요.")
    elif "KR" in markets and (not kr_df.empty) and ("_source" in kr_df.columns) and (kr_df["_source"].eq("fallback_kr_sample").all()):
        st.warning("KR 실데이터 소스 연결 실패로 fallback KR 샘플 목록으로 실행 중입니다.")
        with st.expander("KR 소스 진단 보기", expanded=False):
            diag_rows = diagnose_kr_sources(st.session_state.get("krx_api_key", ""))
            st.dataframe(pd.DataFrame(diag_rows), use_container_width=True, hide_index=True)
    if source_errors:
        st.error("데이터 소스 오류 감지:\n- " + "\n- ".join(source_errors))

    universe = pd.concat([us_df, kr_df], ignore_index=True)
    if max_symbols > 0:
        max_n = int(max_symbols)
        if len(markets) >= 2 and not us_df.empty and not kr_df.empty:
            # Keep both markets represented when limiting scan count.
            per = max(1, max_n // len(markets))
            chunks = []
            chunks.append(us_df.head(per))
            chunks.append(kr_df.head(per))
            limited = pd.concat(chunks, ignore_index=True)
            remain = max_n - len(limited)
            if remain > 0:
                extra_us = us_df.iloc[len(chunks[0]):]
                extra_kr = kr_df.iloc[len(chunks[1]):]
                extra = pd.concat([extra_us, extra_kr], ignore_index=True).head(remain)
                limited = pd.concat([limited, extra], ignore_index=True)
            universe = limited.head(max_n)
        else:
            universe = universe.head(max_n)

    fx = get_usdkrw()
    c1, c2, c3 = st.columns(3)
    c1.metric("대상 종목 수", f"{len(universe):,}")
    c2.metric("US 종목", f"{len(us_df):,}")
    c3.metric("USD/KRW", f"{fx:,.2f}" if fx else "N/A")
    us_source = (us_df["_source"].value_counts().index[0] if (not us_df.empty and "_source" in us_df.columns) else "none")
    kr_source = (kr_df["_source"].value_counts().index[0] if (not kr_df.empty and "_source" in kr_df.columns) else "none")
    st.caption(f"목록 소스: US={us_source}, KR={kr_source}")
    force_exclusion_notice = st.empty()
    force_exclusion_notice.info("강제 제외: SPAC / ETN / 레버리지ETF / 인버스ETF / 싱글스톡ETF / 워런트·Rights / 저유동성 / 거래불가 / 우선주류 / 초저가 / 신규상장")
    diag_col1, diag_col2 = st.columns(2)
    with diag_col1:
        if st.button("실행 전 KRX 상태 확인"):
            ok, msg = check_krx_connection()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
    with diag_col2:
        st.empty()

    if st.button("전체 스캔 실행", type="primary"):
        force_exclusion_notice.empty()
        if universe.empty:
            st.error("대상 종목이 없습니다.")
            return

        started = datetime.now()
        pipeline_t0 = time.time()

        run_selected_conditions = list(active_cfg.get("selected_conditions", [])) if active_cfg else selected_conditions
        run_tf_rule_map = active_cfg.get("tf_rule_map", {}) if active_cfg else tf_rule_map
        run_near_tf_label = active_cfg.get("near_tf_label", "사용 안함") if active_cfg else near_tf_label
        run_near_periods = active_cfg.get("near_periods", []) if active_cfg else near_periods
        run_near_pct = float(active_cfg.get("near_pct", 3.0)) if active_cfg else near_pct
        run_min_price = float(active_cfg.get("min_price", 0.0)) if active_cfg else min_price
        run_max_price = float(active_cfg.get("max_price", 0.0)) if active_cfg else max_price
        run_etf_mode = active_cfg.get("etf_mode", etf_mode) if active_cfg else etf_mode
        run_dividend_only = bool(active_cfg.get("dividend_only", False)) if active_cfg else bool(dividend_only)
        run_dividend_cycles = list(active_cfg.get("dividend_cycles", [])) if active_cfg else list(dividend_cycles)
        run_per_max = float(active_cfg.get("per_max", 0.0)) if active_cfg else float(per_max)
        run_roe_min = float(active_cfg.get("roe_min", 0.0)) if active_cfg else float(roe_min)
        run_per_roe_max = float(active_cfg.get("per_roe_max", 0.0)) if active_cfg else float(per_roe_max)
        run_ev_to_ebitda_max = float(active_cfg.get("ev_to_ebitda_max", 0.0)) if active_cfg else float(ev_to_ebitda_max)
        dividend_filter_active = bool(run_dividend_only or run_dividend_cycles)
        valuation_active = any(v > 0 for v in [run_per_max, run_roe_min, run_per_roe_max, run_ev_to_ebitda_max])
        run_workers = workers
        run_fetch_info = not speed_mode
        run_history_period = adjust_history_period(
            history_period, run_tf_rule_map, run_near_tf_label, run_near_periods
        )
        if run_history_period != history_period:
            st.info(f"선택한 장기 이평 조건으로 조회 기간을 자동 확장했습니다: {history_period} -> {run_history_period}")

        filter_ctx = build_filter_context(
            min_price=run_min_price,
            max_price=run_max_price,
            etf_mode=run_etf_mode,
            dividend_only=run_dividend_only,
            dividend_cycles=run_dividend_cycles,
            selected_conditions=run_selected_conditions,
            tf_rule_map=run_tf_rule_map,
            near_tf_label=run_near_tf_label,
            near_periods=run_near_periods,
            near_pct=run_near_pct,
        )
        fast_universe, prefilter_counts = apply_prefilter_pipeline(universe, filter_ctx)
        scan_stats_prefilter = {
            "prefilter_input_count": float(prefilter_counts.get("pre_input", len(universe))),
            "prefilter_etf_count": float(prefilter_counts.get("pre_etf", len(fast_universe))),
            "prefilter_dividend_cycle_count": float(prefilter_counts.get("pre_dividend_cycle", len(fast_universe))),
            "prefilter_dividend_only_count": float(prefilter_counts.get("pre_dividend_only", len(fast_universe))),
            "prefilter_price_count": float(prefilter_counts.get("pre_price", len(fast_universe))),
        }
        if len(fast_universe) > 1500 and run_workers > 6:
            run_workers = 6
            st.info("대규모 스캔에서 응답성과 429 완화를 위해 병렬 스레드를 6으로 자동 조정했습니다.")
        raw, scan_stats = run_scan(
            fast_universe,
            fx,
            run_workers,
            fetch_info=run_fetch_info,
            history_period=run_history_period,
        )
        scan_stats.update(scan_stats_prefilter)
        scan_stats["t_scan_sec"] = round(time.time() - pipeline_t0, 2)
        sector_stats = {}
        if sector_scoring and not raw.empty:
            t_sector0 = time.time()
            fetch_limit = 0 if int(sector_fetch_limit) == 0 else int(sector_fetch_limit)
            effective_sector_seconds = int(sector_max_seconds)
            if speed_mode:
                # For large scans, auto-extend sector fetch window to avoid early timeout.
                auto_seconds = 120 + int(len(raw) / 30)
                effective_sector_seconds = max(effective_sector_seconds, min(900, auto_seconds))
            st.info("가격 스캔 완료, 섹터 강도 점수 계산 중입니다...")
            raw, sector_stats = enrich_sector_strength(
                raw,
                sector_workers=sector_workers,
                sector_fetch_limit=fetch_limit,
                sector_max_seconds=effective_sector_seconds,
                sector_pick_mode=sector_pick_mode,
            )
            sector_stats["sector_max_seconds_used"] = float(effective_sector_seconds)
            sector_stats["t_sector_sec"] = round(time.time() - t_sector0, 2)
            scan_stats.update(sector_stats)
        st.session_state["scan_stats"] = scan_stats
        if raw.empty:
            st.warning("결과가 비어 있습니다.")
            return
        pre = len(raw)
        filtered = raw
        scan_stats["raw_us_count"] = float((raw["market"] == "US").sum()) if "market" in raw.columns else 0.0
        scan_stats["raw_kr_count"] = float((raw["market"] == "KR").sum()) if "market" in raw.columns else 0.0

        # Fast-reject order: price(KRW basis) -> ETF mode -> hard exclusions -> detailed conditions
        t_filter0 = time.time()
        price_basis = filtered["price_krw"].where(filtered["price_krw"].notna(), filtered["price"])
        filtered = filtered[price_basis >= float(run_min_price)]
        if run_max_price > 0:
            price_basis = filtered["price_krw"].where(filtered["price_krw"].notna(), filtered["price"])
            filtered = filtered[price_basis <= float(run_max_price)]
        post_price = len(filtered)

        filtered = apply_etf_mode(filtered, run_etf_mode)
        post_etf = len(filtered)

        filtered = apply_hard_exclusions(filtered)
        post_hard = len(filtered)

        filtered = apply_dividend_filter(filtered, run_dividend_only, run_dividend_cycles)
        post_dividend = len(filtered)

        filtered = apply_condition_list(filtered, run_selected_conditions)
        post_basic = len(filtered)

        tf_rules = []
        # Speed-oriented order: Monthly -> Weekly -> Daily
        for key in ["M_20", "M_60", "M_120", "W_20", "W_60", "W_120", "D_20", "D_60", "D_120"]:
            op = run_tf_rule_map.get(key, "패스")
            if op not in [">", "<"]:
                continue
            tf, period = key.split("_")
            tf_rules.append((tf, op, int(period)))
        filtered = apply_tf_ma_rules(filtered, tf_rules)
        post_tf = len(filtered)

        if run_near_tf_label != "사용 안함" and run_near_periods:
            near_tf = {"일봉": "D", "주봉": "W", "월봉": "M"}[run_near_tf_label]
            filtered = apply_ma_near_filter(filtered, near_tf, run_near_periods, run_near_pct)
        post_near = len(filtered)
        post_value = post_near
        scan_stats["t_filter_sec"] = round(time.time() - t_filter0, 2)

        quote_stats = {}
        valuation_stats = {}
        if not filtered.empty:
            t_krfund0 = time.time()
            filtered, kr_fund_stats = enrich_kr_fundamentals(filtered)
            kr_fund_stats["t_krfund_sec"] = round(time.time() - t_krfund0, 2)
            scan_stats.update(kr_fund_stats)
        if valuation_active and not filtered.empty:
            t_value0 = time.time()
            filtered, valuation_stats = enrich_valuation_metrics(
                filtered,
                workers=min(run_workers, 6),
                timeout_sec=45 if speed_mode else 90,
                need_per=(run_per_max > 0 or run_per_roe_max > 0),
                need_roe=(run_roe_min > 0 or run_per_roe_max > 0),
                need_ev=(run_ev_to_ebitda_max > 0),
            )
            filtered = apply_valuation_filters(
                filtered,
                per_max=run_per_max,
                roe_min=run_roe_min,
                per_roe_max=run_per_roe_max,
                ev_to_ebitda_max=run_ev_to_ebitda_max,
            )
            post_value = len(filtered)
            valuation_stats["t_value_sec"] = round(time.time() - t_value0, 2)
            scan_stats.update(valuation_stats)
        if speed_mode and not filtered.empty:
            t_quote0 = time.time()
            filtered, quote_stats = enrich_quote_snapshot(filtered, fx, limit=2500)
            quote_stats["t_quote_sec"] = round(time.time() - t_quote0, 2)
            scan_stats.update(quote_stats)
        if not filtered.empty and "dividend_yield_pct" in filtered.columns:
            scan_stats["dividend_covered_pct"] = round(float(pd.to_numeric(filtered["dividend_yield_pct"], errors="coerce").notna().mean() * 100.0), 2)
        if not filtered.empty and "sector" in filtered.columns:
            usf = filtered[filtered["market"].astype(str).str.upper().eq("US")]
            if not usf.empty:
                scan_stats["sector_final_covered_pct"] = round(float((usf["sector"].fillna("Unknown") != "Unknown").mean() * 100.0), 2)
            else:
                scan_stats["sector_final_covered_pct"] = 0.0

        t_score0 = time.time()
        filtered = enrich_score(filtered)
        scan_stats["t_score_sec"] = round(time.time() - t_score0, 2)
        filtered["20일평균거래대금(억원)"] = pd.to_numeric(filtered.get("avg_traded_value_krw_20"), errors="coerce").apply(_to_eok)
        if "score" not in filtered.columns:
            filtered["score"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        if "priority_bucket" not in filtered.columns:
            filtered["priority_bucket"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        if "search_priority" not in filtered.columns:
            filtered["search_priority"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        if "dividend_yield_pct" not in filtered.columns:
            filtered["dividend_yield_pct"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        t_sort0 = time.time()
        filtered = filtered.sort_values(
            ["search_priority", "priority_bucket", "score", "dividend_yield_pct", "roe_pct"],
            ascending=[False, False, False, False, False],
            na_position="last",
        )
        scan_stats["t_sort_sec"] = round(time.time() - t_sort0, 2)
        scan_stats["final_us_count"] = float((filtered["market"] == "US").sum()) if "market" in filtered.columns else 0.0
        scan_stats["final_kr_count"] = float((filtered["market"] == "KR").sum()) if "market" in filtered.columns else 0.0
        scan_stats["t_total_sec"] = round(time.time() - pipeline_t0, 2)

        csv_ok, csv_msg = save_result_csv(filtered) if not filtered.empty else (False, "저장할 결과가 없습니다.")
        st.session_state["last_saved_csv_ok"] = csv_ok
        st.session_state["last_saved_csv_message"] = csv_msg

        elapsed = datetime.now() - started
        dividend_segment = f" -> 배당 {post_dividend:,}" if dividend_filter_active else ""
        value_segment = f" -> 재무조건 {post_value:,}" if valuation_active else ""
        st.success(
            f"{pre:,} -> 가격 {post_price:,} -> ETF {post_etf:,} -> 강제제외 {post_hard:,} "
            f"{dividend_segment} -> 기본조건 {post_basic:,} -> 봉이평 {post_tf:,} -> 근접도 {post_near:,}{value_segment} -> 최종 {len(filtered):,} "
            f"(소요 {str(elapsed).split('.')[0]})"
        )
        if len(filtered) == 0:
            st.warning("결과가 0건입니다. 위 단계별 카운트를 보고 가장 많이 줄어든 필터를 먼저 완화하세요.")
        st.session_state["last_filtered"] = filtered.copy()
        st.session_state["last_summary"] = (
            f"{pre:,} -> 가격 {post_price:,} -> ETF {post_etf:,} -> 강제제외 {post_hard:,} "
            f"{dividend_segment} -> 기본조건 {post_basic:,} -> 봉이평 {post_tf:,} -> 근접도 {post_near:,}{value_segment} -> 최종 {len(filtered):,} "
            f"(소요 {str(elapsed).split('.')[0]})"
        )

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("평균 배당률", f"{filtered['dividend_yield_pct'].dropna().mean():.2f}%" if not filtered.empty else "0.00%")
        k2.metric("120MA 상단 비율", f"{(filtered['price_above_120'].mean()*100):.1f}%" if not filtered.empty else "0.0%")
        k3.metric("거래량 증가 비율", f"{(filtered['vol_surge'].mean()*100):.1f}%" if not filtered.empty else "0.0%")
        k4.metric("최고 점수", f"{filtered['score'].max():.2f}" if not filtered.empty else "0.00")

        tabs = st.tabs(["랭킹", "시장요약", "전체데이터"])

        with tabs[0]:
            st.markdown("#### 검색 결과 리스트")
            q = st.text_input("검색(티커/종목명)", "")
            view = filtered.copy()
            if q.strip():
                s = q.strip().lower()
                view = view[view["ticker"].str.lower().str.contains(s, na=False) | view["name"].str.lower().str.contains(s, na=False)]
            view["가격표시"] = pd.to_numeric(view["price"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            view["현재가(KRW)표시"] = pd.to_numeric(view["price_krw"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            view["20일평균거래대금(억원)표시"] = pd.to_numeric(view["20일평균거래대금(억원)"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            cols = [
                "priority_bucket", "score", "market", "ticker", "name", "가격표시", "현재가(KRW)표시", "20일평균거래대금(억원)표시", "dividend_yield_pct", "dividend_cycle",
                "per", "roe_pct", "per_roe_ratio", "ev_to_ebitda",
                "sector", "sector_strength_score", "sector_new_high_count", "sector_new_high_ratio",
                "D_ma20", "D_ma60", "D_ma120", "W_ma20", "W_ma60", "W_ma120", "M_ma20", "M_ma60", "M_ma120",
                "price_above_120", "ma60_up", "pullback_20", "vol_surge", "box_breakout", "strong_trend", "general_trend",
                "listing_days"
            ]
            safe_cols = [c for c in cols if c in view.columns]
            rename_map = {
                "priority_bucket": "우선순위",
                "score": "점수",
                "market": "시장",
                "ticker": "티커",
                "name": "종목명",
                "가격표시": "가격",
                "현재가(KRW)표시": "현재가(KRW)",
                "dividend_yield_pct": "배당수익률(%)",
                "dividend_cycle": "배당주기",
                "per": "PER",
                "roe_pct": "ROE(%)",
                "per_roe_ratio": "PER/ROE",
                "ev_to_ebitda": "EV/EBITDA",
                "sector": "섹터",
                "sector_strength_score": "섹터강도점수",
                "sector_new_high_count": "섹터신고가수",
                "sector_new_high_ratio": "섹터신고가비율",
                "D_ma20": "일20선",
                "D_ma60": "일60선",
                "D_ma120": "일120선",
                "W_ma20": "주20선",
                "W_ma60": "주60선",
                "W_ma120": "주120선",
                "M_ma20": "월20선",
                "M_ma60": "월60선",
                "M_ma120": "월120선",
                "price_above_120": "120선상단",
                "ma60_up": "60선우상향",
                "pullback_20": "20선눌림",
                "vol_surge": "거래량증가",
                "box_breakout": "박스돌파",
                "strong_trend": "강한상승",
                "general_trend": "일반상승",
                "20일평균거래대금(억원)표시": "20일평균거래대금(억원)",
                "listing_days": "상장일수",
            }
            view_df = view[safe_cols].rename(columns=rename_map)
            st.dataframe(
                view_df,
                use_container_width=True,
                height=650,
                column_config={
                    "배당수익률(%)": st.column_config.NumberColumn(format="%.2f"),
                    "PER": st.column_config.NumberColumn(format="%.2f"),
                    "ROE(%)": st.column_config.NumberColumn(format="%.2f"),
                    "PER/ROE": st.column_config.NumberColumn(format="%.3f"),
                    "EV/EBITDA": st.column_config.NumberColumn(format="%.2f"),
                    "섹터강도점수": st.column_config.NumberColumn(format="%.2f"),
                    "섹터신고가비율": st.column_config.NumberColumn(format="%.4f"),
                },
            )

        with tabs[1]:
            by = filtered.groupby("market", as_index=False).agg(count=("ticker", "count"), avg_score=("score", "mean"), avg_div=("dividend_yield_pct", "mean"))
            if by.empty:
                st.info("요약 데이터 없음")
            else:
                st.bar_chart(by.set_index("market")[["count"]])
                st.dataframe(by.round(2), use_container_width=True)

        with tabs[2]:
            st.dataframe(filtered, use_container_width=True, height=700)
            last_saved_csv_ok = bool(st.session_state.get("last_saved_csv_ok"))
            last_saved_csv_message = str(st.session_state.get("last_saved_csv_message", "")).strip()
            if last_saved_csv_message:
                if last_saved_csv_ok:
                    st.caption(f"서버 CSV 저장: {last_saved_csv_message}")
                else:
                    st.caption(f"서버 CSV 저장 실패: {last_saved_csv_message}")
            csv = filtered.to_csv(index=False).encode("utf-8-sig")
            st.download_button("CSV 다운로드", csv, file_name=f"screener_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

    # Keep previous result visible even when options change (until next scan).
    elif st.session_state.get("last_filtered") is not None:
        prev = st.session_state["last_filtered"]
        if isinstance(prev, pd.DataFrame):
            if st.session_state.get("last_summary"):
                st.info(f"이전 스캔 결과 유지 중: {st.session_state['last_summary']}")
            st.dataframe(prev, use_container_width=True, height=480)

    st.divider()
    with st.expander("데이터 소스 점검", expanded=True):
        us_cache_df = load_universe_cache(US_CACHE_PATH)
        kr_cache_df = load_universe_cache(KR_CACHE_PATH)
        scan_stats = st.session_state.get("scan_stats", {})
        us_cache_time = datetime.fromtimestamp(US_CACHE_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if US_CACHE_PATH.exists() else "-"
        kr_cache_time = datetime.fromtimestamp(KR_CACHE_PATH.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if KR_CACHE_PATH.exists() else "-"
        diagnostics = [
            {
                "구분": "US 목록",
                "현재 소스": us_source,
                "현재 종목수": len(us_df),
                "캐시 종목수": len(us_cache_df),
                "캐시 갱신시각": us_cache_time,
                "연결 점검": probe_endpoint("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"),
            },
            {
                "구분": "KR 목록",
                "현재 소스": kr_source,
                "현재 종목수": len(kr_df),
                "캐시 종목수": len(kr_cache_df),
                "캐시 갱신시각": kr_cache_time,
                "연결 점검": probe_endpoint("https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"),
            },
            {
                "구분": "시세 API",
                "현재 소스": "KR: pykrx / US: yfinance",
                "현재 종목수": "-",
                "캐시 종목수": "-",
                "캐시 갱신시각": "-",
                "연결 점검": probe_endpoint("https://query1.finance.yahoo.com/v7/finance/quote?symbols=AAPL"),
            },
        ]
        st.dataframe(pd.DataFrame(diagnostics), use_container_width=True, height=220)
        if scan_stats:
            debug_rows = [
                {"항목": "스캔 입력 종목수", "값": int(scan_stats.get("scan_input", 0))},
                {"항목": "스캔 성공 종목수", "값": int(scan_stats.get("scan_success", 0))},
                {"항목": "스캔 실패(None) 수", "값": int(scan_stats.get("scan_none", 0))},
                {"항목": "스캔 예외 수", "값": int(scan_stats.get("scan_error", 0))},
                {"항목": "스캔 소요(초)", "값": scan_stats.get("scan_elapsed_sec", 0)},
                {"항목": "단계시간: 스캔(초)", "값": scan_stats.get("t_scan_sec", "-")},
                {"항목": "단계시간: 섹터(초)", "값": scan_stats.get("t_sector_sec", "-")},
                {"항목": "단계시간: 필터(초)", "값": scan_stats.get("t_filter_sec", "-")},
                {"항목": "단계시간: KR보강(초)", "값": scan_stats.get("t_krfund_sec", "-")},
                {"항목": "단계시간: 재무보강(초)", "값": scan_stats.get("t_value_sec", "-")},
                {"항목": "단계시간: 시세보강(초)", "값": scan_stats.get("t_quote_sec", "-")},
                {"항목": "단계시간: 점수(초)", "값": scan_stats.get("t_score_sec", "-")},
                {"항목": "단계시간: 정렬(초)", "값": scan_stats.get("t_sort_sec", "-")},
                {"항목": "전체 파이프라인(초)", "값": scan_stats.get("t_total_sec", "-")},
                {"항목": "사용 스레드", "값": int(scan_stats.get("workers_used", 0))},
                {"항목": "고속 모드", "값": "ON" if int(scan_stats.get("fetch_info", 0)) == 0 else "OFF"},
                {"항목": "스캔 방식", "값": "배치(yf.download)" if int(scan_stats.get("scan_mode", 0)) == 1 else "개별(Ticker.history)"},
                {"항목": "조회 기간", "값": scan_stats.get("history_period", "-")},
                {"항목": "배치 크기", "값": int(scan_stats.get("batch_size", 0)) if "batch_size" in scan_stats else "-"},
                {"항목": "적응형 감속 횟수", "값": int(scan_stats.get("adaptive_down_count", 0)) if "adaptive_down_count" in scan_stats else 0},
                {"항목": "시장별(raw) US/KR", "값": f"{int(scan_stats.get('raw_us_count', 0))}/{int(scan_stats.get('raw_kr_count', 0))}"},
                {"항목": "시장별(final) US/KR", "값": f"{int(scan_stats.get('final_us_count', 0))}/{int(scan_stats.get('final_kr_count', 0))}"},
                {"항목": "섹터 캐시 개수", "값": int(scan_stats.get("sector_cached", 0)) if "sector_cached" in scan_stats else "-"},
                {"항목": "섹터 시드 개수", "값": int(scan_stats.get("sector_seeded", 0)) if "sector_seeded" in scan_stats else "-"},
                {"항목": "섹터 신규수집", "값": int(scan_stats.get("sector_fetched", 0)) if "sector_fetched" in scan_stats else "-"},
                {"항목": "섹터 보강 모드", "값": scan_stats.get("sector_pick_mode", "-")},
                {"항목": "섹터 보강 상한", "값": int(scan_stats.get("sector_pick_limit", 0)) if "sector_pick_limit" in scan_stats else "-"},
                {"항목": "KRX 키 저장 여부", "값": "Y" if bool(st.session_state.get("krx_api_key", "").strip()) else "N"},
                {"항목": "섹터 커버율(%)", "값": scan_stats.get("sector_covered", "-")},
                {"항목": "KR 섹터 커버율(%)", "값": scan_stats.get("sector_covered_kr", "-")},
                {"항목": "섹터 수집 타임아웃", "값": int(scan_stats.get("sector_timeout", 0)) if "sector_timeout" in scan_stats else 0},
                {"항목": "섹터 수집시간 사용(초)", "값": int(scan_stats.get("sector_max_seconds_used", 0)) if "sector_max_seconds_used" in scan_stats else "-"},
                {"항목": "최종 섹터 커버율(%)", "값": scan_stats.get("sector_final_covered_pct", "-")},
                {"항목": "배당 커버율(%)", "값": scan_stats.get("dividend_covered_pct", "-")},
                {"항목": "보강 대상 수", "값": int(scan_stats.get("quote_targets", 0)) if "quote_targets" in scan_stats else "-"},
                {"항목": "배당 보강 건수", "값": int(scan_stats.get("quote_div_filled", 0)) if "quote_div_filled" in scan_stats else "-"},
                {"항목": "개별 보강 호출수", "값": int(scan_stats.get("quote_fallback_used", 0)) if "quote_fallback_used" in scan_stats else "-"},
                {"항목": "KR 배당 보강 건수", "값": int(scan_stats.get("kr_fund_div_filled", 0)) if "kr_fund_div_filled" in scan_stats else "-"},
                {"항목": "KR PER 보강 건수", "값": int(scan_stats.get("kr_per_filled", 0)) if "kr_per_filled" in scan_stats else "-"},
                {"항목": "KR ROE 보강 건수", "값": int(scan_stats.get("kr_roe_filled", 0)) if "kr_roe_filled" in scan_stats else "-"},
                {"항목": "재무 보강 대상 수", "값": int(scan_stats.get("valuation_targets", 0)) if "valuation_targets" in scan_stats else "-"},
                {"항목": "재무 보강 타임아웃 수", "값": int(scan_stats.get("valuation_timeout", 0)) if "valuation_timeout" in scan_stats else "-"},
                {"항목": "재무 PER 보강 건수", "값": int(scan_stats.get("valuation_per_filled", 0)) if "valuation_per_filled" in scan_stats else "-"},
                {"항목": "재무 ROE 보강 건수", "값": int(scan_stats.get("valuation_roe_filled", 0)) if "valuation_roe_filled" in scan_stats else "-"},
                {"항목": "재무 EV/EBITDA 보강 건수", "값": int(scan_stats.get("valuation_ev_filled", 0)) if "valuation_ev_filled" in scan_stats else "-"},
            ]
            st.markdown("#### 임시 디버그 정보")
            st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, height=280)

if __name__ == "__main__":
    main()
