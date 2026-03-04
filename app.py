import concurrent.futures
from datetime import datetime, timedelta
import os
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
# yfinance cache dir should be set before import when possible.
try:
    _pre_yf_cache = (Path(__file__).resolve().parent / "_cache" / "yf_cache")
    _pre_yf_cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("YF_CACHE_DIR", str(_pre_yf_cache))
except Exception:
    pass
import yfinance as yf
import yfinance.cache as yf_cache
from pykrx import stock as pykrx_stock
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
SECTOR_CACHE_PATH = CACHE_DIR / "sector_cache.csv"
SECTOR_SEED_PATH = CACHE_DIR / "sector_seed.csv"
KRX_KEY_PATH = CACHE_DIR / "krx_api_key.txt"
try:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    yf.set_tz_cache_location(str(CACHE_DIR / "yf_tz_cache"))
    yf_cache_dir = CACHE_DIR / "yf_cache"
    yf_cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ["YF_CACHE_DIR"] = str(yf_cache_dir)
    yf_cache.set_cache_location(str(yf_cache_dir))
except Exception:
    pass


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


def save_universe_cache(df: pd.DataFrame, path: Path) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")
    except Exception:
        pass


def load_universe_cache(path: Path) -> pd.DataFrame:
    try:
        if path.exists():
            return pd.read_csv(path)
    except Exception:
        pass
    return pd.DataFrame()


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
    # Connectivity smoke test (KRX endpoint reachability + key format check).
    try:
        r = requests.get("https://openapi.krx.co.kr", timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code >= 200 and r.status_code < 500:
            return True, f"키 형식/접속 확인 OK (포털 응답 {r.status_code})"
        return False, f"포털 응답 비정상: {r.status_code}"
    except Exception as e:
        return False, f"테스트 실패: {e}"


@st.cache_data(ttl=86400)
def get_sector_seed_map() -> pd.DataFrame:
    # One-day sector seed cache from FDR listings (cheap and stable).
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
        r = requests.get(url, timeout=timeout_sec)
        return f"HTTP {r.status_code}"
    except Exception as e:
        return f"ERR: {e}"


@st.cache_data(ttl=3600)
def get_us_universe() -> pd.DataFrame:
    rows: List[Dict] = []
    # Primary: Nasdaq Trader symbol directory
    try:
        for url in US_LISTING_URLS:
            resp = requests.get(url, timeout=20)
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
    # Fill KR market cap/dividend from pykrx in bulk.
    if df.empty:
        return df, {"kr_fund_mcap_filled": 0.0, "kr_fund_div_filled": 0.0}
    out = df.copy()
    kr_mask = out["market"].astype(str).str.upper().eq("KR")
    if kr_mask.sum() == 0:
        return out, {"kr_fund_mcap_filled": 0.0, "kr_fund_div_filled": 0.0}

    before_mc = int(pd.to_numeric(out.loc[kr_mask, "market_cap_krw"], errors="coerce").notna().sum())
    before_div = int(pd.to_numeric(out.loc[kr_mask, "dividend_yield_pct"], errors="coerce").notna().sum())

    date = _nearest_krx_date()
    if not date:
        return out, {"kr_fund_mcap_filled": 0.0, "kr_fund_div_filled": 0.0}

    caps = []
    for m in ["KOSPI", "KOSDAQ", "KONEX"]:
        try:
            cdf = pykrx_stock.get_market_cap_by_ticker(date, market=m)
            if cdf is not None and not cdf.empty:
                cdf = cdf.reset_index().rename(columns={cdf.columns[0]: "ticker"})
                cdf["ticker"] = cdf["ticker"].astype(str).str.zfill(6)
                # 시가총액 column name can vary by locale/package version.
                cap_col = "시가총액" if "시가총액" in cdf.columns else None
                if cap_col is None:
                    for col in cdf.columns:
                        if "시가총액" in str(col):
                            cap_col = col
                            break
                if cap_col:
                    caps.append(cdf[["ticker", cap_col]].rename(columns={cap_col: "mcap_krw_fill"}))
        except Exception:
            continue
    fund = pd.DataFrame()
    try:
        fdf = pykrx_stock.get_market_fundamental_by_ticker(date, market="ALL")
        if fdf is not None and not fdf.empty:
            fdf = fdf.reset_index().rename(columns={fdf.columns[0]: "ticker"})
            fdf["ticker"] = fdf["ticker"].astype(str).str.zfill(6)
            div_col = "DIV" if "DIV" in fdf.columns else None
            if div_col:
                fund = fdf[["ticker", div_col]].rename(columns={div_col: "div_fill"})
    except Exception:
        pass

    merged = out.loc[kr_mask, ["ticker", "market_cap_krw", "dividend_yield_pct"]].copy()
    merged["ticker"] = merged["ticker"].astype(str).str.zfill(6)
    if caps:
        cap_df = pd.concat(caps, ignore_index=True).drop_duplicates(subset=["ticker"], keep="last")
        merged = merged.merge(cap_df, on="ticker", how="left")
        merged["market_cap_krw"] = pd.to_numeric(merged["market_cap_krw"], errors="coerce").where(
            pd.to_numeric(merged["market_cap_krw"], errors="coerce").notna(),
            pd.to_numeric(merged.get("mcap_krw_fill"), errors="coerce"),
        )
    if not fund.empty:
        merged = merged.merge(fund, on="ticker", how="left")
        merged["dividend_yield_pct"] = pd.to_numeric(merged["dividend_yield_pct"], errors="coerce").where(
            pd.to_numeric(merged["dividend_yield_pct"], errors="coerce").notna(),
            pd.to_numeric(merged.get("div_fill"), errors="coerce"),
        )

    out.loc[kr_mask, "market_cap_krw"] = merged["market_cap_krw"].values
    out.loc[kr_mask, "dividend_yield_pct"] = merged["dividend_yield_pct"].values

    after_mc = int(pd.to_numeric(out.loc[kr_mask, "market_cap_krw"], errors="coerce").notna().sum())
    after_div = int(pd.to_numeric(out.loc[kr_mask, "dividend_yield_pct"], errors="coerce").notna().sum())
    return out, {
        "kr_fund_mcap_filled": float(max(0, after_mc - before_mc)),
        "kr_fund_div_filled": float(max(0, after_div - before_div)),
    }


def check_krx_connection() -> Tuple[bool, str]:
    try:
        k1 = _safe_kr_tickers("KOSPI")
        k2 = _safe_kr_tickers("KOSDAQ")
        total = len(k1) + len(k2)
        if total > 0:
            return True, f"KRX 로딩 OK (KOSPI+KOSDAQ {total:,}개)"
        return False, "KRX 응답은 있으나 티커가 비어 있습니다."
    except Exception as e:
        return False, f"KRX 체크 실패: {e}"


@st.cache_data(ttl=3600)
def get_kr_universe() -> pd.DataFrame:
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
        save_universe_cache(out, KR_CACHE_PATH)
        return out
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
                    save_universe_cache(fallback, KR_CACHE_PATH)
                    return fallback
        except Exception:
            pass

    cached = load_universe_cache(KR_CACHE_PATH)
    if not cached.empty:
        if "_source" not in cached.columns:
            cached["_source"] = "cache_kr"
        else:
            cached["_source"] = "cache_kr"
        return cached
    # Last fallback: small KR sample so KR-only mode remains usable.
    return fallback_kr_universe()


@st.cache_data(ttl=120)
def get_usdkrw() -> Optional[float]:
    for url in USDKRW_APIS:
        try:
            r = requests.get(url, timeout=10)
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

    dy = _safe_float(info.get("dividendYield"))
    if dy is not None:
        dy *= 100.0
    # Fast-mode fallback: compute dividend yield from downloaded price actions.
    if dy is None and "Dividends" in hist.columns:
        try:
            divs = pd.to_numeric(hist["Dividends"], errors="coerce").dropna()
            if not divs.empty:
                recent = divs.tail(min(len(divs), 252))
                annual_div = float(recent.sum())
                if price > 0 and annual_div > 0:
                    dy = (annual_div / price) * 100.0
        except Exception:
            pass

    market_cap = _safe_float(info.get("marketCap"))
    if item["currency"] == "USD" and fx and market_cap is not None:
        market_cap_krw = market_cap * fx
    elif item["currency"] == "KRW":
        market_cap_krw = market_cap
    else:
        market_cap_krw = None

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

    is_spac = ("SPAC" in name_text) or ("SPAC" in quote_type)
    is_etn = ("ETN" in name_text) or ("ETN" in quote_type) or ("EXCHANGE TRADED NOTE" in name_text)
    is_etf = ("ETF" in name_text) or ("ETF" in quote_type) or (quote_type == "ETF")

    is_preferred_like = ticker.endswith(("W", "R", "U")) or ("PREFERRED" in name_text) or (" 우" in name_text) or ("우B" in name_text)
    is_untradeable = (info.get("tradeable") is False) or (vol_last == 0)
    is_low_price = (item["market"] == "KR" and price < 1000) or (item["market"] == "US" and price < 1)
    is_small_cap = market_cap_krw is not None and market_cap_krw < 100_000_000_000
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

    return {
        "market": item["market"],
        "ticker": item["ticker"],
        "yf_ticker": item.get("yf_ticker", item["ticker"]),
        "name": item["name"],
        "currency": item["currency"],
        "price": round(price, 4),
        "price_krw": round(price_krw, 2) if price_krw is not None else None,
        "dividend_yield_pct": round(dy, 4) if dy is not None else None,
        "market_cap_krw": round(market_cap_krw, 2) if market_cap_krw is not None else None,
        "avg_traded_value_krw_20": round(traded_value_krw, 2) if traded_value_krw is not None else None,
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
        "is_preferred_like": is_preferred_like,
        "is_untradeable": is_untradeable,
        "is_low_price": is_low_price,
        "is_small_cap": is_small_cap,
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


def _compute_metrics(item: Dict, fx: Optional[float], fetch_info: bool = True, history_period: str = "5y") -> Optional[Dict]:
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
            r = requests.get(u, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
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
            r = requests.get(url, params={"symbols": ",".join(syms)}, timeout=12, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            js = r.json()
            results = (((js or {}).get("quoteResponse") or {}).get("result") or [])
            for q in results:
                sym = str(q.get("symbol", "")).strip()
                dy = q.get("trailingAnnualDividendYield")
                if dy is None:
                    dy = q.get("dividendYield")
                v = _safe_float(dy)
                if sym and v is not None:
                    dy_map[sym] = v * 100.0
        except Exception:
            continue
    if dy_map:
        fill = out["yf_ticker"].map(dy_map)
        out["dividend_yield_pct"] = out["dividend_yield_pct"].where(out["dividend_yield_pct"].notna(), fill)
    return out


def enrich_quote_snapshot(
    df: pd.DataFrame, fx: Optional[float], limit: int = 2000
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    # Fill lightweight quote fields (dividend, market cap) in batch for final candidates.
    if df.empty:
        return df, {"quote_targets": 0.0, "quote_div_filled": 0.0, "quote_mcap_filled": 0.0}
    out = df.copy()
    targets = out["yf_ticker"].dropna().astype(str).unique().tolist()
    if limit > 0:
        targets = targets[: int(limit)]
    if not targets:
        return out, {"quote_targets": 0.0, "quote_div_filled": 0.0, "quote_mcap_filled": 0.0}

    before_div = int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum())
    before_mc = int(pd.to_numeric(out.get("market_cap_krw"), errors="coerce").notna().sum())

    dy_map: Dict[str, float] = {}
    mc_map: Dict[str, float] = {}
    sh_map: Dict[str, float] = {}
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
                    r = requests.get(
                        ep,
                        params={"symbols": ",".join(syms)},
                        timeout=12,
                        headers={"User-Agent": "Mozilla/5.0"},
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
                        vdy = _safe_float(dy)
                        if vdy is None:
                            dps = _safe_float(q.get("trailingAnnualDividendRate"))
                            px = _safe_float(q.get("regularMarketPrice"))
                            if dps is not None and px is not None and px > 0:
                                vdy = dps / px
                        if vdy is not None:
                            dy_map[sym] = vdy * 100.0

                        vmc = _safe_float(q.get("marketCap"))
                        if vmc is not None:
                            mc_map[sym] = vmc
                        vsh = _safe_float(q.get("sharesOutstanding"))
                        if vsh is not None:
                            sh_map[sym] = vsh
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
    if len(dy_map) == 0 or len(mc_map) == 0:
        probe = out[out["yf_ticker"].isin(targets)][["yf_ticker", "price", "currency"]].drop_duplicates()
        probe = probe.head(350)
        for _, r in probe.iterrows():
            sym = str(r.get("yf_ticker", "")).strip()
            if not sym:
                continue
            need_div = sym not in dy_map
            need_mc = sym not in mc_map
            if not (need_div or need_mc):
                continue
            try:
                tk = yf.Ticker(sym)
                if need_mc:
                    finfo = getattr(tk, "fast_info", None) or {}
                    vmc = _safe_float(finfo.get("market_cap"))
                    if vmc is not None:
                        mc_map[sym] = vmc
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
        fill_dy = out["yf_ticker"].map(dy_map)
        out["dividend_yield_pct"] = pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").where(
            pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna(),
            pd.to_numeric(fill_dy, errors="coerce"),
        )

    if mc_map or sh_map:
        mcap_raw = out["yf_ticker"].map(mc_map)
        sh_raw = out["yf_ticker"].map(sh_map)
        px_raw = pd.to_numeric(out.get("price"), errors="coerce")
        mcap_raw = pd.to_numeric(mcap_raw, errors="coerce")
        derived = pd.to_numeric(sh_raw, errors="coerce") * px_raw
        mcap_raw = mcap_raw.where(mcap_raw.notna(), derived)
        mcap_krw = pd.Series([None] * len(out), index=out.index, dtype="object")
        usd_mask = out["currency"].eq("USD")
        krw_mask = out["currency"].eq("KRW")
        if fx and fx > 0:
            mcap_krw.loc[usd_mask] = mcap_raw.loc[usd_mask] * fx
        mcap_krw.loc[krw_mask] = mcap_raw.loc[krw_mask]
        out["market_cap_krw"] = pd.to_numeric(out.get("market_cap_krw"), errors="coerce").where(
            pd.to_numeric(out.get("market_cap_krw"), errors="coerce").notna(),
            pd.to_numeric(mcap_krw, errors="coerce"),
        )

    after_div = int(pd.to_numeric(out.get("dividend_yield_pct"), errors="coerce").notna().sum())
    after_mc = int(pd.to_numeric(out.get("market_cap_krw"), errors="coerce").notna().sum())
    stats = {
        "quote_targets": float(len(targets)),
        "quote_div_filled": float(max(0, after_div - before_div)),
        "quote_mcap_filled": float(max(0, after_mc - before_mc)),
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

    known = out[out["sector"] != "Unknown"].copy()
    if not known.empty:
        grp = (
            known.groupby("sector", as_index=False)
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
            grp[["sector", "sector_new_high_count", "sector_new_high_ratio", "sector_strength_score"]],
            on="sector",
            how="left",
        )
    else:
        out["sector_new_high_count"] = 0.0
        out["sector_new_high_ratio"] = 0.0
        out["sector_strength_score"] = 0.0

    out["sector_strength_score"] = out["sector_strength_score"].fillna(0.0)
    out = out.drop(columns=["ticker_norm"], errors="ignore")
    covered = float((out["sector"] != "Unknown").mean() * 100.0)
    stats = {
        "sector_cached": float(len(cache_key)),
        "sector_seeded": float(0 if seed is None else len(seed)),
        "sector_fetched": float(len(fetched_rows)),
        "sector_covered": round(covered, 2),
        "sector_timeout": float(timed_out),
        "sector_pick_mode": sector_pick_mode,
        "sector_pick_limit": float(sector_fetch_limit),
    }
    return out, stats


def _run_scan_batched(
    universe: pd.DataFrame, fx: Optional[float], workers: int, history_period: str = "2y"
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    items = universe.to_dict(orient="records")
    rows: List[Dict] = []
    none_count = 0
    err_count = 0
    t0 = time.time()
    batch_size = 350
    current_batch = batch_size
    current_threads = max(2, min(int(workers), 12))
    adaptive_down_count = 0
    done = 0
    bar = st.progress(0, text="스캔 계산 중(배치 모드)...")
    i = 0
    while i < len(items):
        batch_items = items[i:i + current_batch]
        tickers = [it["yf_ticker"] for it in batch_items if it.get("yf_ticker")]
        if not tickers:
            none_count += len(batch_items)
            done += len(batch_items)
            i += len(batch_items)
            continue
        data = pd.DataFrame()
        for retry in range(2):
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
                )
                if data is not None and not data.empty:
                    break
            except Exception:
                pass
            time.sleep(0.6 * (retry + 1))
        if data is None or data.empty:
            err_count += len(batch_items)
            done += len(batch_items)
            bar.progress(int(done * 100 / max(1, len(items))), text=f"스캔 계산 중(배치 모드)... {done}/{len(items)}")
            current_batch = max(80, int(current_batch * 0.7))
            current_threads = max(2, current_threads - 1)
            adaptive_down_count += 1
            time.sleep(0.8)
            i += len(batch_items)
            continue
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


def prefilter_universe_fast(universe: pd.DataFrame, etf_mode: str) -> pd.DataFrame:
    # Cheap string-based prefilter before network-heavy yfinance calls.
    if universe.empty:
        return universe
    if etf_mode == "전체":
        return universe
    name_series = universe["name"].fillna("").str.upper()
    etf_like = name_series.str.contains("ETF", regex=False)
    if etf_mode == "ETF만":
        return universe[etf_like]
    return universe[~etf_like]


def apply_hard_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    # Missing boolean flags should not be treated as hard-fail.
    def ok(col: str) -> pd.Series:
        return ~pd.to_numeric(df.get(col, False), errors="coerce").fillna(0).astype(bool)

    return df[
        ok("is_small_cap")
        & ok("is_spac")
        & ok("is_etn")
        & ok("is_low_liquidity")
        & ok("is_untradeable")
        & ok("is_preferred_like")
        & ok("is_low_price")
        & ok("is_new_listing")
    ]


def apply_etf_mode(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "ETF만":
        return df[df["is_etf"] == True]
    if mode == "ETF 제외":
        return df[df["is_etf"] == False]
    return df


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
        if "dividend_yield_pct" not in out.columns:
            out["dividend_yield_pct"] = pd.Series(dtype=float)
        return out
    out["dividend_yield_pct"] = out["dividend_yield_pct"].fillna(0.0)
    out["yield_rank"] = out["dividend_yield_pct"].rank(pct=True)
    out["trend_score"] = (
        out["price_above_120"].fillna(False).astype(float) * 0.2
        + out["ma60_up"].fillna(False).astype(float) * 0.2
        + out["strong_trend"].fillna(False).astype(float) * 0.25
        + out["vol_surge"].fillna(False).astype(float) * 0.15
        + out["box_breakout"].fillna(False).astype(float) * 0.2
    )
    if "sector_strength_score" in out.columns:
        sector_norm = (pd.to_numeric(out["sector_strength_score"], errors="coerce").fillna(0.0) / 100.0).clip(0, 1)
    else:
        sector_norm = pd.Series(0.0, index=out.index)
    out["score"] = ((out["yield_rank"] * 0.35) + (out["trend_score"] * 0.45) + (sector_norm * 0.20)) * 100
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
        st.session_state["saved_filters"] = {}
    if "active_filter" not in st.session_state:
        st.session_state["active_filter"] = None
    if "active_filter_name" not in st.session_state:
        st.session_state["active_filter_name"] = ""
    if "last_filtered" not in st.session_state:
        st.session_state["last_filtered"] = None
    if "last_summary" not in st.session_state:
        st.session_state["last_summary"] = ""

    with st.sidebar:
        st.header("기본 옵션")
        market_mode = st.radio("시장 모드", ["국장(KR)", "미장(US)", "통합(KR+US)"], index=0)
        if market_mode == "국장(KR)":
            markets = ["KR"]
        elif market_mode == "미장(US)":
            markets = ["US"]
        else:
            markets = ["KR", "US"]
        etf_mode = st.selectbox("ETF 모드", ["전체", "ETF만", "ETF 제외"], index=0)

        st.divider()
        with st.expander("KRX Open API 설정", expanded=False):
            st.markdown(
                "KRX 키 발급: https://openapi.krx.co.kr\n\n"
                "1) 회원가입/로그인\n"
                "2) Open API 신청\n"
                "3) 키 발급 후 아래에 입력/저장\n"
                "4) 연결 테스트 확인"
            )
            if "krx_api_key" not in st.session_state:
                st.session_state["krx_api_key"] = load_krx_api_key()
            key_input = st.text_input("KRX API 키", value=st.session_state.get("krx_api_key", ""), type="password")
            st.caption(f"저장된 키: {mask_key(st.session_state.get('krx_api_key', ''))}")
            k1, k2, k3 = st.columns(3)
            with k1:
                if st.button("키 저장"):
                    if save_krx_api_key(key_input):
                        st.session_state["krx_api_key"] = key_input.strip()
                        st.success("키 저장 완료")
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
                    st.success("키 삭제 완료")
            if st.session_state.get("krx_api_test"):
                st.caption(f"마지막 테스트: {st.session_state['krx_api_test']}")

        st.divider()
        st.subheader("가격 범위 (KRW)")
        pcol1, pcol_mid, pcol2 = st.columns([1, 0.2, 1])
        with pcol1:
            min_price_text = st.text_input("최소값", value="0")
        with pcol_mid:
            st.markdown("<div style='text-align:center; margin-top:30px;'>~</div>", unsafe_allow_html=True)
        with pcol2:
            max_price_text = st.text_input("최대값 (0=무제한)", value="0")

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
        st.caption("각 이평선별로 패스 / > / < 선택 (데이터 없으면 자동 패스)")
        rule_options = ["패스", ">", "<"]
        tf_rule_map = {}

        st.markdown("일봉")
        d1, d2, d3 = st.columns(3)
        with d1:
            tf_rule_map["D_20"] = st.selectbox("20MA", rule_options, index=0, key="rule_d20")
        with d2:
            tf_rule_map["D_60"] = st.selectbox("60MA", rule_options, index=0, key="rule_d60")
        with d3:
            tf_rule_map["D_120"] = st.selectbox("120MA", rule_options, index=0, key="rule_d120")

        st.markdown("주봉")
        w1, w2, w3 = st.columns(3)
        with w1:
            tf_rule_map["W_20"] = st.selectbox("20WMA", rule_options, index=0, key="rule_w20")
        with w2:
            tf_rule_map["W_60"] = st.selectbox("60WMA", rule_options, index=0, key="rule_w60")
        with w3:
            tf_rule_map["W_120"] = st.selectbox("120WMA", rule_options, index=0, key="rule_w120")

        st.markdown("월봉")
        m1, m2, m3 = st.columns(3)
        with m1:
            tf_rule_map["M_20"] = st.selectbox("20MMA", rule_options, index=0, key="rule_m20")
        with m2:
            tf_rule_map["M_60"] = st.selectbox("60MMA", rule_options, index=0, key="rule_m60")
        with m3:
            tf_rule_map["M_120"] = st.selectbox("120MMA", rule_options, index=0, key="rule_m120")

        st.divider()
        st.subheader("기본 조건 필터")
        preset = st.selectbox(
            "프리셋",
            [
                "직접 선택",
                "기본 필터 세트 (1~4)",
                "장기추세+눌림목 (120+20)",
                "강한상승 확인 (120+60)",
                "단기가속 (20+5)",
            ],
            index=0,
        )

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

        preset_map = {
            "직접 선택": [],
            "기본 필터 세트 (1~4)": [
                "1) 주가 > 120MA",
                "2) 60MA 우상향",
                "3) 20MA 눌림목",
                "4) 거래량 증가",
            ],
            "장기추세+눌림목 (120+20)": ["1) 주가 > 120MA", "3) 20MA 눌림목"],
            "강한상승 확인 (120+60)": ["중장기 강세 (60MA>120MA)", "2) 60MA 우상향"],
            "단기가속 (20+5)": ["단기 가속 (5MA>20MA)", "일반 상승 구조 (20>60>120)"],
        }
        selected_conditions = st.multiselect("적용 조건", condition_options, default=preset_map[preset])

        st.divider()
        st.subheader("이평 근접도 필터")
        near_tf_label = st.selectbox("근접도 기준 봉", ["사용 안함", "일봉", "주봉", "월봉"], index=0)
        near_periods = st.multiselect("근접 체크 MA", [20, 60, 120], default=[])
        near_pct = st.slider("근접 허용 오차(%)", 0.5, 20.0, 3.0, 0.5)

        st.divider()
        st.subheader("필터 조합 저장")
        save_name = st.text_input("저장 이름")
        if st.button("현재 필터 저장") and save_name.strip():
            st.session_state["saved_filters"][save_name.strip()] = {
                "selected_conditions": selected_conditions,
                "tf_rule_map": tf_rule_map,
                "near_tf_label": near_tf_label,
                "near_periods": near_periods,
                "near_pct": near_pct,
                "min_price": min_price,
                "max_price": max_price,
                "etf_mode": etf_mode,
            }
            st.success(f"저장 완료: {save_name.strip()}")

        saved_keys = list(st.session_state["saved_filters"].keys())
        if saved_keys:
            loaded = st.selectbox("저장된 필터", ["선택 안함"] + saved_keys, index=0)
            if loaded != "선택 안함" and st.button("선택 필터 불러오기"):
                st.session_state["active_filter"] = st.session_state["saved_filters"][loaded]
                st.session_state["active_filter_name"] = loaded
                st.success(f"저장 필터 '{loaded}' 적용됨")

        st.divider()
        with st.expander("고급 옵션", expanded=False):
            workers = st.slider("병렬 스레드 (권장 6~8)", 2, 12, 8, 1)
            speed_mode = st.toggle("고속 모드 (기본 ON)", value=True, help="ON: 속도 우선, 일부 상세 정보 호출 최소화")
            history_period = st.selectbox("조회 기간", ["2y", "3y", "5y"], index=0, help="고속 모드에서 짧을수록 빠름")
            sector_scoring = st.toggle("섹터 강도 점수화", value=True, help="섹터별 52주 신고가 개수 기반 가점")
            sector_pick_mode = st.selectbox("섹터 보강 대상", ["상위", "랜덤"], index=0, help="상위: 현재 정렬 순서 기준 / 랜덤: 무작위")
            sector_pick_label = st.selectbox("섹터 보강 개수", ["상위 50", "상위 200", "상위 400", "상위 800", "전체"], index=2)
            sector_limit_map = {"상위 50": 50, "상위 200": 200, "상위 400": 400, "상위 800": 800, "전체": 0}
            sector_fetch_limit = sector_limit_map.get(sector_pick_label, 400)
            sector_workers = st.slider("섹터 수집 스레드", 1, 8, 4, 1)
            sector_max_seconds = st.slider("섹터 수집 최대시간(초)", 30, 1800, 300, 30)
            max_symbols = st.number_input("최대 스캔 수 (0=전체)", 0, 20000, 0, 100)
            if st.button("KRX 연결 체크"):
                ok, msg = check_krx_connection()
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)

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
            kr_df = get_kr_universe()
        except Exception as e:
            source_errors.append(f"KR 목록 로딩 실패: {e}")
            kr_df = pd.DataFrame()

    if "KR" in markets and kr_df.empty:
        st.warning("KRX 목록을 가져오지 못했습니다. 네트워크/거래소 응답 상태를 'KRX 연결 체크'로 먼저 확인하세요.")
    elif "KR" in markets and (not kr_df.empty) and ("_source" in kr_df.columns) and (kr_df["_source"].eq("fallback_kr_sample").all()):
        st.warning("KR 실데이터 소스 연결 실패로 fallback KR 샘플 목록으로 실행 중입니다.")
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
    force_exclusion_notice.info("강제 제외: 시총<1,000억 / SPAC / ETN / 저유동성 / 거래불가 / 우선주류 / 초저가 / 신규상장")
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

        run_selected_conditions = active_cfg["selected_conditions"] if active_cfg else selected_conditions
        run_tf_rule_map = active_cfg.get("tf_rule_map", {}) if active_cfg else tf_rule_map
        run_near_tf_label = active_cfg["near_tf_label"] if active_cfg else near_tf_label
        run_near_periods = active_cfg["near_periods"] if active_cfg else near_periods
        run_near_pct = active_cfg["near_pct"] if active_cfg else near_pct
        run_min_price = active_cfg["min_price"] if active_cfg else min_price
        run_max_price = active_cfg["max_price"] if active_cfg else max_price
        run_etf_mode = active_cfg["etf_mode"] if active_cfg else etf_mode
        run_workers = workers
        run_fetch_info = not speed_mode
        run_history_period = adjust_history_period(
            history_period, run_tf_rule_map, run_near_tf_label, run_near_periods
        )
        if run_history_period != history_period:
            st.info(f"선택한 장기 이평 조건으로 조회 기간을 자동 확장했습니다: {history_period} -> {run_history_period}")

        # Speed-up: prefilter target list before API calls where possible.
        fast_universe = prefilter_universe_fast(universe, run_etf_mode)
        if len(fast_universe) > 2000 and run_workers > 8:
            run_workers = 8
            st.info("대규모 스캔에서 429 완화를 위해 병렬 스레드를 8로 자동 조정했습니다.")
        raw, scan_stats = run_scan(
            fast_universe,
            fx,
            run_workers,
            fetch_info=run_fetch_info,
            history_period=run_history_period,
        )
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
        scan_stats["t_filter_sec"] = round(time.time() - t_filter0, 2)

        quote_stats = {}
        if not filtered.empty:
            t_krfund0 = time.time()
            filtered, kr_fund_stats = enrich_kr_fundamentals(filtered)
            kr_fund_stats["t_krfund_sec"] = round(time.time() - t_krfund0, 2)
            scan_stats.update(kr_fund_stats)
        if speed_mode and not filtered.empty:
            t_quote0 = time.time()
            filtered, quote_stats = enrich_quote_snapshot(filtered, fx, limit=2500)
            quote_stats["t_quote_sec"] = round(time.time() - t_quote0, 2)
            scan_stats.update(quote_stats)
        if not filtered.empty and "dividend_yield_pct" in filtered.columns:
            scan_stats["dividend_covered_pct"] = round(float(pd.to_numeric(filtered["dividend_yield_pct"], errors="coerce").notna().mean() * 100.0), 2)
        if not filtered.empty and "market_cap_krw" in filtered.columns:
            scan_stats["mcap_covered_pct"] = round(float(pd.to_numeric(filtered["market_cap_krw"], errors="coerce").notna().mean() * 100.0), 2)
        if not filtered.empty and "sector" in filtered.columns:
            scan_stats["sector_final_covered_pct"] = round(float((filtered["sector"].fillna("Unknown") != "Unknown").mean() * 100.0), 2)

        t_score0 = time.time()
        filtered = enrich_score(filtered)
        scan_stats["t_score_sec"] = round(time.time() - t_score0, 2)
        filtered["시가총액(억원)"] = pd.to_numeric(filtered.get("market_cap_krw"), errors="coerce").apply(_to_eok)
        filtered["20일평균거래대금(억원)"] = pd.to_numeric(filtered.get("avg_traded_value_krw_20"), errors="coerce").apply(_to_eok)
        if "score" not in filtered.columns:
            filtered["score"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        if "dividend_yield_pct" not in filtered.columns:
            filtered["dividend_yield_pct"] = pd.Series([0.0] * len(filtered), index=filtered.index, dtype=float)
        t_sort0 = time.time()
        filtered = filtered.sort_values(["score", "dividend_yield_pct"], ascending=[False, False], na_position="last")
        scan_stats["t_sort_sec"] = round(time.time() - t_sort0, 2)
        scan_stats["final_us_count"] = float((filtered["market"] == "US").sum()) if "market" in filtered.columns else 0.0
        scan_stats["final_kr_count"] = float((filtered["market"] == "KR").sum()) if "market" in filtered.columns else 0.0
        scan_stats["t_total_sec"] = round(time.time() - pipeline_t0, 2)

        elapsed = datetime.now() - started
        st.success(
            f"{pre:,} -> 가격 {post_price:,} -> ETF {post_etf:,} -> 강제제외 {post_hard:,} "
            f"-> 기본조건 {post_basic:,} -> 봉이평 {post_tf:,} -> 근접도 {post_near:,} -> 최종 {len(filtered):,} "
            f"(소요 {str(elapsed).split('.')[0]})"
        )
        if len(filtered) == 0:
            st.warning("결과가 0건입니다. 위 단계별 카운트를 보고 가장 많이 줄어든 필터를 먼저 완화하세요.")
        st.session_state["last_filtered"] = filtered.copy()
        st.session_state["last_summary"] = (
            f"{pre:,} -> 가격 {post_price:,} -> ETF {post_etf:,} -> 강제제외 {post_hard:,} "
            f"-> 기본조건 {post_basic:,} -> 봉이평 {post_tf:,} -> 근접도 {post_near:,} -> 최종 {len(filtered):,} "
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
            view["시가총액(억원)표시"] = pd.to_numeric(view["시가총액(억원)"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            view["20일평균거래대금(억원)표시"] = pd.to_numeric(view["20일평균거래대금(억원)"], errors="coerce").map(lambda v: f"{v:,.2f}" if pd.notna(v) else "-")
            cols = [
                "score", "market", "ticker", "name", "가격표시", "현재가(KRW)표시", "시가총액(억원)표시", "20일평균거래대금(억원)표시", "dividend_yield_pct",
                "sector", "sector_strength_score", "sector_new_high_count", "sector_new_high_ratio",
                "D_ma20", "D_ma60", "D_ma120", "W_ma20", "W_ma60", "W_ma120", "M_ma20", "M_ma60", "M_ma120",
                "price_above_120", "ma60_up", "pullback_20", "vol_surge", "box_breakout", "strong_trend", "general_trend",
                "listing_days"
            ]
            safe_cols = [c for c in cols if c in view.columns]
            rename_map = {
                "score": "점수",
                "market": "시장",
                "ticker": "티커",
                "name": "종목명",
                "가격표시": "가격",
                "현재가(KRW)표시": "현재가(KRW)",
                "dividend_yield_pct": "배당수익률(%)",
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
                "시가총액(억원)표시": "시가총액(억원)",
                "20일평균거래대금(억원)표시": "20일평균거래대금(억원)",
                "listing_days": "상장일수",
            }
            render_df = view[safe_cols].rename(columns=rename_map)
            st.dataframe(
                render_df,
                use_container_width=True,
                height=650,
                column_config={
                    "배당수익률(%)": st.column_config.NumberColumn(format="%.2f"),
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
                "현재 소스": "yfinance",
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
                {"항목": "섹터 수집 타임아웃", "값": int(scan_stats.get("sector_timeout", 0)) if "sector_timeout" in scan_stats else 0},
                {"항목": "섹터 수집시간 사용(초)", "값": int(scan_stats.get("sector_max_seconds_used", 0)) if "sector_max_seconds_used" in scan_stats else "-"},
                {"항목": "최종 섹터 커버율(%)", "값": scan_stats.get("sector_final_covered_pct", "-")},
                {"항목": "배당 커버율(%)", "값": scan_stats.get("dividend_covered_pct", "-")},
                {"항목": "시총 커버율(%)", "값": scan_stats.get("mcap_covered_pct", "-")},
                {"항목": "보강 대상 수", "값": int(scan_stats.get("quote_targets", 0)) if "quote_targets" in scan_stats else "-"},
                {"항목": "배당 보강 건수", "값": int(scan_stats.get("quote_div_filled", 0)) if "quote_div_filled" in scan_stats else "-"},
                {"항목": "시총 보강 건수", "값": int(scan_stats.get("quote_mcap_filled", 0)) if "quote_mcap_filled" in scan_stats else "-"},
                {"항목": "개별 보강 호출수", "값": int(scan_stats.get("quote_fallback_used", 0)) if "quote_fallback_used" in scan_stats else "-"},
                {"항목": "KR 배당 보강 건수", "값": int(scan_stats.get("kr_fund_div_filled", 0)) if "kr_fund_div_filled" in scan_stats else "-"},
                {"항목": "KR 시총 보강 건수", "값": int(scan_stats.get("kr_fund_mcap_filled", 0)) if "kr_fund_mcap_filled" in scan_stats else "-"},
            ]
            st.markdown("#### 임시 디버그 정보")
            st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, height=280)

if __name__ == "__main__":
    main()
