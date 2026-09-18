import os
import sys
import json
import time
import sqlite3
import logging
import threading
import concurrent.futures
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

try:
    from google import genai
except Exception:
    genai = None

# ============================================================
# CONFIG
# ============================================================
load_dotenv()

GEMINI_API_KEYS = [
    os.getenv("GEMINI_API_KEY_1", "").strip(),
    os.getenv("GEMINI_API_KEY_2", "").strip(),
]
GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DATABASE_PATH = os.getenv("DATABASE_PATH", "alerts.db")
TICKER_CACHE_FILE = os.getenv("TICKER_CACHE_FILE", "nasdaq_tickers.json")

MIN_PRICE = 0.50
MAX_PRICE = 15.00

# Candidate discovery. These are deliberately broad; the score decides quality.
MIN_DOLLAR_VOLUME = 200_000
MIN_PRICE_DOLLAR_VOLUME = 350_000
MIN_AVG_DAILY_VOLUME = 50_000

# Signal thresholds
MIN_SCORE = 62
STRONG_SCORE = 80
MOMENTUM_SCORE = 70
MIN_RVOL = 1.00
GOOD_RVOL = 1.50
HIGH_RVOL = 2.50
MIN_RR = 1.50
IDEAL_RR = 2.00

# Signal management
SIGNAL_COOLDOWN_MINUTES = 30
MAX_DAILY_GAIN_LONG = 25.0
MIN_RSI_LONG = 45.0
MAX_RSI_LONG = 78.0
COOLDOWN_SCORE_OVERRIDE = 10
COOLDOWN_RVOL_OVERRIDE = 1.5
COOLDOWN_MOVE_OVERRIDE = 0.03
MAX_SIGNAL_DAYS = 10
NORMAL_EXPECTED_DAYS = "1–3 gün"

# API / scan controls
SCREENER_COUNT = 500
QUOTE_BATCH_SIZE = 100
DETAILED_CANDIDATES = 350
MAX_WORKERS = 16
YAHOO_TIMEOUT = 10
YAHOO_RETRIES = 2
YAHOO_BASE_BACKOFF = 1.2
MAX_GEMINI_DAILY_REQUESTS = 1400
SCAN_INTERVAL_MINUTES = 5

NY = ZoneInfo("America/New_York")
TR = ZoneInfo("Europe/Istanbul")

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
YAHOO_SCREENER_URL = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
NASDAQ_MOVERS_URL = "https://api.nasdaq.com/api/marketmovers"
ENABLE_YAHOO_DIRECT_QUOTES = False
ENABLE_YAHOO_SCREENERS = False

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("nasdaq_bot")

# Keep output compact for GitHub Actions / PythonAnywhere storage.

# ============================================================
# HTTP
# ============================================================
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "NASDAQ-Swing-Bot/2.0 contact@example.com",
    "Accept": "application/json,text/plain,*/*",
})

SCAN_LOCK = threading.Lock()


def http_get(url, params=None, timeout=YAHOO_TIMEOUT, retries=YAHOO_RETRIES):
    last_error = None
    for attempt in range(retries + 1):
        try:
            r = SESSION.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 500, 502, 503, 504):
                last_error = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(YAHOO_BASE_BACKOFF * (attempt + 1))
                continue
            r.raise_for_status()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(YAHOO_BASE_BACKOFF * (attempt + 1))
    if last_error:
        raise last_error
    raise RuntimeError("HTTP request failed")

# ============================================================
# DB
# ============================================================
class Database:
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.init_db()

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self):
        with self.connect() as c:
            c.executescript("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp TEXT,
                price REAL,
                score REAL,
                rvol REAL,
                target REAL,
                stop_loss REAL,
                gemini_decision TEXT,
                gemini_reason TEXT
            );

            CREATE TABLE IF NOT EXISTS gemini_usage (
                day TEXT PRIMARY KEY,
                requests INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS swing_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                opened_at TEXT,
                closed_at TEXT,
                status TEXT,
                entry_price REAL,
                current_price REAL,
                stop_loss REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                tp3_hit INTEGER DEFAULT 0,
                max_target REAL,
                score REAL,
                rvol REAL,
                rsi REAL,
                daily_change REAL,
                rr REAL,
                expected_days TEXT,
                gemini_status TEXT,
                gemini_reason TEXT,
                last_update_at TEXT,
                last_notified_price REAL
            );

            CREATE TABLE IF NOT EXISTS signal_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER,
                timestamp TEXT,
                event TEXT,
                price REAL,
                note TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_swing_status ON swing_signals(status);
            CREATE INDEX IF NOT EXISTS idx_swing_symbol ON swing_signals(symbol);
            """)

            # Backward-compatible migration for older gemini_usage schemas.
            # Older versions used names such as date/count. Normalize the tiny
            # daily-counter table before any Gemini query is executed.
            info = c.execute("PRAGMA table_info(gemini_usage)").fetchall()
            cols = {row[1] for row in info}

            if "day" not in cols:
                legacy_date = next((name for name in (
                    "date", "usage_date", "request_date", "day_date"
                ) if name in cols), None)
                legacy_count = next((name for name in (
                    "count", "request_count", "total_requests", "usage", "requests_count"
                ) if name in cols), None)

                c.execute("ALTER TABLE gemini_usage RENAME TO gemini_usage_legacy")
                c.execute("""
                    CREATE TABLE gemini_usage (
                        day TEXT PRIMARY KEY,
                        requests INTEGER DEFAULT 0
                    )
                """)

                if legacy_date and legacy_count:
                    c.execute(
                        f"""INSERT OR REPLACE INTO gemini_usage(day, requests)
                            SELECT CAST("{legacy_date}" AS TEXT),
                                   COALESCE(CAST("{legacy_count}" AS INTEGER), 0)
                            FROM gemini_usage_legacy
                            WHERE "{legacy_date}" IS NOT NULL"""
                    )
                c.execute("DROP TABLE gemini_usage_legacy")

            elif "requests" not in cols:
                c.execute("ALTER TABLE gemini_usage ADD COLUMN requests INTEGER DEFAULT 0")
                legacy = next((name for name in (
                    "count", "request_count", "total_requests", "usage", "requests_count"
                ) if name in cols), None)
                if legacy:
                    c.execute(f'UPDATE gemini_usage SET requests = COALESCE("{legacy}", 0)')

    def gemini_requests_today(self):
        day = datetime.now(TR).date().isoformat()
        with self.connect() as c:
            row = c.execute("SELECT requests FROM gemini_usage WHERE day=?", (day,)).fetchone()
            return int(row[0]) if row else 0

    def increment_gemini(self):
        day = datetime.now(TR).date().isoformat()
        with self.lock, self.connect() as c:
            c.execute("""
                INSERT INTO gemini_usage(day, requests) VALUES(?,1)
                ON CONFLICT(day) DO UPDATE SET requests=requests+1
            """, (day,))

    def has_open_signal(self, symbol):
        with self.connect() as c:
            row = c.execute(
                "SELECT 1 FROM swing_signals WHERE symbol=? AND status='OPEN' LIMIT 1",
                (symbol,),
            ).fetchone()
            return row is not None

    def recent_signal(self, symbol, minutes=SIGNAL_COOLDOWN_MINUTES):
        cutoff = datetime.now(TR) - timedelta(minutes=minutes)
        with self.connect() as c:
            row = c.execute(
                "SELECT opened_at, score, rvol, entry_price FROM swing_signals "
                "WHERE symbol=? ORDER BY id DESC LIMIT 1", (symbol,)
            ).fetchone()
        if not row:
            return None
        try:
            opened = datetime.fromisoformat(row[0])
        except Exception:
            return None
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=TR)
        if opened < cutoff:
            return None
        return dict(row)

    def create_signal(self, data):
        now = datetime.now(TR).isoformat(timespec="seconds")
        with self.lock, self.connect() as c:
            cur = c.execute("""
                INSERT INTO swing_signals(
                    symbol, opened_at, status, entry_price, current_price,
                    stop_loss, tp1, tp2, tp3, max_target, score, rvol, rsi,
                    daily_change, rr, expected_days, gemini_status,
                    gemini_reason, last_update_at, last_notified_price
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data["symbol"], now, "OPEN", data["entry"], data["entry"],
                data["stop"], data["tp1"], data["tp2"], data["tp3"],
                data["max_target"], data["score"], data["rvol"], data["rsi"],
                data["daily_change"], data["rr"], data["expected_days"],
                data.get("gemini_status", "N/A"), data.get("gemini_reason", ""),
                now, data["entry"],
            ))
            return cur.lastrowid

    def get_open_signals(self):
        with self.connect() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM swing_signals WHERE status='OPEN' ORDER BY id"
            ).fetchall()]

    def update_signal(self, signal_id, **fields):
        if not fields:
            return
        fields["last_update_at"] = datetime.now(TR).isoformat(timespec="seconds")
        cols = ", ".join(f"{k}=?" for k in fields)
        vals = list(fields.values()) + [signal_id]
        with self.lock, self.connect() as c:
            c.execute(f"UPDATE swing_signals SET {cols} WHERE id=?", vals)

    def add_update(self, signal_id, event, price, note=""):
        with self.lock, self.connect() as c:
            c.execute(
                "INSERT INTO signal_updates(signal_id,timestamp,event,price,note) VALUES(?,?,?,?,?)",
                (signal_id, datetime.now(TR).isoformat(timespec="seconds"), event, price, note),
            )

    def close_signal(self, signal_id, status, price, note):
        now = datetime.now(TR).isoformat(timespec="seconds")
        with self.lock, self.connect() as c:
            c.execute(
                "UPDATE swing_signals SET status=?, closed_at=?, current_price=?, last_update_at=? WHERE id=?",
                (status, now, price, now, signal_id),
            )
            c.execute(
                "INSERT INTO signal_updates(signal_id,timestamp,event,price,note) VALUES(?,?,?,?,?)",
                (signal_id, now, status, price, note),
            )

DB = Database(DATABASE_PATH)

# ============================================================
# TIME / MARKET
# ============================================================
class TimezoneManager:
    @staticmethod
    def now_ny():
        return datetime.now(NY)

    @staticmethod
    def market_session():
        now = TimezoneManager.now_ny()
        if now.weekday() >= 5:
            return "CLOSED"
        t = now.hour * 60 + now.minute
        if 4 * 60 <= t < 9 * 60 + 30:
            return "PRE_MARKET"
        if 9 * 60 + 30 <= t < 16 * 60:
            return "REGULAR"
        if 16 * 60 <= t < 20 * 60:
            return "AFTER_HOURS"
        return "CLOSED"

# ============================================================
# INDICATORS
# ============================================================
def ema(values, period):
    if not values or len(values) < period:
        return None
    k = 2 / (period + 1)
    out = values[0]
    for v in values[1:]:
        out = v * k + out * (1 - k)
    return out


def ema_series(values, period):
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    out = [None] * (period - 1)
    current = sum(values[:period]) / period
    out.append(current)
    for v in values[period:]:
        current = v * k + current * (1 - k)
        out.append(current)
    return out


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = values[i] - values[i - 1]
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        trs.append(max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        ))
    return sum(trs[-period:]) / period


def safe_float(v, default=None):
    try:
        x = float(v)
        if x != x or x in (float("inf"), float("-inf")):
            return default
        return x
    except Exception:
        return default

# ============================================================
# YAHOO DATA
# ============================================================
def yahoo_chart(symbol, range_value="1mo", interval="1h"):
    url = YAHOO_CHART_URL.format(symbol=symbol)
    params = {
        "range": range_value,
        "interval": interval,
        "includePrePost": "true",
        "events": "div,splits",
        "includeAdjustedClose": "true",
    }
    r = http_get(url, params=params)
    data = r.json()
    result = (data.get("chart", {}).get("result") or [None])[0]
    if not result:
        return None
    ts = result.get("timestamp") or []
    q = result.get("indicators", {}).get("quote", [{}])[0]
    adj = result.get("indicators", {}).get("adjclose", [{}])[0]
    closes = q.get("close", [])
    opens = q.get("open", [])
    highs = q.get("high", [])
    lows = q.get("low", [])
    volumes = q.get("volume", [])
    rows = []
    for i, t in enumerate(ts):
        if i >= len(closes):
            continue
        c = safe_float(closes[i])
        if c is None:
            continue
        rows.append({
            "ts": int(t),
            "open": safe_float(opens[i]) if i < len(opens) else None,
            "high": safe_float(highs[i]) if i < len(highs) else None,
            "low": safe_float(lows[i]) if i < len(lows) else None,
            "close": c,
            "volume": safe_float(volumes[i], 0) if i < len(volumes) else 0,
            "adjclose": safe_float(adj.get("adjclose", [])[i]) if i < len(adj.get("adjclose", [])) else None,
        })
    return rows

# ============================================================
# UNIVERSE
# ============================================================
class UniverseLoader:
    ALLOWED_EXCHANGES = {"NASDAQ", "NYSE", "NYSE AMERICAN", "NYSE MKT", "NYSE ARCA"}

    def __init__(self):
        self.symbols = []

    def load_sec(self):
        try:
            r = http_get(SEC_TICKER_URL, timeout=15, retries=2)
            data = r.json()
            fields = data.get("fields", [])
            idx = {name: i for i, name in enumerate(fields)}
            rows = data.get("data", [])
            symbols = set()
            for row in rows:
                exch = str(row[idx.get("exchange", -1)] if idx.get("exchange", -1) >= 0 else "").upper()
                ticker = str(row[idx.get("ticker", -1)] if idx.get("ticker", -1) >= 0 else "").upper().strip()
                if exch in self.ALLOWED_EXCHANGES and 1 <= len(ticker) <= 5 and ticker.isascii():
                    symbols.add(ticker.replace(".", "-"))
            if len(symbols) >= 500:
                self.symbols = sorted(symbols)
                with open(TICKER_CACHE_FILE, "w", encoding="utf-8") as f:
                    json.dump({"updated": datetime.now(TR).isoformat(), "symbols": self.symbols}, f)
                log.info("SEC universe loaded: %d symbols", len(self.symbols))
                return self.symbols
        except Exception as e:
            log.warning("SEC universe update failed: %s", e)
        return self.load_cache()

    def load_cache(self):
        try:
            with open(TICKER_CACHE_FILE, "r", encoding="utf-8") as f:
                obj = json.load(f)
            symbols = obj.get("symbols", obj if isinstance(obj, list) else [])
            if len(symbols) >= 500:
                self.symbols = symbols
                log.info("Ticker cache loaded: %d symbols", len(symbols))
                return symbols
        except Exception as e:
            log.warning("Ticker cache unavailable: %s", e)
        return []

# ============================================================
# BROAD CANDIDATE DISCOVERY
# ============================================================
class CandidateScanner:
    PREDEFINED = [
        "small_cap_gainers",
        "day_gainers",
        "most_actives",
        "day_losers",
    ]

    def __init__(self, allowed_symbols):
        self.allowed = set(allowed_symbols)

    def _screener(self, scr_id):
        params = {"scrIds": scr_id, "count": SCREENER_COUNT, "start": 0}
        r = http_get(YAHOO_SCREENER_URL, params=params, timeout=12, retries=2)
        data = r.json()
        result = data.get("finance", {}).get("result") or []
        if not result:
            return []
        quotes = result[0].get("quotes") or []
        return quotes

    def _merge_quote(self, q, session, merged):
        sym = str(q.get("symbol", "")).upper()
        if sym not in self.allowed:
            return
        price = safe_float(q.get("regularMarketPrice"))
        pre = safe_float(q.get("preMarketPrice"))
        post = safe_float(q.get("postMarketPrice"))
        if session == "PRE_MARKET" and pre:
            price = pre
        elif session == "AFTER_HOURS" and post:
            price = post
        if price is None or not (MIN_PRICE <= price <= MAX_PRICE):
            return
        volume = safe_float(q.get("regularMarketVolume"), 0) or 0
        avg_volume = safe_float(q.get("averageDailyVolume3Month"), 0) or 0
        regular_change = safe_float(q.get("regularMarketChangePercent"), 0) or 0
        prev_close = safe_float(q.get("regularMarketPreviousClose"), 0) or 0
        pre_volume = safe_float(q.get("preMarketVolume"), 0) or 0
        pre_change = safe_float(q.get("preMarketChangePercent"), 0) or 0
        if not pre_change and pre and prev_close:
            pre_change = (pre / prev_close - 1.0) * 100.0
        if session == "PRE_MARKET":
            change = pre_change
            if pre_volume > 0:
                volume = pre_volume
        elif session == "AFTER_HOURS":
            change = safe_float(q.get("postMarketChangePercent"), 0) or regular_change
        else:
            change = regular_change
        dollar = price * max(volume, avg_volume * 0.15)
        if avg_volume < MIN_AVG_DAILY_VOLUME and dollar < MIN_DOLLAR_VOLUME:
            return
        old = merged.get(sym)
        item = {
            "symbol": sym,
            "price": price,
            "volume": volume,
            "avg_volume": avg_volume,
            "change": change,
            "regular_change": regular_change,
            "premarket_change": pre_change,
            "premarket_volume": pre_volume,
            "dollar_volume": dollar,
        }
        if old is None or item["change"] > old["change"] or item["volume"] > old["volume"]:
            merged[sym] = item

    def _nasdaq_market_movers(self, session, merged):
        # Yahoo's undocumented quote/screener endpoints can return 401/400
        # without the required session/crumb state. Do not make the whole
        # discovery layer depend on those endpoints. Nasdaq's public market-
        # movers feed is designed for current US market movers and exposes
        # gainers and most-active names.
        if session not in {"PRE_MARKET", "REGULAR", "AFTER_HOURS"}:
            return
        try:
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/140.0 Safari/537.36"
                ),
                "Accept": "application/json, text/plain, */*",
                "Referer": "https://www.nasdaq.com/",
                "Origin": "https://www.nasdaq.com",
            }
            r = SESSION.get(
                NASDAQ_MOVERS_URL,
                params={"assetclass": "stocks", "exchangeStatus": "currentMarket"},
                headers=headers,
                timeout=12,
            )
            r.raise_for_status()
            payload = r.json()

            # The public endpoint has changed nesting names over time. Walk
            # all nested lists and accept rows that look like stock-mover
            # records instead of depending on one brittle response shape.
            rows = []
            def walk(obj):
                if isinstance(obj, dict):
                    if str(obj.get("symbol", "")).upper():
                        rows.append(obj)
                    for v in obj.values():
                        walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        walk(v)
            walk(payload)

            added = 0
            seen = set()
            for q in rows:
                sym = str(q.get("symbol", "")).upper().strip()
                if not sym or sym in seen or sym not in self.allowed:
                    continue
                seen.add(sym)
                price = (
                    safe_float(q.get("lastSale"))
                    or safe_float(q.get("lastTrade"))
                    or safe_float(q.get("lastPrice"))
                    or safe_float(q.get("price"))
                )
                change = (
                    safe_float(q.get("percentChange"))
                    if q.get("percentChange") is not None
                    else safe_float(q.get("percent_change"), 0)
                ) or 0.0
                volume = (
                    safe_float(q.get("cummulativeVolume"))
                    or safe_float(q.get("cumulativeVolume"))
                    or safe_float(q.get("volume"))
                    or 0.0
                )
                dollar = (
                    safe_float(q.get("dollarAmountTraded"))
                    or safe_float(q.get("dollarVolume"))
                    or (price * volume if price and volume else 0.0)
                )
                if price is None or not (MIN_PRICE <= price <= MAX_PRICE):
                    continue
                if dollar < MIN_DOLLAR_VOLUME and volume <= 0:
                    continue
                avg_volume = 0.0
                prev_close = price / (1.0 + change / 100.0) if change > -99.9 else 0.0
                item = {
                    "symbol": sym,
                    "price": price,
                    "volume": volume,
                    "avg_volume": avg_volume,
                    "change": change,
                    "regular_change": change,
                    "premarket_change": 0.0,
                    "premarket_volume": 0.0,
                    "dollar_volume": dollar,
                    "prev_close": prev_close,
                    "source": "NASDAQ_MARKET_MOVERS",
                }
                old = merged.get(sym)
                if old is None or change > old.get("change", -999) or volume > old.get("volume", 0):
                    merged[sym] = item
                    added += 1
            log.info("Nasdaq market movers: %d aday bulundu", added)
        except Exception as e:
            log.warning("Nasdaq market movers failed: %s", e)

    def _direct_quote_scan(self, session, merged):
        if not ENABLE_YAHOO_DIRECT_QUOTES:
            return
        symbols = sorted(self.allowed)
        for i in range(0, len(symbols), QUOTE_BATCH_SIZE):
            batch = symbols[i:i + QUOTE_BATCH_SIZE]
            try:
                r = http_get(
                    YAHOO_QUOTE_URL,
                    params={"symbols": ",".join(batch)},
                    timeout=12,
                    retries=1,
                )
                data = r.json()
                quotes = ((data.get("quoteResponse") or {}).get("result") or [])
                for q in quotes:
                    self._merge_quote(q, session, merged)
            except Exception as e:
                log.warning("Yahoo direct quote batch %d-%d failed: %s", i, i + len(batch), e)

    def get_active_universe(self):
        merged = {}
        session = TimezoneManager.market_session()

        # PRIMARY DISCOVERY: Nasdaq market movers. This catches the names
        # already accelerating without relying on Yahoo's private quote API.
        self._nasdaq_market_movers(session, merged)

        # Optional Yahoo fallbacks. Disabled by default because Yahoo's
        # undocumented endpoints are currently returning 401/400 in CI.
        self._direct_quote_scan(session, merged)
        if ENABLE_YAHOO_SCREENERS:
            for sid in self.PREDEFINED:
                try:
                    quotes = self._screener(sid)
                    for q in quotes:
                        self._merge_quote(q, session, merged)
                except Exception as e:
                    log.warning("Yahoo screener %s failed: %s", sid, e)

        candidates = list(merged.values())
        # Discovery score deliberately gives pre-market movers a direct path
        # into technical analysis. This is important for explosive names that
        # can move 50%+ before the regular session and otherwise be invisible
        # to a regular-session day-gainer screener.
        premarket = TimezoneManager.market_session() == "PRE_MARKET"
        def quick_score(x):
            s = 0
            change = x["change"]
            if change > 0: s += 10
            if change >= 3: s += 5
            if change >= 10: s += 10
            if change >= 25: s += 15
            if change >= 50: s += 15
            if x["volume"] >= x["avg_volume"] * 1.5 > 0: s += 10
            if x["volume"] >= x["avg_volume"] * 2.5 > 0: s += 10
            if x["dollar_volume"] >= 1_000_000: s += 10
            elif x["dollar_volume"] >= 500_000: s += 5
            if premarket and x["premarket_volume"] > 0:
                s += 8
            return s
        candidates.sort(key=quick_score, reverse=True)
        return candidates

# ============================================================
# SHORT-TERM MOMENTUM HORIZON
# ============================================================
def pct_change_from_bars(rows, bars):
    if len(rows) <= bars:
        return 0.0
    old = rows[-(bars + 1)]["close"]
    new = rows[-1]["close"]
    if not old:
        return 0.0
    return (new / old - 1.0) * 100.0


def classify_momentum_horizon(m5, m15, m30, h1, rvol, near_high, breakout):
    # This is a time-horizon label, not a price prediction or guarantee.
    explosive = (rvol >= 2.0 and near_high and (breakout or m15 >= 8.0))
    if m5 >= 8.0 and m15 >= 12.0 and explosive:
        return "5–15 dk | ÇOK KISA VADE MOMENTUM", "VERY_SHORT"
    if m15 >= 10.0 and m30 >= 15.0 and explosive:
        return "15–60 dk | INTRADAY MOMENTUM", "SHORT"
    if m30 >= 8.0 and h1 >= 10.0 and rvol >= 1.5 and (breakout or near_high):
        return "1–4 saat | GÜN İÇİ MOMENTUM", "INTRADAY"
    if h1 >= 5.0 and rvol >= 1.3:
        return "Bugün | GÜN İÇİ / KISA VADE", "DAY"
    return "1–3 gün | SWING", "SWING"


# ============================================================
# TECHNICAL ANALYSIS / BULLISH SCORE
# ============================================================
def analyze_swing(symbol, quote):
    try:
        daily = yahoo_chart(symbol, "6mo", "1d")
        hourly = yahoo_chart(symbol, "2mo", "1h")
        intraday = yahoo_chart(symbol, "5d", "5m")
        if not daily or len(daily) < 60 or not hourly or len(hourly) < 30 or not intraday:
            return None

        d_close = [x["close"] for x in daily]
        d_high = [x["high"] for x in daily if x["high"] is not None]
        d_low = [x["low"] for x in daily if x["low"] is not None]
        d_vol = [x["volume"] for x in daily]
        price = safe_float(quote.get("price")) or d_close[-1]
        if not (MIN_PRICE <= price <= MAX_PRICE):
            return None

        d_ema20_series = ema_series(d_close, 20)
        d_ema50 = ema(d_close, 50)
        d_ema20 = d_ema20_series[-1]
        d_ema20_prev = d_ema20_series[-6] if len(d_ema20_series) >= 6 else None
        d_rsi = rsi(d_close, 14)
        d_atr = atr(
            [x["high"] for x in daily],
            [x["low"] for x in daily],
            d_close,
            14,
        )
        if None in (d_ema20, d_ema50, d_rsi, d_atr):
            return None

        # Daily RVOL is only a fallback. The primary RVOL is the latest
        # completed 5-minute candle versus the 20 candles immediately before it.
        prev20 = d_vol[-21:-1] if len(d_vol) >= 21 else d_vol[:-1]
        avg_prev20 = sum(prev20) / len(prev20) if prev20 else 0
        day_rvol = (d_vol[-1] / avg_prev20) if avg_prev20 else 0

        # Hourly momentum.
        h_close = [x["close"] for x in hourly]
        h_ema9_series = ema_series(h_close, 9)
        h_ema20_series = ema_series(h_close, 20)
        h_ema9 = h_ema9_series[-1] if h_ema9_series else None
        h_ema20 = h_ema20_series[-1] if h_ema20_series else None
        h_rsi = rsi(h_close, 14)
        h_momentum = (h_close[-1] / h_close[-5] - 1) if len(h_close) >= 5 and h_close[-5] else 0

        # Intraday: latest closed-ish bar, session VWAP, local volume trend.
        now_ts = int(time.time())
        ny_today = datetime.now(NY).date()
        today_rows = []
        for x in intraday:
            dt = datetime.fromtimestamp(x["ts"], NY)
            if dt.date() == ny_today and dt.hour >= 4 and x["close"] is not None:
                today_rows.append((dt, x))
        if not today_rows:
            today_rows = [(datetime.fromtimestamp(x["ts"], NY), x) for x in intraday[-80:]]

        tp = today_rows
        pv = 0.0
        vv = 0.0
        for _, x in tp:
            typical = ((x["high"] or x["close"]) + (x["low"] or x["close"]) + x["close"]) / 3
            vol = x["volume"] or 0
            pv += typical * vol
            vv += vol
        vwap = pv / vv if vv else None

        recent_intraday = [x for _, x in tp]

        # Multi-horizon momentum. Five-minute bars let us distinguish a stock
        # that may move in the next few minutes from a slower 1-3 day setup.
        m5_change = pct_change_from_bars(recent_intraday, 1)
        m15_change = pct_change_from_bars(recent_intraday, 3)
        m30_change = pct_change_from_bars(recent_intraday, 6)
        h1_change = pct_change_from_bars(recent_intraday, 12)
        recent_30_high = max((x["high"] for x in recent_intraday[-30:] if x.get("high") is not None), default=price)
        near_intraday_high = price >= recent_30_high * 0.985 if recent_30_high else False

        last5 = recent_intraday[-5:] if len(recent_intraday) >= 5 else recent_intraday
        rising_count = sum(1 for i in range(1, len(last5)) if last5[i]["close"] > last5[i-1]["close"])
        last3 = recent_intraday[-3:] if len(recent_intraday) >= 3 else recent_intraday
        last3_rising = sum(1 for i in range(1, len(last3)) if last3[i]["close"] > last3[i-1]["close"]) >= max(1, len(last3)-1)
        vol_last = [x["volume"] or 0 for x in recent_intraday[-10:]]
        volume_increasing = len(vol_last) >= 6 and sum(vol_last[-3:]) / 3 > sum(vol_last[:3]) / 3

        # Primary RVOL: last COMPLETED 5m candle / average of the 20
        # immediately preceding candles. The current in-progress candle is excluded.
        intraday_rvol = 0.0
        usable = recent_intraday if len(recent_intraday) >= 22 else intraday[-22:]
        if len(usable) >= 22:
            current_bar = usable[-2]
            baseline = [x["volume"] or 0 for x in usable[-22:-2]]
            baseline = [v for v in baseline if v > 0]
            if len(baseline) >= 10:
                base = sum(baseline) / len(baseline)
                if base > 0:
                    intraday_rvol = (current_bar["volume"] or 0) / base
        # Do not mix Yahoo screener's cumulative intraday volume ratio into the
        # candle RVOL; that was the source of misleading values such as 0.25.
        rvol = intraday_rvol if intraday_rvol > 0 else day_rvol
        momentum_horizon, momentum_horizon_code = classify_momentum_horizon(
            m5_change, m15_change, m30_change, h1_change, rvol,
            near_intraday_high, False,
        )

        # Resistance / breakout levels from completed daily bars.
        prev_daily = daily[:-1]
        resistance5 = max(x["high"] for x in prev_daily[-5:] if x["high"] is not None)
        resistance20 = max(x["high"] for x in prev_daily[-20:] if x["high"] is not None)
        high52 = max(d_high[-252:]) if d_high else resistance20
        near_resistance = resistance20 > 0 and price >= resistance20 * 0.97
        breakout = price > resistance20
        distance_to_res = ((resistance20 - price) / price) if price else 999
        momentum_horizon, momentum_horizon_code = classify_momentum_horizon(
            m5_change, m15_change, m30_change, h1_change, rvol,
            near_intraday_high, breakout,
        )

        # Spread proxy if quote provides bid/ask; otherwise don't reject.
        bid = safe_float(quote.get("bid"))
        ask = safe_float(quote.get("ask"))
        spread_pct = ((ask - bid) / price) if bid and ask and ask >= bid and price else 0
        if spread_pct > 0.06:
            return None

        dollar_volume = max(
            safe_float(quote.get("dollar_volume"), 0) or 0,
            price * (safe_float(quote.get("volume"), 0) or 0),
        )
        if dollar_volume < MIN_DOLLAR_VOLUME and (safe_float(quote.get("avg_volume"), 0) or 0) * price < MIN_DOLLAR_VOLUME:
            return None

        # Hard rejects: not a clean bullish setup.
        if d_rsi > 82:
            return None
        # Very large moves are not automatically rejected anymore. We want to
        # catch genuine momentum runners early. Instead, an extended move must
        # have stronger confirmation (high RVOL + rising volume + breakout/near
        # resistance) before it can become a LONG candidate.
        daily_change = safe_float(quote.get("change"), 0) or 0
        extended_move = daily_change > MAX_DAILY_GAIN_LONG
        if h_rsi is not None and h_rsi > 82 and h_momentum < 0:
            return None
        if price < d_ema20 * 0.94 and (h_ema9 is None or h_ema9 <= h_ema20):
            return None
        # No volume confirmation = no bullish swing signal.
        if rvol < MIN_RVOL:
            return None
        # RSI below 45 is treated as early/mixed momentum, not a LONG setup.
        if d_rsi < MIN_RSI_LONG:
            return None
        # Explosive setups must still have positive short-term momentum. This
        # prevents the scanner from chasing a stock that already reversed.
        if extended_move and m15_change < 3.0 and not breakout:
            return None
        if extended_move and not (rvol >= 2.0 and volume_increasing and (breakout or near_resistance)):
            return None
        # Do not send weak non-breakout setups merely because several soft
        # indicators happen to score points.
        if (not breakout and not near_resistance and rvol < 1.20
                and daily_change < 1.0):
            return None

        # ---------------- SCORE ----------------
        score = 0
        reasons = []

        if price > d_ema20:
            score += 10; reasons.append("Price > EMA20")
        if h_ema9 is not None and h_ema20 is not None and h_ema9 > h_ema20:
            score += 10; reasons.append("EMA9 > EMA20")
        if d_ema20_prev is not None and d_ema20 > d_ema20_prev:
            score += 8; reasons.append("EMA20 yükseliyor")
        if h_momentum > 0:
            score += 10; reasons.append("1H momentum pozitif")
        if 45 <= d_rsi <= 70:
            score += 10; reasons.append("RSI uygun")
        elif 70 < d_rsi <= 78:
            score += 5; reasons.append("RSI güçlü")
        if rvol >= 1.5:
            score += 10; reasons.append("RVOL ≥ 1.5")
        if rvol >= 2.5:
            score += 8; reasons.append("RVOL ≥ 2.5")
        if vwap is not None and price >= vwap:
            score += 10; reasons.append("VWAP üstü")
        if near_resistance:
            score += 8; reasons.append("Dirence yakın")
        if breakout:
            score += 12; reasons.append("Breakout")
        if rising_count >= max(2, len(last5) - 2):
            score += 8; reasons.append("Son mumlar yükseliyor")
        if last3_rising:
            score += 3
        if volume_increasing:
            score += 8; reasons.append("Hacim artıyor")

        # Short-term momentum bonuses. These deliberately have a large weight
        # so a fresh runner can reach detailed analysis even when its daily
        # swing indicators have not caught up yet.
        if m5_change >= 3:
            score += 8; reasons.append("5dk momentum +3%")
        if m5_change >= 8:
            score += 7; reasons.append("5dk momentum +8%")
        if m15_change >= 5:
            score += 8; reasons.append("15dk momentum +5%")
        if m15_change >= 12:
            score += 7; reasons.append("15dk momentum +12%")
        if m30_change >= 8:
            score += 6; reasons.append("30dk momentum +8%")
        if h1_change >= 10:
            score += 5; reasons.append("1s momentum +10%")
        if near_intraday_high:
            score += 6; reasons.append("Gün içi zirveye yakın")

        # Small bonus for meaningful liquidity, but do not let liquidity dominate.
        if dollar_volume >= 1_000_000:
            score += 4
        elif dollar_volume >= 500_000:
            score += 2

        # ---------------- TARGET / STOP ----------------
        recent_support = min(x["low"] for x in daily[-10:] if x["low"] is not None)
        atr_stop = price - max(d_atr * 1.0, price * 0.04)
        structural_stop = recent_support * 0.985
        stop = max(0.01, min(price * 0.97, max(atr_stop, structural_stop)))
        risk = price - stop
        if risk <= 0 or risk / price > 0.25:
            return None

        # Targets are deliberately bounded by risk/ATR so a distant 52-week
        # high cannot create absurd RR values such as 17x.
        min_tp1 = max(price * 1.04, price + 1.5 * risk)
        max_tp1 = price + 2.5 * risk
        resistance_levels = sorted({
            round(r, 6) for r in (resistance5, resistance20, high52)
            if r and r >= min_tp1 and r <= max_tp1
        })
        if resistance_levels:
            tp1 = resistance_levels[0]
        else:
            tp1 = min(max_tp1, max(min_tp1, price + 1.5 * d_atr))

        # Keep the targets ordered and progressively farther away, while
        # capping the total target distance at 4R.
        tp2 = min(price + 3.25 * risk, max(tp1 + 0.75 * risk, price + 2.0 * d_atr))
        tp3 = min(price + 4.0 * risk, max(tp2 + 0.75 * risk, price + 3.0 * d_atr))
        if tp2 <= tp1 or tp3 <= tp2:
            return None

        tp1_gain = tp1 / price - 1
        tp2_gain = tp2 / price - 1
        tp3_gain = tp3 / price - 1
        if tp1_gain < 0.04 or tp2_gain < 0.08 or tp3_gain < 0.12:
            return None

        # RR shown in Telegram is explicitly TP1 RR.
        rr = (tp1 - price) / risk if risk else 0
        if rr < MIN_RR or rr > 2.5:
            return None
        if rr >= IDEAL_RR:
            score += 5
            reasons.append("RR ≥ 2")

        # Avoid calling weak setups merely because a stock happened to be in a screener.
        if score < MIN_SCORE:
            return None

        if score >= STRONG_SCORE:
            strength = "STRONG MOMENTUM"
        elif score >= MOMENTUM_SCORE:
            strength = "BULLISH MOMENTUM"
        else:
            strength = "WATCH / EARLY MOMENTUM"

        max_target = tp3
        if tp3 >= price * 1.35:
            max_target = price * 1.35

        daily_change = safe_float(quote.get("change"), 0) or 0
        return {
            "symbol": symbol,
            "entry": price,
            "stop": round(stop, 4),
            "tp1": round(tp1, 4),
            "tp2": round(tp2, 4),
            "tp3": round(tp3, 4),
            "max_target": round(max_target, 4),
            "score": round(min(score, 100), 1),
            "rvol": round(rvol, 2),
            "rsi": round(d_rsi, 1),
            "daily_change": round(daily_change, 2),
            "extended_move": extended_move,
            "premarket_change": round(safe_float(quote.get("premarket_change"), 0) or 0, 2),
            "m5_change": round(m5_change, 2),
            "m15_change": round(m15_change, 2),
            "m30_change": round(m30_change, 2),
            "h1_change": round(h1_change, 2),
            "near_intraday_high": near_intraday_high,
            "momentum_horizon": momentum_horizon,
            "momentum_horizon_code": momentum_horizon_code,
            "rr": round(rr, 2),
            "expected_days": momentum_horizon,
            "strength": strength,
            "vwap": vwap,
            "vwap_status": "ABOVE" if vwap is not None and price >= vwap else "BELOW",
            "breakout": breakout,
            "near_resistance": near_resistance,
            "volume_increasing": volume_increasing,
            "dollar_volume": dollar_volume,
            "reasons": reasons,
            "session": TimezoneManager.market_session(),
            "h_momentum": h_momentum,
        }
    except Exception as e:
        log.debug("Analysis failed %s: %s", symbol, e)
        return None

# ============================================================
# GEMINI
# ============================================================
class GeminiEvaluator:
    def __init__(self, keys):
        self.keys = keys
        self.index = 0

    def _client(self, key):
        if genai is None:
            return None
        return genai.Client(api_key=key)

    def evaluate(self, candidates):
        if not candidates:
            return {}
        if not self.keys:
            return {c["symbol"]: {"decision": "WATCH", "reason": "Gemini anahtarı yok; teknik skor kullanıldı."} for c in candidates}
        if DB.gemini_requests_today() >= MAX_GEMINI_DAILY_REQUESTS:
            return {c["symbol"]: {"decision": "WATCH", "reason": "Gemini günlük limitine ulaşıldı; teknik skor kullanıldı."} for c in candidates}

        batch = candidates[:8]
        compact = []
        for c in batch:
            compact.append({
                "symbol": c["symbol"],
                "price": round(c["entry"], 4),
                "score": c["score"],
                "rvol": c["rvol"],
                "rsi": c["rsi"],
                "rr": c["rr"],
                "daily_change": c["daily_change"],
                "m5_change": c.get("m5_change", 0),
                "m15_change": c.get("m15_change", 0),
                "m30_change": c.get("m30_change", 0),
                "h1_change": c.get("h1_change", 0),
                "momentum_horizon": c.get("momentum_horizon", NORMAL_EXPECTED_DAYS),
                "vwap": c["vwap_status"],
                "breakout": c["breakout"],
                "volume_increasing": c["volume_increasing"],
                "reasons": c["reasons"][:8],
            })
        prompt = """
You are a neutral technical screener assisting a short-term US stock alert bot.
Evaluate only the supplied candidates. Do not invent news or fundamentals.
Focus on whether the supplied setup has actionable bullish momentum and which time horizon best matches the CURRENT momentum: 5-15 minutes, 15-60 minutes, 1-4 hours, today, or 1-3 days. Do not assume a stock will rise; classify the evidence only.
Return ONLY valid JSON as an array. Each item must be:
{"symbol":"XYZ","decision":"BUY|WATCH|PASS","reason":"short Turkish reason"}
BUY = technically coherent bullish setup.
WATCH = mixed but potentially developing.
PASS = clearly weak/contradictory setup.
Do not use price prediction certainty and do not guarantee gains.
Candidates:
""" + json.dumps(compact, ensure_ascii=False)

        for _ in range(len(self.keys)):
            key = self.keys[self.index % len(self.keys)]
            self.index += 1
            try:
                client = self._client(key)
                if client is None:
                    break
                # Gemini 3.6 Flash is supported through the current Interactions API.
                # Keep generate_content as a compatibility fallback for older SDKs.
                text = ""
                try:
                    interaction = client.interactions.create(
                        model=GEMINI_MODEL,
                        input=prompt,
                        generation_config={"thinking_level": "low"},
                    )
                    text = (getattr(interaction, "output_text", "") or "").strip()
                except Exception as interaction_error:
                    log.warning("Gemini Interactions API failed; trying legacy generateContent: %s", interaction_error)
                    response = client.models.generate_content(
                        model=GEMINI_MODEL,
                        contents=prompt,
                    )
                    text = (getattr(response, "text", "") or "").strip()

                if not text:
                    raise RuntimeError("Gemini boş yanıt döndürdü")
                DB.increment_gemini()
                if text.startswith("```"):
                    text = text.replace("```json", "").replace("```", "").strip()
                parsed = json.loads(text)
                out = {}
                for item in parsed:
                    sym = str(item.get("symbol", "")).upper()
                    decision = str(item.get("decision", "WATCH")).upper()
                    if decision not in {"BUY", "WATCH", "PASS"}:
                        decision = "WATCH"
                    out[sym] = {"decision": decision, "reason": str(item.get("reason", ""))[:300]}
                return out
            except Exception as e:
                log.warning("Gemini attempt failed: %s", e)
                continue
        return {c["symbol"]: {"decision": "WATCH", "reason": "Gemini yanıtı alınamadı; teknik skor kullanıldı."} for c in candidates}

GEMINI = GeminiEvaluator(GEMINI_API_KEYS)

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram_message(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram ENV eksik; mesaj gönderilmedi.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        )
        if r.status_code == 200:
            return True
        log.warning("Telegram error %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        log.warning("Telegram failed: %s", e)
    return False


def build_open_message(c):
    breakout_text = "BROKE RESISTANCE" if c["breakout"] else ("NEAR RESISTANCE" if c["near_resistance"] else "NO BREAKOUT")
    vol_text = "INCREASING" if c["volume_increasing"] else "NORMAL"
    gem = c.get("gemini_reason", "")
    return (
        "🚀 NASDAQ SWING ALERT\n\n"
        f"📌 {c['symbol']}\n"
        "🟢 LONG\n"
        f"💵 Entry: ${c['entry']:.4f}\n\n"
        f"🟢 {c['strength']}\n"
        f"⭐ Score: {c['score']:.0f}/100\n\n"
        f"📈 EMA Trend: {('BULLISH' if 'EMA9 > EMA20' in c['reasons'] else 'MIXED')}\n"
        f"📊 RSI: {c['rsi']:.1f}\n"
        f"🔥 RVOL: {c['rvol']:.2f}\n"
        f"📍 VWAP: {c['vwap_status']}\n"
        f"🚀 Breakout: {breakout_text}\n"
        f"💰 Volume: {vol_text}\n\n"
        f"🎯 TP1: ${c['tp1']:.4f}\n"
        f"🎯 TP2: ${c['tp2']:.4f}\n"
        f"🎯 TP3: ${c['tp3']:.4f}\n\n"
        f"🛑 Stop: ${c['stop']:.4f}\n"
        f"⚖️ RR: {c['rr']:.2f}\n\n"
        f"⏱ Momentum ufku: {c.get('momentum_horizon', c['expected_days'])}\n"
        f"⚡ 5dk: {c.get('m5_change', 0):+.2f}% | 15dk: {c.get('m15_change', 0):+.2f}% | 30dk: {c.get('m30_change', 0):+.2f}%\n"
        f"🕐 1s: {c.get('h1_change', 0):+.2f}% | Günlük: {c['daily_change']:+.2f}%\n"
        + (f"🔥 Pre-market: {c.get('premarket_change', 0):+.2f}%\n" if c.get('premarket_change', 0) else "")
        + ("⚠️ Yüksek volatilite / momentum\n" if c.get('extended_move') else "")
        + "\n"
        + f"🤖 Gemini: {gem or 'Teknik skor baz alındı.'}\n\n"
        "🟢 Durum: AKTİF\n\n"
        "⚠️ Not: Bu bir momentum sinyalidir; yükseliş garantisi yoktur.\n"
        "Kısa vadeli hareketlerde volatilite ve işlem durdurmaları görülebilir."
    )


def build_update_message(s, event, price, note=""):
    return (
        f"📢 SIGNAL UPDATE\n\n"
        f"📌 {s['symbol']}\n"
        f"💵 Fiyat: ${price:.4f}\n"
        f"🔔 {event}\n"
        f"{note}\n"
        f"🟢 Durum: {'AKTİF' if event not in ('STOP', 'TP3', 'CLOSED') else 'KAPANDI'}"
    )

# ============================================================
# SIGNAL TRACKER
# ============================================================
def current_price(symbol):
    try:
        rows = yahoo_chart(symbol, "1d", "5m")
        if rows:
            return rows[-1]["close"]
    except Exception:
        pass
    return None


def track_open_signals():
    opens = DB.get_open_signals()
    if not opens:
        return
    for s in opens:
        price = current_price(s["symbol"])
        if price is None:
            continue

        opened = datetime.fromisoformat(s["opened_at"])
        if opened.tzinfo is None:
            opened = opened.replace(tzinfo=TR)
        age_days = (datetime.now(TR) - opened).total_seconds() / 86400

        # Stop first: a bar that crosses both a target and stop is treated conservatively.
        if price <= s["stop_loss"]:
            DB.close_signal(s["id"], "STOP", price, "Stop seviyesi görüldü.")
            send_telegram_message(build_update_message(s, "STOP", price, "🛑 Stop çalıştı."))
            continue

        if not s["tp1_hit"] and price >= s["tp1"]:
            DB.update_signal(s["id"], tp1_hit=1, stop_loss=s["entry_price"], current_price=price)
            DB.add_update(s["id"], "TP1", price, "TP1 görüldü; stop giriş fiyatına taşındı.")
            send_telegram_message(build_update_message(s, "TP1", price, "🎯 TP1 görüldü. Stop → giriş."))
            s["tp1_hit"] = 1
            s["stop_loss"] = s["entry_price"]

        if s["tp1_hit"] and not s["tp2_hit"] and price >= s["tp2"]:
            DB.update_signal(s["id"], tp2_hit=1, stop_loss=s["tp1"], current_price=price)
            DB.add_update(s["id"], "TP2", price, "TP2 görüldü; stop TP1'e taşındı.")
            send_telegram_message(build_update_message(s, "TP2", price, "🎯 TP2 görüldü. Stop → TP1."))
            s["tp2_hit"] = 1
            s["stop_loss"] = s["tp1"]

        if price >= s["tp3"]:
            DB.close_signal(s["id"], "TP3", price, "TP3 görüldü; sinyal kapandı.")
            send_telegram_message(build_update_message(s, "TP3", price, "🎯 TP3 görüldü. Sinyal kapandı."))
            continue

        if age_days >= MAX_SIGNAL_DAYS:
            DB.close_signal(s["id"], "CLOSED", price, "10 günlük maksimum süre doldu.")
            send_telegram_message(build_update_message(s, "CLOSED", price, "⏱ Maksimum 10 gün doldu."))
            continue

        DB.update_signal(s["id"], current_price=price)

# ============================================================
# PIPELINE
# ============================================================
def cooldown_allows(symbol, candidate):
    if DB.has_open_signal(symbol):
        return False
    recent = DB.recent_signal(symbol)
    if not recent:
        return True
    old_score = safe_float(recent.get("score"), 0) or 0
    old_rvol = safe_float(recent.get("rvol"), 0) or 0
    old_entry = safe_float(recent.get("entry_price"), 0) or 0
    new_score = candidate["score"]
    new_rvol = candidate["rvol"]
    new_entry = candidate["entry"]
    move = abs(new_entry - old_entry) / old_entry if old_entry else 0
    if new_score - old_score >= COOLDOWN_SCORE_OVERRIDE:
        return True
    if new_rvol - old_rvol >= COOLDOWN_RVOL_OVERRIDE:
        return True
    if move >= COOLDOWN_MOVE_OVERRIDE:
        return True
    return False


def run_market_pipeline():
    if not SCAN_LOCK.acquire(blocking=False):
        log.info("Önceki tarama hâlâ çalışıyor; bu tur atlandı.")
        return
    try:
        session = TimezoneManager.market_session()
        if session == "CLOSED":
            return

        track_open_signals()

        loader = UniverseLoader()
        allowed = loader.symbols or loader.load_sec()
        if len(allowed) < 500:
            log.warning("Universe <500 (%d). Fallback yok; tarama durduruldu.", len(allowed))
            return

        scanner = CandidateScanner(allowed)
        quick = scanner.get_active_universe()
        if not quick:
            log.info("Broad screener bu tur aday döndürmedi.")
            return

        quick = quick[:DETAILED_CANDIDATES]
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [ex.submit(analyze_swing, q["symbol"], q) for q in quick]
            for f in concurrent.futures.as_completed(futures):
                try:
                    item = f.result()
                    if item:
                        results.append(item)
                except Exception:
                    pass

        if not results:
            log.info("Detaylı teknik taramada uygun aday çıkmadı.")
            return

        # Prioritize fresh, short-horizon momentum instead of letting a slower
        # 1-5 day setup bury a stock that is accelerating RIGHT NOW.
        horizon_priority = {"VERY_SHORT": 5, "SHORT": 4, "INTRADAY": 3, "DAY": 2, "SWING": 1}
        results.sort(
            key=lambda x: (
                horizon_priority.get(x.get("momentum_horizon_code"), 0),
                x.get("m15_change", 0),
                x.get("m5_change", 0),
                x["score"],
                x["rvol"],
            ),
            reverse=True,
        )
        eligible = [r for r in results if cooldown_allows(r["symbol"], r)]
        if not eligible:
            log.info("Adaylar bulundu ancak açık/soğuma filtresinden geçmedi.")
            return

        top = eligible[:12]
        gemini = GEMINI.evaluate(top)

        sent = 0
        for c in top:
            g = gemini.get(c["symbol"], {"decision": "WATCH", "reason": ""})
            decision = g.get("decision", "WATCH")
            # PASS always rejects. WATCH can pass only when the technical setup
            # is exceptionally strong OR it is an explosive, strongly confirmed
            # momentum move that we explicitly want to catch early.
            if decision == "PASS":
                continue
            if decision == "WATCH":
                strong_explosive = (
                    c.get("momentum_horizon_code") in {"VERY_SHORT", "SHORT"}
                    and c["score"] >= 75
                    and c["rvol"] >= 2.0
                    and c["volume_increasing"]
                    and (c["breakout"] or c["near_resistance"] or c.get("near_intraday_high"))
                    and c.get("m15_change", 0) >= 8.0
                )
                weak_setup = (
                    (c["score"] < MOMENTUM_SCORE and not strong_explosive)
                    or c["rsi"] < MIN_RSI_LONG
                    or (not strong_explosive and not c["breakout"] and c["rvol"] < 1.20 and c["daily_change"] < 1.0)
                )
                if weak_setup:
                    continue
            c["gemini_status"] = decision
            c["gemini_reason"] = g.get("reason", "")[:300]
            msg = build_open_message(c)
            if send_telegram_message(msg):
                DB.create_signal(c)
                DB.add_update(DB.get_open_signals()[-1]["id"], "OPEN", c["entry"], "Yeni sinyal açıldı.")
                sent += 1
                if sent >= 5:
                    break

        log.info(
            "Tarama tamamlandı | session=%s quick=%d detailed=%d eligible=%d sent=%d",
            session, len(quick), len(results), len(eligible), sent
        )
    finally:
        SCAN_LOCK.release()

# ============================================================
# SCHEDULER
# ============================================================
def run_once():
    run_market_pipeline()


def run_daemon():
    scheduler = BackgroundScheduler(timezone=NY)
    # Premarket + regular market: every 5 minutes.
    scheduler.add_job(
        run_market_pipeline,
        CronTrigger(day_of_week="mon-fri", hour="4-20", minute="*/5", timezone=NY),
        id="market_scan",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info("NASDAQ Trading Bot çalışıyor | NY session=%s | model=%s", TimezoneManager.market_session(), GEMINI_MODEL)
    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown(wait=False)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    log.info("NASDAQ Swing Bot başlatılıyor...")
    log.info("ENV: Gemini keys=%d | Telegram=%s | DB=%s", len(GEMINI_API_KEYS), bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID), DATABASE_PATH)

    # Load/update SEC universe at startup.
    UniverseLoader().load_sec()

    if "--once" in sys.argv:
        run_once()
    else:
        run_daemon()
