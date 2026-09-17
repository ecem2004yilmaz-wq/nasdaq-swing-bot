import os
import sys
import json
import time
import sqlite3
import logging
import random
import threading
import requests
import concurrent.futures
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from google import genai

# ============================================================
# ENV / CONFIG
# ============================================================
load_dotenv()

GEMINI_API_KEYS = [
    os.getenv("GEMINI_API_KEY_1", ""),
    os.getenv("GEMINI_API_KEY_2", ""),
]
GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

DATABASE_PATH = os.getenv("DATABASE_PATH", "alerts.db")
TICKER_CACHE_FILE = os.getenv("TICKER_CACHE_FILE", "nasdaq_tickers.json")

# Kullanıcının sermayesine uygun tarama aralığı.
MIN_PRICE = 0.01
MAX_PRICE = 15.00

# Swing stratejisi: hedef 1 haftaya kadar; çok güçlü yapı varsa 10 güne kadar takip.
MAX_SIGNAL_DAYS = 10
NORMAL_SIGNAL_DAYS = 5

# Sıkı kalite filtreleri.
MIN_SCORE = 58
PENNY_MIN_SCORE = 65
MIN_RVOL = 1.00
PENNY_MIN_RVOL = 0.50
MIN_DAILY_CHANGE = 1.0
MIN_DOLLAR_VOLUME = 200_000
PENNY_MIN_DOLLAR_VOLUME = 350_000
MIN_TP1_GAIN = 0.04       # %4
MIN_TP2_GAIN = 0.08       # %8
MIN_TP3_GAIN = 0.12       # %12
MIN_RR = 1.30

# Yahoo screener en fazla 250 sonuç döndürür. Ayrı ön filtreler birleştiriliyor.
SCREENER_COUNT = 250
DETAILED_CANDIDATES = 100
MAX_WORKERS = 3
YAHOO_TIMEOUT = 12
YAHOO_RETRIES = 3
YAHOO_BASE_BACKOFF = 1.5

# Açık sinyaller yalnızca kendileri için takip edilir.
POSITION_CHECK_INTERVAL_MINUTES = 5
SIGNAL_COOLDOWN_HOURS = 24

MAX_GEMINI_DAILY_REQUESTS = 1400

NY_TZ = ZoneInfo("America/New_York")
TR_TZ = ZoneInfo("Europe/Istanbul")
SCAN_LOCK = threading.Lock()
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "Mozilla/5.0 (NASDAQ Swing Bot/2.0)"})

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("NASDAQ-SWING-BOT")

# ============================================================
# DATABASE
# ============================================================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_database()

    def get_connection(self):
        return sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)

    def init_database(self):
        conn = self.get_connection()
        cur = conn.cursor()

        # Eski alerts tablosunu silmiyoruz; mevcut geçmiş korunuyor.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                price REAL,
                score REAL,
                rvol REAL,
                target REAL,
                stop_loss REAL,
                gemini_decision TEXT,
                gemini_reason TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS gemini_usage (
                date TEXT PRIMARY KEY,
                request_count INTEGER DEFAULT 0
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS swing_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT,
                status TEXT NOT NULL DEFAULT 'OPEN',
                entry_price REAL NOT NULL,
                current_price REAL,
                stop_loss REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp3 REAL NOT NULL,
                tp1_hit INTEGER DEFAULT 0,
                tp2_hit INTEGER DEFAULT 0,
                tp3_hit INTEGER DEFAULT 0,
                max_target TEXT DEFAULT 'TP1',
                score REAL,
                rvol REAL,
                rsi REAL,
                daily_change REAL,
                rr REAL,
                expected_days INTEGER,
                gemini_status TEXT,
                gemini_reason TEXT,
                last_update_at TEXT,
                last_notified_price REAL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_updates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                event TEXT NOT NULL,
                price REAL,
                note TEXT
            )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_swing_status ON swing_signals(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_swing_symbol ON swing_signals(symbol)")
        conn.commit()
        conn.close()

    def get_gemini_usage(self):
        today = datetime.now().strftime("%Y-%m-%d")
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT request_count FROM gemini_usage WHERE date = ?", (today,))
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0

    def increment_gemini_usage(self):
        today = datetime.now().strftime("%Y-%m-%d")
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO gemini_usage(date, request_count)
            VALUES (?, 1)
            ON CONFLICT(date)
            DO UPDATE SET request_count = request_count + 1
        """, (today,))
        conn.commit()
        conn.close()

    def has_recent_signal(self, symbol):
        cutoff = (datetime.now() - timedelta(hours=SIGNAL_COOLDOWN_HOURS)).isoformat()
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM swing_signals
            WHERE symbol = ? AND opened_at >= ?
            ORDER BY id DESC LIMIT 1
        """, (symbol, cutoff))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def has_open_signal(self, symbol):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM swing_signals WHERE symbol = ? AND status = 'OPEN' LIMIT 1", (symbol,))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def create_signal(self, c, gemini_status, gemini_reason):
        now = datetime.now().isoformat()
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO swing_signals (
                symbol, opened_at, status, entry_price, current_price,
                stop_loss, tp1, tp2, tp3, score, rvol, rsi,
                daily_change, rr, expected_days, max_target,
                gemini_status, gemini_reason, last_update_at, last_notified_price
            ) VALUES (?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            c["symbol"], now, c["price"], c["price"], c["stop_loss"],
            c["tp1"], c["tp2"], c["tp3"], c["score"], c["rvol"], c["rsi"],
            c["daily_change"], c["rr"], c["expected_days"], c["max_target"],
            gemini_status, gemini_reason, now, c["price"],
        ))
        signal_id = cur.lastrowid
        cur.execute("""
            INSERT INTO signal_updates(signal_id, timestamp, event, price, note)
            VALUES (?, ?, 'OPEN', ?, ?)
        """, (signal_id, now, c["price"], "Yeni swing sinyali"))
        conn.commit()
        conn.close()
        return signal_id

    def get_open_signals(self):
        conn = self.get_connection()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM swing_signals WHERE status = 'OPEN' ORDER BY id ASC")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        return rows

    def update_signal(self, signal_id, **fields):
        if not fields:
            return
        allowed = {
            "status", "closed_at", "current_price", "tp1_hit", "tp2_hit", "tp3_hit",
            "last_update_at", "last_notified_price", "stop_loss", "max_target"
        }
        fields = {k: v for k, v in fields.items() if k in allowed}
        if not fields:
            return
        fields["last_update_at"] = datetime.now().isoformat()
        cols = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [signal_id]
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute(f"UPDATE swing_signals SET {cols} WHERE id = ?", values)
        conn.commit()
        conn.close()

    def add_update(self, signal_id, event, price, note):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO signal_updates(signal_id, timestamp, event, price, note)
            VALUES (?, ?, ?, ?, ?)
        """, (signal_id, datetime.now().isoformat(), event, price, note))
        conn.commit()
        conn.close()

    def get_signal_opened_at(self, signal_id):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT opened_at FROM swing_signals WHERE id = ?", (signal_id,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None


db = DatabaseManager(DATABASE_PATH)

# ============================================================
# TIME / MARKET
# ============================================================
class TimezoneManager:
    @staticmethod
    def now_ny():
        return datetime.now(NY_TZ)

    @staticmethod
    def now_tr():
        return datetime.now(TR_TZ)

    @staticmethod
    def get_market_session():
        now = TimezoneManager.now_ny()
        if now.weekday() >= 5:
            return "CLOSED"
        t = now.time()
        if datetime.strptime("04:00", "%H:%M").time() <= t < datetime.strptime("09:30", "%H:%M").time():
            return "PRE_MARKET"
        if datetime.strptime("09:30", "%H:%M").time() <= t < datetime.strptime("16:00", "%H:%M").time():
            return "REGULAR"
        if datetime.strptime("16:00", "%H:%M").time() <= t < datetime.strptime("20:00", "%H:%M").time():
            return "AFTER_HOURS"
        return "CLOSED"

# ============================================================
# UNIVERSE
# ============================================================
class UniverseLoader:
    SEC_URL = "https://www.sec.gov/files/company_tickers_exchange.json"

    @staticmethod
    def load_universe():
        if os.path.exists(TICKER_CACHE_FILE):
            try:
                age = time.time() - os.path.getmtime(TICKER_CACHE_FILE)
                if age < 24 * 3600:
                    with open(TICKER_CACHE_FILE, "r", encoding="utf-8") as f:
                        tickers = json.load(f)
                    if len(tickers) >= 500:
                        return tickers
            except Exception:
                pass

        try:
            r = HTTP.get(UniverseLoader.SEC_URL, timeout=20, headers={"User-Agent": "NASDAQ Swing Bot/2.0 contact: bot@example.com"})
            r.raise_for_status()
            rows = r.json().get("data", [])
            exchanges = {"NASDAQ", "NYSE", "NYSE AMERICAN", "NYSE MKT", "NYSE ARCA"}
            tickers = []
            for row in rows:
                if len(row) < 3:
                    continue
                ticker = str(row[1]).upper().strip()
                exchange = str(row[2]).upper().strip()
                if exchange in exchanges and ticker and len(ticker) <= 5 and ticker.isalpha():
                    tickers.append(ticker)
            tickers = sorted(set(tickers))
            if len(tickers) < 500:
                raise RuntimeError(f"Universe çok küçük: {len(tickers)}")
            with open(TICKER_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(tickers, f)
            logger.info("Universe hazır: %s NASDAQ/NYSE ticker", len(tickers))
            return tickers
        except Exception as e:
            logger.warning("SEC universe alınamadı: %s", e)
            return []

# ============================================================
# YAHOO SCREENER: 4,000+ TICKER'I TEK TEK TARAMAK YERİNE
# ÖNCE O AN HAREKETLİ OLANLARI BULUR.
# ============================================================
SCREENER_URL = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
CUSTOM_SCREENER_URL = "https://query2.finance.yahoo.com/v1/finance/screener"


def _screener_get(params):
    for attempt in range(YAHOO_RETRIES):
        try:
            r = HTTP.get(SCREENER_URL, params=params, timeout=YAHOO_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504) and attempt < YAHOO_RETRIES - 1:
                time.sleep(min(YAHOO_BASE_BACKOFF * (2 ** attempt) + random.random(), 8))
                continue
            logger.warning("Yahoo screener HTTP %s", r.status_code)
            return None
        except requests.RequestException as e:
            if attempt < YAHOO_RETRIES - 1:
                time.sleep(min(YAHOO_BASE_BACKOFF * (2 ** attempt), 8))
            else:
                logger.warning("Yahoo screener request error: %s", e)
    return None


def get_active_universe():
    """Pre-market + regular-session aday havuzu.

    Yahoo'nun crumb isteyen custom screener endpoint'i kullanılmaz. Bunun yerine
    public predefined screener listeleri ve pre-market alanları kullanılır.
    Böylece Invalid Crumb/401 hatası botu bozmaz.
    """
    quotes = []
    scrids = ["small_cap_gainers", "day_gainers", "most_actives"]

    for scr_id in scrids:
        data = _screener_get({
            "formatted": "false",
            "lang": "en-US",
            "region": "US",
            "scrIds": scr_id,
            "count": SCREENER_COUNT,
            "corsDomain": "finance.yahoo.com",
        })
        if data:
            try:
                quotes.extend(data["finance"]["result"][0].get("quotes", []))
            except Exception:
                pass

    now_ny = TimezoneManager.now_ny()
    session = TimezoneManager.get_market_session()
    unique = {}

    for q in quotes:
        symbol = str(q.get("symbol", "")).upper().strip()
        if not symbol:
            continue

        # Pre-market'te güncel fiyat/hacim alanlarını önceliklendir.
        if session == "PRE_MARKET":
            price = q.get("preMarketPrice") or q.get("regularMarketPrice") or q.get("intradayPrice")
            change = q.get("preMarketChangePercent")
            if change is None:
                change = q.get("regularMarketChangePercent", q.get("percentChange", 0))
            volume = q.get("preMarketVolume") or q.get("regularMarketVolume") or q.get("dayVolume", 0)
        else:
            price = q.get("regularMarketPrice") or q.get("preMarketPrice") or q.get("intradayPrice")
            change = q.get("regularMarketChangePercent")
            if change is None:
                change = q.get("preMarketChangePercent", q.get("percentChange", 0))
            volume = q.get("regularMarketVolume") or q.get("dayVolume") or q.get("preMarketVolume", 0)

        avg_volume = q.get("averageDailyVolume3Month", q.get("avgDailyVol3M", 0)) or 0
        try:
            price = float(price)
            change = float(change or 0)
            volume = float(volume or 0)
            avg_volume = float(avg_volume or 0)
        except (TypeError, ValueError):
            continue

        if not (MIN_PRICE <= price <= MAX_PRICE):
            continue

        dollar_volume = price * volume
        min_dollar = PENNY_MIN_DOLLAR_VOLUME if price < 1 else MIN_DOLLAR_VOLUME
        # Pre-market hacmi regular seansa göre doğal olarak düşüktür.
        if dollar_volume < min_dollar:
            continue

        # Regular session'da momentum şartı daha güçlü; pre-market'te %1.5 gap bile
        # aday havuzuna girebilir, fakat detaylı teknik filtreler son kararı verir.
        min_change = 0.5 if session == "PRE_MARKET" else MIN_DAILY_CHANGE
        if change < min_change and (avg_volume <= 0 or volume < avg_volume * 0.50):
            continue

        rvol = volume / avg_volume if avg_volume > 0 else 0
        score = 0
        if change >= 10:
            score += 30
        elif change >= 5:
            score += 22
        elif change >= 3:
            score += 16
        elif change >= min_change:
            score += 10
        elif session == "PRE_MARKET" and change >= 0.5:
            score += 5

        if rvol >= 4:
            score += 30
        elif rvol >= 3:
            score += 26
        elif rvol >= 2:
            score += 20
        elif rvol >= 1.5:
            score += 14
        elif rvol >= 1:
            score += 8

        score += min(20, max(0, int(dollar_volume / 1_000_000 * 5)))

        q = dict(q)
        q["_bot_price"] = price
        q["_bot_change"] = change
        q["_bot_volume"] = volume
        q["_bot_rvol"] = rvol
        unique[symbol] = (score, q)

    ordered = [q for _, q in sorted(unique.values(), key=lambda x: x[0], reverse=True)]
    logger.info("Hızlı screener | %s | %s aktif aday bulundu", session, len(ordered))
    return ordered

# ============================================================
# INDICATORS
# ============================================================
def ema(values, span):
    if not values:
        return None
    alpha = 2 / (span + 1)
    value = float(values[0])
    for x in values[1:]:
        value = alpha * float(x) + (1 - alpha) * value
    return value


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(values)):
        d = float(values[i]) - float(values[i - 1])
        gains.append(max(d, 0))
        losses.append(max(-d, 0))
    ag = sum(gains[-period:]) / period
    al = sum(losses[-period:]) / period
    if al == 0:
        return 100 if ag > 0 else 50
    return 100 - 100 / (1 + ag / al)


def atr(bars, period=14):
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        h = bars[i]["high"]
        l = bars[i]["low"]
        pc = bars[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return sum(trs[-period:]) / period


def vwap(bars):
    total_v = sum(max(0, b["volume"]) for b in bars)
    if total_v <= 0:
        return sum((b["high"] + b["low"] + b["close"]) / 3 for b in bars) / len(bars)
    return sum(((b["high"] + b["low"] + b["close"]) / 3) * max(0, b["volume"]) for b in bars) / total_v

# ============================================================
# YAHOO CHART FETCH
# ============================================================
def yahoo_chart(symbol, range_value, interval):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range": range_value,
        "interval": interval,
        "includePrePost": "true",
        "events": "div,splits",
    }
    for attempt in range(YAHOO_RETRIES):
        try:
            r = HTTP.get(url, params=params, timeout=YAHOO_TIMEOUT)
            if r.status_code == 200:
                payload = r.json()
                result = payload.get("chart", {}).get("result")
                if not result:
                    return []
                data = result[0]
                ts = data.get("timestamp", [])
                q = data.get("indicators", {}).get("quote", [{}])[0]
                arrays = [q.get(k, []) for k in ("open", "high", "low", "close", "volume")]
                n = min([len(ts)] + [len(a) for a in arrays]) if ts else 0
                bars = []
                for i in range(n):
                    if any(a[i] is None for a in arrays[:4]):
                        continue
                    bars.append({
                        "timestamp": ts[i],
                        "open": float(arrays[0][i]),
                        "high": float(arrays[1][i]),
                        "low": float(arrays[2][i]),
                        "close": float(arrays[3][i]),
                        "volume": float(arrays[4][i] or 0),
                    })
                return bars
            if r.status_code in (429, 500, 502, 503, 504) and attempt < YAHOO_RETRIES - 1:
                time.sleep(min(YAHOO_BASE_BACKOFF * (2 ** attempt) + random.random(), 8))
                continue
            return []
        except Exception:
            if attempt < YAHOO_RETRIES - 1:
                time.sleep(min(YAHOO_BASE_BACKOFF * (2 ** attempt), 8))
    return []

# ============================================================
# SWING ANALYSIS
# ============================================================
def analyze_swing(symbol, quote):
    try:
        price = float(quote.get("_bot_price") or quote.get("regularMarketPrice") or quote.get("preMarketPrice") or quote.get("intradayPrice"))
        day_change = float(quote.get("_bot_change") if quote.get("_bot_change") is not None else (quote.get("regularMarketChangePercent", quote.get("preMarketChangePercent", quote.get("percentChange", 0)))))
        day_volume = float(quote.get("_bot_volume") or quote.get("regularMarketVolume") or quote.get("preMarketVolume") or quote.get("dayVolume", 0) or 0)
        avg_volume = float(quote.get("averageDailyVolume3Month", quote.get("avgDailyVol3M", 0)) or 0)
        market_cap = float(quote.get("marketCap", 0) or 0)
        bid = float(quote.get("bid", 0) or 0)
        ask = float(quote.get("ask", 0) or 0)
    except (TypeError, ValueError):
        return None

    if not (MIN_PRICE <= price <= MAX_PRICE):
        return None

    # Günlük geçmiş: swing trendi için asıl veri.
    daily = yahoo_chart(symbol, "6mo", "1d")
    if len(daily) < 60:
        return None

    closes = [b["close"] for b in daily]
    highs = [b["high"] for b in daily]
    lows = [b["low"] for b in daily]
    vols = [b["volume"] for b in daily]

    ema20 = ema(closes[-80:], 20)
    ema50 = ema(closes[-80:], 50)
    rsi14 = rsi(closes, 14)
    atr14 = atr(daily, 14)
    if None in (ema20, ema50, rsi14, atr14) or atr14 <= 0:
        return None

    daily_avg_volume = sum(vols[-21:-1]) / max(1, len(vols[-21:-1]))
    rvol = day_volume / daily_avg_volume if daily_avg_volume > 0 else 0
    if quote.get("_bot_rvol", 0):
        rvol = max(rvol, float(quote.get("_bot_rvol", 0)))

    # Son 5/20 seans dirençleri.
    prior5 = daily[-6:-1]
    prior20 = daily[-21:-1]
    resistance1 = max(b["high"] for b in prior5) if prior5 else price
    resistance2 = max(b["high"] for b in prior20) if prior20 else price
    high_52w = max(highs[-252:]) if highs else price

    # 1h veri ile yakın dönem trend/momentum teyidi.
    hourly = yahoo_chart(symbol, "2mo", "1h")
    if len(hourly) >= 30:
        h_closes = [b["close"] for b in hourly]
        h_ema9 = ema(h_closes[-50:], 9)
        h_ema20 = ema(h_closes[-50:], 20)
        h_rsi = rsi(h_closes, 14)
    else:
        h_ema9 = h_ema20 = h_rsi = None

    # VWAP sadece gün içi teyit; swing kararının tek sebebi değil.
    intraday = yahoo_chart(symbol, "5d", "5m")
    today = TimezoneManager.now_ny().date()
    today_bars = []
    for b in intraday:
        dt = datetime.fromtimestamp(b["timestamp"], tz=NY_TZ)
        if dt.date() == today and dt.time() >= datetime.strptime("04:00", "%H:%M").time():
            today_bars.append(b)
    day_vwap = vwap(today_bars) if today_bars else price

    spread_pct = 0.0
    if bid > 0 and ask >= bid:
        spread_pct = (ask - bid) / ((ask + bid) / 2) * 100

    dollar_volume = day_volume * price
    if dollar_volume < (PENNY_MIN_DOLLAR_VOLUME if price < 1 else MIN_DOLLAR_VOLUME):
        return None
    session_now = TimezoneManager.get_market_session()
    rvol_floor = (PENNY_MIN_RVOL if price < 1 else MIN_RVOL)
    if session_now == "PRE_MARKET":
        rvol_floor = 0.50 if price < 1 else 0.30
    if rvol < rvol_floor:
        return None
    if rsi14 < 42 or rsi14 > 78:
        return None
    # Ana trend korunuyor: fiyat EMA50 üzerinde olmalı veya saatlik momentum
    # yukarı dönmüş olmalı. EMA20/EMA50 kusursuz hizalanması şart değil.
    trend_ok = price >= ema50 or (h_ema9 is not None and h_ema20 is not None and h_ema9 > h_ema20)
    if not trend_ok:
        return None
    min_session_change = 0.5 if session_now == "PRE_MARKET" else MIN_DAILY_CHANGE
    if day_change < min_session_change and price < resistance1 * 0.98:
        return None
    if spread_pct > 4.0 and price < 1:
        return None
    if spread_pct > 2.5 and price >= 1:
        return None

    # Hedefleri önce gerçek dirençlerden, yoksa ATR uzatmasından üret.
    candidates = [x for x in (resistance1, resistance2, high_52w) if x > price * 1.02]
    candidates = sorted(set(round(x, 6) for x in candidates))

    fallback1 = price + 1.5 * atr14
    fallback2 = price + 2.5 * atr14
    fallback3 = price + 4.0 * atr14

    tp1 = candidates[0] if len(candidates) >= 1 else fallback1
    tp2 = candidates[1] if len(candidates) >= 2 else max(fallback2, tp1 * 1.12)
    tp3 = candidates[2] if len(candidates) >= 3 else max(fallback3, tp2 * 1.18)

    # Çok yakın dirençleri hedef kabul etmiyoruz.
    if tp1 < price * (1 + MIN_TP1_GAIN):
        tp1 = fallback1
    if tp2 < price * (1 + MIN_TP2_GAIN):
        tp2 = max(fallback2, tp1 * 1.10)
    if tp3 < price * (1 + MIN_TP3_GAIN):
        tp3 = max(fallback3, tp2 * 1.15)

    # Stop: trend desteği / ATR. Stop'u aşırı genişletmiyoruz.
    recent_support = min(b["low"] for b in daily[-10:])
    stop_atr = price - 1.5 * atr14
    stop_support = recent_support * 0.98
    stop_loss = max(stop_atr, stop_support)
    if stop_loss >= price:
        stop_loss = price - 1.5 * atr14
    if stop_loss <= 0 or stop_loss >= price:
        return None

    risk = price - stop_loss
    rr = (tp1 - price) / risk if risk > 0 else 0
    if rr < MIN_RR:
        return None

    tp1_gain = tp1 / price - 1
    tp2_gain = tp2 / price - 1
    tp3_gain = tp3 / price - 1
    if tp1_gain < MIN_TP1_GAIN or tp2_gain < MIN_TP2_GAIN or tp3_gain < MIN_TP3_GAIN:
        return None

    # 100 puanlık swing skoru.
    score = 0
    if rvol >= 3:
        score += 20
    elif rvol >= 2:
        score += 16
    elif rvol >= 1.5:
        score += 12
    elif rvol >= 1.0:
        score += 8
    elif rvol >= 0.5:
        score += 5

    if price > ema20 > ema50:
        score += 20
    elif price > ema20 and ema20 >= ema50:
        score += 15
    elif price >= ema50:
        score += 8

    if 52 <= rsi14 <= 68:
        score += 12
    elif 45 <= rsi14 < 52 or 68 < rsi14 <= 75:
        score += 6

    if day_change >= 10:
        score += 15
    elif day_change >= 5:
        score += 12
    elif day_change >= 1.0:
        score += 8
    elif day_change >= 0.5:
        score += 5

    if price >= resistance1 * 0.995:
        score += 15
    elif price >= resistance1 * 0.97:
        score += 10

    if h_ema9 is not None and h_ema20 is not None and h_ema9 > h_ema20:
        score += 8
    if h_rsi is not None and 48 <= h_rsi <= 75:
        score += 5

    if day_vwap and price >= day_vwap:
        score += 3

    if dollar_volume >= 10_000_000:
        score += 2

    if price < 1:
        required_score = PENNY_MIN_SCORE
    else:
        required_score = MIN_SCORE
    if score < required_score:
        return None

    # ATR tabanlı kaba zaman ufku; kesinlik iddiası değildir.
    daily_move = max(atr14 / price, 0.02)
    expected_days = int(max(1, min(MAX_SIGNAL_DAYS, round(tp1_gain / daily_move))))
    max_target = "TP3" if tp3_gain >= 0.35 else ("TP2" if tp2_gain >= 0.20 else "TP1")

    return {
        "symbol": symbol,
        "price": round(price, 4),
        "score": int(score),
        "rvol": round(rvol, 2),
        "rsi": round(rsi14, 2),
        "daily_change": round(day_change, 2),
        "ema20": round(ema20, 4),
        "ema50": round(ema50, 4),
        "h_ema9": round(h_ema9, 4) if h_ema9 else None,
        "h_ema20": round(h_ema20, 4) if h_ema20 else None,
        "h_rsi": round(h_rsi, 2) if h_rsi else None,
        "vwap": round(day_vwap, 4) if day_vwap else None,
        "atr": round(atr14, 4),
        "resistance1": round(resistance1, 4),
        "resistance2": round(resistance2, 4),
        "high_52w": round(high_52w, 4),
        "spread_pct": round(spread_pct, 2),
        "dollar_volume": round(dollar_volume, 2),
        "stop_loss": round(stop_loss, 4),
        "tp1": round(tp1, 4),
        "tp2": round(tp2, 4),
        "tp3": round(tp3, 4),
        "tp1_gain": round(tp1_gain * 100, 2),
        "tp2_gain": round(tp2_gain * 100, 2),
        "tp3_gain": round(tp3_gain * 100, 2),
        "rr": round(rr, 2),
        "expected_days": expected_days,
        "max_target": max_target,
        "session": TimezoneManager.get_market_session(),
    }

# ============================================================
# DETAIL FETCH
# ============================================================
def detailed_scan(quotes):
    # Ön skor ile en fazla 60 sembolü pahalı 6mo/1h/5m analizine sokuyoruz.
    def quick_key(q):
        try:
            price = float(q.get("regularMarketPrice", q.get("intradayPrice")) or 0)
            change = float(q.get("regularMarketChangePercent", q.get("percentChange", 0)) or 0)
            volume = float(q.get("regularMarketVolume", q.get("dayVolume", 0)) or 0)
            avg = float(q.get("averageDailyVolume3Month", q.get("avgDailyVol3M", 0)) or 0)
            rv = volume / avg if avg > 0 else 0
            return (rv * 20 + change * 5 + min(volume * price / 1e6, 20))
        except Exception:
            return 0

    quotes = sorted(quotes, key=quick_key, reverse=True)[:DETAILED_CANDIDATES]
    logger.info("Detaylı swing analizi: %s ticker", len(quotes))

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for q in quotes:
            symbol = str(q.get("symbol", "")).upper()
            futures[executor.submit(analyze_swing, symbol, q)] = symbol
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                logger.debug("%s detail error: %s", symbol, e)

    results.sort(key=lambda x: (x["score"], x["rr"], x["tp1_gain"]), reverse=True)
    logger.info("Swing teknik filtreden geçen: %s", len(results))
    return results

# ============================================================
# GEMINI
# ============================================================
class GeminiEvaluator:
    def __init__(self, api_keys):
        self.api_keys = api_keys

    def evaluate(self, candidates):
        if not candidates:
            return {}, "NO_CANDIDATES"
        if not self.api_keys:
            return {}, "NO_API_KEY"
        if db.get_gemini_usage() >= MAX_GEMINI_DAILY_REQUESTS:
            return {}, "LIMIT"

        selected = candidates[:5]
        prompt = (
            "You are a strict US stock swing-trading screener.\n"
            "The holding horizon is usually 1-5 trading days, with an absolute tracking horizon up to 10 days.\n"
            "Do NOT promise or claim certainty.\n"
            "For EACH ticker choose exactly BUY, WATCH, or PASS.\n"
            "Prefer strong trend, volume confirmation, breakout/near-breakout, healthy RSI, liquidity, and realistic TP1/TP2/TP3.\n"
            "Reject weak liquidity, excessive spread, exhausted momentum, or unrealistic targets.\n"
            "Return ONLY valid JSON, no markdown and no code fences, in this exact shape: "
            "[{\"symbol\":\"ABC\",\"decision\":\"BUY\",\"reason\":\"short Turkish reason\"}]\n\n"
        )
        for c in selected:
            prompt += json.dumps(c, ensure_ascii=False) + "\n"

        for key in self.api_keys:
            try:
                client = genai.Client(api_key=key)
                response = client.models.generate_content(
                    model="gemini-3.6-flash",
                    contents=prompt,
                )
                text = getattr(response, "text", None)
                if not text:
                    continue

                db.increment_gemini_usage()
                cleaned = text.strip()
                if cleaned.startswith("```"):
                    cleaned = cleaned.replace("```json", "", 1).replace("```", "").strip()

                try:
                    data = json.loads(cleaned)
                    if isinstance(data, list):
                        blocks = {}
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            symbol = str(item.get("symbol", "")).upper().strip()
                            decision = str(item.get("decision", "WATCH")).upper().strip()
                            reason = str(item.get("reason", "")).strip()
                            if symbol:
                                blocks[symbol] = {
                                    "decision": decision if decision in {"BUY", "WATCH", "PASS"} else "WATCH",
                                    "reason": reason,
                                }
                        if blocks:
                            return blocks, "OK"
                except Exception:
                    pass

                # JSON parse edilemezse teknik adayları kaybetmeyiz; ham Gemini metnini gösteririz.
                return {c["symbol"]: {"decision": "WATCH", "reason": text.strip()[:700]} for c in selected}, "OK_RAW"
            except Exception as e:
                logger.warning("Gemini API error: %s", e)
        return {}, "ERROR"


gemini = GeminiEvaluator(GEMINI_API_KEYS)

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram env eksik")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = HTTP.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        if r.status_code != 200:
            logger.warning("Telegram HTTP %s: %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception as e:
        logger.warning("Telegram error: %s", e)
        return False


def gemini_for_symbol(gemini_blocks, symbol):
    item = gemini_blocks.get(symbol)
    if not item:
        return "🤖 Gemini: analiz alınamadı."
    return f"🤖 Gemini: {item['decision']} — {item['reason']}"


def build_open_message(c, gemini_text):
    return (
        "🎯 NASDAQ SWING SIGNAL\n\n"
        f"📌 {c['symbol']}\n"
        "🟢 LONG\n\n"
        f"💵 Giriş: ${c['price']:.4f}\n"
        f"🛑 Stop: ${c['stop_loss']:.4f}\n\n"
        f"🎯 TP1: ${c['tp1']:.4f}  (+{c['tp1_gain']:.1f}%)\n"
        f"🎯 TP2: ${c['tp2']:.4f}  (+{c['tp2_gain']:.1f}%)\n"
        f"🎯 TP3: ${c['tp3']:.4f}  (+{c['tp3_gain']:.1f}%)\n\n"
        f"📊 Score: {c['score']}/100\n"
        f"🔥 RVOL: {c['rvol']:.2f}\n"
        f"📈 RSI: {c['rsi']:.1f}\n"
        f"📈 Günlük: {c['daily_change']:+.2f}%\n"
        f"⚖️ RR (TP1): {c['rr']:.2f}\n"
        f"⏳ Beklenen: {c['expected_days']} gün\n"
        f"🏁 Maksimum hedef: {c['max_target']}\n\n"
        f"{gemini_text}\n\n"
        "🟢 Durum: AKTİF"
    )

# ============================================================
# OPEN SIGNAL TRACKER
# ============================================================
def fetch_current_price(symbol):
    # Takipteki açık sinyaller az olduğu için tek sembol chart çağrısı yeterli.
    bars = yahoo_chart(symbol, "1d", "5m")
    if not bars:
        return None
    return bars[-1]["close"]


def human_age(opened_at):
    try:
        dt = datetime.fromisoformat(opened_at)
        delta = datetime.now() - dt
        days = delta.days
        hours = delta.seconds // 3600
        if days:
            return f"{days} gün {hours} saat"
        return f"{hours} saat"
    except Exception:
        return "-"


def track_open_signals():
    signals = db.get_open_signals()
    if not signals:
        return

    logger.info("Açık swing sinyali takibi: %s", len(signals))
    for s in signals:
        price = fetch_current_price(s["symbol"])
        if price is None:
            continue

        entry = s["entry_price"]
        tp1, tp2, tp3 = s["tp1"], s["tp2"], s["tp3"]
        stop = s["stop_loss"]
        age = human_age(s["opened_at"])
        gain = (price / entry - 1) * 100

        # Stop önce kontrol edilir; aynı bar içinde TP/SL ikisi birden görülürse
        # muhafazakar tarafta stop'u önce kabul ediyoruz.
        if price <= stop:
            db.update_signal(s["id"], status="STOPPED", closed_at=datetime.now().isoformat(), current_price=price)
            db.add_update(s["id"], "STOP", price, f"Sonuç: {gain:+.2f}%")
            send_telegram_message(
                "🛑 STOP\n\n"
                f"📌 {s['symbol']}\n"
                f"Giriş: ${entry:.4f}\n"
                f"Stop: ${stop:.4f}\n\n"
                f"📉 Sonuç: {gain:+.2f}%\n"
                f"⏱ Süre: {age}\n\n"
                "❌ Sinyal kapandı."
            )
            continue

        event = None
        if not s["tp1_hit"] and price >= tp1:
            event = "TP1"
            db.update_signal(s["id"], tp1_hit=1, current_price=price, stop_loss=entry)
            db.add_update(s["id"], "TP1", price, f"Kazanç: {gain:+.2f}%")
            send_telegram_message(
                "🎯 TP1 VURULDU\n\n"
                f"📌 {s['symbol']}\n"
                f"Giriş: ${entry:.4f}\n"
                f"TP1: ${tp1:.4f} ✅\n\n"
                f"📈 Kazanç: {gain:+.2f}%\n"
                f"⏱ Süre: {age}\n\n"
                f"🎯 TP2: ${tp2:.4f}\n"
                f"🎯 TP3: ${tp3:.4f}\n\n"
                "🟢 Sinyal hâlâ aktif. Stop giriş fiyatına çekildi."
            )
            s["tp1_hit"] = 1
            s["stop_loss"] = entry

        if s["tp1_hit"] and not s["tp2_hit"] and price >= tp2:
            event = "TP2"
            db.update_signal(s["id"], tp2_hit=1, current_price=price, stop_loss=tp1)
            db.add_update(s["id"], "TP2", price, f"Kazanç: {gain:+.2f}%")
            send_telegram_message(
                "🔥 TP2 VURULDU\n\n"
                f"📌 {s['symbol']}\n"
                "TP1: ✅\n"
                "TP2: ✅\n"
                "TP3: ⏳\n\n"
                f"📈 Güncel kazanç: {gain:+.2f}%\n"
                f"⏱ Süre: {age}\n\n"
                f"🛡 Yeni stop: ${tp1:.4f}\n"
                f"🎯 Maksimum hedef: ${tp3:.4f}"
            )
            s["tp2_hit"] = 1
            s["stop_loss"] = tp1

        if s["tp2_hit"] and not s["tp3_hit"] and price >= tp3:
            db.update_signal(s["id"], tp3_hit=1, status="CLOSED", closed_at=datetime.now().isoformat(), current_price=price)
            db.add_update(s["id"], "TP3", price, f"Sonuç: {gain:+.2f}%")
            send_telegram_message(
                "🏆 TP3 VURULDU\n\n"
                f"📌 {s['symbol']}\n"
                "TP1: ✅\nTP2: ✅\nTP3: ✅\n\n"
                f"📈 Sonuç: {gain:+.2f}%\n"
                f"⏱ Süre: {age}\n\n"
                "🟢 SİNYAL BAŞARIYLA TAMAMLANDI"
            )
            continue

        # TP1/TP2 gerçekleşmedi ama sinyal 10 günü doldurduysa kapat.
        try:
            opened = datetime.fromisoformat(s["opened_at"])
            if datetime.now() - opened >= timedelta(days=MAX_SIGNAL_DAYS):
                db.update_signal(s["id"], status="EXPIRED", closed_at=datetime.now().isoformat(), current_price=price)
                db.add_update(s["id"], "EXPIRED", price, f"10 gün doldu. Sonuç: {gain:+.2f}%")
                send_telegram_message(
                    "⏰ SİNYAL SÜRESİ DOLDU\n\n"
                    f"📌 {s['symbol']}\n"
                    f"Giriş: ${entry:.4f}\n"
                    f"Son fiyat: ${price:.4f}\n\n"
                    f"📊 Sonuç: {gain:+.2f}%\n"
                    "Sinyal kapatıldı."
                )
                continue
        except Exception:
            pass

        # Her 5 dakikada fiyat mesajı spamlamıyoruz; yalnızca TP/SL/expire olaylarında haber veriyoruz.
        if event is None:
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("UPDATE swing_signals SET current_price = ? WHERE id = ?", (price, s["id"]))
            conn.commit()
            conn.close()

# ============================================================
# MARKET PIPELINE
# ============================================================
def run_market_pipeline():
    if not SCAN_LOCK.acquire(blocking=False):
        logger.warning("Önceki scan hâlâ çalışıyor; yeni scan atlandı.")
        return
    try:
        session = TimezoneManager.get_market_session()
        if session == "CLOSED":
            return

        # Önce açık pozisyonları takip et. Bu kısım yalnızca açık sinyal kadar istek atar.
        track_open_signals()

        logger.info("==================================================")
        logger.info("SWING PIPELINE | %s | NY=%s | TR=%s", session,
                    TimezoneManager.now_ny().strftime("%Y-%m-%d %H:%M:%S"),
                    TimezoneManager.now_tr().strftime("%Y-%m-%d %H:%M:%S"))

        quotes = get_active_universe()
        if not quotes:
            logger.info("Aktif screener adayı bulunamadı.")
            return

        candidates = detailed_scan(quotes)
        if not candidates:
            logger.info("❌ Sıkı swing filtresinden geçen hisse yok.")
            return

        # Aynı sembol için açık veya çok yeni sinyal varsa tekrar yollama.
        final = []
        for c in candidates:
            if db.has_open_signal(c["symbol"]):
                continue
            if db.has_recent_signal(c["symbol"]):
                continue
            final.append(c)

        final = final[:5]
        logger.info("Final swing adayları: %s", len(final))
        if not final:
            return

        gemini_blocks, gemini_status = gemini.evaluate(final)

        # Gemini BUY olmayanları ana sinyalden çıkarıyoruz; API çalışmıyorsa teknik skorun
        # güçlü adaylarını yine kaybetmemek için API hata/limit durumunda teknik sonuç kullanılır.
        selected = []
        for c in final:
            gitem = gemini_blocks.get(c["symbol"], {})
            decision = str(gitem.get("decision", "WATCH")).upper()
            if gemini_status == "OK" and decision == "PASS":
                continue
            selected.append((c, gemini_for_symbol(gemini_blocks, c["symbol"])))

        for c, gtext in selected:
            message = build_open_message(c, gtext)
            if send_telegram_message(message):
                signal_id = db.create_signal(c, gemini_status, gtext)
                logger.info("Telegram OPEN gönderildi: %s | signal_id=%s", c["symbol"], signal_id)

        logger.info("SWING PIPELINE bitti. Gönderilen=%s", len(selected))
    except Exception as e:
        logger.exception("SWING PIPELINE ERROR: %s", e)
    finally:
        SCAN_LOCK.release()

# ============================================================
# SCHEDULER
# ============================================================
def start_scheduler():
    scheduler = BackgroundScheduler(
        timezone=NY_TZ,
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 60},
    )

    # Pre-market + regular session: 04:00-16:00 NY, her 5 dakikada bir.
    # After-hours taraması özellikle istenmediği için 16:00'da durur.
    scheduler.add_job(
        run_market_pipeline,
        CronTrigger(day_of_week="mon-fri", hour="4-15", minute="*/5"),
        id="swing_scan_premarket_regular",
        replace_existing=True,
    )
    scheduler.add_job(
        run_market_pipeline,
        CronTrigger(day_of_week="mon-fri", hour="16", minute="0"),
        id="swing_scan_close",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler başladı | NY 04:00-16:00 | pre-market + regular")
    return scheduler

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    logger.info("==============================================")
    logger.info("NASDAQ / NYSE SWING BOT BAŞLIYOR")
    logger.info("NY: %s", TimezoneManager.now_ny().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("TR: %s", TimezoneManager.now_tr().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("Price range: $%.2f - $%.2f", MIN_PRICE, MAX_PRICE)
    logger.info("Score minimum: %s | Penny score: %s", MIN_SCORE, PENNY_MIN_SCORE)
    logger.info("Gemini keys: %s", len(GEMINI_API_KEYS))
    logger.info("Telegram: %s", "OK" if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID else "YOK")
    logger.info("==============================================")

    # SEC cache'i hazır tut. Asıl hızlı tarama Yahoo screener ile yapılır.
    UniverseLoader.load_universe()
    # GitHub Actions / tek seferlik cloud çalıştırma modu.
    # `python main.py --once` yalnızca bir scan yapar ve çıkar.
    # Böylece bilgisayarın açık kalmasına gerek kalmaz.
    if "--once" in sys.argv:
        logger.info("ONE-SHOT MODE: tek swing scan çalıştırılıyor.")
        run_market_pipeline()
        logger.info("ONE-SHOT MODE tamamlandı.")
        raise SystemExit(0)

    scheduler = start_scheduler()

    # Normal sürekli çalışma modu.
    run_market_pipeline()
    logger.info("Bot çalışıyor. Scheduler bekleniyor...")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Bot durduruluyor...")
        scheduler.shutdown(wait=False)
        logger.info("Bot durdu.")
