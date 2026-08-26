#!/usr/bin/env python3
"""
SPACEMAN HTML Strategy Bot — Telegram + Render  [v15.1 — señales activadas]
────────────────────────────────────────────────────────────────────────────
Estrategia: 10 patrones optimizados para cuota 1.80x
+ Sistema de 8 agentes con SCORE PONDERADO (anti-ruido)
+ Emisión inmediata cuando tendencia es CLARAMENTE ALCISTA FUERTE
Mecánica: intento 1 silencioso a 1.70x. Si falla, señal para intento 2 y 3.
"""
import asyncio
import sqlite3
import sys
import threading
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from flask import Flask, request
import websockets
from telebot.async_telebot import AsyncTeleBot
from telebot import types
import aiohttp

# ─── ML — dependencias opcionales ──
try:
    import joblib
    import pandas as pd
    ML_LIBS_OK = True
except ImportError:
    ML_LIBS_OK = False

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── CONFIG — TELEGRAM ────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN",  "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
CHAT_ID    = int(os.environ.get("CHAT_ID", "-1003274770136"))

# ─── CONFIG — WEBSOCKET ───────────────────────────────────────────────────────
WS_URL    = os.environ.get("WS_URL",    "wss://dga.pragmaticplaylive.net/ws")
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY",  "BRL")
GAME_ID   = int(os.environ.get("GAME_ID", "1301"))
DB_FILE = os.environ.get("DB_FILE", "spaceman.db")

# ─── CONFIG — ML ──────────────────────────────────────────────────────────────
MODEL_FILE     = os.environ.get("MODEL_FILE", "signal_model.joblib")
MODEL_MIN_PROB = float(os.environ.get("MODEL_MIN_PROB", "0.55"))
AUTO_TRAIN_ENABLED     = os.environ.get("AUTO_TRAIN_ENABLED", "1") == "1"
AUTO_TRAIN_MIN_ROWS    = int(os.environ.get("AUTO_TRAIN_MIN_ROWS", "100"))
AUTO_TRAIN_MIN_NEW     = int(os.environ.get("AUTO_TRAIN_MIN_NEW", "30"))
AUTO_TRAIN_INTERVAL_SEC = int(os.environ.get("AUTO_TRAIN_INTERVAL_SEC", "1800"))

def colombia_now() -> datetime:
    return datetime.utcnow() - timedelta(hours=5)

def colombia_time() -> str:
    return colombia_now().strftime("%H:%M")

# ─── UMBRALES DE TENDENCIA ──────────
UMBRAL_BELOW2 = 53.51
UMBRAL_2TO5   = 26.99
HISTORY_MAX   = 150

# ─── CUOTA OPTIMIZADA PARA 1.8x ─────────────────────────────────────────────
CASHOUT_TARGET  = 1.80
CASHOUT_TRIGGER = 1.80
CONFIRM_TRIGGER = 1.70

# ─── MECÁNICA DE CONFIRMACIÓN ───────
MAX_ATTEMPTS_NORMAL    = 3
MAX_ATTEMPTS_IMMEDIATE = 2

GAME_LINK = "https://1win.lat/casino/play/v_pragmatic:spaceman"

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTES DEL SISTEMA HTML
# ═══════════════════════════════════════════════════════════════════════════
UMBRAL_ACTIVACION      = 30
MAX_LUCKY_GALES        = 2
COOLDOWN_VERDE         = 10
COOLDOWN_ROJO          = 6
NIVEL_SOBREVENTA       = -4
RACHA_SOBREVENTA       = 4

# ─── UMBRALES OPTIMIZADOS v15.1 ─────────────────────────────────────────────
CONF_ALERTA_MIN        = 30
CONF_ALERTA_MAX        = 40
UMBRAL_AGRESIVO_V2     = 4.0
UMBRAL_VALLE_V3        = 1.5
DIST_MIN_EMA8_V3       = 1.05
DIST_MAX_EMA8_V3       = 1.50

# UMBRALES ANTI-RUIDO (RELAJADOS para permitir señales)
SCORE_UMBRAL_EMISION   = 25   # REDUCIDO de 35 a 25
SCORE_UMBRAL_INMEDIATA = 35   # REDUCIDO de 45 a 35
COHERENCIA_MIN_DIFF    = 1    # REDUCIDO de 2 a 1
VOLATILIDAD_STD_MIN    = 0.2  # REDUCIDO de 0.3 a 0.2
VOLATILIDAD_STD_MAX    = 4.0  # AUMENTADO de 3.0 a 4.0

# UMBRALES PARA PATRONES 1.8x
UMBRAL_INTERCALADO     = 3
UMBRAL_DOS_AZULES      = 2
UMBRAL_FIB_REBOTE      = 0.618
RSI_SOBREVENTA         = 30
RSI_SOBRECOMPRA        = 70
DISTANCIA_SOPORTE_MAX  = 0.8

SOPORTE_COOLDOWN       = 5
MIN_DATOS_ENTRE_TOQUES = 2
TOQUES_NECESARIOS      = 3
RANGO_KEYS = ['muyBajo', 'bajo', 'medio', 'medioAlto', 'alto']

# ─── MAPEO DE PATRONES v15.1 (10 patrones) ──────────────────────────────────
ALERT_LABELS = {
    'video':                'PATRÓN V1 💎',
    'combo_verde_agresiva': 'PATRÓN V2 💎',
    'martillo':             'PATRÓN V3 💎',
    'intercalado':          'PATRÓN V4 💎',
    'dos_azules':           'PATRÓN V5 💎',
    'soporte_dinamico':     'PATRÓN V6 💎',
    'tendencia_media':      'PATRÓN V7 💎',
    'multi_agente':         'PATRÓN V8 💎',
    'fibonacci_rebote':     'PATRÓN V9 💎',
    'rsi_extremo':          'PATRÓN V10 💎',
}

PATTERN_ORDER = [
    'video', 'combo_verde_agresiva', 'martillo', 'intercalado',
    'dos_azules', 'soporte_dinamico', 'tendencia_media',
    'multi_agente', 'fibonacci_rebote', 'rsi_extremo',
]

# ─── PESOS DE AGENTES ─────────────────────────────────────────────────────
AGENT_WEIGHTS = {
    'a1': 1.2, 'a2': 1.5, 'a3': 0.8, 'a4': 1.3,
    'a5': 1.0, 'a6': 1.1, 'a7': 0.7, 'a8': 1.2,
}

# ─── SQLITE — ESQUEMA ─────────────────────────────────────────────────────────
def db_init():
    con = sqlite3.connect(DB_FILE)
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS history (
        id      INTEGER PRIMARY KEY AUTOINCREMENT,
        value   REAL    NOT NULL,
        created TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS state (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    CREATE TABLE IF NOT EXISTS pattern_stats (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo_key   TEXT    NOT NULL,
        tipo_label TEXT,
        result     TEXT    NOT NULL,
        value      REAL,
        attempt    INTEGER,
        features_json TEXT,
        created    TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    """)
    try:
        cur.execute("ALTER TABLE pattern_stats ADD COLUMN features_json TEXT")
        con.commit()
    except sqlite3.OperationalError:
        pass
    con.commit()
    con.close()

def _db():
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

# ─── PERSISTENCIA — ESTADO ───────────────────────────────────────────────────
def save_state():
    values = {
        "sig_state":        sig_state,
        "sig_attempt":      str(sig_attempt),
        "sig_last_attempt": str(sig_last_attempt),
        "sig_msg_id":       str(sig_msg_id) if sig_msg_id is not None else "",
        "sig_tipo":         sig_tipo or "",
        "sig_tipo_key":     sig_tipo_key or "",
        "sig_features":     sig_features or "",
        "sig_inmediata":    "1" if sig_inmediata else "0",
        "stats_msg_id":     str(stats_msg_id) if stats_msg_id is not None else "",
        "daily_wins":       str(daily_wins),
        "daily_losses":     str(daily_losses),
        "consecutive_wins": str(consecutive_wins),
        "consecutive_losses": str(consecutive_losses),
        "ml_last_trained_count": str(ml_last_trained_count),
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global sig_inmediata, stats_msg_id
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses, ml_last_trained_count
    d = _load_dict()
    sig_state         = d.get("sig_state", "idle") or "idle"
    if sig_state not in ("idle", "pending", "active"):
        sig_state = "idle"
    sig_attempt        = int(d.get("sig_attempt", "0") or "0")
    sig_last_attempt   = int(d.get("sig_last_attempt", str(MAX_ATTEMPTS_NORMAL)) or str(MAX_ATTEMPTS_NORMAL))
    _mid              = d.get("sig_msg_id", "")
    sig_msg_id        = int(_mid) if _mid else None
    sig_tipo          = d.get("sig_tipo", "") or None
    sig_tipo_key      = d.get("sig_tipo_key", "") or None
    sig_features      = d.get("sig_features", "") or None
    sig_inmediata     = (d.get("sig_inmediata", "0") or "0") == "1"
    _sid              = d.get("stats_msg_id", "")
    stats_msg_id      = int(_sid) if _sid else None
    daily_wins        = int(d.get("daily_wins", "0"))
    daily_losses      = int(d.get("daily_losses", "0"))
    consecutive_wins  = int(d.get("consecutive_wins", "0"))
    consecutive_losses = int(d.get("consecutive_losses", "0"))
    ml_last_trained_count = int(d.get("ml_last_trained_count", "0") or "0")
    logger.info(f"[v15.1] Estado cargado | estado={sig_state} intento={sig_attempt} tipo={sig_tipo}")

def _save_dict(values: dict):
    try:
        con = _db()
        con.cursor().executemany(
            "INSERT INTO state(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            list(values.items())
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando estado: {e}")

def _load_dict() -> dict:
    try:
        con = _db()
        rows = con.execute("SELECT key, value FROM state").fetchall()
        con.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception as e:
        logger.warning(f"Error cargando estado: {e}")
        return {}

# ─── PERSISTENCIA — HISTORIAL ─────────────────────────────────────────────────
def save_value(value: float):
    try:
        con = _db()
        con.execute("INSERT INTO history(value) VALUES(?)", (value,))
        con.execute("""
        DELETE FROM history WHERE id NOT IN (
            SELECT id FROM history ORDER BY id DESC LIMIT ?
        )
        """, (HISTORY_MAX,))
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error insertando en history: {e}")

def load_history() -> List[float]:
    try:
        con = _db()
        rows = con.execute(
            "SELECT value FROM history ORDER BY id DESC LIMIT ?", (HISTORY_MAX,)
        ).fetchall()
        con.close()
        return [r["value"] for r in reversed(rows)]
    except Exception as e:
        logger.warning(f"Error cargando history: {e}")
        return []

# ─── PERSISTENCIA — EFECTIVIDAD POR PATRÓN ───────────────────────────────────
def log_pattern_result(tipo_key: str, tipo_label: str, result: str, value: float,
                       attempt: int = 0, features_json: Optional[str] = None):
    try:
        con = _db()
        con.execute(
            "INSERT INTO pattern_stats(tipo_key, tipo_label, result, value, attempt, features_json) "
            "VALUES(?,?,?,?,?,?)",
            (tipo_key or "desconocido", tipo_label or "", result, value, attempt, features_json)
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando pattern_stats: {e}")

def get_pattern_stats_24h() -> Dict[str, dict]:
    try:
        con = _db()
        rows = con.execute("""
        SELECT tipo_key,
               SUM(CASE WHEN result = 'win'  THEN 1 ELSE 0 END) AS wins,
               SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS losses,
               COUNT(*) AS total
        FROM pattern_stats
        WHERE created >= datetime('now', '-24 hours')
        GROUP BY tipo_key
        """).fetchall()
        con.close()
        return {r["tipo_key"]: {"wins": r["wins"], "losses": r["losses"], "total": r["total"]} for r in rows}
    except Exception as e:
        logger.warning(f"Error consultando pattern_stats: {e}")
        return {}

def build_pattern_stats_msg() -> str:
    data = get_pattern_stats_24h()
    lines = [
        "📊 <b>EFECTIVIDAD POR PATRÓN — Últimas 24h</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━",
    ]
    total_wins = total_losses = 0
    conocidos = set(PATTERN_ORDER)
    hubo_datos = False
    for key in PATTERN_ORDER:
        label = ALERT_LABELS.get(key, key)
        d = data.get(key)
        if not d or d["total"] == 0:
            lines.append(f"{label}: <i>sin señales</i>")
            continue
        hubo_datos = True
        wins, losses, total = d["wins"], d["losses"], d["total"]
        pct = (wins / total * 100) if total else 0.0
        total_wins   += wins
        total_losses += losses
        lines.append(f"{label}: ✅{wins} ❌{losses} — <b>{pct:.1f}%</b> ({total})")
    for key, d in data.items():
        if key in conocidos or not d or d["total"] == 0:
            continue
        hubo_datos = True
        wins, losses, total = d["wins"], d["losses"], d["total"]
        pct = (wins / total * 100) if total else 0.0
        total_wins   += wins
        total_losses += losses
        lines.append(f"{key}: ✅{wins} ❌{losses} — <b>{pct:.1f}%</b> ({total})")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    grand_total = total_wins + total_losses
    grand_pct   = (total_wins / grand_total * 100) if grand_total else 0.0
    lines.append(f"🌐 <b>TOTAL: ✅{total_wins} ❌{total_losses} — {grand_pct:.1f}%</b>")
    if not hubo_datos:
        lines.append("<i>Sin señales registradas en las últimas 24h.</i>")
    return "\n".join(lines)

# ─── ESTADO GLOBAL ─────────────────────────────────────────────────────────
history: List[float] = []
last_result: Optional[float] = None

sig_state:         str           = "idle"
sig_attempt:       int           = 0
sig_last_attempt:  int           = MAX_ATTEMPTS_NORMAL
sig_msg_id:        Optional[int] = None
sig_tipo:          Optional[str] = None
sig_tipo_key:      Optional[str] = None
sig_features:      Optional[str] = None
sig_inmediata:     bool          = False
stats_msg_id:      Optional[int] = None
daily_wins:        int           = 0
daily_losses:      int           = 0
consecutive_wins:  int           = 0
consecutive_losses: int          = 0

# ─── BOTS + FLASK ─────────────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None
flask_app = Flask(__name__)

# ─── TELEGRAM HELPERS ─────────────────────────────────────────────────────────
async def send_msg(text: str, no_preview: bool = False) -> Optional[int]:
    try:
        msg = await bot.send_message(
            CHAT_ID, text, parse_mode="HTML",
            disable_web_page_preview=no_preview
        )
        return msg.message_id
    except Exception as e:
        logger.warning(f"[v15.1] send error: {e}")
        return None

async def edit_msg(msg_id: int, text: str, no_preview: bool = False) -> bool:
    try:
        await bot.edit_message_text(
            text, CHAT_ID, msg_id, parse_mode="HTML",
            disable_web_page_preview=no_preview
        )
        return True
    except Exception as e:
        logger.debug(f"[v15.1] edit error {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[v15.1] delete error {msg_id}: {e}")
        return False

# ─── ANÁLISIS DE TENDENCIA ────────────────────────────────────────────────────
def get_stats() -> dict:
    total = len(history)
    if total == 0:
        return {"total": 0, "below2": 0, "two_to_five": 0,
                "pct_below2": 0.0, "pct_2to5": 0.0, "favorable": False}
    below2      = sum(1 for v in history if v < 2.00)
    two_to_five = sum(1 for v in history if 2.00 <= v < 5.00)
    pct_below2  = (below2 / total) * 100
    pct_2to5    = (two_to_five / total) * 100
    favorable   = (pct_below2 < UMBRAL_BELOW2) and (pct_2to5 > UMBRAL_2TO5)
    return {
        "total": total, "below2": below2, "two_to_five": two_to_five,
        "pct_below2": pct_below2, "pct_2to5": pct_2to5, "favorable": favorable,
    }

# ═══════════════════════════════════════════════════════════════════════════
# INDICADORES BASE
# ═══════════════════════════════════════════════════════════════════════════
def compute_niveles(vals: List[float]) -> List[float]:
    niveles = []
    nivel = 0
    for v in vals:
        if v >= 2.00:
            nivel += 1
        elif 1.00 <= v <= 1.99:
            nivel -= 1
        niveles.append(nivel)
    return niveles

def ema_html(period: int, vals: List[float]) -> List[float]:
    if not vals:
        return []
    k = 2 / (period + 1)
    prev = vals[0]
    out = [prev]
    for i in range(1, len(vals)):
        actual = vals[i] * k + prev * (1 - k)
        out.append(actual)
        prev = actual
    return out

def calcular_confianza(vals: List[float]) -> float:
    if len(vals) < 5:
        return 50
    last5 = vals[-5:]
    altos = sum(1 for v in last5 if v >= 2.0)
    bajos = sum(1 for v in last5 if v < 1.5)
    if altos >= 3:
        conf = 80 + altos * 5
    elif bajos >= 3:
        conf = 25 + altos * 5
    else:
        conf = 40 + altos * 10 - bajos * 5
    return min(99, max(5, conf))

def calcular_tendencia_lucky(vals: List[float], niveles: List[float]) -> str:
    if len(niveles) < 8:
        return 'ROJO'
    datos = niveles[-12:]
    slope, _ = calcular_regresion_lineal(datos)
    racha_bajos = 0
    i = len(vals) - 2
    while i >= 0 and vals[i] < 2.0:
        racha_bajos += 1
        i -= 1
    rebote = racha_bajos >= 2 and len(vals) >= 2 and vals[-1] > vals[-2]
    return 'VERDE' if (slope > 0.05 or rebote) else 'ROJO'

def calcular_regresion_lineal(data: List[float]):
    n = len(data)
    if n < 2:
        return 0.0, 0.0
    sum_x = sum_y = sum_xy = sum_x2 = sum_y2 = 0.0
    for i, v in enumerate(data):
        sum_x += i; sum_y += v; sum_xy += i * v; sum_x2 += i * i; sum_y2 += v * v
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return 0.0, 0.0
    slope = (n * sum_xy - sum_x * sum_y) / denom
    mean_y = sum_y / n
    ss_tot = sum_y2 - n * mean_y * mean_y
    intercept = (sum_y - slope * sum_x) / n
    ss_res = 0.0
    for i, v in enumerate(data):
        pred = slope * i + intercept
        ss_res += (v - pred) ** 2
    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
    return slope, r2

# ═══════════════════════════════════════════════════════════════════════════
# FILTROS OPTIMIZADOS v15.1
# ═══════════════════════════════════════════════════════════════════════════

def filtro_v1_video(vals: List[float], ema8: List[float]) -> bool:
    if len(vals) < 3 or len(ema8) < 2:
        return False
    if not all(v < 2.0 for v in vals[-3:]):
        return False
    if not (ema8[-1] > ema8[-2]):
        return False
    return True

def filtro_v2_combo(vals: List[float], ema4: List[float], ema8: List[float]) -> bool:
    if len(vals) < 5 or not ema4 or not ema8:
        return False
    ultimos_5 = vals[-5:]
    conteo_altos = sum(1 for v in ultimos_5 if v >= 2.0)
    if conteo_altos < 2:
        return False
    if not (ema4[-1] > ema8[-1]):
        return False
    return True

def filtro_v3_martillo(vals: List[float], ema8: List[float]) -> bool:
    if len(vals) < 3 or len(ema8) < 3:
        return False
    if vals[-2] >= UMBRAL_VALLE_V3:
        return False
    e8 = ema8[-1]
    if e8 <= 0:
        return False
    ratio = vals[-1] / e8
    if not (DIST_MIN_EMA8_V3 <= ratio <= DIST_MAX_EMA8_V3):
        return False
    if not (ema8[-1] > ema8[-2] > ema8[-3]):
        return False
    return True

# ═══════════════════════════════════════════════════════════════════════════
# NUEVOS PATRONES v15.1
# ═══════════════════════════════════════════════════════════════════════════

def detectar_intercalado(vals: List[float]) -> bool:
    if len(vals) < 8:
        return False
    ultimos_8 = vals[-8:]
    alternancias = 0
    for i in range(1, len(ultimos_8)):
        prev_bajo = ultimos_8[i-1] < 2.0
        curr_alto = ultimos_8[i] >= 2.0
        prev_alto = ultimos_8[i-1] >= 2.0
        curr_bajo = ultimos_8[i] < 2.0
        if (prev_bajo and curr_alto) or (prev_alto and curr_bajo):
            alternancias += 1
    if alternancias >= UMBRAL_INTERCALADO and ultimos_8[-1] < 2.0:
        return True
    return False

def detectar_dos_azules(vals: List[float], niveles: List[float]) -> bool:
    if len(vals) < 3 or len(niveles) < 3:
        return False
    if not (vals[-2] < 2.0 and vals[-3] < 2.0):
        return False
    if vals[-1] < 1.8:
        return False
    racha_bajos = 0
    for i in range(len(vals) - 1, -1, -1):
        if vals[i] < 2.0:
            racha_bajos += 1
        else:
            break
    if racha_bajos > 6:
        return False
    return True

def detectar_soporte_dinamico(vals: List[float], ema8: List[float], 
                               ema20: List[float], niveles: List[float]) -> bool:
    if len(vals) < 3 or not ema8 or not ema20:
        return False
    current = vals[-1]
    e8 = ema8[-1]
    e20 = ema20[-1]
    prev = vals[-2]
    dist_prev_e8 = abs(prev - e8)
    dist_prev_e20 = abs(prev - e20)
    cerca_soporte = dist_prev_e8 <= DISTANCIA_SOPORTE_MAX or dist_prev_e20 <= DISTANCIA_SOPORTE_MAX
    rebote = current > prev and current >= 1.8
    emas_subiendo = len(ema8) >= 2 and ema8[-1] > ema8[-2]
    return cerca_soporte and rebote and emas_subiendo

def detectar_tendencia_media(ema8: List[float], ema20: List[float]) -> bool:
    if len(ema8) < 8 or not ema20:
        return False
    ventana = ema8[-8:] if len(ema8) >= 8 else ema8
    slope, r2 = calcular_regresion_lineal(ventana)
    if slope <= 0.03:
        return False
    if ema8[-1] <= ema20[-1]:
        return False
    if len(ema8) >= 2 and ema8[-1] <= ema8[-2]:
        return False
    return True

def detectar_fibonacci_rebote(vals: List[float], fib: dict) -> bool:
    if not fib or len(vals) < 3:
        return False
    fib_618 = fib.get('61.8%')
    if fib_618 is None:
        return False
    current = vals[-1]
    prev = vals[-2]
    dist_prev = abs(prev - fib_618)
    cerca_fib = dist_prev <= 0.5
    rebote = current > prev and current >= 1.8
    return cerca_fib and rebote

def detectar_rsi_extremo(rsi: List[float], vals: List[float]) -> bool:
    if not rsi or len(rsi) < 2 or len(vals) < 2:
        return False
    rsi_actual = rsi[-1]
    rsi_anterior = rsi[-2]
    if rsi_actual < RSI_SOBREVENTA and rsi_actual > rsi_anterior and vals[-1] >= 1.8:
        return True
    if rsi_actual > RSI_SOBRECOMPRA and vals[-1] >= 1.8:
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════
# FILTRO DE VOLATILIDAD
# ═══════════════════════════════════════════════════════════════════════════

def filtro_volatilidad(vals: List[float]) -> bool:
    if len(vals) < 10:
        return True
    ultimos_10 = vals[-10:]
    mean = sum(ultimos_10) / 10
    variance = sum((v - mean) ** 2 for v in ultimos_10) / 10
    std = variance ** 0.5
    if std < VOLATILIDAD_STD_MIN and mean < 2.5:
        return False
    if std > VOLATILIDAD_STD_MAX:
        return False
    return True

# ═══════════════════════════════════════════════════════════════════════════
# EMISIÓN INMEDIATA
# ═══════════════════════════════════════════════════════════════════════════

def es_tendencia_claramente_alcista_fuerte(vals, ema4, ema8, ema20, rsi, tendencia_lucky) -> bool:
    if len(vals) < 5 or len(ema4) < 2 or len(ema8) < 2 or len(ema20) < 2:
        return False
    if tendencia_lucky != 'VERDE':
        return False
    e4, e8, e20 = ema4[-1], ema8[-1], ema20[-1]
    if not (e4 > e8 > e20):
        return False
    e4p, e8p, e20p = ema4[-2], ema8[-2], ema20[-2]
    if not (e4 > e4p and e8 > e8p and e20 > e20p):
        return False
    if not rsi or len(rsi) == 0:
        return False
    rsi_val = rsi[-1]
    if not (45 <= rsi_val <= 65):
        return False
    if vals[-1] < 2.0:
        return False
    ultimos_5 = vals[-5:]
    if sum(1 for v in ultimos_5 if v >= 2.0) < 2:
        return False
    return True

def cumple_condiciones_opcionales_inmediata(vals, macd) -> int:
    cumplidas = 0
    if len(vals) >= 3:
        v1, v2, v3 = vals[-3], vals[-2], vals[-1]
        if v1 < v2 < v3:
            cumplidas += 1
    if len(vals) >= 4:
        racha_bajos = 0
        i = len(vals) - 2
        while i >= 0 and vals[i] < 2.0:
            racha_bajos += 1
            i -= 1
        if racha_bajos >= 3 and vals[-1] >= 2.0:
            cumplidas += 1
    if macd and len(macd) >= 2:
        if macd[-1] > 0 and macd[-1] > macd[-2]:
            cumplidas += 1
    return cumplidas

# ═══════════════════════════════════════════════════════════════════════════
# PATRONES BASE
# ═══════════════════════════════════════════════════════════════════════════

def detectar_martillo_base(vals: List[float], ema8: List[float]) -> bool:
    if len(vals) < 3 or not ema8:
        return False
    last = len(vals) - 1
    current_val, prev_val = vals[last], vals[last - 1]
    return (current_val > prev_val) and (prev_val < vals[last - 2]) and (current_val > ema8[-1])

# ═══════════════════════════════════════════════════════════════════════════
# SOPORTE/RESISTENCIA · RSI · MACD · FIBONACCI
# ═══════════════════════════════════════════════════════════════════════════

def calcular_soporte_resistencia_fuerte(niveles: List[float]) -> dict:
    if len(niveles) < 10:
        return {'soporte': None, 'resistencia': None}
    highs, lows = [], []
    for i in range(1, len(niveles) - 1):
        if niveles[i] > niveles[i - 1] and niveles[i] > niveles[i + 1]:
            highs.append(niveles[i])
        if niveles[i] < niveles[i - 1] and niveles[i] < niveles[i + 1]:
            lows.append(niveles[i])
    highs.sort(reverse=True)
    lows.sort()
    resistencia = sum(highs[:3]) / min(3, len(highs)) if highs else None
    soporte     = sum(lows[:3]) / min(3, len(lows)) if lows else None
    return {'soporte': soporte, 'resistencia': resistencia}

def calcular_rsi(niveles: List[float]) -> List[float]:
    if len(niveles) < 15:
        return []
    gains, losses = [], []
    for i in range(1, len(niveles)):
        change = niveles[i] - niveles[i - 1]
        if change > 0:
            gains.append(change); losses.append(0)
        else:
            gains.append(0); losses.append(abs(change))
    rsi = []
    for i in range(13, len(gains)):
        avg_gain = sum(gains[i - 13:i + 1]) / 14
        avg_loss = sum(losses[i - 13:i + 1]) / 14
        rsi.append(100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss))))
    return rsi

def calcular_macd(niveles: List[float]) -> List[float]:
    if len(niveles) < 26:
        return []
    ema12 = ema_html(12, niveles)
    ema26 = ema_html(26, niveles)
    return [ema12[i] - ema26[i] for i in range(len(niveles))]

def calcular_fibonacci(niveles: List[float]) -> dict:
    if len(niveles) < 2:
        return {}
    mx, mn = max(niveles), min(niveles)
    diff = mx - mn
    return {'0.0%': mx, '38.2%': mx - diff * 0.382, '61.8%': mx - diff * 0.618, '100.0%': mn}

# ═══════════════════════════════════════════════════════════════════════════
# ESTADÍSTICAS AVANZADAS
# ═══════════════════════════════════════════════════════════════════════════

def get_tipo_dato(v: float) -> str:
    if v < 2.0:
        return 'low'
    if v < 5.0:
        return 'mid'
    return 'high'

def calcular_rachas(data: List[float]) -> dict:
    current_type = get_tipo_dato(data[-1])
    current_streak = 1
    for i in range(len(data) - 1, -1, -1):
        if get_tipo_dato(data[i]) == current_type:
            current_streak += 1
        else:
            break
    max_low = max_mid = max_high = 0
    prev_tipo = None
    streak_count = 0
    for v in data:
        tipo = get_tipo_dato(v)
        if tipo == prev_tipo:
            streak_count += 1
        else:
            if prev_tipo is not None:
                if prev_tipo == 'low':
                    max_low = max(max_low, streak_count)
                elif prev_tipo == 'mid':
                    max_mid = max(max_mid, streak_count)
                else:
                    max_high = max(max_high, streak_count)
            streak_count = 1
            prev_tipo = tipo
    if prev_tipo is not None:
        if prev_tipo == 'low':
            max_low = max(max_low, streak_count)
        elif prev_tipo == 'mid':
            max_mid = max(max_mid, streak_count)
        else:
            max_high = max(max_high, streak_count)
    return {'currentType': current_type, 'currentLength': current_streak - 1,
            'maxLow': max_low, 'maxMid': max_mid, 'maxHigh': max_high}

def calcular_probabilidad_condicional(data: List[float]) -> dict:
    result = {'after3Low': 0.0, 'after2High': 0.0, 'afterStreak5Low': 0.0}
    n = len(data)
    if n < 10:
        return result
    a = at = 0
    for i in range(3, n):
        if data[i - 1] < 2 and data[i - 2] < 2 and data[i - 3] < 2:
            at += 1
            if data[i] >= 2:
                a += 1
    result['after3Low'] = (a / at * 100) if at > 0 else 0.0
    b = bt = 0
    for i in range(2, n):
        if data[i - 1] >= 2 and data[i - 2] >= 2:
            bt += 1
            if data[i] < 2:
                b += 1
    result['after2High'] = (b / bt * 100) if bt > 0 else 0.0
    c = ct = 0
    for i in range(5, n):
        all_low = all(data[i - j] < 2 for j in range(1, 6))
        if all_low:
            ct += 1
            if data[i] >= 2:
                c += 1
    result['afterStreak5Low'] = (c / ct * 100) if ct > 0 else 0.0
    return result

def calcular_estadisticas_avanzadas(data: List[float]) -> dict:
    n = len(data)
    mean = sum(data) / n
    variance = sum((v - mean) ** 2 for v in data) / n
    std_dev = variance ** 0.5
    streaks = calcular_rachas(data)
    cond = calcular_probabilidad_condicional(data)
    slope, r2 = calcular_regresion_lineal(data)
    return {'mean': mean, 'stdDev': std_dev, 'variance': variance,
            'slope': slope, 'r2': r2, 'streaks': streaks, 'conditionalProb': cond}

# ═══════════════════════════════════════════════════════════════════════════
# AGENTE 6 — BLOQUES / RACHAS DE BAJOS
# ═══════════════════════════════════════════════════════════════════════════

def obtener_rango_agente6(v: float) -> str:
    if v < 1.50: return 'muyBajo'
    if v < 2.00: return 'bajo'
    if v < 10.00: return 'medio'
    if v < 50.00: return 'alto'
    return 'muyAlto'

def calcular_rachas_agente6(lista: List[float]):
    racha_temp = 0
    racha_max = 0
    for v in lista:
        if v < 2.00:
            racha_temp += 1
            racha_max = max(racha_max, racha_temp)
        else:
            racha_temp = 0
    racha_actual = 0
    for v in reversed(lista):
        if v < 2.00:
            racha_actual += 1
        else:
            break
    return racha_actual, racha_max

def generar_recomendacion_agente6(vals: List[float]) -> dict:
    if len(vals) < 30:
        return {'tipo': 'analizando', 'racha': 0, 'totalBajos30': 0.0}
    bloque30 = vals[-30:]
    conteo = {'muyBajo': 0, 'bajo': 0, 'medio': 0, 'alto': 0, 'muyAlto': 0}
    for v in bloque30:
        conteo[obtener_rango_agente6(v)] += 1
    total = len(bloque30)
    total_bajos30 = (conteo['muyBajo'] + conteo['bajo']) / total * 100
    racha_actual, _ = calcular_rachas_agente6(vals)
    if racha_actual >= 4 and total_bajos30 > 60:
        tipo = 'segura'
    elif racha_actual >= 3 and 50 <= total_bajos30 <= 60:
        tipo = 'moderada'
    elif total_bajos30 < 45 or racha_actual < 2:
        tipo = 'esperar'
    else:
        tipo = 'analizando'
    return {'tipo': tipo, 'racha': racha_actual, 'totalBajos30': total_bajos30}

# ═══════════════════════════════════════════════════════════════════════════
# AGENTE 7 — RACHAS DE RANGO
# ═══════════════════════════════════════════════════════════════════════════

def obtener_rango(v: float) -> str:
    if v < 1.50: return 'muyBajo'
    if v < 2.00: return 'bajo'
    if v < 4.00: return 'medio'
    if v < 10.00: return 'medioAlto'
    return 'alto'

def detectar_rachas_rango(vals: List[float]):
    if len(vals) < UMBRAL_ACTIVACION:
        return False, None, {}
    racha_actual = 1
    rango_actual = obtener_rango(vals[-1])
    start = max(0, len(vals) - 10)
    i = len(vals) - 2
    while i >= start:
        if obtener_rango(vals[i]) == rango_actual and vals[i + 1] > vals[i]:
            racha_actual += 1
            i -= 1
        else:
            break
    rangos_racha = {rango_actual: racha_actual}
    racha_rango_activa = racha_actual >= 3
    rango_activo = rango_actual if racha_rango_activa else None
    return racha_rango_activa, rango_activo, rangos_racha

# ═══════════════════════════════════════════════════════════════════════════
# AGENTE 8 — FUERZA / VELOCIDAD
# ═══════════════════════════════════════════════════════════════════════════

def calcular_fuerza(vals: List[float]):
    if len(vals) < UMBRAL_ACTIVACION:
        return None
    v1, v2, v3 = vals[-3], vals[-2], vals[-1]
    velocidad = ((v2 - v1) + (v3 - v2)) / 2
    tendencia = 0.0
    if len(vals) >= 10:
        reciente, antiguo = vals[-5:], vals[-10:-5]
        f_r = sum(reciente[i] - reciente[i - 1] for i in range(1, len(reciente)))
        f_a = sum(antiguo[i] - antiguo[i - 1] for i in range(1, len(antiguo)))
        tendencia = f_r - f_a
    return {'velocidad': velocidad, 'tendencia': tendencia}

# ═══════════════════════════════════════════════════════════════════════════
# SISTEMA DE 8 AGENTES — SCORE PONDERADO
# ═══════════════════════════════════════════════════════════════════════════

def ejecutar_multiagente(vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib,
                         sr_strong, stats, agente6, racha_rango_activa,
                         rango_activo, rangos_racha, fuerza, ia_prob):
    current = niveles[-1]
    scores = {}
    
    # Agente 1: EMAs
    s1 = 0
    if ema4 and ema8 and ema20 and ema50:
        e4, e8, e20, e50 = ema4[-1], ema8[-1], ema20[-1], ema50[-1]
        e4p, e8p = ema4[-2], ema8[-2]
        if e4 > e8 > e20 > e50: s1 += 3
        elif e4 > e8 > e20:     s1 += 2
        elif e4 < e8 < e20 < e50: s1 -= 4
        if e4 > e4p and e8 > e8p: s1 += 2
        elif e4 < e4p and e8 < e8p: s1 -= 2
    scores['a1'] = max(-5, min(5, s1))
    
    # Agente 2: S/R
    s2 = 0
    if sr_strong.get('soporte') and abs(current - sr_strong['soporte']) <= 1.0:
        s2 += 4
    elif sr_strong.get('resistencia') and abs(current - sr_strong['resistencia']) <= 1.0:
        s2 -= 4
    scores['a2'] = max(-5, min(5, s2))
    
    # Agente 3: Historial
    s3 = 0
    if len(vals) >= 10:
        last10 = vals[-10:]
        altos = sum(1 for v in last10 if v >= 2.0)
        if altos >= 6: s3 += 3
        elif altos <= 2: s3 -= 3
    scores['a3'] = max(-5, min(5, s3))
    
    # Agente 4: RSI+MACD
    s4 = 0
    if rsi and len(rsi) >= 2:
        r = rsi[-1]
        if 45 <= r <= 60: s4 += 3
        elif 30 <= r < 45: s4 += 2
        elif r > 75: s4 -= 4
        elif r < 25: s4 -= 2
    if macd and len(macd) >= 3:
        if macd[-1] > 0 and macd[-2] <= 0: s4 += 3
        elif macd[-1] < 0 and macd[-2] >= 0: s4 -= 3
        elif macd[-1] > macd[-2] > macd[-3]: s4 += 2
    scores['a4'] = max(-5, min(5, s4))
    
    # Agente 5: Estadístico
    s5 = 0
    if stats:
        if stats['slope'] > 0.05 and stats['r2'] > 0.4: s5 += 3
        elif stats['slope'] < -0.05 and stats['r2'] > 0.4: s5 -= 4
        streak = stats['streaks']
        if streak['currentType'] == 'low' and streak['currentLength'] >= 3:
            if stats['conditionalProb']['after3Low'] > 55: s5 += 3
        elif streak['currentType'] == 'high' and streak['currentLength'] >= 2:
            s5 -= 2
    scores['a5'] = max(-5, min(5, s5))
    
    # Agente 6: Bloques
    s6 = 0
    if agente6:
        if agente6['tipo'] == 'segura': s6 += 4
        elif agente6['tipo'] == 'moderada': s6 += 2
        elif agente6['tipo'] == 'esperar': s6 -= 3
    scores['a6'] = max(-5, min(5, s6))
    
    # Agente 7: Rachas
    s7 = 0
    if racha_rango_activa and rango_activo:
        racha = rangos_racha.get(rango_activo, 0)
        if rango_activo in ('muyBajo', 'bajo') and racha >= 4: s7 += 3
        elif rango_activo == 'medio': s7 += 2
        elif rango_activo in ('medioAlto', 'alto'): s7 -= 2
    scores['a7'] = max(-5, min(5, s7))
    
    # Agente 8: Fuerza
    s8 = 0
    if fuerza:
        vel, ten = fuerza['velocidad'], fuerza['tendencia']
        if vel > 1.5 and ten > 0.5: s8 += 4
        elif vel > 0.5 and ten > 0: s8 += 2
        elif vel < -1.0: s8 -= 3
    scores['a8'] = max(-5, min(5, s8))
    
    # Score ponderado final
    score_total = sum(scores[k] * AGENT_WEIGHTS[k] for k in scores)
    max_posible = sum(5 * AGENT_WEIGHTS[k] for k in AGENT_WEIGHTS)
    score_pct = (score_total / max_posible) * 100
    
    # Votos para compatibilidad con ML
    votos = {}
    for k, s in scores.items():
        if s >= 2: votos[k] = 'ENTRAR'
        elif s <= -2: votos[k] = 'NO_ENTRAR'
        else: votos[k] = 'ESPERAR'
    
    contar_entrar = sum(1 for v in votos.values() if v == 'ENTRAR')
    contar_no_entrar = sum(1 for v in votos.values() if v == 'NO_ENTRAR')
    
    return score_pct, contar_entrar, contar_no_entrar, votos, scores

def agentes_bloquean_señal(score_pct, contar_entrar, contar_no_entrar) -> bool:
    diff_abs = abs(contar_entrar - contar_no_entrar)
    if diff_abs < COHERENCIA_MIN_DIFF:
        return True
    if score_pct < SCORE_UMBRAL_EMISION:
        return True
    return False

def agentes_permiten_emision_inmediata(score_pct, contar_entrar, contar_no_entrar) -> bool:
    diff_abs = abs(contar_entrar - contar_no_entrar)
    if diff_abs < COHERENCIA_MIN_DIFF:
        return False
    if score_pct < SCORE_UMBRAL_INMEDIATA:
        return False
    return True

# ═══════════════════════════════════════════════════════════════════════════
# MOTOR DE ESTRATEGIA v15.1 — SIN CONFIRMACIÓN TEMPORAL
# ═══════════════════════════════════════════════════════════════════════════

class HtmlEngine:
    def __init__(self):
        self.tendencia_estado = 'ROJO'
        self.last_video_idx   = -999
        self.last_combo_idx   = -999
        self.last_martillo_idx = -999
        self.last_intercalado_idx = -999
        self.last_dos_azules_idx = -999
        self.last_soporte_idx = -999
        self.last_tendencia_media_idx = -999
        self.last_fib_idx = -999
        self.last_rsi_idx = -999
        self.fuerza_memoria   = []

    def evaluar(self, vals: List[float]):
        """v15.1: SIN confirmación temporal — emite señal inmediatamente"""
        if len(vals) < 3:
            return None

        # Filtro de volatilidad (relajado)
        if not filtro_volatilidad(vals):
            logger.debug("[v15.1] 🛑 Volatilidad extrema — no evaluar")
            return None

        niveles = compute_niveles(vals)
        ema4  = ema_html(4, niveles)
        ema8  = ema_html(8, niveles)
        ema20 = ema_html(20, niveles)
        ema50 = ema_html(50, niveles)
        confidence = calcular_confianza(vals)
        idx = len(vals) - 1

        nuevo = calcular_tendencia_lucky(vals, niveles)
        self.tendencia_estado = nuevo

        sr_strong = calcular_soporte_resistencia_fuerte(niveles)
        rsi  = calcular_rsi(niveles)
        macd = calcular_macd(niveles)
        fib  = calcular_fibonacci(niveles)

        stats = agente6 = None
        racha_rango_activa = False
        rango_activo = None
        rangos_racha = {}
        fuerza = None
        ia_prob = None
        if len(vals) >= UMBRAL_ACTIVACION:
            stats = calcular_estadisticas_avanzadas(vals)
            agente6 = generar_recomendacion_agente6(vals)
            racha_rango_activa, rango_activo, rangos_racha = detectar_rachas_rango(vals)
            fuerza = calcular_fuerza(vals)
            if fuerza is not None and len(vals) >= 5:
                self.fuerza_memoria.append({
                    'fuerza': vals[-4] - vals[-5],
                    'resultado': 'alto' if vals[-1] >= 2.0 else 'bajo'
                })
                if len(self.fuerza_memoria) > 500:
                    self.fuerza_memoria = self.fuerza_memoria[-500:]
                if len(self.fuerza_memoria) >= 10:
                    fa_a = fa_t = 0
                    for i in range(1, len(self.fuerza_memoria)):
                        if self.fuerza_memoria[i - 1]['fuerza'] > 1.0:
                            fa_t += 1
                            if self.fuerza_memoria[i]['resultado'] == 'alto':
                                fa_a += 1
                    if fa_t > 0:
                        ia_prob = (fa_a / fa_t) * 100

        score_pct, contar_entrar, contar_no_entrar, votos, scores = ejecutar_multiagente(
            vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib, sr_strong,
            stats, agente6, racha_rango_activa, rango_activo, rangos_racha, fuerza, ia_prob
        )

        # Filtro de agentes (relajado)
        if agentes_bloquean_señal(score_pct, contar_entrar, contar_no_entrar):
            logger.debug(f"[v15.1] 🛑 Agentes bloquean: score={score_pct:.1f}%")
            # NO retornar None — permitir que los patrones específicos evalúen

        agresiva_condicion = False
        max_rec = max(vals[-10:]) if len(vals) >= 10 else (max(vals) if vals else 0)
        agresiva_condicion = max_rec >= UMBRAL_AGRESIVO_V2

        features_base = {
            'idx': idx,
            'ultimo_valor': vals[-1],
            'confidence': confidence,
            'tendencia_lucky': nuevo,
            'agresiva_condicion': agresiva_condicion,
            'ema4': ema4[-1] if ema4 else None,
            'ema8': ema8[-1] if ema8 else None,
            'ema20': ema20[-1] if ema20 else None,
            'ema50': ema50[-1] if ema50 else None,
            'votos': votos,
            'scores': scores,
            'score_pct': score_pct,
            'contar_entrar': contar_entrar,
            'contar_no_entrar': contar_no_entrar,
            'rsi': rsi[-1] if rsi else None,
            'macd': macd[-1] if macd else None,
            'fuerza': fuerza,
            'ia_prob': ia_prob,
            'racha_rango_activa': racha_rango_activa,
            'rango_activo': rango_activo,
        }

        candidatos = []

        # V1: Video
        if len(vals) >= 5:
            conf = int(confidence)
            if CONF_ALERTA_MIN <= conf <= CONF_ALERTA_MAX and filtro_v1_video(vals, ema8):
                self.last_video_idx = idx
                candidatos.append(('video', ALERT_LABELS['video'],
                                   f'Confianza {conf}% + últimos 3 < 2x + EMA8 subiendo',
                                   dict(features_base)))

        # V2: Combo verde agresiva
        if nuevo == 'VERDE' and agresiva_condicion and filtro_v2_combo(vals, ema4, ema8):
            self.last_combo_idx = idx
            candidatos.append(('combo_verde_agresiva', ALERT_LABELS['combo_verde_agresiva'],
                               f'Tendencia VERDE + máx≥{UMBRAL_AGRESIVO_V2}x + EMA4>EMA8',
                               dict(features_base)))

        # V3: Martillo
        if detectar_martillo_base(vals, ema8) and filtro_v3_martillo(vals, ema8):
            self.last_martillo_idx = idx
            candidatos.append(('martillo', ALERT_LABELS['martillo'],
                               f'Martillo + valle<{UMBRAL_VALLE_V3}x + EMA8 subiendo 3 periodos',
                               dict(features_base)))

        # V4: Intercalado
        if detectar_intercalado(vals):
            self.last_intercalado_idx = idx
            candidatos.append(('intercalado', ALERT_LABELS['intercalado'],
                               f'Patrón intercalado detectado',
                               dict(features_base)))

        # V5: Dos Azules
        if detectar_dos_azules(vals, niveles):
            self.last_dos_azules_idx = idx
            candidatos.append(('dos_azules', ALERT_LABELS['dos_azules'],
                               '2 valores bajos consecutivos + rebote >= 1.8x',
                               dict(features_base)))

        # V6: Soporte Dinámico
        if detectar_soporte_dinamico(vals, ema8, ema20, niveles):
            self.last_soporte_idx = idx
            candidatos.append(('soporte_dinamico', ALERT_LABELS['soporte_dinamico'],
                               'Rebote en soporte/EMA dinámico',
                               dict(features_base)))

        # V7: Tendencia Media
        if detectar_tendencia_media(ema8, ema20):
            self.last_tendencia_media_idx = idx
            candidatos.append(('tendencia_media', ALERT_LABELS['tendencia_media'],
                               'EMA8 alcista confirmada sobre EMA20',
                               dict(features_base)))

        # V8: Multi-Agente
        if contar_entrar >= 5 and score_pct >= 30:
            candidatos.append(('multi_agente', ALERT_LABELS['multi_agente'],
                               f'{contar_entrar}/8 agentes a favor (score {score_pct:.1f}%)',
                               dict(features_base)))

        # V9: Fibonacci Rebote
        if detectar_fibonacci_rebote(vals, fib):
            self.last_fib_idx = idx
            candidatos.append(('fibonacci_rebote', ALERT_LABELS['fibonacci_rebote'],
                               'Rebote en nivel 61.8% Fibonacci',
                               dict(features_base)))

        # V10: RSI Extremo
        if detectar_rsi_extremo(rsi, vals):
            self.last_rsi_idx = idx
            candidatos.append(('rsi_extremo', ALERT_LABELS['rsi_extremo'],
                               f'RSI extremo',
                               dict(features_base)))

        if not candidatos:
            return None

        elegido = self._elegir_mejor(candidatos)
        if elegido is None:
            return None

        tipo_key, label, motivo, features = elegido

        # SIN confirmación temporal — emitir inmediatamente

        # ¿EMISIÓN INMEDIATA?
        es_inmediata = False
        if es_tendencia_claramente_alcista_fuerte(vals, ema4, ema8, ema20, rsi, nuevo):
            opc_cumplidas = cumple_condiciones_opcionales_inmediata(vals, macd)
            if opc_cumplidas >= 2:
                if agentes_permiten_emision_inmediata(score_pct, contar_entrar, contar_no_entrar):
                    es_inmediata = True
                    features['emision_inmediata'] = True
                    features['opc_cumplidas'] = opc_cumplidas
                    motivo += f' | 🚀 INMEDIATA (opc={opc_cumplidas}/3)'
                    logger.info(f"[v15.1] 🚀 Emisión inmediata: {tipo_key}")

        return tipo_key, label, motivo, json.dumps(features, default=str), es_inmediata

    def _elegir_mejor(self, candidatos: list):
        if ml_model is None:
            return candidatos[0]
        mejor = None
        mejor_prob = -1.0
        for cand in candidatos:
            tipo_key, label, motivo, features = cand
            prob = predict_prob(tipo_key, features)
            if prob is None:
                prob = MODEL_MIN_PROB
            features['ml_prob'] = round(prob, 4)
            if prob >= MODEL_MIN_PROB and prob > mejor_prob:
                mejor = cand
                mejor_prob = prob
        return mejor

engine = HtmlEngine()

# Continúa con el resto del código (ML, WebSocket, Flask, etc.)...
# [El código restante es idéntico al v15 original]
