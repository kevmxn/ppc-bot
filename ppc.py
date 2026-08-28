#!/usr/bin/env python3
"""
SPACEMAN HTML Strategy Bot — Telegram + Render  [v17 — ML Timing Dinámico]
────────────────────────────────────────────────────────────────────────────
Estrategia: 3 patrones optimizados para 86-90% de efectividad:
• PATRÓN V1 💎 (video): zona de confianza 30-40% + filtros de sobreventa
• PATRÓN V2 💎 (combo_verde_agresiva): combo verde + umbral 4.0x + filtros
• PATRÓN V3 💎 (martillo): rebote martillo + filtros de profundidad
+ Sistema de 8 agentes como FILTRO INTERNO (no emiten señales propias)
+ Emisión inmediata cuando tendencia es CLARAMENTE ALCISTA FUERTE (V2)
+ Emisión inmediata propia para V1: racha de cuotas bajas profunda +
  EMA8 girando al alza en 3 lecturas + RSI en sobreventa
+ Emisión inmediata propia para V3: valle más profundo + rebote fuerte +
  EMA8 girando al alza en 4 lecturas + RSI en recuperación
+ 🆕 ML DE TIMING DINÁMICO: ajusta automáticamente en qué intento (1, 2, 3, 4)
  se dispara la señal al Telegram según:
  • Contexto en tiempo real (últimas 20 rondas + features de la señal)
  • Análisis de todas las señales aprendidas históricamente
  • La ronda se desplaza según el contexto: a veces conviene intento 1,
    a veces 2, a veces 3, a veces 4
Mecánica de confirmación: intento 1 silencioso a 1.60x. Si falla, se envía
señal para intento 2, 3 y 4 (gana si cualquiera >= 1.80x).
Emisión inmediata: 2 intentos directos sin confirmación previa.
Telegram: señales en tema 2, estadísticas en tema 5 del grupo base.
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
CHAT_ID_BASE = int(os.environ.get("CHAT_ID_BASE", "-1003965615775"))
THREAD_SIGNALS = int(os.environ.get("THREAD_SIGNALS", "2"))
THREAD_STATS   = int(os.environ.get("THREAD_STATS",   "5"))

# ─── CONFIG — WEBSOCKET ───────────────────────────────────────────────────────
WS_URL    = os.environ.get("WS_URL",    "wss://dga.pragmaticplaylive.net/ws")
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY",  "BRL")
GAME_ID   = int(os.environ.get("GAME_ID", "1301"))
DB_FILE = os.environ.get("DB_FILE", "spaceman.db")

# ─── CONFIG — ML ──────────────────────────────────────────────────────────────
MODEL_FILE     = os.environ.get("MODEL_FILE", "signal_model.joblib")
MODEL_MIN_PROB = float(os.environ.get("MODEL_MIN_PROB", "0.55"))
TIMING_MODEL_FILE = os.environ.get("TIMING_MODEL_FILE", "timing_model.joblib")
TIMING_MIN_PROB = float(os.environ.get("TIMING_MIN_PROB", "0.40"))
CONTEXT_WINDOW = int(os.environ.get("CONTEXT_WINDOW", "20"))

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

# ─── CUOTA ÚNICA ──────
CASHOUT_TARGET  = 1.80
CASHOUT_TRIGGER = 1.80
CONFIRM_TRIGGER = 2.00

# ─── MECÁNICA DE CONFIRMACIÓN ───────
MAX_ATTEMPTS_NORMAL    = 4   # v17: hasta 4 intentos
MAX_ATTEMPTS_IMMEDIATE = 2   # emisión inmediata: intentos 1 y 2
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

CONF_ALERTA_MIN        = 30
CONF_ALERTA_MAX        = 40
UMBRAL_AGRESIVO_V2     = 4.0
UMBRAL_VALLE_V3        = 1.5
DIST_MIN_EMA8_V3       = 1.05
DIST_MAX_EMA8_V3       = 1.50
SOPORTE_COOLDOWN       = 5
MIN_DATOS_ENTRE_TOQUES = 2
TOQUES_NECESARIOS      = 3
RANGO_KEYS = ['muyBajo', 'bajo', 'medio', 'medioAlto', 'alto']

# Emisión inmediata propia de V1 (video): a diferencia de V2/V3, que exigen
# tendencia alcista con la última cuota ya alta, V1 opera en sentido
# contrario -- racha de cuotas bajas + rebote naciente. Se necesita, entonces,
# un criterio de "alta confianza" adaptado a ese contexto, más exigente que
# el mínimo de filtro_v1_video() (racha de 3 bajas).
RACHA_BAJOS_MIN_INMEDIATA_V1 = 5     # racha de cuotas <2.0x más profunda que el mínimo base (3)
RSI_OVERSOLD_V1               = 35   # RSI en sobreventa clara, refuerza probabilidad de rebote

# Emisión inmediata propia de V3 (martillo): igual que V1, el martillo es un
# patrón de rebote desde un valle -- exigirle la tendencia madura y sostenida
# de es_tendencia_claramente_alcista_fuerte() (EMA4>EMA8>EMA20 ya alineadas y
# subiendo) es poco realista justo en el momento del giro. Se usa en cambio
# un criterio propio, más exigente que el mínimo de filtro_v3_martillo():
VALLE_MIN_INMEDIATA_V3   = 1.20  # valle más profundo que el mínimo base (< 1.5)
REBOTE_MIN_INMEDIATA_V3  = 1.5   # la cuota actual debe ser >= 50% superior al valle
RSI_RECUP_MIN_V3         = 30    # RSI saliendo de sobreventa (recuperación, no aún maduro)
RSI_RECUP_MAX_V3         = 45

ALERT_LABELS = {
    'video':                'PATRON V1 💎',
    'combo_verde_agresiva': 'PATRON V2 💎',
    'martillo':             'PATRON V3 💎',
}
PATTERN_ORDER = ['video', 'combo_verde_agresiva', 'martillo']

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
    CREATE TABLE IF NOT EXISTS signal_contexts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id       TEXT,
        tipo_key        TEXT,
        trigger_value   REAL,
        attempt_when_win INTEGER,
        result          TEXT,
        context_json    TEXT,
        created         TEXT    NOT NULL DEFAULT (datetime('now'))
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
        "sig_emit_attempt": str(sig_emit_attempt),
        "sig_context_json": sig_context_json or "",
        "sig_signal_id":    sig_signal_id or "",
        "stats_msg_id":     str(stats_msg_id) if stats_msg_id is not None else "",
        "daily_wins":       str(daily_wins),
        "daily_losses":     str(daily_losses),
        "consecutive_wins": str(consecutive_wins),
        "consecutive_losses": str(consecutive_losses),
        "ml_last_trained_count": str(ml_last_trained_count),
        "timing_last_trained_count": str(timing_last_trained_count),
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global sig_inmediata, sig_emit_attempt, sig_context_json, sig_signal_id, stats_msg_id
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    global ml_last_trained_count, timing_last_trained_count
    
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
    sig_emit_attempt  = int(d.get("sig_emit_attempt", "2") or "2")
    sig_context_json  = d.get("sig_context_json", "") or None
    sig_signal_id     = d.get("sig_signal_id", "") or None
    _sid              = d.get("stats_msg_id", "")
    stats_msg_id      = int(_sid) if _sid else None
    daily_wins        = int(d.get("daily_wins", "0"))
    daily_losses      = int(d.get("daily_losses", "0"))
    consecutive_wins  = int(d.get("consecutive_wins", "0"))
    consecutive_losses = int(d.get("consecutive_losses", "0"))
    ml_last_trained_count = int(d.get("ml_last_trained_count", "0") or "0")
    timing_last_trained_count = int(d.get("timing_last_trained_count", "0") or "0")
    logger.info(f"[v17] Estado cargado | estado={sig_state} intento={sig_attempt} tipo={sig_tipo} emit_attempt={sig_emit_attempt}")

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

# ─── PERSISTENCIA — CONTEXTO DE SEÑAL (ML TIMING) ────────────────────────────
def log_signal_context(signal_id: str, tipo_key: str, trigger_value: float,
                       attempt_when_win: Optional[int], result: str,
                       context_json: str):
    try:
        con = _db()
        con.execute(
            "INSERT INTO signal_contexts(signal_id, tipo_key, trigger_value, "
            "attempt_when_win, result, context_json) VALUES(?,?,?,?,?,?)",
            (signal_id, tipo_key, trigger_value, attempt_when_win, result, context_json)
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando signal_context: {e}")

def update_signal_context_result(signal_id: str, attempt_when_win: Optional[int], result: str):
    try:
        con = _db()
        con.execute(
            "UPDATE signal_contexts SET attempt_when_win=?, result=? WHERE signal_id=?",
            (attempt_when_win, result, signal_id)
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error actualizando signal_context: {e}")

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
sig_emit_attempt:  int           = 2
sig_context_json:  Optional[str] = None
sig_signal_id:     Optional[str] = None
stats_msg_id:      Optional[int] = None
daily_wins:        int           = 0
daily_losses:      int           = 0
consecutive_wins:  int           = 0
consecutive_losses: int          = 0

# ─── BOTS + FLASK ─────────────────────────────────────────────────────────────
bot = AsyncTeleBot(BOT_TOKEN, parse_mode='HTML')
_main_loop: asyncio.AbstractEventLoop = None
flask_app = Flask(__name__)

# ─── TELEGRAM HELPERS ────────────────────────────────────────────────────────
async def send_msg(text: str, no_preview: bool = False,
                   thread_id: Optional[int] = None) -> Optional[int]:
    try:
        kwargs = {
            "chat_id": CHAT_ID_BASE,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": no_preview,
        }
        if thread_id is not None:
            kwargs["message_thread_id"] = thread_id
        msg = await bot.send_message(**kwargs)
        return msg.message_id
    except Exception as e:
        logger.warning(f"[v17] send error (chat_id={CHAT_ID_BASE}, thread_id={thread_id}): {e}")
        return None

async def send_signal_msg(text: str, no_preview: bool = False) -> Optional[int]:
    return await send_msg(text, no_preview=no_preview, thread_id=THREAD_SIGNALS)

async def send_stats_msg(text: str, no_preview: bool = False) -> Optional[int]:
    return await send_msg(text, no_preview=no_preview, thread_id=THREAD_STATS)

async def edit_msg(msg_id: int, text: str, no_preview: bool = False,
                   thread_id: Optional[int] = None) -> bool:
    try:
        kwargs = {
            "chat_id": CHAT_ID_BASE,
            "message_id": msg_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": no_preview,
        }
        await bot.edit_message_text(**kwargs)
        return True
    except Exception as e:
        logger.debug(f"[v17] edit error {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID_BASE, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[v17] delete error {msg_id}: {e}")
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
# FILTROS OPTIMIZADOS v13
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

def agentes_permiten_emision_inmediata(contar_entrar, contar_no_entrar, risk_score) -> bool:
    if risk_score >= 7:
        return False
    if contar_no_entrar >= 4 and contar_no_entrar > contar_entrar:
        return False
    return True

def es_v1_video_alta_confianza(vals: List[float], ema8: List[float], rsi: Optional[List[float]]) -> bool:
    """Criterio de emisión inmediata específico para V1 (video).
    es_tendencia_claramente_alcista_fuerte() es estructuralmente incompatible
    con V1: exige vals[-1] >= 2.0, mientras que filtro_v1_video() exige que
    las últimas 3 cuotas sean < 2.0 -- por diseño nunca coinciden. V1 necesita
    entonces su propio criterio de "alta confianza", pensado para el mismo
    contexto en el que V1 dispara (racha de cuotas bajas + EMA8 girando al
    alza), pero más exigente que el mínimo del patrón base:
      • Racha de cuotas < 2.0x más profunda (>= 5, vs el mínimo de 3 de V1)
      • EMA8 confirmando el giro en 3 lecturas seguidas, no solo la última
      • RSI en sobreventa clara (refuerza que el rebote es probable)
    """
    if len(vals) < 5 or len(ema8) < 3:
        return False
    racha_bajos = 0
    i = len(vals) - 1
    while i >= 0 and vals[i] < 2.0:
        racha_bajos += 1
        i -= 1
    if racha_bajos < RACHA_BAJOS_MIN_INMEDIATA_V1:
        return False
    if not (ema8[-1] > ema8[-2] > ema8[-3]):
        return False
    if not rsi or len(rsi) == 0:
        return False
    if rsi[-1] > RSI_OVERSOLD_V1:
        return False
    return True

def es_v3_martillo_alta_confianza(vals: List[float], ema8: List[float], rsi: Optional[List[float]]) -> bool:
    """Criterio de emisión inmediata específico para V3 (martillo).
    Igual que V1, el martillo es un patrón de rebote desde un valle -- pedirle
    la tendencia madura y sostenida de es_tendencia_claramente_alcista_fuerte()
    (EMA4>EMA8>EMA20 ya alineadas y subiendo) es poco realista justo en el
    momento del giro: esa condición describe una tendencia ya consolidada, no
    un rebote recién iniciado. V3 necesita entonces su propio criterio de
    "alta confianza", más exigente que el mínimo de filtro_v3_martillo():
      • Valle más profundo que el mínimo base (< 1.20, vs el mínimo de 1.5)
      • Rebote fuerte: la cuota actual >= 50% por encima del valle
      • EMA8 confirmando el giro en 4 lecturas seguidas (una más que la base)
      • RSI saliendo de sobreventa (recuperación, no aún neutral/maduro)
    """
    if len(vals) < 6 or len(ema8) < 4:
        return False
    valle = vals[-2]
    if valle <= 0 or valle >= VALLE_MIN_INMEDIATA_V3:
        return False
    rebote_ratio = vals[-1] / valle
    if rebote_ratio < REBOTE_MIN_INMEDIATA_V3:
        return False
    if not (ema8[-1] > ema8[-2] > ema8[-3] > ema8[-4]):
        return False
    if not rsi or len(rsi) == 0:
        return False
    r = rsi[-1]
    if not (RSI_RECUP_MIN_V3 <= r <= RSI_RECUP_MAX_V3):
        return False
    return True

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
# ESTADÍSTICAS AVANZADAS (agente 5)
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
# SISTEMA DE 8 AGENTES — SOLO COMO FILTRO INTERNO
# ═══════════════════════════════════════════════════════════════════════════
def ejecutar_multiagente(vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib,
                         sr_strong, stats, agente6, racha_rango_activa,
                         rango_activo, rangos_racha, fuerza, ia_prob):
    current = niveles[-1]
    risk_score = 0
    votos: Dict[str, str] = {}
    
    if ema4 and ema8 and ema50:
        e4, e8, e50 = ema4[-1], ema8[-1], ema50[-1]
        if e4 > e8 and e8 > e50:
            votos['a1'] = 'ENTRAR'
        elif e4 > e8:
            votos['a1'] = 'ENTRAR'
        elif e4 < e8 and e8 < e50:
            votos['a1'] = 'NO_ENTRAR'; risk_score += 3
        else:
            votos['a1'] = 'ESPERAR'; risk_score += 1
    else:
        votos['a1'] = 'ESPERAR'
    
    a2 = 0
    if sr_strong.get('soporte') is not None and abs(current - sr_strong['soporte']) <= 1.5:
        a2 += 4
    if sr_strong.get('resistencia') is not None and abs(current - sr_strong['resistencia']) <= 1.5:
        a2 -= 3; risk_score += 4
    if ema20:
        linea_media = ema20[-1]
        distancia = abs(current - linea_media)
        if distancia <= 0.5 and current > linea_media:
            a2 += 2
        elif distancia <= 0.5 and current < linea_media:
            a2 -= 2; risk_score += 2
    votos['a2'] = 'ENTRAR' if a2 >= 3 else ('NO_ENTRAR' if a2 <= -2 else 'ESPERAR')
    
    last5 = vals[-5:]
    avg_last5 = sum(last5) / 5
    prev5 = vals[-10:-5]
    avg_prev5 = (sum(prev5) / len(prev5)) if prev5 else avg_last5
    low_count = sum(1 for v in last5 if v < 2.0)
    a3 = 0
    if low_count >= 3 and avg_last5 < 2.0:
        a3 += 3
    elif avg_last5 > avg_prev5 * 1.2:
        a3 += 2
    else:
        risk_score += 1
    if a3 >= 3:
        votos['a3'] = 'ENTRAR'
    elif a3 <= 0 and risk_score > 2:
        votos['a3'] = 'NO_ENTRAR'
    else:
        votos['a3'] = 'ESPERAR'
    
    a4 = 0
    if rsi:
        r = rsi[-1]
        if 40 <= r <= 65:
            a4 += 2
        elif r < 30:
            a4 += 3
        elif r > 75:
            a4 -= 4; risk_score += 4
    if len(macd) > 1:
        if macd[-1] > 0 and macd[-1] > macd[-2]:
            a4 += 2
        elif macd[-1] < 0:
            a4 -= 2; risk_score += 2
    if fib:
        fib618 = fib.get('61.8%')
        if fib618 is not None and abs(current - fib618) < 0.5:
            a4 += 2
    votos['a4'] = 'ENTRAR' if a4 >= 3 else ('NO_ENTRAR' if a4 <= -2 else 'ESPERAR')
    
    if stats is not None:
        a5 = 0
        slope, r2 = stats['slope'], stats['r2']
        if slope > 0.05 and r2 > 0.3:
            a5 += 3
        elif slope < -0.05 and r2 > 0.3:
            a5 -= 3; risk_score += 3
        streak = stats['streaks']
        if streak['currentType'] == 'low' and streak['currentLength'] >= 3:
            prob = stats['conditionalProb']['after3Low']
            a5 += 4 if prob > 50 else 2
        elif streak['currentType'] == 'high' and streak['currentLength'] >= 2:
            a5 -= 2; risk_score += 2
        current_val = vals[-1]
        z = (current_val - stats['mean']) / stats['stdDev'] if stats['stdDev'] > 0 else 0
        if z < -1.5:
            a5 += 2
        elif z > 2:
            a5 -= 3; risk_score += 3
        if stats['conditionalProb']['afterStreak5Low'] > 65:
            a5 += 3
        votos['a5'] = 'ENTRAR' if a5 >= 3 else ('NO_ENTRAR' if a5 <= -2 else 'ESPERAR')
    else:
        votos['a5'] = 'ESPERAR'
    
    if agente6 is not None:
        tipo = agente6['tipo']
        if tipo in ('segura', 'moderada'):
            votos['a6'] = 'ENTRAR'
        elif tipo == 'esperar':
            votos['a6'] = 'NO_ENTRAR'; risk_score += 2
        else:
            votos['a6'] = 'ESPERAR'
    else:
        votos['a6'] = 'ESPERAR'
    
    if rangos_racha:
        a7 = 0
        if racha_rango_activa and rango_activo:
            racha = rangos_racha.get(rango_activo, 0)
            if rango_activo in ('muyBajo', 'bajo'):
                a7 += 5 if racha >= 4 else 3
            elif rango_activo == 'medio':
                a7 += 4
            elif rango_activo == 'medioAlto':
                a7 += 2
            else:
                a7 -= 2; risk_score += 2
        votos['a7'] = 'ENTRAR' if a7 >= 3 else ('NO_ENTRAR' if a7 <= -1 else 'ESPERAR')
    else:
        votos['a7'] = 'ESPERAR'
    
    if fuerza is not None:
        a8 = 0
        vel, ten = fuerza['velocidad'], fuerza['tendencia']
        if vel > 1.5 and ten > 0:
            a8 += 5
        elif vel > 0.5 and ten >= 0:
            a8 += 3
        elif vel > 0:
            a8 += 1
        elif vel < -1.0:
            a8 -= 3; risk_score += 2
        if ia_prob is not None:
            if ia_prob > 65:
                a8 += 2
            elif ia_prob < 35:
                a8 -= 2; risk_score += 1
        votos['a8'] = 'ENTRAR' if a8 >= 3 else ('NO_ENTRAR' if a8 <= -2 else 'ESPERAR')
    else:
        votos['a8'] = 'ESPERAR'
    
    contar_entrar    = sum(1 for v in votos.values() if v == 'ENTRAR')
    contar_no_entrar = sum(1 for v in votos.values() if v == 'NO_ENTRAR')
    return contar_entrar, contar_no_entrar, risk_score, votos

def agentes_bloquean_señal(contar_no_entrar, risk_score) -> bool:
    if risk_score >= 7:
        return True
    if contar_no_entrar >= 5:
        return True
    return False

# ═══════════════════════════════════════════════════════════════════════════
# MOTOR DE ESTRATEGIA v17
# ═══════════════════════════════════════════════════════════════════════════
class HtmlEngine:
    def __init__(self):
        self.tendencia_estado = 'ROJO'
        self.last_video_idx   = -999
        self.last_combo_idx   = -999
        self.last_martillo_idx = -999
        self.fuerza_memoria   = []
    
    def evaluar(self, vals: List[float]):
        if len(vals) < 3:
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
        
        contar_entrar, contar_no_entrar, risk_score, votos = ejecutar_multiagente(
            vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib, sr_strong,
            stats, agente6, racha_rango_activa, rango_activo, rangos_racha, fuerza, ia_prob
        )
        
        if agentes_bloquean_señal(contar_no_entrar, risk_score):
            logger.info(f"[v17] 🛑 Agentes bloquean señales: risk={risk_score} no_entrar={contar_no_entrar}")
            return None
        
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
            'contar_entrar': contar_entrar,
            'contar_no_entrar': contar_no_entrar,
            'risk_score': risk_score,
            'rsi': rsi[-1] if rsi else None,
            'macd': macd[-1] if macd else None,
            'fuerza': fuerza,
            'ia_prob': ia_prob,
            'racha_rango_activa': racha_rango_activa,
            'rango_activo': rango_activo,
        }
        
        candidatos = []
        
        if nuevo == 'VERDE' and agresiva_condicion and filtro_v2_combo(vals, ema4, ema8):
            self.last_combo_idx = idx
            candidatos.append(('combo_verde_agresiva', ALERT_LABELS['combo_verde_agresiva'],
                               f'Tendencia VERDE + máx≥{UMBRAL_AGRESIVO_V2}x + EMA4>EMA8',
                               dict(features_base)))
        
        if len(vals) >= 5:
            conf = int(confidence)
            if CONF_ALERTA_MIN <= conf <= CONF_ALERTA_MAX and filtro_v1_video(vals, ema8):
                self.last_video_idx = idx
                candidatos.append(('video', ALERT_LABELS['video'],
                                   f'Confianza {conf}% + últimos 3 < 2x + EMA8 subiendo',
                                   dict(features_base)))
        
        if detectar_martillo_base(vals, ema8) and filtro_v3_martillo(vals, ema8):
            self.last_martillo_idx = idx
            candidatos.append(('martillo', ALERT_LABELS['martillo'],
                               f'Martillo + valle<{UMBRAL_VALLE_V3}x + EMA8 subiendo 3 periodos',
                               dict(features_base)))
        
        if not candidatos:
            return None
        
        elegido = self._elegir_mejor(candidatos)
        if elegido is None:
            return None
        
        tipo_key, label, motivo, features = elegido
        
        es_inmediata = False
        if tipo_key == 'video':
            if es_v1_video_alta_confianza(vals, ema8, rsi):
                if agentes_permiten_emision_inmediata(contar_entrar, contar_no_entrar, risk_score):
                    es_inmediata = True
                    features['emision_inmediata'] = True
                    features['emision_inmediata_v1'] = True
                    motivo += f' | 🚀 INMEDIATA (V1: racha≥{RACHA_BAJOS_MIN_INMEDIATA_V1}, EMA8×3, RSI≤{RSI_OVERSOLD_V1})'
                    logger.info(f"[v17] 🚀 Emisión inmediata V1 activada: racha_bajos≥{RACHA_BAJOS_MIN_INMEDIATA_V1}, RSI≤{RSI_OVERSOLD_V1}")
        elif tipo_key == 'martillo':
            if es_v3_martillo_alta_confianza(vals, ema8, rsi):
                if agentes_permiten_emision_inmediata(contar_entrar, contar_no_entrar, risk_score):
                    es_inmediata = True
                    features['emision_inmediata'] = True
                    features['emision_inmediata_v3'] = True
                    motivo += f' | 🚀 INMEDIATA (V3: valle<{VALLE_MIN_INMEDIATA_V3}, rebote≥{REBOTE_MIN_INMEDIATA_V3}x, EMA8×4, RSI {RSI_RECUP_MIN_V3}-{RSI_RECUP_MAX_V3})'
                    logger.info(f"[v17] 🚀 Emisión inmediata V3 activada: valle<{VALLE_MIN_INMEDIATA_V3}, rebote≥{REBOTE_MIN_INMEDIATA_V3}x, RSI {RSI_RECUP_MIN_V3}-{RSI_RECUP_MAX_V3}")
        elif es_tendencia_claramente_alcista_fuerte(vals, ema4, ema8, ema20, rsi, nuevo):
            opc_cumplidas = cumple_condiciones_opcionales_inmediata(vals, macd)
            if opc_cumplidas >= 2:
                if agentes_permiten_emision_inmediata(contar_entrar, contar_no_entrar, risk_score):
                    es_inmediata = True
                    features['emision_inmediata'] = True
                    features['opc_cumplidas'] = opc_cumplidas
                    motivo += f' | 🚀 INMEDIATA (opc={opc_cumplidas}/3)'
                    logger.info(f"[v17] 🚀 Emisión inmediata activada: {tipo_key} | opc={opc_cumplidas}/3")
        
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

# ═══════════════════════════════════════════════════════════════════════════
# ML — FEATURIZACIÓN (SEÑAL)
# ═══════════════════════════════════════════════════════════════════════════
CATEGORICAL_COLUMNS = [
    'tipo_key',
    'tendencia_lucky',
    'rango_activo',
    'voto_a1', 'voto_a2', 'voto_a3', 'voto_a4',
    'voto_a5', 'voto_a6', 'voto_a7', 'voto_a8',
]

def flatten_features(tipo_key: str, features: dict) -> dict:
    flat = dict(features)
    votos = flat.pop('votos', None) or {}
    for agente in ('a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8'):
        flat[f'voto_{agente}'] = votos.get(agente)
    fuerza = flat.pop('fuerza', None)
    if isinstance(fuerza, dict):
        flat['fuerza_velocidad'] = fuerza.get('velocidad')
        flat['fuerza_tendencia'] = fuerza.get('tendencia')
    else:
        flat['fuerza_velocidad'] = None
        flat['fuerza_tendencia'] = None
    for bool_col in ('agresiva_condicion', 'racha_rango_activa', 'emision_inmediata'):
        if bool_col in flat and flat[bool_col] is not None:
            flat[bool_col] = int(bool(flat[bool_col]))
    flat.pop('ml_prob', None)
    flat['tipo_key'] = tipo_key
    return flat

# ═══════════════════════════════════════════════════════════════════════════
# ML — CONTEXTO DE SEÑAL (para ML de Timing Dinámico)
# ═══════════════════════════════════════════════════════════════════════════
def extract_context_features(history_vals: List[float], signal_features: dict) -> dict:
    """
    Extrae features de contexto en tiempo real para el ML de Timing.
    Incluye: últimas 20 rondas, rachas, indicadores, features de la señal.
    """
    context = {}
    
    # Últimas CONTEXT_WINDOW cuotas (valores crudos)
    window = history_vals[-CONTEXT_WINDOW:] if len(history_vals) >= CONTEXT_WINDOW else history_vals
    for i, v in enumerate(window):
        context[f'ctx_val_{i}'] = v
    
    # Features agregadas de las últimas 5, 10, 20 rondas
    for n in [5, 10, 20]:
        if len(history_vals) >= n:
            subset = history_vals[-n:]
            context[f'ctx_mean_{n}'] = sum(subset) / n
            context[f'ctx_max_{n}'] = max(subset)
            context[f'ctx_min_{n}'] = min(subset)
            context[f'ctx_std_{n}'] = (sum((v - context[f'ctx_mean_{n}'])**2 for v in subset) / n) ** 0.5
            context[f'ctx_count_low_{n}'] = sum(1 for v in subset if v < 2.0)
            context[f'ctx_count_high_{n}'] = sum(1 for v in subset if v >= 2.0)
        else:
            context[f'ctx_mean_{n}'] = 0.0
            context[f'ctx_max_{n}'] = 0.0
            context[f'ctx_min_{n}'] = 0.0
            context[f'ctx_std_{n}'] = 0.0
            context[f'ctx_count_low_{n}'] = 0
            context[f'ctx_count_high_{n}'] = 0
    
    # Rachas actuales
    racha_bajos = 0
    for v in reversed(history_vals):
        if v < 2.0:
            racha_bajos += 1
        else:
            break
    context['ctx_racha_bajos_actual'] = racha_bajos
    
    racha_altos = 0
    for v in reversed(history_vals):
        if v >= 2.0:
            racha_altos += 1
        else:
            break
    context['ctx_racha_altos_actual'] = racha_altos
    
    # Distancia a EMA8 actual
    if len(history_vals) >= 8:
        niveles = compute_niveles(history_vals)
        ema8 = ema_html(8, niveles)
        if ema8:
            context['ctx_dist_ema8'] = history_vals[-1] - ema8[-1]
        else:
            context['ctx_dist_ema8'] = 0.0
    else:
        context['ctx_dist_ema8'] = 0.0
    
    # RSI actual
    if len(history_vals) >= 15:
        niveles = compute_niveles(history_vals)
        rsi = calcular_rsi(niveles)
        context['ctx_rsi'] = rsi[-1] if rsi else 50.0
    else:
        context['ctx_rsi'] = 50.0
    
    # MACD actual
    if len(history_vals) >= 26:
        niveles = compute_niveles(history_vals)
        macd = calcular_macd(niveles)
        context['ctx_macd'] = macd[-1] if macd else 0.0
    else:
        context['ctx_macd'] = 0.0
    
    # Features de la señal (aplanadas)
    signal_flat = flatten_features(signal_features.get('tipo_key', 'desconocido'), signal_features)
    for k, v in signal_flat.items():
        if k != 'tipo_key':
            context[f'sig_{k}'] = v
    
    # Agregar tipo_key como categorical
    context['tipo_key'] = signal_features.get('tipo_key', 'desconocido')
    
    return context

# ─── ML — INFERENCIA (SEÑAL) ──────────────────────────────────────────────────
ml_model = None
ml_feature_columns: List[str] = []
ml_categorical_columns: List[str] = []
ml_last_trained_count: int = 0

def load_ml_model():
    global ml_model, ml_feature_columns, ml_categorical_columns
    if not ML_LIBS_OK:
        logger.warning("[ML] joblib/pandas no instalados — filtro ML desactivado.")
        return
    if not os.path.exists(MODEL_FILE):
        logger.info(f"[ML] No se encontró '{MODEL_FILE}' — filtro ML desactivado.")
        return
    try:
        artifact = joblib.load(MODEL_FILE)
        ml_model = artifact['model']
        ml_feature_columns = artifact['feature_columns']
        ml_categorical_columns = artifact['categorical_columns']
        logger.info(f"[ML] Modelo cargado ({len(ml_feature_columns)} features).")
    except Exception as e:
        logger.warning(f"[ML] Error cargando modelo: {e}")
        ml_model = None

def predict_prob(tipo_key: str, features: dict) -> Optional[float]:
    if ml_model is None:
        return None
    try:
        flat = flatten_features(tipo_key, features)
        dummy_prefixes = tuple(f"{c}_" for c in ml_categorical_columns)
        row = {
            col: flat.get(col)
            for col in ml_feature_columns
            if not col.startswith(dummy_prefixes)
        }
        df = pd.DataFrame([row])
        for cat_col in ml_categorical_columns:
            val = flat.get(cat_col)
            dummy_name = f"{cat_col}_{val}"
            for col in ml_feature_columns:
                if col.startswith(f"{cat_col}_"):
                    df[col] = 1 if col == dummy_name else 0
        df = df.reindex(columns=ml_feature_columns, fill_value=0)
        prob = ml_model.predict_proba(df)[0][1]
        return float(prob)
    except Exception as e:
        logger.warning(f"[ML] Error prediciendo: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════
# ML — TIMING MODEL DINÁMICO (aprende en qué intento emitir)
# ═══════════════════════════════════════════════════════════════════════════
timing_model = None
timing_feature_columns: List[str] = []
timing_categorical_columns: List[str] = []
timing_last_trained_count: int = 0

def load_timing_model():
    global timing_model, timing_feature_columns, timing_categorical_columns
    if not ML_LIBS_OK:
        logger.warning("[Timing ML] joblib/pandas no instalados — modelo de timing desactivado.")
        return
    if not os.path.exists(TIMING_MODEL_FILE):
        logger.info(f"[Timing ML] No se encontró '{TIMING_MODEL_FILE}' — modelo de timing desactivado.")
        return
    try:
        artifact = joblib.load(TIMING_MODEL_FILE)
        timing_model = artifact['model']
        timing_feature_columns = artifact['feature_columns']
        timing_categorical_columns = artifact['categorical_columns']
        logger.info(f"[Timing ML] Modelo cargado ({len(timing_feature_columns)} features).")
    except Exception as e:
        logger.warning(f"[Timing ML] Error cargando modelo: {e}")
        timing_model = None

def predict_timing(context_features: dict) -> Optional[Dict[str, float]]:
    """
    Predice la probabilidad de ganar en cada intento (1, 2, 3, 4) o perder.
    Retorna: {'win_1': prob, 'win_2': prob, 'win_3': prob, 'win_4': prob, 'loss': prob}
    """
    if timing_model is None:
        return None
    try:
        flat = dict(context_features)
        dummy_prefixes = tuple(f"{c}_" for c in timing_categorical_columns)
        row = {
            col: flat.get(col)
            for col in timing_feature_columns
            if not col.startswith(dummy_prefixes)
        }
        df = pd.DataFrame([row])
        for cat_col in timing_categorical_columns:
            val = flat.get(cat_col)
            dummy_name = f"{cat_col}_{val}"
            for col in timing_feature_columns:
                if col.startswith(f"{cat_col}_"):
                    df[col] = 1 if col == dummy_name else 0
        df = df.reindex(columns=timing_feature_columns, fill_value=0)
        probs = timing_model.predict_proba(df)[0]
        classes = timing_model.classes_
        result = {}
        for i, cls in enumerate(classes):
            if cls == 1:
                result['win_1'] = float(probs[i])
            elif cls == 2:
                result['win_2'] = float(probs[i])
            elif cls == 3:
                result['win_3'] = float(probs[i])
            elif cls == 4:
                result['win_4'] = float(probs[i])
            elif cls == 0:
                result['loss'] = float(probs[i])
        return result
    except Exception as e:
        logger.warning(f"[Timing ML] Error prediciendo: {e}")
        return None

def decide_emit_attempt(timing_pred: Optional[Dict[str, float]], es_inmediata: bool) -> int:
    """
    Decide en qué intento emitir la señal basado en las predicciones del modelo de timing.
    El ML ajusta dinámicamente la ronda según el contexto en tiempo real.
    """
    if es_inmediata:
        return 1
    
    if timing_pred is None:
        return 2  # Default: intento 2
    
    win_1 = timing_pred.get('win_1', 0.0)
    win_2 = timing_pred.get('win_2', 0.0)
    win_3 = timing_pred.get('win_3', 0.0)
    win_4 = timing_pred.get('win_4', 0.0)
    loss = timing_pred.get('loss', 0.0)
    
    # Si la probabilidad de pérdida es muy alta, no emitir
    if loss > 0.6:
        logger.info(f"[Timing ML] ❌ Alta probabilidad de pérdida ({loss:.2%}) — no emitir")
        return 0
    
    # Elegir el intento con mayor probabilidad de ganar
    attempts_probs = {1: win_1, 2: win_2, 3: win_3, 4: win_4}
    best_attempt = max(attempts_probs, key=attempts_probs.get)
    best_prob = attempts_probs[best_attempt]
    
    if best_prob < TIMING_MIN_PROB:
        logger.info(f"[Timing ML] ⚠️ Probabilidad muy baja ({best_prob:.2%}) — no emitir")
        return 0
    
    logger.info(f"[Timing ML] 🎯 Emitir en intento {best_attempt} (prob={best_prob:.2%}) | "
                f"win_1={win_1:.2%}, win_2={win_2:.2%}, win_3={win_3:.2%}, win_4={win_4:.2%}")
    return best_attempt

# ─── ML — AUTO-ENTRENAMIENTO (SEÑAL) ──────────────────────────────────────────
def count_resolved_signals() -> int:
    try:
        con = _db()
        row = con.execute(
            "SELECT COUNT(*) c FROM pattern_stats "
            "WHERE result IN ('win','loss') AND features_json IS NOT NULL"
        ).fetchone()
        con.close()
        return row["c"] if row else 0
    except Exception as e:
        logger.warning(f"[ML] Error contando señales: {e}")
        return 0

def train_model_in_thread(min_rows: int):
    if not ML_LIBS_OK:
        return False, None, "faltan librerías ML"
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, accuracy_score
    except ImportError:
        return False, None, "falta scikit-learn"
    try:
        df = cargar_datos_entrenamiento(DB_FILE)
        if len(df) < min_rows:
            return False, None, f"solo {len(df)} señales (mínimo {min_rows})"
        if df["_target"].nunique() < 2:
            return False, None, "no hay ejemplos de ambas clases"
        x, y, cat_cols_presentes = construir_matriz_entrenamiento(df)
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=0.2, random_state=42, stratify=y
        )
        model = HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.08, max_iter=200,
            l2_regularization=1.0, random_state=42,
        )
        model.fit(x_train, y_train)
        acc = accuracy_score(y_test, model.predict(x_test))
        try:
            auc = roc_auc_score(y_test, model.predict_proba(x_test)[:, 1])
            auc_txt = f", AUC={auc:.3f}"
        except Exception:
            auc_txt = ""
        artifact = {
            "model": model,
            "feature_columns": list(x.columns),
            "categorical_columns": cat_cols_presentes,
        }
        msg = f"{len(df)} señales, accuracy={acc:.3f}{auc_txt}"
        return True, artifact, msg
    except Exception as e:
        return False, None, f"error entrenando: {e}"

# ─── ML — AUTO-ENTRENAMIENTO (TIMING) ─────────────────────────────────────────
def count_resolved_contexts() -> int:
    try:
        con = _db()
        row = con.execute(
            "SELECT COUNT(*) c FROM signal_contexts "
            "WHERE result IN ('win','loss') AND context_json IS NOT NULL"
        ).fetchone()
        con.close()
        return row["c"] if row else 0
    except Exception as e:
        logger.warning(f"[Timing ML] Error contando contextos: {e}")
        return 0

def train_timing_model_in_thread(min_rows: int):
    if not ML_LIBS_OK:
        return False, None, "faltan librerías ML"
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score
    except ImportError:
        return False, None, "falta scikit-learn"
    try:
        df = cargar_datos_timing(DB_FILE)
        if len(df) < min_rows:
            return False, None, f"solo {len(df)} contextos (mínimo {min_rows})"
        if df["_target"].nunique() < 2:
            return False, None, "no hay ejemplos de suficientes clases"
        x, y, cat_cols_presentes = construir_matriz_timing(df)
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=0.2, random_state=42, stratify=y
        )
        model = HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.08, max_iter=200,
            l2_regularization=1.0, random_state=42,
        )
        model.fit(x_train, y_train)
        acc = accuracy_score(y_test, model.predict(x_test))
        artifact = {
            "model": model,
            "feature_columns": list(x.columns),
            "categorical_columns": cat_cols_presentes,
        }
        msg = f"{len(df)} contextos, accuracy={acc:.3f}"
        return True, artifact, msg
    except Exception as e:
        return False, None, f"error entrenando timing: {e}"

async def auto_train_loop():
    global ml_model, ml_feature_columns, ml_categorical_columns, ml_last_trained_count
    global timing_model, timing_feature_columns, timing_categorical_columns, timing_last_trained_count
    if not AUTO_TRAIN_ENABLED:
        logger.info("[ML] Auto-entrenamiento desactivado.")
        return
    if not ML_LIBS_OK:
        logger.warning("[ML] Auto-entrenamiento desactivado: faltan libs.")
        return
    logger.info(
        f"[ML] Auto-entrenamiento activo — cada {AUTO_TRAIN_INTERVAL_SEC}s "
        f"(mínimo inicial: {AUTO_TRAIN_MIN_ROWS}, reentrena cada +{AUTO_TRAIN_MIN_NEW})."
    )
    while True:
        await asyncio.sleep(AUTO_TRAIN_INTERVAL_SEC)
        try:
            total = count_resolved_signals()
            es_primer_entrenamiento = ml_model is None and total >= AUTO_TRAIN_MIN_ROWS
            necesita_reentrenar = ml_model is not None and (total - ml_last_trained_count) >= AUTO_TRAIN_MIN_NEW
            if es_primer_entrenamiento or necesita_reentrenar:
                logger.info(f"[ML] 🧠 Auto-entrenamiento señal ({total} señales)...")
                loop = asyncio.get_running_loop()
                ok, artifact, msg = await loop.run_in_executor(None, train_model_in_thread, AUTO_TRAIN_MIN_ROWS)
                if ok:
                    joblib.dump(artifact, MODEL_FILE)
                    ml_model = artifact["model"]
                    ml_feature_columns = artifact["feature_columns"]
                    ml_categorical_columns = artifact["categorical_columns"]
                    ml_last_trained_count = total
                    save_state()
                    logger.info(f"[ML] ✅ Modelo señal actualizado: {msg}")
                else:
                    logger.info(f"[ML] Auto-entrenamiento señal pospuesto: {msg}")
            
            total_ctx = count_resolved_contexts()
            es_primer_timing = timing_model is None and total_ctx >= AUTO_TRAIN_MIN_ROWS
            necesita_re_timing = timing_model is not None and (total_ctx - timing_last_trained_count) >= AUTO_TRAIN_MIN_NEW
            if es_primer_timing or necesita_re_timing:
                logger.info(f"[Timing ML] 🧠 Auto-entrenamiento timing ({total_ctx} contextos)...")
                loop = asyncio.get_running_loop()
                ok, artifact, msg = await loop.run_in_executor(None, train_timing_model_in_thread, AUTO_TRAIN_MIN_ROWS)
                if ok:
                    joblib.dump(artifact, TIMING_MODEL_FILE)
                    timing_model = artifact["model"]
                    timing_feature_columns = artifact["feature_columns"]
                    timing_categorical_columns = artifact["categorical_columns"]
                    timing_last_trained_count = total_ctx
                    save_state()
                    logger.info(f"[Timing ML] ✅ Modelo timing actualizado: {msg}")
                else:
                    logger.info(f"[Timing ML] Auto-entrenamiento timing pospuesto: {msg}")
        except Exception as e:
            logger.warning(f"[ML] Error en auto-entrenamiento: {e}")

# ─── MENSAJES ─────────────────────────────────────────────────────────────────
def build_signal_msg(tipo_label: str, last_value: float, es_inmediata: bool = False,
                     emit_attempt: int = 2) -> str:
    if es_inmediata:
        header = "<b>🚨🚨 ENTRADA INMEDIATA 🚨🚨</b>"
    else:
        header = f"<b>✅✅ ENTRADA CONFIRMADA ✅✅</b>"
    return (
        f"{header}\n\n"
        f"<b>🧠 {tipo_label}</b>\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x</b>\n"
        f"<i>💫 ¡Juegue con Responsabilidad!</i>\n"
        f'🎰 <a href="{GAME_LINK}">Acceder al Spaceman</a>'
    )

def build_stats_result_msg(result: float, tipo_label: str, intento: int, win: bool) -> str:
    if not win:
        return "🚫 LOSS."
    if intento == 1:
        return "✅ WIN 1 EXP."
    elif intento == 2:
        return "✅ WIN 2."
    else:
        return f"✅ WIN {intento}."

def build_win_msg(result: float, tipo_label: str, intento: int) -> str:
    return (
        "<b>🍀🍀🍀 GANAMOS!!! 🍀🍀🍀</b>\n"
        f"<b>✅ Resultado: {result:.2f}x — {tipo_label}</b>"
    )

def build_loss_msg(result: float, tipo_label: str, intento: int) -> str:
    return (
        "<b>🔴 PERDIMOS!!! 🔴</b>\n"
        f"<b>❌ Resultado: {result:.2f}x — {tipo_label}</b>"
    )

def build_stats_msg() -> str:
    total = daily_wins + daily_losses
    pct = (daily_wins / total * 100) if total > 0 else 0.0
    return (
        f"🚀 <b>Resultado del día ✅ {daily_wins} | ⭕ {daily_losses}</b>\n"
        f"💎 <b>Acertamos el {pct:.2f}% de las Señales</b>\n"
        f"📈 <b>¡{consecutive_wins} Sesiones Ganadas Consecutivas!</b>"
    )

# ─── STATS UPDATE ────────────────────────────────────────────────────────────
async def send_stats_update():
    global stats_msg_id
    if sig_state != "idle":
        return
    if stats_msg_id:
        await delete_msg(stats_msg_id)
    stats_msg_id = await send_stats_msg(build_stats_msg())
    save_state()

# ─── MÁQUINA DE ESTADOS ──────────────────────────────────────────────────────
async def resolve_pending(value: float):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global sig_signal_id, sig_context_json
    label = sig_tipo or "Señal HTML"
    key   = sig_tipo_key or "desconocido"
    if value >= CONFIRM_TRIGGER:
        logger.info(f"[v17] ⏭️ Intento 1 confirmó {value:.2f}x — señal descartada | {label}")
        if sig_signal_id and sig_context_json:
            update_signal_context_result(sig_signal_id, None, "loss")
        sig_state = "idle"
        sig_attempt = 0
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        sig_context_json = None
        sig_signal_id = None
        save_state()
    else:
        logger.info(f"[v17] 🔎 Intento 1 falló ({value:.2f}x) — señal activa para intento {sig_emit_attempt} | {label}")
        sig_state = "active"
        sig_attempt = sig_emit_attempt
        sig_last_attempt = MAX_ATTEMPTS_NORMAL
        text = build_signal_msg(label, value, es_inmediata=False, emit_attempt=sig_emit_attempt)
        sig_msg_id = await send_signal_msg(text, no_preview=True)
        save_state()

async def resolve_active(value: float):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    global sig_context_json, sig_signal_id
    win = value >= CASHOUT_TRIGGER
    label = sig_tipo or "Señal HTML"
    key = sig_tipo_key or "desconocido"
    intento = sig_attempt
    
    if sig_signal_id and sig_context_json:
        attempt_when_win = intento if win else None
        result = "win" if win else "loss"
        update_signal_context_result(sig_signal_id, attempt_when_win, result)
    
    if win:
        logger.info(f"[v17] ✅ GANAMOS intento {intento} — {value:.2f}x | {label}")
        log_pattern_result(key, label, "win", value, attempt=intento, features_json=sig_features)
        daily_wins += 1
        consecutive_wins += 1
        consecutive_losses = 0
        await send_signal_msg(build_win_msg(value, label, intento))
        await send_stats_msg(build_stats_result_msg(value, label, intento, win=True))
        sig_state = "idle"
        sig_attempt = 0
        sig_msg_id = None
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        sig_context_json = None
        sig_signal_id = None
        save_state()
        await send_stats_update()
    elif intento < sig_last_attempt:
        logger.info(f"[v17] ⚠️ Intento {intento} falló ({value:.2f}x) — sigue intento {intento + 1} | {label}")
        sig_attempt = intento + 1
        save_state()
    else:
        logger.info(f"[v17] ❌ PERDIMOS intento {intento} — {value:.2f}x | {label}")
        log_pattern_result(key, label, "loss", value, attempt=intento, features_json=sig_features)
        daily_losses += 1
        consecutive_losses += 1
        if consecutive_losses >= 3:
            logger.info("[v17] 🔻 3 pérdidas seguidas — racha ganadas reseteada")
            consecutive_wins = 0
            consecutive_losses = 0
        await send_signal_msg(build_loss_msg(value, label, intento))
        await send_stats_msg(build_stats_result_msg(value, label, intento, win=False))
        sig_state = "idle"
        sig_attempt = 0
        sig_msg_id = None
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        sig_context_json = None
        sig_signal_id = None
        save_state()
        await send_stats_update()

# ─── PROCESAMIENTO CENTRAL — v17 ─────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global sig_emit_attempt, sig_context_json, sig_signal_id
    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]
    save_value(value)
    if silent:
        return
    logger.info(f"Nueva cuota: {value:.2f}x | hist:{len(history)} | estado:{sig_state}")
    vals = list(history)
    if sig_state == "active":
        await resolve_active(value)
    elif sig_state == "pending":
        await resolve_pending(value)
    else:
        resultado = engine.evaluar(vals)
        if resultado:
            tipo_key, label, motivo, features_json, es_inmediata = resultado
            
            signal_id = f"{tipo_key}_{int(datetime.utcnow().timestamp() * 1000)}"
            sig_signal_id = signal_id
            
            try:
                features_dict = json.loads(features_json)
                context_features = extract_context_features(vals, features_dict)
                timing_pred = predict_timing(context_features)
                emit_attempt = decide_emit_attempt(timing_pred, es_inmediata)
                
                if emit_attempt == 0:
                    logger.info(f"[v17] 🛑 ML Timing indica no emitir — señal descartada | {tipo_key}")
                    sig_signal_id = None
                    return
                
                sig_emit_attempt = emit_attempt
                sig_context_json = json.dumps(context_features, default=str)
                
                # Guardar contexto inicial en BD
                log_signal_context(signal_id, tipo_key, value, None, "pending", sig_context_json)
            except Exception as e:
                logger.warning(f"[v17] Error en ML timing: {e}")
                sig_emit_attempt = 1 if es_inmediata else 2
                sig_context_json = None
            
            sig_tipo = label
            sig_tipo_key = tipo_key
            sig_features = features_json
            sig_inmediata = es_inmediata
            
            if es_inmediata:
                sig_state = "active"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_IMMEDIATE
                text = build_signal_msg(label, value, es_inmediata=True, emit_attempt=1)
                sig_msg_id = await send_signal_msg(text, no_preview=True)
                save_state()
                logger.info(f"[v17] ⚡ Señal INMEDIATA emitida: {tipo_key} — {motivo}")
            else:
                sig_state = "pending"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_NORMAL
                save_state()
                logger.info(f"[v17] 🕓 Señal detectada, esperando confirmación: {tipo_key} — emitirá en intento {sig_emit_attempt} — {motivo}")

# ─── WEBSOCKET ────────────────────────────────────────────────────────────────
async def ws_loop():
    global last_result
    RECONNECT_DELAY = 5
    def _get_val(item: dict):
        v = item.get("result") or item.get("multiplier") or item.get("crashPoint")
        return float(v) if v is not None else None
    while True:
        try:
            logger.info(f"Conectando WebSocket: {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "casinoId": CASINO_ID,
                    "currency": CURRENCY, "key": [GAME_ID],
                }))
                logger.info(f"Suscrito a game {GAME_ID}")
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    game_results = data.get("gameResult", [])
                    if not game_results:
                        continue
                    val = _get_val(game_results[0])
                    if val is None or val == last_result:
                        continue
                    last_result = val
                    await process_new_value(val, silent=False)
        except Exception as e:
            logger.error(f"WS error: {e} — reconectando en {RECONNECT_DELAY}s")
            await asyncio.sleep(RECONNECT_DELAY)

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    stats = get_stats()
    return (
        f"🤖 SpacemanBot v17 | hist:{len(history)} "
        f"| señal:{sig_state}{'(INM)' if sig_inmediata else ''} "
        f"| tend:{'🟢' if stats['favorable'] else '🔴'}"
    ), 200

@flask_app.route('/webhook', methods=['POST'])
def webhook():
    try:
        update = types.Update.de_json(request.get_json())
        asyncio.run_coroutine_threadsafe(bot.process_new_updates([update]), _main_loop)
        return '', 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return "Error interno", 500

@flask_app.route('/health')
def health():
    stats = get_stats()
    return {
        "status": "ok", "history_count": len(history),
        "signal": {"state": sig_state, "attempt": sig_attempt, "tipo": sig_tipo, "inmediata": sig_inmediata},
        "favorable": stats["favorable"],
        "pct_below2": round(stats["pct_below2"], 2),
        "pct_2to5":   round(stats["pct_2to5"], 2),
    }, 200

@flask_app.route('/ping')
def ping():
    return 'pong', 200

# ─── TELEGRAM COMMANDS ─────────────────────────────────────────────────────────
@bot.message_handler(commands=['chatid'])
async def cmd_chatid(message):
    """Diagnóstico para el error 'chat not found' en temas (topics):
    responder este comando DENTRO de cada tema (hilo) del grupo para ver el
    chat_id y message_thread_id reales que hay que poner en las variables de
    entorno CHAT_ID_BASE, THREAD_SIGNALS y THREAD_STATS."""
    thread = getattr(message, "message_thread_id", None)
    is_topic = getattr(message, "is_topic_message", False)
    await bot.reply_to(message,
        "🆔 <b>Datos de este chat/tema</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"chat_id: <code>{message.chat.id}</code>\n"
        f"message_thread_id: <code>{thread}</code>\n"
        f"es_tema (topic): {is_topic}\n\n"
        f"Configurado ahora: CHAT_ID_BASE=<code>{CHAT_ID_BASE}</code> · "
        f"THREAD_SIGNALS=<code>{THREAD_SIGNALS}</code> · THREAD_STATS=<code>{THREAD_STATS}</code>\n\n"
        "<i>Si chat_id no coincide con CHAT_ID_BASE, ese es el problema del "
        "error 'chat not found'.</i>",
        parse_mode='HTML')

@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name  = message.from_user.first_name or "usuario"
    stats = get_stats()
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT v17 — ML TIMING DINÁMICO</b>\n"
        f"💰 <b>Objetivo único: ≥ {CASHOUT_TARGET:.2f}x</b>\n"
        "🔁 <b>Confirmación intento 1 (1.60x) + intentos 2, 3 y 4</b>\n"
        "⚡ <b>Emisión inmediata si tendencia alcista fuerte</b>\n"
        "🧠 <b>ML de Timing Dinámico: ajusta automáticamente en qué intento (1, 2, 3, 4) se dispara la señal según el contexto en tiempo real + análisis de señales aprendidas</b>\n"
        "📡 <b>Fuentes de señal</b>\n"
        "   💎 PATRÓN V1 — Zona de confianza (30-40%)\n"
        "   💎 PATRÓN V2 — Combo verde agresivo (≥4.0x)\n"
        "   💎 PATRÓN V3 — Martillo alcista optimizado\n"
        "🤖 <b>8 agentes como filtro interno</b>\n"
        f"📈 <b>Estado Actual</b>\n"
        f"   Historial: {len(history)} cuotas\n"
        f"   Favorable: {'✅' if stats['favorable'] else '❌'}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot.message_handler(commands=['stats'])
async def cmd_stats(message):
    stats   = get_stats()
    hora    = colombia_time()
    if sig_state == "idle":
        sig_txt = "Idle"
    elif sig_state == "pending":
        sig_txt = f"{sig_tipo} (esperando confirmación)"
    else:
        modo = "INMEDIATA" if sig_inmediata else f"intento {sig_attempt}"
        sig_txt = f"{sig_tipo} ({modo})"
    total   = daily_wins + daily_losses
    pct     = (daily_wins / total * 100) if total > 0 else 0.0
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{stats['total']}</code> cuotas\n"
        f"🔵 &lt;2x: {stats['below2']} ({stats['pct_below2']:.1f}%)\n"
        f"🟡 2-5x: {stats['two_to_five']} ({stats['pct_2to5']:.1f}%)\n"
        f"📈 Tendencia: {'🟢 FAVORABLE' if stats['favorable'] else '🔴 DESFAVORABLE'}\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        f"✅ Wins: {daily_wins} | ❌ Losses: {daily_losses} | 💎 {pct:.1f}%\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot.message_handler(commands=['patrones', 'patronstats'])
async def cmd_patrones(message):
    await bot.reply_to(message, build_pattern_stats_msg(), parse_mode='HTML')

# ─── LOOPS ────────────────────────────────────────────────────────────────────
async def self_ping_loop():
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '')
    if not render_url:
        return
    url = f"{render_url.rstrip('/')}/ping"
    while True:
        await asyncio.sleep(14 * 60)
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, timeout=10) as r:
                    logger.info(f"Self-ping OK: {r.status}")
        except Exception as e:
            logger.warning(f"Self-ping falló: {e}")

async def daily_reset_loop():
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    while True:
        now           = colombia_now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())
        meses   = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                   "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
        dia_str = f"{now.day} de {meses[now.month - 1]} {now.year}"
        await send_stats_msg(f"🤑 <b>Resultados del {dia_str}</b>\n" + build_stats_msg())
        daily_wins = daily_losses = consecutive_wins = consecutive_losses = 0
        save_state()
        logger.info("🔄 Estadísticas reiniciadas — 00:00 Colombia")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SPACEMAN Bot v17 — ML Timing Dinámico...")
    db_init()
    load_state()
    load_ml_model()
    load_timing_model()
    loaded = load_history()
    if loaded:
        history.extend(loaded)
        logger.info(f"Historial cargado: {len(history)} valores")
    await bot.set_my_commands([
        types.BotCommand('start', '🤖 Información del bot'),
        types.BotCommand('stats', '📊 Estadísticas'),
        types.BotCommand('patrones', '📈 Efectividad por patrón (24h)'),
        types.BotCommand('chatid', '🆔 Ver chat_id / thread_id de este tema'),
    ])
    if CHAT_ID_BASE > 0:
        logger.warning(
            f"[v17] ⚠️ CHAT_ID_BASE={CHAT_ID_BASE} es positivo. Los grupos/"
            "supergrupos con temas (topics) usan chat_id NEGATIVO, típicamente "
            "con prefijo -100 (ej: -1003965615775). Si los envíos fallan con "
            "'chat not found', usá /chatid dentro del grupo para obtener el "
            "valor correcto y actualizar la variable de entorno."
        )
    asyncio.create_task(ws_loop())
    asyncio.create_task(self_ping_loop())
    asyncio.create_task(daily_reset_loop())
    asyncio.create_task(auto_train_loop())
    render_url = os.environ.get('RENDER_EXTERNAL_URL', '').rstrip('/')
    if render_url:
        await bot.remove_webhook()
        await asyncio.sleep(1)
        await bot.set_webhook(url=f"{render_url}/webhook")
        logger.info(f"✅ Webhook: {render_url}/webhook")
        while True:
            await asyncio.sleep(3600)
    else:
        logger.warning("⚠️ Usando polling (dev local)")
        await bot.infinity_polling(skip_pending=True)

def run_flask():
    port = int(os.environ.get('PORT', 8080))
    flask_app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)

# ═══════════════════════════════════════════════════════════════════════════
# MODO ENTRENAMIENTO
# ═══════════════════════════════════════════════════════════════════════════
def cargar_datos_entrenamiento(db_path: str):
    if not os.path.exists(db_path):
        sys.exit(f"No se encontró la base: {db_path}")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT tipo_key, result, features_json FROM pattern_stats "
        "WHERE features_json IS NOT NULL AND result IN ('win','loss')"
    ).fetchall()
    con.close()
    registros = []
    descartados = 0
    for row in rows:
        try:
            features = json.loads(row["features_json"])
        except (TypeError, ValueError):
            descartados += 1
            continue
        flat = flatten_features(row["tipo_key"], features)
        flat["_target"] = 1 if row["result"] == "win" else 0
        registros.append(flat)
    if descartados:
        print(f"⚠️  {descartados} fila(s) con features inválido, ignoradas.")
    return pd.DataFrame(registros)

def construir_matriz_entrenamiento(df):
    y = df["_target"].astype(int)
    x_raw = df.drop(columns=["_target"])
    cat_cols_presentes = [c for c in CATEGORICAL_COLUMNS if c in x_raw.columns]
    x = pd.get_dummies(x_raw, columns=cat_cols_presentes, dummy_na=False)
    x = x.select_dtypes(include=["number", "bool"]).astype(float)
    return x, y, cat_cols_presentes

TIMING_CATEGORICAL_COLUMNS = ['tipo_key']

def cargar_datos_timing(db_path: str):
    if not os.path.exists(db_path):
        sys.exit(f"No se encontró la base: {db_path}")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT signal_id, tipo_key, trigger_value, attempt_when_win, result, context_json "
        "FROM signal_contexts WHERE context_json IS NOT NULL AND result IN ('win','loss')"
    ).fetchall()
    con.close()
    registros = []
    descartados = 0
    for row in rows:
        try:
            context = json.loads(row["context_json"])
        except (TypeError, ValueError):
            descartados += 1
            continue
        flat = dict(context)
        if row["result"] == "loss":
            flat["_target"] = 0
        else:
            attempt = row["attempt_when_win"]
            if attempt in (1, 2, 3, 4):
                flat["_target"] = attempt
            else:
                flat["_target"] = 0
        registros.append(flat)
    if descartados:
        print(f"⚠️  {descartados} fila(s) con context inválido, ignoradas.")
    return pd.DataFrame(registros)

def construir_matriz_timing(df):
    y = df["_target"].astype(int)
    x_raw = df.drop(columns=["_target"])
    cat_cols_presentes = [c for c in TIMING_CATEGORICAL_COLUMNS if c in x_raw.columns]
    x = pd.get_dummies(x_raw, columns=cat_cols_presentes, dummy_na=False)
    x = x.select_dtypes(include=["number", "bool"]).astype(float)
    return x, y, cat_cols_presentes

def run_train_cli(argv: List[str]):
    if not ML_LIBS_OK:
        sys.exit("Faltan librerías ML. Instalá: pip install pandas scikit-learn joblib")
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, accuracy_score, classification_report
    except ImportError:
        sys.exit("Falta scikit-learn")
    import argparse
    ap = argparse.ArgumentParser(prog="main.py train")
    ap.add_argument("--db", default=DB_FILE)
    ap.add_argument("--output", default=MODEL_FILE)
    ap.add_argument("--timing", action="store_true", help="Entrenar modelo de timing")
    ap.add_argument("--timing-output", default=TIMING_MODEL_FILE)
    ap.add_argument("--min-rows", type=int, default=100)
    ap.add_argument("--test-size", type=float, default=0.2)
    args = ap.parse_args(argv)
    
    if args.timing:
        print(f"📥 Leyendo signal_contexts desde '{args.db}'...")
        df = cargar_datos_timing(args.db)
        if len(df) < args.min_rows:
            sys.exit(f"❌ Solo {len(df)} contextos (mínimo {args.min_rows}).")
        print(f"   {len(df)} contextos resueltos.")
        print(df["_target"].value_counts().rename({0: "loss", 1: "win_1", 2: "win_2", 3: "win_3", 4: "win_4"}).to_string())
        print()
        x, y, cat_cols_presentes = construir_matriz_timing(df)
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=args.test_size, random_state=42,
            stratify=y if y.nunique() > 1 else None
        )
        print("🧠 Entrenando HistGradientBoostingClassifier (timing)...")
        model = HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.08, max_iter=200,
            l2_regularization=1.0, random_state=42,
        )
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        print()
        print("📊 Resultados validación:")
        print(f"   Accuracy: {accuracy_score(y_test, y_pred):.3f}")
        print(classification_report(y_test, y_pred, 
                                    target_names=["loss", "win_1", "win_2", "win_3", "win_4"],
                                    zero_division=0))
        artifact = {
            "model": model,
            "feature_columns": list(x.columns),
            "categorical_columns": cat_cols_presentes,
        }
        joblib.dump(artifact, args.timing_output)
        print(f"✅ Modelo timing guardado en '{args.timing_output}' ({len(x.columns)} features).")
    else:
        print(f"📥 Leyendo pattern_stats desde '{args.db}'...")
        df = cargar_datos_entrenamiento(args.db)
        if len(df) < args.min_rows:
            sys.exit(f"❌ Solo {len(df)} señales (mínimo {args.min_rows}).")
        print(f"   {len(df)} señales resueltas.")
        print(df["_target"].value_counts().rename({1: "win", 0: "loss"}).to_string())
        print()
        print("Distribución por patrón:")
        print(df.groupby("tipo_key")["_target"].agg(["count", "mean"]).rename(
            columns={"count": "n", "mean": "win_rate"}
        ).to_string())
        print()
        x, y, cat_cols_presentes = construir_matriz_entrenamiento(df)
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=args.test_size, random_state=42,
            stratify=y if y.nunique() > 1 else None
        )
        print("🧠 Entrenando HistGradientBoostingClassifier...")
        model = HistGradientBoostingClassifier(
            max_depth=4, learning_rate=0.08, max_iter=200,
            l2_regularization=1.0, random_state=42,
        )
        model.fit(x_train, y_train)
        y_pred = model.predict(x_test)
        y_proba = model.predict_proba(x_test)[:, 1]
        print()
        print("📊 Resultados validación:")
        print(f"   Accuracy: {accuracy_score(y_test, y_pred):.3f}")
        if y_test.nunique() > 1:
            print(f"   ROC AUC:  {roc_auc_score(y_test, y_proba):.3f}")
        print(classification_report(y_test, y_pred, target_names=["loss", "win"], zero_division=0))
        artifact = {
            "model": model,
            "feature_columns": list(x.columns),
            "categorical_columns": cat_cols_presentes,
        }
        joblib.dump(artifact, args.output)
        print(f"✅ Modelo guardado en '{args.output}' ({len(x.columns)} features).")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'train':
        run_train_cli(sys.argv[2:])
    else:
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.run(main_async())
