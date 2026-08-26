#!/usr/bin/env python3
"""
SPACEMAN HTML Strategy Bot — Telegram + Render  [v14 — agentes optimizados anti-ruido]
────────────────────────────────────────────────────────────────────────────
Estrategia: 3 patrones optimizados + sistema de agentes mejorado:
  • PATRÓN V1 💎 (video): zona de confianza 30-40% + filtros
  • PATRÓN V2 💎 (combo_verde_agresiva): combo verde + umbral 4.0x + filtros
  • PATRÓN V3 💎 (martillo): rebote martillo + filtros de profundidad
+ Sistema de 8 agentes con SCORE PONDERADO (anti-ruido):
  - Score continuo -100 a +100 en lugar de votos binarios
  - Pesos por confiabilidad de cada agente
  - Filtro de coherencia (descarta si muy divididos)
  - Confirmación temporal (2 cuotas seguidas)
  - Filtro de volatilidad (descarta mercados extremos)
+ Emisión inmediata cuando tendencia es CLARAMENTE ALCISTA FUERTE
Mecánica de confirmación: intento 1 silencioso a 1.60x. Si falla, se envía
señal para intento 2 y 3 (gana si cualquiera >= 2.00x).
Emisión inmediata: 2 intentos directos sin confirmación previa.
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

# ─── CUOTA ÚNICA ───────────────────
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

# ─── NUEVOS UMBRALES OPTIMIZADOS (v14) ──────────────────────────────────────
CONF_ALERTA_MIN        = 30
CONF_ALERTA_MAX        = 40
UMBRAL_AGRESIVO_V2     = 4.0
UMBRAL_VALLE_V3        = 1.5
DIST_MIN_EMA8_V3       = 1.05
DIST_MAX_EMA8_V3       = 1.50

# ─── NUEVOS UMBRALES ANTI-RUIDO (v14) ───────────────────────────────────────
SCORE_UMBRAL_EMISION   = 35   # score ponderado mínimo para emitir señal
SCORE_UMBRAL_INMEDIATA = 45   # score mínimo para emisión inmediata
COHERENCIA_MIN_DIFF    = 2    # diferencia mínima entre entrar/no_entrar
CONFIRMACION_MIN_CUOTAS = 2   # cuotas consecutivas para confirmar señal
VOLATILIDAD_STD_MIN    = 0.3  # std mínimo (mercado muy plano = ruido)
VOLATILIDAD_STD_MAX    = 3.0  # std máximo (mercado muy volátil = ruido)

SOPORTE_COOLDOWN       = 5
MIN_DATOS_ENTRE_TOQUES = 2
TOQUES_NECESARIOS      = 3
RANGO_KEYS = ['muyBajo', 'bajo', 'medio', 'medioAlto', 'alto']

# ─── MAPEO DE PATRONES ──────────────────────────────────────────────────────
ALERT_LABELS = {
    'video':                'PATRON V1 💎',
    'combo_verde_agresiva': 'PATRON V2 💎',
    'martillo':             'PATRON V3 💎',
}

PATTERN_ORDER = ['video', 'combo_verde_agresiva', 'martillo']

# ─── PESOS DE AGENTES (v14 — confiabilidad por agente) ──────────────────────
AGENT_WEIGHTS = {
    'a1': 1.2,  # EMAs (tendencia)
    'a2': 1.5,  # S/R (soporte/resistencia)
    'a3': 0.8,  # Historial (menos confiable)
    'a4': 1.3,  # RSI+MACD (indicadores técnicos)
    'a5': 1.0,  # Estadístico
    'a6': 1.1,  # Bloques
    'a7': 0.7,  # Rachas (redundante, menos peso)
    'a8': 1.2,  # Fuerza
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
        "candidato_pendiente_key": candidato_pendiente_key or "",
        "candidato_pendiente_idx": str(candidato_pendiente_idx),
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global sig_inmediata, stats_msg_id
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses, ml_last_trained_count
    global candidato_pendiente_key, candidato_pendiente_idx
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
    candidato_pendiente_key = d.get("candidato_pendiente_key", "") or None
    candidato_pendiente_idx = int(d.get("candidato_pendiente_idx", "-999") or "-999")
    logger.info(f"[v14] Estado cargado | estado={sig_state} intento={sig_attempt} tipo={sig_tipo} inmediata={sig_inmediata}")

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

# ─── CONFIRMACIÓN TEMPORAL (v14 — anti-ruido) ─────────────────────────────
candidato_pendiente_key: Optional[str] = None
candidato_pendiente_idx: int = -999

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
        logger.warning(f"[v14] send error: {e}")
        return None

async def edit_msg(msg_id: int, text: str, no_preview: bool = False) -> bool:
    try:
        await bot.edit_message_text(
            text, CHAT_ID, msg_id, parse_mode="HTML",
            disable_web_page_preview=no_preview
        )
        return True
    except Exception as e:
        logger.debug(f"[v14] edit error {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[v14] delete error {msg_id}: {e}")
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
# FILTROS OPTIMIZADOS v14 — para V1, V2, V3
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
# FILTRO DE VOLATILIDAD (v14 — anti-ruido)
# ═══════════════════════════════════════════════════════════════════════════

def filtro_volatilidad(vals: List[float]) -> bool:
    """
    Descarta señales en mercados extremos:
      - Muy plano (std < 0.3): agentes fallan
      - Muy volátil (std > 3.0): demasiado ruido
    """
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
# EMISIÓN INMEDIATA — condiciones detalladas
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
# SISTEMA DE 8 AGENTES — SCORE PONDERADO (v14 — anti-ruido)
# ═══════════════════════════════════════════════════════════════════════════

def ejecutar_multiagente(vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib,
                         sr_strong, stats, agente6, racha_rango_activa,
                         rango_activo, rangos_racha, fuerza, ia_prob):
    """
    v14: Score continuo ponderado (-100 a +100) en lugar de votos binarios.
    Cada agente tiene un peso según su confiabilidad histórica.
    """
    current = niveles[-1]
    scores = {}
    
    # Agente 1: EMAs (peso 1.2)
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
    
    # Agente 2: S/R (peso 1.5)
    s2 = 0
    if sr_strong.get('soporte') and abs(current - sr_strong['soporte']) <= 1.0:
        s2 += 4
    elif sr_strong.get('resistencia') and abs(current - sr_strong['resistencia']) <= 1.0:
        s2 -= 4
    scores['a2'] = max(-5, min(5, s2))
    
    # Agente 3: Historial (peso 0.8)
    s3 = 0
    if len(vals) >= 10:
        last10 = vals[-10:]
        altos = sum(1 for v in last10 if v >= 2.0)
        if altos >= 6: s3 += 3
        elif altos <= 2: s3 -= 3
    scores['a3'] = max(-5, min(5, s3))
    
    # Agente 4: RSI+MACD (peso 1.3)
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
    
    # Agente 5: Estadístico (peso 1.0)
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
    
    # Agente 6: Bloques (peso 1.1)
    s6 = 0
    if agente6:
        if agente6['tipo'] == 'segura': s6 += 4
        elif agente6['tipo'] == 'moderada': s6 += 2
        elif agente6['tipo'] == 'esperar': s6 -= 3
    scores['a6'] = max(-5, min(5, s6))
    
    # Agente 7: Rachas (peso 0.7)
    s7 = 0
    if racha_rango_activa and rango_activo:
        racha = rangos_racha.get(rango_activo, 0)
        if rango_activo in ('muyBajo', 'bajo') and racha >= 4: s7 += 3
        elif rango_activo == 'medio': s7 += 2
        elif rango_activo in ('medioAlto', 'alto'): s7 -= 2
    scores['a7'] = max(-5, min(5, s7))
    
    # Agente 8: Fuerza (peso 1.2)
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
    """
    v14: Bloquea señal si:
      - Score ponderado < 35%
      - Filtro de coherencia: diferencia <= 1 (muy divididos)
    """
    # Filtro de coherencia
    diff_abs = abs(contar_entrar - contar_no_entrar)
    if diff_abs < COHERENCIA_MIN_DIFF:
        return True
    # Score mínimo
    if score_pct < SCORE_UMBRAL_EMISION:
        return True
    return False

def agentes_permiten_emision_inmediata(score_pct, contar_entrar, contar_no_entrar) -> bool:
    """
    v14: Permite emisión inmediata si:
      - Score ponderado >= 45%
      - Filtro de coherencia: diferencia >= 2
    """
    diff_abs = abs(contar_entrar - contar_no_entrar)
    if diff_abs < COHERENCIA_MIN_DIFF:
        return False
    if score_pct < SCORE_UMBRAL_INMEDIATA:
        return False
    return True

# ═══════════════════════════════════════════════════════════════════════════
# MOTOR DE ESTRATEGIA v14
# ═══════════════════════════════════════════════════════════════════════════

class HtmlEngine:
    def __init__(self):
        self.tendencia_estado = 'ROJO'
        self.last_video_idx   = -999
        self.last_combo_idx   = -999
        self.last_martillo_idx = -999
        self.fuerza_memoria   = []

    def evaluar(self, vals: List[float]):
        """
        v14: Con confirmación temporal (2 cuotas seguidas) + filtro de volatilidad.
        Devuelve (tipo_key, label, motivo, features_json, es_inmediata)
        """
        if len(vals) < 3:
            return None

        # Filtro de volatilidad
        if not filtro_volatilidad(vals):
            logger.debug("[v14] 🛑 Volatilidad extrema — no evaluar")
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

        if agentes_bloquean_señal(score_pct, contar_entrar, contar_no_entrar):
            logger.info(f"[v14] 🛑 Agentes bloquean: score={score_pct:.1f}% diff={abs(contar_entrar-contar_no_entrar)}")
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

        # Confirmación temporal (v14)
        global candidato_pendiente_key, candidato_pendiente_idx
        if candidato_pendiente_key == tipo_key and idx - candidato_pendiente_idx == 1:
            # ✅ Confirmado por 2 cuotas seguidas
            candidato_pendiente_key = None
            candidato_pendiente_idx = -999
        else:
            # Guardar como pendiente
            candidato_pendiente_key = tipo_key
            candidato_pendiente_idx = idx
            return None  # aún no emitir

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
                    logger.info(f"[v14] 🚀 Emisión inmediata: {tipo_key} | score={score_pct:.1f}% opc={opc_cumplidas}/3")

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
# ML — FEATURIZACIÓN
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
    flat.pop('scores', None)  # no se usa como feature
    flat['tipo_key'] = tipo_key
    return flat

# ─── ML — INFERENCIA ──────────────────────────────────────────────────────────
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

# ─── ML — AUTO-ENTRENAMIENTO ──────────────────────────────────────────────────
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

async def auto_train_loop():
    global ml_model, ml_feature_columns, ml_categorical_columns, ml_last_trained_count
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
            if not (es_primer_entrenamiento or necesita_reentrenar):
                continue
            logger.info(f"[ML] 🧠 Auto-entrenamiento ({total} señales)...")
            loop = asyncio.get_running_loop()
            ok, artifact, msg = await loop.run_in_executor(None, train_model_in_thread, AUTO_TRAIN_MIN_ROWS)
            if not ok:
                logger.info(f"[ML] Auto-entrenamiento pospuesto: {msg}")
                continue
            joblib.dump(artifact, MODEL_FILE)
            ml_model = artifact["model"]
            ml_feature_columns = artifact["feature_columns"]
            ml_categorical_columns = artifact["categorical_columns"]
            ml_last_trained_count = total
            save_state()
            logger.info(f"[ML] ✅ Modelo actualizado: {msg}")
        except Exception as e:
            logger.warning(f"[ML] Error en auto-entrenamiento: {e}")

# ─── MENSAJES ─────────────────────────────────────────────────────────────────
def build_signal_msg(tipo_label: str, last_value: float, es_inmediata: bool = False) -> str:
    if es_inmediata:
        header = "<b>⚡⚡ ENTRADA INMEDIATA ⚡⚡</b>"
    else:
        header = "<b>✅✅ ENTRADA CONFIRMADA ✅✅</b>"
    return (
        f"{header}\n\n"
        f"<b>🧠 {tipo_label}</b>\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x</b>\n"
        f"<i>💫 ¡Juegue con Responsabilidad!</i>\n"
        f'🎰 <a href="{GAME_LINK}">Acceder al Spaceman</a>'
    )

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
    stats_msg_id = await send_msg(build_stats_msg())
    save_state()

# ─── MÁQUINA DE ESTADOS ──────────────────────────────────────────────────────
async def resolve_pending(value: float):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    label = sig_tipo or "Señal HTML"
    key   = sig_tipo_key or "desconocido"
    if value >= CONFIRM_TRIGGER:
        logger.info(f"[v14] ⏭️ Intento 1 confirmó {value:.2f}x — señal descartada | {label}")
        sig_state = "idle"
        sig_attempt = 0
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        save_state()
    else:
        logger.info(f"[v14] 🔎 Intento 1 falló ({value:.2f}x) — señal activa | {label}")
        sig_state = "active"
        sig_attempt = 2
        sig_last_attempt = MAX_ATTEMPTS_NORMAL
        text = build_signal_msg(label, value, es_inmediata=False)
        sig_msg_id = await send_msg(text, no_preview=True)
        save_state()

async def resolve_active(value: float):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    win = value >= CASHOUT_TRIGGER
    label = sig_tipo or "Señal HTML"
    key = sig_tipo_key or "desconocido"
    intento = sig_attempt
    if win:
        logger.info(f"[v14] ✅ GANAMOS intento {intento} — {value:.2f}x | {label}")
        log_pattern_result(key, label, "win", value, attempt=intento, features_json=sig_features)
        daily_wins += 1
        consecutive_wins += 1
        consecutive_losses = 0
        await send_msg(build_win_msg(value, label, intento))
        sig_state = "idle"
        sig_attempt = 0
        sig_msg_id = None
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        save_state()
        await send_stats_update()
    elif intento < sig_last_attempt:
        logger.info(f"[v14] ⚠️ Intento {intento} falló ({value:.2f}x) — sigue intento {intento + 1} | {label}")
        sig_attempt = intento + 1
        save_state()
    else:
        logger.info(f"[v14] ❌ PERDIMOS intento {intento} — {value:.2f}x | {label}")
        log_pattern_result(key, label, "loss", value, attempt=intento, features_json=sig_features)
        daily_losses += 1
        consecutive_losses += 1
        if consecutive_losses >= 3:
            logger.info("[v14] 🔻 3 pérdidas seguidas — racha reseteada")
            consecutive_wins = 0
            consecutive_losses = 0
        await send_msg(build_loss_msg(value, label, intento))
        sig_state = "idle"
        sig_attempt = 0
        sig_msg_id = None
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        sig_inmediata = False
        save_state()
        await send_stats_update()

# ─── PROCESAMIENTO CENTRAL ────────────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
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
            sig_tipo = label
            sig_tipo_key = tipo_key
            sig_features = features_json
            sig_inmediata = es_inmediata
            if es_inmediata:
                sig_state = "active"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_IMMEDIATE
                text = build_signal_msg(label, value, es_inmediata=True)
                sig_msg_id = await send_msg(text, no_preview=True)
                save_state()
                logger.info(f"[v14] ⚡ Señal INMEDIATA: {tipo_key} — {motivo}")
            else:
                sig_state = "pending"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_NORMAL
                save_state()
                logger.info(f"[v14] 🕓 Señal confirmada: {tipo_key} — {motivo}")

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
        f"🤖 SpacemanBot v14 | hist:{len(history)} "
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
@bot.message_handler(commands=['start'])
async def cmd_start(message):
    name  = message.from_user.first_name or "usuario"
    stats = get_stats()
    await bot.reply_to(message,
        f"🚀 <b>¡Bienvenido {name}!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT v14 — ANTI-RUIDO</b>\n"
        f"💰 <b>Objetivo único: ≥ {CASHOUT_TARGET:.2f}x</b>\n"
        "🔁 <b>Confirmación intento 1 (1.60x) + intentos 2 y 3</b>\n"
        "⚡ <b>Emisión inmediata si tendencia alcista fuerte</b>\n"
        "📡 <b>Fuentes de señal</b>\n"
        "   💎 PATRÓN V1 — Zona de confianza (30-40%)\n"
        "   💎 PATRÓN V2 — Combo verde agresivo (≥4.0x)\n"
        "   💎 PATRÓN V3 — Martillo alcista optimizado\n"
        "🤖 <b>8 agentes con SCORE PONDERADO</b>\n"
        "   🎯 Filtro de coherencia + volatilidad\n"
        "   ✅ Confirmación temporal (2 cuotas)\n"
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
        modo = "INMEDIATA" if sig_inmediata else "normal"
        sig_txt = f"{sig_tipo} (intento {sig_attempt}, {modo})"
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
        await send_msg(f"🤑 <b>Resultados del {dia_str}</b>\n" + build_stats_msg())
        daily_wins = daily_losses = consecutive_wins = consecutive_losses = 0
        save_state()
        logger.info("🔄 Estadísticas reiniciadas — 00:00 Colombia")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SPACEMAN Bot v14 — agentes optimizados anti-ruido...")
    db_init()
    load_state()
    load_ml_model()
    loaded = load_history()
    if loaded:
        history.extend(loaded)
        logger.info(f"Historial cargado: {len(history)} valores")
    await bot.set_my_commands([
        types.BotCommand('start', '🤖 Información del bot'),
        types.BotCommand('stats', '📊 Estadísticas'),
        types.BotCommand('patrones', '📈 Efectividad por patrón (24h)'),
    ])
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
    ap.add_argument("--min-rows", type=int, default=100)
    ap.add_argument("--test-size", type=float, default=0.2)
    args = ap.parse_args(argv)
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
