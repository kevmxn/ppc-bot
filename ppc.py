#!/usr/bin/env python3
"""
SPACEMAN HTML Strategy Bot — Telegram + Render  [v12 — sistema HTML + Agentes]
────────────────────────────────────────────────────────────────────────────
Estrategia: réplica fiel del panel HTML (index_2.html), + patrones nuevos
(Martillo, Tendencia media alcista, Combo Tendencia VERDE + Lucky Agresiva).

Mecánica de confirmación: al detectar señal se espera 1 cuota de
confirmación (intento 1, NO se envía a Telegram). Si esa cuota >= 1.65x, la
señal se descarta. Si no, se envía la señal a Telegram para intento 2 y 3
(gana si cualquiera de los dos alcanza >= 2.00x).

Fuentes de señal (cualquiera dispara, se procesan en este orden):
  1) Combo Tendencia VERDE + Lucky Agresiva (combo_verde_agresiva)
  2) Patrón zona de confianza (video)     (video)
  3) Tendencia media alcista (EMA8)       (tendencia_media_alcista)
  4) Patrón Martillo alcista              (martillo)
  5) Sistema de Agentes (IA):             (agent_*)
     MOMENTUM · POSIBLE ENTRADA
     (bloqueado si riskScore >= 7 o mayoría de agentes vota NO_ENTRAR)

Todas las señales apuntan a la misma cuota: RETIRAR EN 2.00x (gana si el
resultado es >= 2.00x). CANAL ÚNICO: todas las señales se envían al mismo
bot/chat de Telegram. Persistencia SQLite · Reset estadísticas 00:00 Colombia
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

# ─── ML — dependencias opcionales (el bot funciona igual si no están instaladas) ──
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

# Auto-entrenamiento: reentrena y recarga el modelo solo, sin correr
# `python3 main.py train` a mano. Corre en background, no bloquea el bot.
AUTO_TRAIN_ENABLED     = os.environ.get("AUTO_TRAIN_ENABLED", "1") == "1"
AUTO_TRAIN_MIN_ROWS    = int(os.environ.get("AUTO_TRAIN_MIN_ROWS", "100"))    # mínimo para el primer entrenamiento
AUTO_TRAIN_MIN_NEW     = int(os.environ.get("AUTO_TRAIN_MIN_NEW", "30"))     # señales nuevas para volver a reentrenar
AUTO_TRAIN_INTERVAL_SEC = int(os.environ.get("AUTO_TRAIN_INTERVAL_SEC", "1800"))  # cada cuánto chequea (seg)

def colombia_now() -> datetime:
    return datetime.utcnow() - timedelta(hours=5)

def colombia_time() -> str:
    return colombia_now().strftime("%H:%M")

# ─── UMBRALES DE TENDENCIA (solo informativos, no bloquean señales) ──────────
UMBRAL_BELOW2 = 53.51
UMBRAL_2TO5   = 26.99
HISTORY_MAX   = 150

# ─── CUOTA ÚNICA (todas las señales del HTML apuntan aquí) ───────────────────
CASHOUT_TARGET  = 1.70   # valor mostrado en los mensajes ("RETIRAR EN")
CASHOUT_TRIGGER = 1.70   # umbral REAL de resolucion (intentos 2/3 e inmediatas): se gana con >= 2.00x
CONFIRM_TRIGGER = 1.60   # umbral del intento 1 de confirmación (silencioso): si lo supera, se descarta la señal

# ─── MECÁNICA DE CONFIRMACIÓN (intento 1 se omite, se juega intento 2 y 3) ───
# Al detectar señal: NO se envía a Telegram todavía. Se espera la siguiente
# cuota (intento 1, "de confirmación", silenciosa). Si esa cuota >= 1.60x,
# la señal se descarta (no se manda nada). Si esa cuota < 1.60x, se envía a
# Telegram el mensaje de señal cubriendo intento 2 y 3 (2 intentos, cuota
# objetivo 2.00x en cualquiera de los dos).

# Nº total de intentos jugables por señal (después de enviada a Telegram).
MAX_ATTEMPTS_NORMAL    = 3   # señales normales: intentos 2 y 3 (2 jugadas)
MAX_ATTEMPTS_IMMEDIATE = 2   # emisión inmediata (sin patrones activos actualmente; IMMEDIATE_PATTERNS vacío)

GAME_LINK = "https://1win.lat/casino/play/v_pragmatic:spaceman"

# ═══════════════════════════════════════════════════════════════════════════
# CONSTANTES DEL SISTEMA HTML (idénticas a index_2.html)
# ═══════════════════════════════════════════════════════════════════════════
UMBRAL_ACTIVACION      = 30   # datos mínimos para agentes 5/6/7/8, rachas, fuerza
MAX_LUCKY_GALES        = 2
COOLDOWN_VERDE         = 10
COOLDOWN_ROJO          = 6
NIVEL_SOBREVENTA       = -4
RACHA_SOBREVENTA       = 4
CONF_ALERTA_MIN        = 25
CONF_ALERTA_MAX        = 45
SOPORTE_COOLDOWN       = 5
MIN_DATOS_ENTRE_TOQUES = 2
TOQUES_NECESARIOS      = 3

RANGO_KEYS = ['muyBajo', 'bajo', 'medio', 'medioAlto', 'alto']

ALERT_LABELS = {
    'video':                'PATRON V1 💎',
    'agent_momentum':       'PATRON V2 💎',
    'agent_posible_entrada':'PATRON V3 💎',
    'combo_verde_agresiva': 'PATRON V4 💎',
    'tendencia_media_alcista': 'PATRON V5 💎',
    'martillo':              'PATRON V6 💎',
}

# Emisión inmediata: ningún patrón restante usa este modo (era exclusivo del
# patrón EMA4-soporte-sobre-EMA8, eliminado). Todos siguen el flujo normal:
# se espera que el intento 1 falle (< 1.65x) para recién enviar la señal.
IMMEDIATE_PATTERNS = set()

# Orden de presentación para el reporte de efectividad por patrón
PATTERN_ORDER = [
    'video', 'agent_momentum', 'agent_posible_entrada',
    'combo_verde_agresiva', 'tendencia_media_alcista',
    'martillo',
]

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
        pass  # la columna ya existe (bases creadas antes de este cambio)
    con.commit()
    con.close()

def _db():
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

# ─── PERSISTENCIA — ESTADO DE LA SEÑAL (sin gestion/gale/columnas) ───────────
def save_state():
    values = {
        "sig_state":        sig_state,
        "sig_attempt":      str(sig_attempt),
        "sig_last_attempt": str(sig_last_attempt),
        "sig_msg_id":       str(sig_msg_id) if sig_msg_id is not None else "",
        "sig_tipo":         sig_tipo or "",
        "sig_tipo_key":     sig_tipo_key or "",
        "sig_features":     sig_features or "",
        "stats_msg_id":     str(stats_msg_id) if stats_msg_id is not None else "",
        "daily_wins":       str(daily_wins),
        "daily_losses":     str(daily_losses),
        "consecutive_wins": str(consecutive_wins),
        "consecutive_losses": str(consecutive_losses),
        "ml_last_trained_count": str(ml_last_trained_count),
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, stats_msg_id
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
    _sid              = d.get("stats_msg_id", "")
    stats_msg_id      = int(_sid) if _sid else None
    daily_wins        = int(d.get("daily_wins", "0"))
    daily_losses      = int(d.get("daily_losses", "0"))
    consecutive_wins  = int(d.get("consecutive_wins", "0"))
    consecutive_losses = int(d.get("consecutive_losses", "0"))
    ml_last_trained_count = int(d.get("ml_last_trained_count", "0") or "0")
    logger.info(f"[HTML] Estado cargado | estado={sig_state} intento={sig_attempt} tipo={sig_tipo}")

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
    """Registra el resultado final (win/loss) de una señal, identificada por
    su patrón de origen, para poder calcular la efectividad por patrón.
    features_json guarda el vector de indicadores vigente al momento en que
    se detectó la señal, para poder entrenar un modelo de ML más adelante."""
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
    """Devuelve {tipo_key: {wins, losses, total}} para las últimas 24 horas."""
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

    # Por si aparece algún tipo_key no mapeado en PATTERN_ORDER (p.ej. legacy)
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

# sig_state: 'idle' (buscando señal) | 'pending' (esperando confirmación del
#            intento 1, sin enviar nada a Telegram) | 'active' (señal enviada,
#            jugando intento 2 o intento 3 — ver sig_attempt)
sig_state:         str           = "idle"
sig_attempt:       int           = 0
sig_last_attempt:  int           = MAX_ATTEMPTS_NORMAL   # último intento válido de la señal actual
sig_msg_id:        Optional[int] = None
sig_tipo:          Optional[str] = None
sig_tipo_key:      Optional[str] = None
sig_features:      Optional[str] = None   # JSON con el vector de features al momento de detectar la señal
stats_msg_id:      Optional[int] = None
daily_wins:        int           = 0
daily_losses:      int           = 0
consecutive_wins:  int           = 0
consecutive_losses: int          = 0   # rachas de pérdidas; a las 3 seguidas resetea consecutive_wins

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
        logger.warning(f"[HTML] send error: {e}")
        return None

async def edit_msg(msg_id: int, text: str, no_preview: bool = False) -> bool:
    try:
        await bot.edit_message_text(
            text, CHAT_ID, msg_id, parse_mode="HTML",
            disable_web_page_preview=no_preview
        )
        return True
    except Exception as e:
        logger.debug(f"[HTML] edit error {msg_id}: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID, msg_id)
        return True
    except Exception as e:
        logger.debug(f"[HTML] delete error {msg_id}: {e}")
        return False

# ─── ANÁLISIS DE TENDENCIA (solo informativo) ─────────────────────────────────
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
# INDICADORES BASE — fieles a index_2.html
# ═══════════════════════════════════════════════════════════════════════════
def compute_niveles(vals: List[float]) -> List[float]:
    """+1 si >= 2.00 · -1 si 1.00 <= v <= 1.99 (idéntico al HTML)."""
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
    """EMA continua del HTML: semilla = vals[0], sin minimo de longitud."""
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

def es_tendencia_alcista(ema4, ema8, ema20, tendencia_estado: str) -> bool:
    if len(ema4) < 2 or len(ema8) < 2 or len(ema20) < 2:
        return False
    e4, e8, e20 = ema4[-1], ema8[-1], ema20[-1]
    e4p, e8p, e20p = ema4[-2], ema8[-2], ema20[-2]
    orden_alcista = e4 > e8 and e8 > e20
    todas_subiendo = e4 > e4p and e8 > e8p and e20 > e20p
    estado_verde = tendencia_estado == 'VERDE'
    return orden_alcista and (todas_subiendo or estado_verde)

def es_tendencia_alcista_o_mixta(ema4, ema8, ema20) -> bool:
    if len(ema4) < 2 or len(ema8) < 2 or len(ema20) < 2:
        return False
    e4, e8, e20 = ema4[-1], ema8[-1], ema20[-1]
    bajista = e4 < e8 and e8 < e20
    return not bajista

def emas_subiendo_individualmente(ema4, ema8, ema20) -> bool:
    if len(ema4) < 2 or len(ema8) < 2 or len(ema20) < 2:
        return False
    return (ema4[-1] > ema4[-2] and ema8[-1] > ema8[-2] and ema20[-1] > ema20[-2])

def detectar_minimos_locales(niveles: List[float]):
    minimos = []
    n = len(niveles)
    if n < 5:
        return minimos
    inicio = max(0, n - 30)
    for i in range(inicio + 1, n - 1):
        if niveles[i] < niveles[i - 1] and niveles[i] < niveles[i + 1]:
            minimos.append((i, niveles[i]))
    return minimos

def detectar_canal_alcista(niveles: List[float]):
    minimos = detectar_minimos_locales(niveles)
    if len(minimos) < 2:
        return None
    ascendentes = []
    for idx, val in minimos:
        if not ascendentes:
            ascendentes.append((idx, val))
        else:
            if val > ascendentes[-1][1]:
                ascendentes.append((idx, val))
    if len(ascendentes) < 2:
        return None
    puntos = ascendentes[-3:]
    n = len(puntos)
    sum_x = sum_y = sum_xy = sum_x2 = 0
    for idx, val in puntos:
        sum_x += idx; sum_y += val; sum_xy += idx * val; sum_x2 += idx * idx
    denom = n * sum_x2 - sum_x * sum_x
    pendiente = 0 if denom == 0 else (n * sum_xy - sum_x * sum_y) / denom
    intercepto = (sum_y - pendiente * sum_x) / n
    if pendiente <= 0:
        return None
    last_idx = len(niveles) - 1
    linea_inferior = pendiente * last_idx + intercepto
    ventana = niveles[max(0, last_idx - 15):last_idx + 1]
    linea_superior = max(ventana)
    return {'inferior': linea_inferior, 'superior': linea_superior,
            'pendiente': pendiente, 'intercepto': intercepto}

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

def hay_sobreventa(vals: List[float], niveles: List[float], idx: int) -> bool:
    racha_bajos = 0
    i = idx
    while i >= 0 and vals[i] < 2.0:
        racha_bajos += 1
        i -= 1
    return niveles[idx] <= NIVEL_SOBREVENTA or racha_bajos >= RACHA_SOBREVENTA

def hay_pullback(vals: List[float], idx: int) -> bool:
    if idx < 1:
        return False
    ventana = vals[max(0, idx - 5):idx]
    max_prev = max(ventana) if ventana else 0
    return vals[idx] < vals[idx - 1] or vals[idx] < max_prev * 0.6

# ─── NUEVOS PATRONES (fieles a evaluarNuevasAlertas() del HTML) ─────────────
def detectar_martillo(vals: List[float], ema8: List[float]) -> bool:
    """Martillo alcista: réplica exacta de esMartillo del HTML.
    Rebote sobre un valle reciente + valor actual por encima de EMA8."""
    if len(vals) < 3 or not ema8:
        return False
    last = len(vals) - 1
    current_val, prev_val = vals[last], vals[last - 1]
    return (current_val > prev_val) and (prev_val < vals[last - 2]) and (current_val > ema8[-1])

def detectar_ema4_soporte_ema8(ema4: List[float], ema8: List[float], niveles: List[float]) -> bool:
    """Nuevo patrón: tendencia alcista de 8 periodos + EMA4 cae sobre la EMA8
    y la usa como soporte (se acerca desde arriba y rebota sin cruzar abajo)."""
    if len(ema4) < 3 or len(ema8) < 3 or len(niveles) < 8:
        return False
    ventana8 = niveles[-8:]
    slope8, _ = calcular_regresion_lineal(ventana8)
    tendencia_8_alcista = slope8 > 0.03
    e4, e4p = ema4[-1], ema4[-2]
    e8, e8p = ema8[-1], ema8[-2]
    distancia      = e4 - e8
    distancia_prev = e4p - e8p
    # EMA4 sigue por encima de EMA8 (no cruza abajo), se acercó (soporte tocado) y rebota
    tocando_soporte = 0 <= distancia <= 0.5 and distancia_prev > distancia
    return tendencia_8_alcista and tocando_soporte

def es_tendencia_media_alcista(ema8: List[float]) -> bool:
    """Tendencia media (EMA8) alcista: pendiente positiva y EMA8 subiendo."""
    if len(ema8) < 5:
        return False
    ventana = ema8[-8:] if len(ema8) >= 8 else ema8
    slope, _ = calcular_regresion_lineal(ventana)
    return slope > 0.03 and ema8[-1] > ema8[-2]

# ─── SOPORTE/RESISTENCIA · RSI · MACD · FIBONACCI ────────────────────────────
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

# ─── ESTADÍSTICAS AVANZADAS (agente 5) ───────────────────────────────────────
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

# ─── AGENTE 6 — BLOQUES / RACHAS DE BAJOS ────────────────────────────────────
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

# ─── AGENTE 7 — RACHAS DE RANGO ───────────────────────────────────────────────
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

# ─── AGENTE 8 — FUERZA / VELOCIDAD ────────────────────────────────────────────
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

def detectar_momentum(vals: List[float]) -> bool:
    if len(vals) < 3:
        return False
    v1, v2, v3 = vals[-3], vals[-2], vals[-1]
    return v1 < v2 < v3

# ═══════════════════════════════════════════════════════════════════════════
# SISTEMA DE 8 AGENTES (réplica de ejecutarMultiAgente)
# ═══════════════════════════════════════════════════════════════════════════
def ejecutar_multiagente(vals, niveles, ema4, ema8, ema20, ema50, rsi, macd, fib,
                          sr_strong, stats, agente6, racha_rango_activa,
                          rango_activo, rangos_racha, fuerza, ia_prob):
    current = niveles[-1]
    risk_score = 0
    votos: Dict[str, str] = {}

    # Agente 1 — Tendencia EMAs
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

    # Agente 2 — S/R estadístico
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

    # Agente 3 — Historial/Patrones
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
    # (patronesExitosos requiere feedback manual del panel HTML; en el bot
    #  automático no existe, por lo que ese bonus nunca aplica — matchExitoso=False)
    if a3 >= 3:
        votos['a3'] = 'ENTRAR'
    elif a3 <= 0 and risk_score > 2:
        votos['a3'] = 'NO_ENTRAR'
    else:
        votos['a3'] = 'ESPERAR'

    # Agente 4 — Indicadores técnicos
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

    # Agente 5 — Estadístico (500)
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

    # Agente 6 — Bloques (500)
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

    # Agente 7 — Rachas de rango (500)
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

    # Agente 8 — Fuerza (500)
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

def decidir_agente(contar_entrar, contar_no_entrar, risk_score, momentum_activo,
                    racha_rango_activa, rango_activo, rangos_racha, fuerza):
    if risk_score >= 7:
        return None, None
    if momentum_activo:
        return 'agent_momentum', f'¡3 alzas consecutivas! Votos: {contar_entrar}/8 a favor.'
    if contar_no_entrar >= 4 and contar_no_entrar > contar_entrar:
        return None, None
    if contar_entrar > contar_no_entrar and contar_entrar >= 3:
        return 'agent_posible_entrada', f'Votos: {contar_entrar} entrar / {contar_no_entrar} contra.'
    return None, None

# ═══════════════════════════════════════════════════════════════════════════
# MOTOR DE ESTRATEGIA — combina las alertas de patrón + el sistema de 8 agentes
# ═══════════════════════════════════════════════════════════════════════════
class HtmlEngine:
    def __init__(self):
        self.tendencia_estado       = 'ROJO'
        self.last_agres_idx         = -999
        self.last_video_idx         = -999
        self.fuerza_memoria         = []
        self.last_combo_idx         = -999
        self.last_tendencia_media_idx = -999
        self.last_martillo_idx      = -999

    def evaluar(self, vals: List[float]):
        """Devuelve (tipo_key, label, motivo, features_json) si dispara señal,
        o None. Junta TODOS los patrones que cumplen su condición en esta
        ronda (no se queda con el primero) y, si hay un modelo ML cargado,
        elige el de mayor probabilidad estimada de acierto (siempre que supere
        MODEL_MIN_PROB). Si no hay modelo, se mantiene el orden de prioridad
        original: combo > video > tendencia_media > martillo > agentes."""
        if len(vals) < 3:
            return None

        niveles = compute_niveles(vals)
        ema4  = ema_html(4, niveles)
        ema8  = ema_html(8, niveles)
        ema20 = ema_html(20, niveles)
        ema50 = ema_html(50, niveles)
        confidence = calcular_confianza(vals)
        idx = len(vals) - 1

        candidatos = []  # lista de (tipo_key, label, motivo, features)

        # ── Lucky (siempre actualiza tendencia_estado) ──
        nuevo = calcular_tendencia_lucky(vals, niveles)
        self.tendencia_estado = nuevo

        agresiva_condicion = False
        if nuevo == 'VERDE':
            max_rec = max(vals[-10:]) if vals else 0
            agresiva_condicion = max_rec >= 3.5

        # Features "base": disponibles siempre, sin importar qué patrón dispare.
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
        }

        # ── Combo: Tendencia VERDE + Lucky Agresiva al mismo tiempo ──
        if nuevo == 'VERDE' and agresiva_condicion:
            self.last_combo_idx = idx
            self.last_agres_idx = idx
            candidatos.append(('combo_verde_agresiva', ALERT_LABELS['combo_verde_agresiva'],
                                'Tendencia VERDE y Lucky Agresiva activas a la vez', dict(features_base)))

        # ── Video (zona de confianza) ──
        if len(vals) >= 5:
            conf = int(confidence)
            if CONF_ALERTA_MIN <= conf <= CONF_ALERTA_MAX:
                self.last_video_idx = idx
                candidatos.append(('video', ALERT_LABELS['video'], f'Confianza {conf}% en zona video',
                                    dict(features_base)))

        # ── Tendencia media alcista (EMA8) ──
        if es_tendencia_media_alcista(ema8):
            self.last_tendencia_media_idx = idx
            candidatos.append(('tendencia_media_alcista', ALERT_LABELS['tendencia_media_alcista'],
                                'EMA8 (tendencia media) con pendiente alcista', dict(features_base)))

        # ── Patrón Martillo de trading ──
        if detectar_martillo(vals, ema8):
            self.last_martillo_idx = idx
            candidatos.append(('martillo', ALERT_LABELS['martillo'],
                                'Rebote tipo martillo sobre valle reciente, por encima de EMA8',
                                dict(features_base)))

        # ── Sistema de 8 agentes ──
        sr_strong = calcular_soporte_resistencia_fuerte(niveles)
        rsi  = calcular_rsi(niveles)
        macd = calcular_macd(niveles)
        fib  = calcular_fibonacci(niveles)
        momentum_activo = detectar_momentum(vals)

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
        tipo_key_agente, motivo_agente = decidir_agente(
            contar_entrar, contar_no_entrar, risk_score, momentum_activo,
            racha_rango_activa, rango_activo, rangos_racha, fuerza
        )
        if tipo_key_agente:
            features_agente = dict(features_base)
            features_agente.update({
                'votos': votos,
                'contar_entrar': contar_entrar,
                'contar_no_entrar': contar_no_entrar,
                'risk_score': risk_score,
                'momentum_activo': momentum_activo,
                'rsi': rsi[-1] if rsi else None,
                'macd': macd[-1] if macd else None,
                'racha_rango_activa': racha_rango_activa,
                'rango_activo': rango_activo,
                'fuerza': fuerza,
                'ia_prob': ia_prob,
            })
            candidatos.append((tipo_key_agente, ALERT_LABELS[tipo_key_agente], motivo_agente, features_agente))

        if not candidatos:
            return None

        elegido = self._elegir_mejor(candidatos)
        if elegido is None:
            return None
        tipo_key, label, motivo, features = elegido
        return tipo_key, label, motivo, json.dumps(features, default=str)

    def _elegir_mejor(self, candidatos: list):
        """Si hay modelo ML cargado: puntúa cada candidato y devuelve el de
        mayor probabilidad, descartando los que no superen MODEL_MIN_PROB
        (puede devolver None si ninguno la supera). Si no hay modelo: respeta
        el orden de prioridad original (el primero de la lista)."""
        if ml_model is None:
            return candidatos[0]

        mejor = None
        mejor_prob = -1.0
        for cand in candidatos:
            tipo_key, label, motivo, features = cand
            prob = predict_prob(tipo_key, features)
            if prob is None:
                # No se pudo evaluar (error puntual) -> no se descarta por las
                # dudas, se lo trata como si pasara el umbral al mínimo.
                prob = MODEL_MIN_PROB
            features['ml_prob'] = round(prob, 4)
            if prob >= MODEL_MIN_PROB and prob > mejor_prob:
                mejor = cand
                mejor_prob = prob
        return mejor

engine = HtmlEngine()

# ═══════════════════════════════════════════════════════════════════════════
# ML — FEATURIZACIÓN COMPARTIDA (la usan tanto la inferencia en vivo como el
# modo `python3 main.py train`, así ambos lados codifican los datos igual)
# ═══════════════════════════════════════════════════════════════════════════
CATEGORICAL_COLUMNS = [
    'tipo_key',
    'tendencia_lucky',
    'rango_activo',
    'voto_a1', 'voto_a2', 'voto_a3', 'voto_a4',
    'voto_a5', 'voto_a6', 'voto_a7', 'voto_a8',
]

def flatten_features(tipo_key: str, features: dict) -> dict:
    """Aplana el dict de features (tal como lo guarda evaluar()/pattern_stats)
    a columnas simples aptas para un DataFrame: separa 'votos' en a1..a8 y
    'fuerza' en sus componentes numéricos."""
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

    for bool_col in ('agresiva_condicion', 'momentum_activo', 'racha_rango_activa'):
        if bool_col in flat and flat[bool_col] is not None:
            flat[bool_col] = int(bool(flat[bool_col]))

    flat.pop('ml_prob', None)  # nunca se usa como feature de entrada
    flat['tipo_key'] = tipo_key
    return flat

# ─── ML — INFERENCIA ──────────────────────────────────────────────────────────
ml_model = None
ml_feature_columns: List[str] = []
ml_categorical_columns: List[str] = []
ml_last_trained_count: int = 0   # cuántas señales resueltas había la última vez que se entrenó

def load_ml_model():
    """Carga el modelo entrenado (si existe). Si falta el archivo o faltan
    las librerías, el bot sigue funcionando con la lógica de prioridad
    original — el modelo es un filtro adicional, no un requisito."""
    global ml_model, ml_feature_columns, ml_categorical_columns
    if not ML_LIBS_OK:
        logger.warning("[ML] joblib/pandas no instalados — el filtro ML queda desactivado.")
        return
    if not os.path.exists(MODEL_FILE):
        logger.info(f"[ML] No se encontró '{MODEL_FILE}' — el filtro ML queda desactivado hasta entrenar uno (train_model.py).")
        return
    try:
        artifact = joblib.load(MODEL_FILE)
        ml_model = artifact['model']
        ml_feature_columns = artifact['feature_columns']
        ml_categorical_columns = artifact['categorical_columns']
        logger.info(f"[ML] Modelo cargado desde '{MODEL_FILE}' ({len(ml_feature_columns)} features).")
    except Exception as e:
        logger.warning(f"[ML] Error cargando el modelo, se sigue sin filtro ML: {e}")
        ml_model = None

def predict_prob(tipo_key: str, features: dict) -> Optional[float]:
    """Devuelve la probabilidad de acierto estimada por el modelo para esta
    señal, o None si no hay modelo cargado (en ese caso no se filtra nada)."""
    if ml_model is None or flatten_features is None:
        return None
    try:
        flat = flatten_features(tipo_key, features)

        # Nombres de las columnas dummy que genera el one-hot en entrenamiento,
        # ej. 'tipo_key_video', 'voto_a1_ENTRAR', etc.
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
        logger.warning(f"[ML] Error prediciendo probabilidad, se ignora el filtro esta vez: {e}")
        return None

# ─── ML — AUTO-ENTRENAMIENTO EN BACKGROUND ────────────────────────────────────
def count_resolved_signals() -> int:
    """Cuenta señales con resultado final (win/loss) y features guardadas."""
    try:
        con = _db()
        row = con.execute(
            "SELECT COUNT(*) c FROM pattern_stats "
            "WHERE result IN ('win','loss') AND features_json IS NOT NULL"
        ).fetchone()
        con.close()
        return row["c"] if row else 0
    except Exception as e:
        logger.warning(f"[ML] Error contando señales resueltas: {e}")
        return 0

def train_model_in_thread(min_rows: int):
    """Entrena el modelo. Pensada para correr en un thread aparte (executor)
    y así no bloquear el loop asyncio del bot mientras entrena. Devuelve
    (ok, artifact_o_None, mensaje) — NO toca variables globales ni escribe
    en disco; eso lo hace el llamador, de vuelta en el loop principal."""
    if not ML_LIBS_OK:
        return False, None, "faltan librerías de ML (pandas/scikit-learn/joblib)"
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, accuracy_score
    except ImportError:
        return False, None, "falta scikit-learn"

    try:
        df = cargar_datos_entrenamiento(DB_FILE)
        if len(df) < min_rows:
            return False, None, f"solo {len(df)} señales resueltas (mínimo {min_rows})"
        if df["_target"].nunique() < 2:
            return False, None, "todavía no hay ejemplos de ambas clases (win y loss)"

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
    """Chequea periódicamente si conviene (re)entrenar y, si hay suficientes
    señales nuevas, entrena en un thread aparte y recarga el modelo en
    caliente — sin reiniciar el bot ni correr nada a mano."""
    global ml_model, ml_feature_columns, ml_categorical_columns, ml_last_trained_count

    if not AUTO_TRAIN_ENABLED:
        logger.info("[ML] Auto-entrenamiento desactivado (AUTO_TRAIN_ENABLED=0).")
        return
    if not ML_LIBS_OK:
        logger.warning("[ML] Auto-entrenamiento desactivado: faltan pandas/scikit-learn/joblib.")
        return

    logger.info(
        f"[ML] Auto-entrenamiento activo — chequea cada {AUTO_TRAIN_INTERVAL_SEC}s "
        f"(mínimo inicial: {AUTO_TRAIN_MIN_ROWS}, reentrena cada +{AUTO_TRAIN_MIN_NEW} señales nuevas)."
    )

    while True:
        await asyncio.sleep(AUTO_TRAIN_INTERVAL_SEC)
        try:
            total = count_resolved_signals()
            es_primer_entrenamiento = ml_model is None and total >= AUTO_TRAIN_MIN_ROWS
            necesita_reentrenar = ml_model is not None and (total - ml_last_trained_count) >= AUTO_TRAIN_MIN_NEW

            if not (es_primer_entrenamiento or necesita_reentrenar):
                continue

            logger.info(f"[ML] 🧠 Disparando auto-entrenamiento ({total} señales resueltas)...")
            loop = asyncio.get_running_loop()
            ok, artifact, msg = await loop.run_in_executor(None, train_model_in_thread, AUTO_TRAIN_MIN_ROWS)

            if not ok:
                logger.info(f"[ML] Auto-entrenamiento pospuesto: {msg}")
                continue

            joblib.dump(artifact, MODEL_FILE)
            # El swap de variables globales queda en el loop principal (thread único
            # de asyncio), así nunca corre al mismo tiempo que evaluar()/predict_prob().
            ml_model = artifact["model"]
            ml_feature_columns = artifact["feature_columns"]
            ml_categorical_columns = artifact["categorical_columns"]
            ml_last_trained_count = total
            save_state()
            logger.info(f"[ML] ✅ Modelo actualizado en caliente: {msg}")
        except Exception as e:
            logger.warning(f"[ML] Error en el ciclo de auto-entrenamiento: {e}")

# ─── MENSAJES ─────────────────────────────────────────────────────────────────
def build_signal_msg(tipo_label: str, last_value: float) -> str:
    return (
        "<b>✅✅ ENTRADA CONFIRMADA ✅✅</b>\n\n"
        f"<b>🧠 {tipo_label}</b>\n"
        f"<b>👉 INGRESAR DESPUÉS: {last_value:.2f}x</b>\n"
        f"<b>💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x</b>\n\n"
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

# ─── STATS UPDATE — MARCADOR ÚNICO ────────────────────────────────────────────
async def send_stats_update():
    global stats_msg_id
    if sig_state != "idle":
        return
    if stats_msg_id:
        await delete_msg(stats_msg_id)
    stats_msg_id = await send_msg(build_stats_msg())
    save_state()

# ─── MÁQUINA DE ESTADOS — CONFIRMACIÓN + INTENTO 2/3 ─────────────────────────
async def resolve_pending(value: float):
    """Resuelve la cuota de confirmación (intento 1, silencioso).
    Si gana -> se descarta la señal (no se envía nada a Telegram) y NO se
    registra en pattern_stats, ya que nunca fue una señal real enviada.
    Si pierde -> se envía la señal a Telegram para intento 2 y 3."""
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features

    label = sig_tipo or "Señal HTML"
    key   = sig_tipo_key or "desconocido"

    if value >= CONFIRM_TRIGGER:
        logger.info(f"[HTML] ⏭️ Intento 1 confirmó {value:.2f}x (≥{CONFIRM_TRIGGER:.2f}x) — señal descartada (sin envío, no cuenta en stats) | {label}")
        sig_state = "idle"
        sig_attempt = 0
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        save_state()
    else:
        logger.info(f"[HTML] 🔎 Intento 1 falló ({value:.2f}x) — se activa señal para intento 2 y 3 | {label}")
        sig_state = "active"
        sig_attempt = 2
        sig_last_attempt = MAX_ATTEMPTS_NORMAL
        text = build_signal_msg(label, value)
        sig_msg_id = await send_msg(text, no_preview=True)
        save_state()

async def resolve_active(value: float):
    """Resuelve un intento de una señal ya enviada a Telegram. El último
    intento válido de la señal actual (2 intentos en total) está en
    sig_last_attempt: 3 para señales normales (intentos 2 y 3) y 2 para
    señales de emisión inmediata (intentos 1 y 2), si las hubiera.

    Racha de "sesiones ganadas consecutivas": aumenta con cada señal ganada;
    solo se resetea a 0 tras 3 señales perdidas seguidas (no con cada
    pérdida individual)."""
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses

    win = value >= CASHOUT_TRIGGER
    label = sig_tipo or "Señal HTML"
    key = sig_tipo_key or "desconocido"
    intento = sig_attempt

    if win:
        logger.info(f"[HTML] ✅ GANAMOS intento {intento} — {value:.2f}x | {label}")
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
        save_state()
        await send_stats_update()
    elif intento < sig_last_attempt:
        # Falló este intento pero queda otro dentro de la misma señal
        logger.info(f"[HTML] ⚠️ Intento {intento} falló ({value:.2f}x) — se juega intento {intento + 1} | {label}")
        sig_attempt = intento + 1
        save_state()
    else:
        logger.info(f"[HTML] ❌ PERDIMOS intento {intento} — {value:.2f}x | {label}")
        log_pattern_result(key, label, "loss", value, attempt=intento, features_json=sig_features)
        daily_losses += 1
        consecutive_losses += 1
        if consecutive_losses >= 3:
            logger.info("[HTML] 🔻 3 señales perdidas seguidas — racha de ganadas consecutivas reseteada a 0")
            consecutive_wins = 0
            consecutive_losses = 0
        await send_msg(build_loss_msg(value, label, intento))
        sig_state = "idle"
        sig_attempt = 0
        sig_msg_id = None
        sig_tipo = None
        sig_tipo_key = None
        sig_features = None
        save_state()
        await send_stats_update()

# ─── PROCESAMIENTO CENTRAL ────────────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features

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
            tipo_key, label, motivo, features_json = resultado
            sig_tipo = label
            sig_tipo_key = tipo_key
            sig_features = features_json

            if tipo_key in IMMEDIATE_PATTERNS:
                # Emisión inmediata: sin esperar la confirmación silenciosa
                # del intento 1. Actualmente IMMEDIATE_PATTERNS está vacío
                # (ningún patrón activo la usa), esta rama queda lista por
                # si se agrega uno en el futuro.
                sig_state = "active"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_IMMEDIATE
                text = build_signal_msg(label, value)
                sig_msg_id = await send_msg(text, no_preview=True)
                save_state()
                logger.info(f"[HTML] 🚀 Señal inmediata emitida: {tipo_key} — {motivo}")
            else:
                sig_state = "pending"
                sig_attempt = 1
                sig_last_attempt = MAX_ATTEMPTS_NORMAL
                save_state()
                logger.info(f"[HTML] 🕓 Señal detectada, esperando confirmación (intento 1): {tipo_key} — {motivo}")

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
        f"🤖 SpacemanBot HTML | hist:{len(history)} "
        f"| señal:{sig_state} "
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
        "signal": {"state": sig_state, "attempt": sig_attempt, "tipo": sig_tipo},
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
        f"🚀 <b>¡Bienvenido {name}!</b>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 <b>SPACEMAN BOT — ESTRATEGIA HTML</b>\n\n"
        f"💰 <b>Objetivo único: ≥ {CASHOUT_TARGET:.2f}x</b>\n"
        f"🔁 <b>Intento 1 de confirmación (silencioso) + envío en intento 2 y 3</b>\n\n"
        "📡 <b>Fuentes de señal</b>\n"
        "   🔥 Combo Tendencia VERDE + Lucky Agresiva\n"
        "   📹 Patrón zona de confianza\n"
        "   📊 Tendencia media alcista · 🔨 Martillo\n"
        "   🤖 Sistema de 8 Agentes (IA)\n\n"
        f"📊 <b>Filtro de Tendencia (informativo)</b>\n"
        f"   &lt;2x &lt; {UMBRAL_BELOW2}% ✅\n"
        f"   2-5x &gt; {UMBRAL_2TO5}% ✅\n\n"
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
        sig_txt = f"{sig_tipo} (esperando confirmación intento 1)"
    else:
        sig_txt = f"{sig_tipo} (intento {sig_attempt})"
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
    """Reinicia estadísticas a las 00:00 Colombia."""
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

    logger.info("🤖 Iniciando SPACEMAN Bot — Estrategia HTML...")
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
# MODO ENTRENAMIENTO — `python3 main.py train [--db ...] [--output ...] ...`
# No requiere BOT_TOKEN válido ni conexión a Telegram/websocket; solo lee la
# base SQLite. Los imports de sklearn son locales a esta función para no
# exigir esa dependencia cuando el bot corre en modo normal.
# ═══════════════════════════════════════════════════════════════════════════
def cargar_datos_entrenamiento(db_path: str):
    if not os.path.exists(db_path):
        sys.exit(f"No se encontró la base de datos: {db_path}")

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
        print(f"⚠️  {descartados} fila(s) con features_json inválido, se ignoraron.")

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
        sys.exit("Faltan las librerías de ML. Instalá: pip install pandas scikit-learn joblib")
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import roc_auc_score, accuracy_score, classification_report
    except ImportError:
        sys.exit("Falta scikit-learn. Instalá: pip install scikit-learn")

    import argparse
    ap = argparse.ArgumentParser(prog="main.py train",
                                  description="Entrena el modelo de ML que filtra/elige entre patrones.")
    ap.add_argument("--db", default=DB_FILE, help=f"Ruta a la base SQLite (default: {DB_FILE})")
    ap.add_argument("--output", default=MODEL_FILE, help=f"Archivo de salida (default: {MODEL_FILE})")
    ap.add_argument("--min-rows", type=int, default=100,
                     help="Mínimo de señales resueltas requeridas para entrenar (default: 100)")
    ap.add_argument("--test-size", type=float, default=0.2, help="Proporción para validación (default: 0.2)")
    args = ap.parse_args(argv)

    print(f"📥 Leyendo pattern_stats desde '{args.db}'...")
    df = cargar_datos_entrenamiento(args.db)

    if len(df) < args.min_rows:
        sys.exit(
            f"❌ Solo hay {len(df)} señales resueltas con features registradas "
            f"(mínimo requerido: {args.min_rows}). Dejá correr el bot un poco más "
            f"y volvé a intentar."
        )

    print(f"   {len(df)} señales resueltas encontradas.")
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
    print("📊 Resultados en el set de validación:")
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
    print("   Reiniciá el bot (o redeployá) para que lo cargue.")

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'train':
        run_train_cli(sys.argv[2:])
    else:
        threading.Thread(target=run_flask, daemon=True).start()
        asyncio.run(main_async())
