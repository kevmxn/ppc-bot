#!/usr/bin/env python3
"""
SPACEMAN HTML Strategy Bot — Telegram + Render  [v22 — Señales del gráfico de tendencia]
────────────────────────────────────────────────────────────────────────────
- Señal única de entrada: gráfico de tendencia (posiciones ≥2x/<2x + EMA4/8/20,
  réplica exacta de drawTrend()/calculateEMAForTrend() del panel HTML)
- Líneas calientes ≥5x (amarilla) y ≥10x (morada): no disparan por sí solas,
  se registran cada ronda y se anexan como feature de ML a la señal de tendencia
- Señal de tiempo "rebote 3x-5x": predictor de horario (réplica de
  calcularPrediccionInteligente()/checkAutoPredictions()) que dispara señal
  de entrada al llegar el horario previsto
- Objetivo de retiro SIEMPRE 2.00x; se registra además si la ronda llegó a 4x
  (supero_4x) para entrenar el modelo con esa distinción
- Sesión de hasta 5 señales: se gana y CORTA apenas acierta una. Solo se
  pierde si las 5 fallan seguidas
- Fuente: Spaceman — Pragmatic Play (WebSocket en tiempo real)
- MODIFICACIÓN: Se eliminó el bloqueo por tendencia general desfavorable
  para las señales de tiempo (check_timing_round_trigger y emit_timing_signal)
- NUEVO: Acumulación de fuerza por fallos en señales de tiempo. Cada fallo
  reduce la ventana de anticipación y amplía la ventana posterior, aumentando
  la probabilidad de éxito en los últimos intentos.
- NUEVO: Filtro de confirmación de tendencia para la señal de tiempo 3x-5x
  (réplica de precioSobreTresEMAs del HTML): solo dispara si la posición
  actual del gráfico de tendencia está por encima de EMA4, EMA8 y EMA20 a
  la vez. Si no está alineado, se espera sin cortar las predicciones activas.
- NUEVO: Filtro duro de gestión de dinero (réplica del límite de la
  Estrategia Dinero Real del HTML: Martingale 2 intentos x 3 columnas, sin
  simular capital/apuestas en $). Si la sesión ya agotó SESSION_MAX_SIGNALS
  columnas, check_timing_round_trigger no dispara más señales de tiempo.
- NUEVO: Las señales ahora se envían a Telegram desde la fase de
  ENTRENAMIENTO (antes solo se registraban en la DB hasta completar
  TRAINING_SIGNALS_REQUIRED). El único filtro para enviar sigue siendo
  favorable_actual/sig_favorable (tendencia de rangos + resto de filtros
  nuevos); ya no depende de is_trained. Las señales sombra (tendencia
  desfavorable) se siguen ocultando igual que antes, en cualquier fase.
- NUEVO: Se movió el filtro de tendencia favorable (pct1/pct2 vs
  TREND_RANGO1_MAX/TREND_RANGO2_MIN) al inicio de check_timing_round_trigger.
  Ya no se generan señales sombra: si la tendencia es desfavorable, la ronda
  se descarta antes de armar candidatos — solo se consideran/procesan
  señales de tendencia favorable, de punta a punta.
"""
import asyncio
import sqlite3
import sys
import threading
import json
import logging
import os
import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict
from flask import Flask, request
import aiohttp
import websockets
from telebot.async_telebot import AsyncTeleBot
from telebot import types

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

# ─── CONFIG — TELEGRAM ─────────────────────────────────────────────────────────
BOT_TOKEN  = os.environ.get("BOT_TOKEN",  "8620810853:AAHw-3JXcQt7Oz6Qcdv16Yt6JBG9m05UyYo")
CHAT_ID_BASE = int(os.environ.get("CHAT_ID_BASE", "-1003986868798"))
THREAD_SIGNALS = int(os.environ.get("THREAD_SIGNALS", "1590"))
THREAD_STATS   = int(os.environ.get("THREAD_STATS",   "1591"))

# ─── CONFIG — WEBSOCKET (Pragmatic Play — Spaceman) ───────────────────────────
WS_URL    = os.environ.get("WS_URL",    "wss://dga.pragmaticplaylive.net/ws")
CASINO_ID = os.environ.get("CASINO_ID", "ppcdk00000005349")
CURRENCY  = os.environ.get("CURRENCY",  "BRL")
GAME_ID   = int(os.environ.get("GAME_ID", "1301"))

DB_FILE = os.environ.get("DB_FILE", "spaceman.db")
ARG_TZ = timezone(timedelta(hours=-3))

# ─── CONFIG — ML ──────────────────────────────────────────────────────────────
MODEL_FILE     = os.environ.get("MODEL_FILE", "signal_model.joblib")
MODEL_MIN_PROB = float(os.environ.get("MODEL_MIN_PROB", "0.40"))

# Umbrales del gráfico de tendencia (posiciones +1/-1 según ≥2x, igual que
# drawTrend() del panel HTML) — reemplaza a los antiguos patrones de score.
TREND_MIN_HISTORY      = int(os.environ.get("TREND_MIN_HISTORY", "21"))
TREND_SIGNAL_SCORE_MIN = float(os.environ.get("TREND_SIGNAL_SCORE_MIN", "75"))
TIMING_MODEL_FILE = os.environ.get("TIMING_MODEL_FILE", "timing_model.joblib")
TIMING_MIN_PROB = float(os.environ.get("TIMING_MIN_PROB", "0.35"))

# Líneas calientes ≥5x (amarilla) y ≥10x (morada) — igual que drawHotLines()
# del panel HTML. No emiten señal propia: se registran cada ronda y se anexan
# como feature a la señal de tendencia, para que el modelo de ML aprenda su
# aporte real.
HOTLINE_THRESHOLD_5X  = float(os.environ.get("HOTLINE_THRESHOLD_5X", "5.00"))
HOTLINE_THRESHOLD_10X = float(os.environ.get("HOTLINE_THRESHOLD_10X", "10.00"))
HOTLINE_TOLERANCE     = float(os.environ.get("HOTLINE_TOLERANCE", "0.5"))
HOTLINE_DECAY_ROUNDS  = int(os.environ.get("HOTLINE_DECAY_ROUNDS", "6"))

# Predictor de tiempo "rebote 3x-5x" — igual que calcularPrediccionInteligente()
# / checkAutoPredictions() del panel HTML: registra el horario de cada ronda
# ≥3x, promedia el intervalo entre las últimas y predice el próximo horario.
# Al llegar ese horario se envía la señal de ENTRADA (el retiro sigue en 2x).
TIMING_HIGH_THRESHOLD   = float(os.environ.get("TIMING_HIGH_THRESHOLD", "3.00"))
TIMING_HISTORY_MAX      = int(os.environ.get("TIMING_HISTORY_MAX", "15"))
TIMING_SAMPLE_WINDOW    = int(os.environ.get("TIMING_SAMPLE_WINDOW", "5"))
# Ventana (en rondas) para la volatilidad reciente que se agrega como feature
# del modelo de timing, y cuántos resultados de señales SOMBRA recientes se
# conservan para medir "humedad" del patrón durante tendencia desfavorable.
TIMING_VOLATILITY_WINDOW = int(os.environ.get("TIMING_VOLATILITY_WINDOW", "20"))
SHADOW_RESULTS_MAX        = int(os.environ.get("SHADOW_RESULTS_MAX", "10"))
# El horario predicho es el momento estimado en que caerá la próxima ronda
# 3x-5x (el "rebote"). La señal de ENTRADA SOLO se procesa si una ronda real
# cae dentro de la franja de 15 a 30 segundos ANTES de ese horario (es decir,
# una ronda debe caer en esos 15s de ventana). No basta con que el reloj
# entre en la franja: tiene que llegar una ronda nueva estando dentro de ella.
TIMING_PREALERT_MIN_SEC = int(os.environ.get("TIMING_PREALERT_MIN_SEC", "15"))
TIMING_PREALERT_MAX_SEC = int(os.environ.get("TIMING_PREALERT_MAX_SEC", "30"))
TIMING_ALERT_WINDOW_SEC = int(os.environ.get("TIMING_ALERT_WINDOW_SEC", "10"))
TIMING_DEDUPE_SEC       = int(os.environ.get("TIMING_DEDUPE_SEC", "15"))

# Precisión mínima exigida por NIVEL de señal (C1/C2/C3). Cada nivel exige una
# probabilidad de éxito más alta que el anterior: si hay más de una predicción
# de horario vigente en la ventana al momento de disparar, se elige "la que
# mejor" (mayor probabilidad estimada por el modelo de timing) para ESE nivel,
# en vez de la primera que caiga en ventana. Así C2 exige más precisión que
# C1, y C3 más que C2, para no arriesgar la sesión en los niveles avanzados.
TIMING_MIN_PROB_C1 = float(os.environ.get("TIMING_MIN_PROB_C1", "0.35"))
TIMING_MIN_PROB_C2 = float(os.environ.get("TIMING_MIN_PROB_C2", "0.45"))
TIMING_MIN_PROB_C3 = float(os.environ.get("TIMING_MIN_PROB_C3", "0.55"))
TIMING_MIN_PROB_BY_NIVEL = {1: TIMING_MIN_PROB_C1, 2: TIMING_MIN_PROB_C2, 3: TIMING_MIN_PROB_C3}

def timing_min_prob_nivel(nivel: int) -> float:
    return TIMING_MIN_PROB_BY_NIVEL.get(nivel, TIMING_MIN_PROB)

def intento_global_actual(nivel: int, intento_local: int, intentos_por_nivel: int) -> int:
    """Numera los intentos de forma continua a lo largo de toda la sesión:
    con 2 intentos por nivel, C1 = intentos 1-2, C2 = intentos 3-4,
    C3 = intentos 5-6."""
    return (nivel - 1) * intentos_por_nivel + intento_local

AUTO_TRAIN_ENABLED     = os.environ.get("AUTO_TRAIN_ENABLED", "1") == "1"
AUTO_TRAIN_MIN_ROWS    = int(os.environ.get("AUTO_TRAIN_MIN_ROWS", "100"))
AUTO_TRAIN_MIN_NEW     = int(os.environ.get("AUTO_TRAIN_MIN_NEW", "30"))
AUTO_TRAIN_INTERVAL_SEC = int(os.environ.get("AUTO_TRAIN_INTERVAL_SEC", "1800"))

def colombia_now() -> datetime:
    return datetime.utcnow() - timedelta(hours=5)

def colombia_time() -> str:
    return colombia_now().strftime("%H:%M")

# ─── UMBRALES ──────────────────────────────────────────────────────────────
HISTORY_MAX   = 200
# Objetivo de retiro ÚNICO para TODAS las señales/patrones, sin importar su
# "objetivo natural" (p.ej. la señal de tiempo apunta conceptualmente a un
# rebote 3x-5x, las líneas calientes a 5x/10x): el bot siempre indica retirar
# en 2x, y resolve_active() define "ganada" para el modelo de ML únicamente
# con value >= CASHOUT_TRIGGER (2x) — nunca con la cuota "propia" del patrón.
CASHOUT_TARGET  = 2.00
CASHOUT_TRIGGER = 2.00

# ─── FASE DE ENTRENAMIENTO / EN VIVO ──────────────────────────────────────
# Las primeras TRAINING_SIGNALS_REQUIRED señales de tiempo se usan SOLO para
# entrenar el modelo (nunca se envían a Telegram, solo se registran en la DB).
# En esa fase la sesión usa hasta SESSION_MAX_SIGNALS_TRAIN señales de
# MAX_ATTEMPTS_TRAIN intentos cada una (igual que en vivo: 2 intentos por
# señal), para que el dataset de entrenamiento ya contemple el comportamiento
# de reintento dentro del nivel. Al llegar a ese número se entrena el modelo y
# se pasa a modo EN VIVO: sesión de SESSION_MAX_SIGNALS_LIVE niveles (C1/C2/C3),
# MAX_ATTEMPTS_LIVE intentos seguidos por nivel — la sesión se pierde si se
# pierden todos los niveles.
TRAINING_SIGNALS_REQUIRED = int(os.environ.get("TRAINING_SIGNALS_REQUIRED", "120"))
# FIX: antes valía "6" acá, lo que combinado con MAX_ATTEMPTS_TRAIN=2 daba
# 6 niveles x 2 intentos = 12 intentos por sesión durante entrenamiento (el
# mensaje de resolución mostraba "intento X de 12"). Debe ser igual a
# SESSION_MAX_SIGNALS_LIVE (3 niveles C1/C2/C3) para que la sesión sea
# siempre de 6 intentos totales (3 niveles x 2 intentos), en entrenamiento y en vivo.
SESSION_MAX_SIGNALS_TRAIN = int(os.environ.get("SESSION_MAX_SIGNALS_TRAIN", "3"))
SESSION_MAX_SIGNALS_LIVE  = int(os.environ.get("SESSION_MAX_SIGNALS_LIVE",  "3"))
MAX_ATTEMPTS_TRAIN        = int(os.environ.get("MAX_ATTEMPTS_TRAIN", "2"))
MAX_ATTEMPTS_LIVE         = int(os.environ.get("MAX_ATTEMPTS_LIVE",  "2"))
MAX_ATTEMPTS_NORMAL       = MAX_ATTEMPTS_TRAIN  # compat: valor por defecto antes de cargar estado

def get_session_max_signals() -> int:
    return SESSION_MAX_SIGNALS_LIVE if is_trained else SESSION_MAX_SIGNALS_TRAIN

def get_max_attempts() -> int:
    return MAX_ATTEMPTS_LIVE if is_trained else MAX_ATTEMPTS_TRAIN

def nivel_senal_label(n: int) -> str:
    return f"C{n}"
GAME_LINK = "https://1win.lat/casino/play/v_pragmatic:spaceman"

# Umbral de confirmación previa al envío: si al detectar el patrón la última
# cuota ya fue ≥ este valor, la señal queda pendiente hasta que una ronda
# posterior resulte < este valor.
CONFIRM_BELOW = float(os.environ.get("CONFIRM_BELOW", "2.00"))

# ═══════════════════════════════════════════════════════════════════════════
# GRÁFICO DE TENDENCIA (posiciones + EMA4/8/20) — réplica exacta de
# drawTrend()/calculateEMAForTrend() del panel HTML. Esta es ahora la ÚNICA
# fuente de señales de entrada del bot.
# ═══════════════════════════════════════════════════════════════════════════

def calc_trend_positions(vals: List[float]) -> List[float]:
    """Igual que el `positions` de drawTrend(): arranca en 0 y suma +1 si la
    ronda fue ≥2.00x, o resta 1 si fue <2.00x."""
    if not vals:
        return []
    positions = [0.0]
    current = 0.0
    for v in vals[1:]:
        current += 1.0 if v >= 2.00 else -1.0
        positions.append(current)
    return positions

def calc_ema_trend(positions: List[float], period: int) -> List[float]:
    """Réplica exacta de calculateEMAForTrend(): primer valor = SMA del
    primer `period`, luego EMA estándar con k = 2/(period+1)."""
    if len(positions) < period:
        return []
    k = 2 / (period + 1)
    ema_value = sum(positions[:period]) / period
    ema_result = [ema_value]
    for i in range(period, len(positions)):
        ema_value = (positions[i] * k) + (ema_value * (1 - k))
        ema_result.append(ema_value)
    return ema_result

def _ema_at(ema_list: List[float], period: int, data_index: int) -> Optional[float]:
    """El primer valor de `ema_list` corresponde al índice real `period-1`
    (dataIndex = period - 1 + i en el HTML). Traduce índice real → índice EMA."""
    i = data_index - (period - 1)
    if i < 0 or i >= len(ema_list):
        return None
    return ema_list[i]

def detect_trend_cross_signal(vals: List[float]) -> Dict:
    """Señal basada 100% en el gráfico de tendencia: cruce alcista de EMA4
    sobre EMA8 con alineación EMA4>EMA8>EMA20 (mismas 3 EMAs que se dibujan
    en el panel HTML: celeste=4, amarilla=8, naranja=20) y momentum positivo
    de las posiciones (≥2x más frecuente que <2x en las últimas 4 rondas)."""
    out = {'signal': False, 'score': 0, 'ema4': None, 'ema8': None, 'ema20': None,
           'momentum': 0, 'cruce_alcista': False, 'alineacion_alcista': False}
    if len(vals) < TREND_MIN_HISTORY:
        return out

    positions = calc_trend_positions(vals)
    ema4 = calc_ema_trend(positions, 4)
    ema8 = calc_ema_trend(positions, 8)
    ema20 = calc_ema_trend(positions, 20)
    if not ema4 or not ema8 or not ema20:
        return out

    idx_now, idx_prev = len(vals) - 1, len(vals) - 2
    e4_now, e4_prev = _ema_at(ema4, 4, idx_now), _ema_at(ema4, 4, idx_prev)
    e8_now, e8_prev = _ema_at(ema8, 8, idx_now), _ema_at(ema8, 8, idx_prev)
    e20_now = _ema_at(ema20, 20, idx_now)
    if None in (e4_now, e4_prev, e8_now, e8_prev, e20_now):
        return out

    cruce_alcista = e4_prev <= e8_prev and e4_now > e8_now
    alineacion_alcista = e4_now > e8_now > e20_now
    momentum = positions[-1] - positions[-4] if len(positions) >= 4 else 0

    if cruce_alcista and alineacion_alcista:
        score = 100
    elif alineacion_alcista:
        score = 75
    elif e4_now > e8_now:
        score = 55
    else:
        score = 25

    out.update({
        'signal': cruce_alcista and momentum > 0 and score >= TREND_SIGNAL_SCORE_MIN,
        'score': score, 'ema4': e4_now, 'ema8': e8_now, 'ema20': e20_now,
        'momentum': momentum, 'cruce_alcista': cruce_alcista,
        'alineacion_alcista': alineacion_alcista,
    })
    return out

# ═══════════════════════════════════════════════════════════════════════════
# LÍNEAS CALIENTES ≥5x (amarilla) y ≥10x (morada) — réplica de buildLevels()
# / drawHotLines() del panel HTML. No disparan señal propia: se registran
# cada ronda (log_hotline_snapshot) y se anexan como feature a la señal de
# tendencia, para entrenar el modelo de ML con su aporte real.
# ═══════════════════════════════════════════════════════════════════════════

def detect_hot_lines(vals: List[float], threshold: float,
                      tolerance: float = HOTLINE_TOLERANCE,
                      decay_rounds: int = HOTLINE_DECAY_ROUNDS) -> List[Dict]:
    if not vals:
        return []
    positions = calc_trend_positions(vals)
    levels: Dict[float, Dict] = {}
    for i, v in enumerate(vals):
        pos = positions[i]
        key = round(pos * 2) / 2
        if v >= threshold:
            found = None
            for k in levels:
                if abs(k - key) <= tolerance:
                    found = k
                    break
            if found is not None:
                levels[found]['fuerza'] += 1
                levels[found]['hits'] += 1
                levels[found]['sin_hit'] = 0
            else:
                levels[key] = {'pos': pos, 'fuerza': 1, 'hits': 1, 'sin_hit': 0}
        else:
            for k in levels:
                if abs(k - key) <= tolerance:
                    levels[k]['sin_hit'] += 1
                    if levels[k]['sin_hit'] >= decay_rounds:
                        levels[k]['fuerza'] = max(0, levels[k]['fuerza'] - 1)
                        levels[k]['sin_hit'] = 0
    return [lv for lv in levels.values() if lv['fuerza'] > 0]

def _summarize_hotline(levels: List[Dict]) -> Dict:
    if not levels:
        return {'activas': 0, 'fuerza_max': 0, 'hits_max': 0, 'fuerte': False}
    return {
        'activas': len(levels),
        'fuerza_max': max(lv['fuerza'] for lv in levels),
        'hits_max': max(lv['hits'] for lv in levels),
        'fuerte': any(lv['hits'] >= 2 and lv['fuerza'] >= 2 for lv in levels),
    }

def get_hotline_features(vals: List[float]) -> Dict:
    lines5 = detect_hot_lines(vals, HOTLINE_THRESHOLD_5X)
    lines10 = detect_hot_lines(vals, HOTLINE_THRESHOLD_10X)
    return {'linea_5x': _summarize_hotline(lines5), 'linea_10x': _summarize_hotline(lines10)}

def log_hotline_snapshot(vals: List[float]):
    """Registra el estado actual de ambas líneas calientes en cada ronda,
    independientemente de si dispara señal — insumo puro para ML."""
    try:
        hl = get_hotline_features(vals)
        con = _db()
        con.execute(
            "INSERT INTO hotline_log(tipo, activas, fuerza_max, hits_max, fuerte, ultimo_valor) "
            "VALUES(?,?,?,?,?,?)",
            ("5x", hl['linea_5x']['activas'], hl['linea_5x']['fuerza_max'],
             hl['linea_5x']['hits_max'], int(hl['linea_5x']['fuerte']), vals[-1])
        )
        con.execute(
            "INSERT INTO hotline_log(tipo, activas, fuerza_max, hits_max, fuerte, ultimo_valor) "
            "VALUES(?,?,?,?,?,?)",
            ("10x", hl['linea_10x']['activas'], hl['linea_10x']['fuerza_max'],
             hl['linea_10x']['hits_max'], int(hl['linea_10x']['fuerte']), vals[-1])
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando hotline_log: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# PREDICTOR DE TIEMPO "REBOTE 3x-5x" — réplica de calcularPrediccionInteligente()
# / checkAutoPredictions() del panel HTML.
# ═══════════════════════════════════════════════════════════════════════════
historial_valores_altos: List[Dict] = []   # {'valor','tiempo_seg'}
recorded_times: List[Dict] = []            # {'tiempo_seg','pre_alert_shown','alert_shown','created','fail_count'}
# Resultados (win/loss) de las últimas SHADOW_RESULTS_MAX señales SOMBRA
# (tendencia desfavorable) ya resueltas — se usa como feature de "humedad" del
# patrón para el modelo de timing (shadow_win_rate_reciente/shadow_last_result),
# sin que estas señales cuenten para la sesión visible.
shadow_results_recent: List[bool] = []
# tiempo_seg de la predicción cuya señal de tiempo PERDIÓ y aún está dentro
# de los -10s posteriores al horario predicho: habilita el reenvío de un
# nuevo intento con la ronda siguiente (sin esperar un patrón nuevo).
timing_retry_pred: Optional[int] = None

def calcular_prediccion_inteligente(valor: float):
    """Cada vez que aparece una ronda ≥TIMING_HIGH_THRESHOLD, guarda su hora y
    recalcula el próximo horario probable de rebote 3x-5x, promediando el
    intervalo entre las últimas TIMING_SAMPLE_WINDOW rondas altas."""
    global historial_valores_altos, recorded_times
    if valor < TIMING_HIGH_THRESHOLD:
        return
    ahora = colombia_now()
    tiempo_actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
    historial_valores_altos.append({'valor': valor, 'tiempo': tiempo_actual})
    if len(historial_valores_altos) > TIMING_HISTORY_MAX:
        historial_valores_altos.pop(0)
    if len(historial_valores_altos) < 2:
        return
    ultimos = historial_valores_altos[-min(TIMING_SAMPLE_WINDOW, len(historial_valores_altos)):]
    diffs = [ultimos[i]['tiempo'] - ultimos[i-1]['tiempo'] for i in range(1, len(ultimos))]
    if not diffs:
        return
    promedio = sum(diffs) / len(diffs)
    tiempo_predicho = (ultimos[-1]['tiempo'] + promedio) % 86400
    ya_existe = any(abs(r['tiempo_seg'] - tiempo_predicho) < TIMING_DEDUPE_SEC for r in recorded_times)
    if ya_existe:
        return
    recorded_times.append({
        'tiempo_seg': tiempo_predicho, 'pre_alert_shown': False, 'alert_shown': False,
        'created': tiempo_actual, 'fail_count': 0,  # ← nuevo campo
    })
    logger.info(f"[Timing] 🔮 Predicción inteligente: {int(tiempo_predicho//3600):02d}:{int((tiempo_predicho%3600)//60):02d}:{int(tiempo_predicho%60):02d}")

async def check_timing_predictions():
    """Limpieza de predicciones de tiempo: descarta las que vencieron sin
    dispararse. La señal ya NO se emite por reloj: solo se dispara cuando una
    RONDA real cae dentro de la ventana de ±TIMING_ALERT_WINDOW_SEC segundos
    del horario predicho (ver check_timing_round_trigger, llamada desde
    process_new_value al llegar cada ronda nueva)."""
    global recorded_times
    ahora = colombia_now()
    actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
    vigentes = []
    for r in recorded_times:
        diff = r['tiempo_seg'] - actual
        # Se permite que la ventana posterior se amplíe según fail_count
        post_window = TIMING_ALERT_WINDOW_SEC + r.get('fail_count', 0) * 2
        if diff < -(post_window + 1):
            logger.info("[Timing] ⏰ Predicción vencida — ninguna ronda cayó en la ventana")
            continue
        vigentes.append(r)
    recorded_times = vigentes

def calc_volatilidad_reciente(vals: List[float], window: int) -> float:
    """Desvío estándar de las últimas `window` rondas — mide qué tan errático
    está el juego ahora mismo (una tanda muy volátil puede necesitar una
    ventana de entrada distinta a una tanda estable)."""
    if not vals:
        return 0.0
    muestra = vals[-window:] if len(vals) > window else vals
    if len(muestra) < 2:
        return 0.0
    return statistics.pstdev(muestra)

def calc_racha_sin_2x(vals: List[float]) -> int:
    """Cantidad de rondas consecutivas (desde la más reciente hacia atrás)
    que NO llegaron a 2.00x — cuánto lleva "seco" el juego sin pagar."""
    racha = 0
    for v in reversed(vals):
        if v >= 2.00:
            break
        racha += 1
    return racha

def calc_ema4_rapida(vals: List[float]) -> float:
    """EMA4 del gráfico de tendencia (posiciones ≥2x/<2x) como feature de
    momentum de corto plazo, reutilizando la misma réplica exacta de
    calculateEMAForTrend() que usa la señal de tendencia."""
    positions = calc_trend_positions(vals)
    ema4 = calc_ema_trend(positions, 4)
    return ema4[-1] if ema4 else 0.0

def calc_tres_emas_status(vals: List[float]) -> dict:
    """Réplica de `precioSobreTresEMAs` del panel HTML: posición actual del
    gráfico de tendencia por encima de EMA4, EMA8 Y EMA20 a la vez (mismo
    filtro que el HTML usa para confirmar 'Skrill 2.0'). Se usa como gate de
    confirmación de tendencia para la señal de tiempo 3x-5x."""
    out = {'sobre_tres_emas': False, 'pos_actual': None, 'ema4': None, 'ema8': None, 'ema20': None}
    positions = calc_trend_positions(vals)
    ema4 = calc_ema_trend(positions, 4)
    ema8 = calc_ema_trend(positions, 8)
    ema20 = calc_ema_trend(positions, 20)
    if not positions or not ema4 or not ema8 or not ema20:
        return out
    idx_now = len(vals) - 1
    e4 = _ema_at(ema4, 4, idx_now)
    e8 = _ema_at(ema8, 8, idx_now)
    e20 = _ema_at(ema20, 20, idx_now)
    if None in (e4, e8, e20):
        return out
    pos_actual = positions[-1]
    out.update({
        'sobre_tres_emas': pos_actual > e4 and pos_actual > e8 and pos_actual > e20,
        'pos_actual': pos_actual, 'ema4': e4, 'ema8': e8, 'ema20': e20,
    })
    return out

def shadow_win_rate_reciente() -> float:
    """Fracción de wins entre las últimas SHADOW_RESULTS_MAX señales SOMBRA
    (tendencia desfavorable) ya resueltas. 0.5 (neutral) si todavía no hay
    ninguna registrada."""
    if not shadow_results_recent:
        return 0.5
    return sum(1 for w in shadow_results_recent if w) / len(shadow_results_recent)

def shadow_last_result_feature() -> int:
    """1 = la última señal sombra ganó, 0 = perdió, -1 = todavía no hay ninguna."""
    if not shadow_results_recent:
        return -1
    return 1 if shadow_results_recent[-1] else 0

def build_timing_features(nivel_actual: int, intento_global: int, fail_count: int = 0) -> dict:
    """Features de la señal de tiempo (ventana 3x-5x), incluyendo el nivel de
    señal (C1/C2/C3) y el intento global (1-6: C1=1-2, C2=3-4, C3=5-6), más un
    conjunto de features adicionales pensadas para mejorar la predicción del
    horario de entrada: fail_count del candidato, volatilidad reciente, racha
    sin llegar a 2x, hora del día (seno/coseno), valor y gap del último rebote
    3x-5x, momentum de corto plazo (EMA4 de tendencia) y el resultado/racha de
    las señales sombra más recientes (humedad del patrón en tendencia
    desfavorable), para que el modelo de ML aprenda a ajustar el horario de
    forma independiente por cada nivel/intento y contexto de mercado."""
    hist = list(history)
    ultimo_valor = hist[-1] if hist else 0.0
    ahora = colombia_now()
    tiempo_actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
    angulo = 2 * math.pi * (tiempo_actual / 86400)
    if historial_valores_altos:
        ultimo_rebote = historial_valores_altos[-1]
        valor_ultimo_rebote = ultimo_rebote['valor']
        gap_ultimo_rebote = (tiempo_actual - ultimo_rebote['tiempo']) % 86400
    else:
        valor_ultimo_rebote = 0.0
        gap_ultimo_rebote = 0
    tres_emas = calc_tres_emas_status(hist)
    return {
        'tipo_key': 'timing_3x_5x',
        'ultimo_valor': ultimo_valor,
        'confidence': 60,
        'tendencia_lucky': 'AMARILLO',
        'agresiva_condicion': False,
        'ema4': tres_emas['ema4'], 'ema8': tres_emas['ema8'], 'ema20': tres_emas['ema20'], 'ema50': None,
        'precio_sobre_tres_emas': tres_emas['sobre_tres_emas'],
        'votos': {}, 'contar_entrar': 0, 'contar_no_entrar': 0, 'risk_score': 0,
        'rsi': None, 'macd': None, 'fuerza': None, 'ia_prob': None,
        'racha_rango_activa': False, 'rango_activo': "3.00x-5.00x",
        'nivel_actual': nivel_actual,
        'intento_global': intento_global,
        # ── Features agregadas para mejorar la predicción de timing ──
        'fail_count_actual': fail_count,
        'volatilidad_reciente': calc_volatilidad_reciente(hist, TIMING_VOLATILITY_WINDOW),
        'racha_sin_2x': calc_racha_sin_2x(hist),
        'hora_sin': math.sin(angulo),
        'hora_cos': math.cos(angulo),
        'valor_ultimo_rebote_3x5x': valor_ultimo_rebote,
        'gap_ultimo_rebote_seg': gap_ultimo_rebote,
        'ema4_rapida': calc_ema4_rapida(hist),
        'shadow_win_rate_reciente': shadow_win_rate_reciente(),
        'shadow_last_result': shadow_last_result_feature(),
    }

def score_timing_candidate(nivel_actual: int, intento_global: int, diff: float, fail_count: int = 0) -> float:
    """Puntúa una predicción de horario candidata para el nivel/intento dado.
    Con modelo de timing entrenado: probabilidad de éxito estimada (suma de
    win_1..win_4). Sin modelo todavía: se prioriza la predicción más cercana
    al horario exacto (diff más chico), como aproximación razonable de
    'la que mejor' mientras se recolectan datos."""
    if timing_model is not None:
        pred = predict_timing(build_timing_features(nivel_actual, intento_global, fail_count))
        if pred:
            return sum(v for k, v in pred.items() if k.startswith('win_'))
    return -abs(diff)

async def check_timing_round_trigger():
    """Dispara la señal de tiempo eligiendo, entre TODAS las predicciones que
    caen en ventana en este momento, "la que mejor" le queda al nivel actual
    (C1/C2/C3) — mayor precisión estimada por el modelo de timing, en vez de
    disparar con la primera que coincida. Cada nivel exige una probabilidad
    mínima creciente (TIMING_MIN_PROB_C1 < C2 < C3): C1 usa los intentos
    globales 1-2, C2 los intentos 3-4 y C3 los intentos 5-6, y a medida que se
    sube de nivel la entrada debe ser más precisa para no perder la sesión.
    Cada fallo además reduce el preaviso mínimo/máximo y amplía la ventana
    posterior del candidato afectado."""
    global recorded_times, timing_retry_pred
    if sig_state != "idle" or pending_confirmation:
        return
    tres_emas = calc_tres_emas_status(list(history))
    if not tres_emas['sobre_tres_emas']:
        return
    pct1, pct2 = calc_pct_rangos(list(history))
    favorable_ahora = pct1 < TREND_RANGO1_MAX and pct2 > TREND_RANGO2_MIN
    if not favorable_ahora:
        logger.info(f"[Timing] 🛑 Tendencia desfavorable (pct1={pct1:.2f}%, pct2={pct2:.2f}%) — no se dispara")
        return
    nivel_actual = session_signal_count + 1
    if nivel_actual > get_session_max_signals():
        logger.info(f"[Timing] 🛑 Gestión de dinero: columnas agotadas "
                    f"({get_session_max_signals()}x{get_max_attempts()}) — no se dispara")
        return
    intentos_por_nivel = get_max_attempts()
    intento_global = intento_global_actual(nivel_actual, 1, intentos_por_nivel)
    ahora = colombia_now()
    actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
    candidatos = []
    for r in recorded_times:
        diff = r['tiempo_seg'] - actual
        fail_count = r.get('fail_count', 0)
        # Umbrales dinámicos
        min_sec = max(0, TIMING_PREALERT_MIN_SEC - fail_count * 3)
        max_sec = max(5, TIMING_PREALERT_MAX_SEC - fail_count * 3)
        post_window = TIMING_ALERT_WINDOW_SEC + fail_count * 2

        if not (-post_window <= diff <= max_sec):
            continue
        # Primer intento: debe estar entre min_sec y max_sec
        primera_vez = (not r['alert_shown'] and min_sec <= diff <= max_sec)
        # Reintento: si ya se emitió y estamos dentro de la ventana posterior ampliada
        reintento = (r['alert_shown'] and timing_retry_pred == r['tiempo_seg']
                     and diff >= -post_window)
        if primera_vez or reintento:
            candidatos.append((r, diff, primera_vez))

    if not candidatos:
        return

    mejor_r, mejor_diff, era_primera = max(
        candidatos, key=lambda c: score_timing_candidate(nivel_actual, intento_global, c[1], c[0].get('fail_count', 0))
    )
    umbral = timing_min_prob_nivel(nivel_actual)
    mejor_fail_count = mejor_r.get('fail_count', 0)
    mejor_score = score_timing_candidate(nivel_actual, intento_global, mejor_diff, mejor_fail_count)
    if is_trained and timing_model is not None and mejor_score < umbral:
        logger.info(f"[Timing] ⚠️ Mejor candidato para {nivel_senal_label(nivel_actual)} "
                    f"no alcanza precisión mínima ({mejor_score:.2f} < {umbral:.2f}) — se espera otra ronda")
        return

    mejor_r['alert_shown'] = True
    timing_retry_pred = None
    if not era_primera:
        logger.info(f"[Timing] 🔁 Reintento de señal de tiempo — {nivel_senal_label(nivel_actual)} "
                    f"(fail_count={mejor_fail_count}, diff={mejor_diff:.0f}s)")
    else:
        logger.info(f"[Timing] ⏰ Enviando señal {nivel_senal_label(nivel_actual)} "
                    f"(intento global {intento_global}/{get_session_max_signals() * intentos_por_nivel}) "
                    f"{mejor_diff:.0f}s antes del rebote, score={mejor_score:.2f} "
                    f"(fail_count={mejor_fail_count})")
    await emit_timing_signal(nivel_actual, intento_global, mejor_fail_count)

async def emit_timing_signal(nivel_actual: int, intento_global: int, fail_count: int = 0):
    """Emite la señal de tiempo (ventana 3x-5x) reutilizando el mismo pipeline
    de sesión/ML que la señal de tendencia — el objetivo de retiro sigue
    siendo 2x. Ya no se bloquea por tendencia general. Incluye nivel_actual,
    intento_global, fail_count y el resto de features de contexto (volatilidad,
    racha sin 2x, hora del día, último rebote, EMA4 rápida, humedad de señales
    sombra) para que el modelo de ML de timing aprenda a ajustar el horario de
    entrada de forma independiente para C1, C2 y C3."""
    features = build_timing_features(nivel_actual, intento_global, fail_count)
    ultimo_valor = features['ultimo_valor']
    features_json = json.dumps(features, default=str)
    label = "SEÑAL DE TIEMPO 3x-5x ⏰"
    motivo = (f"Horario predicho de rebote 3x-5x alcanzado — {nivel_senal_label(nivel_actual)}, "
              f"intento global {intento_global}, objetivo de retiro 2.00x")
    await emit_signal(ultimo_valor, 'timing_3x_5x', label, motivo, features_json,
                       confirmada_por_espera=False)

def evaluate_signal(vals: List[float]) -> Optional[tuple]:
    """Única fuente de señales de entrada: cruce alcista del gráfico de
    tendencia (EMA4/8/20 sobre las posiciones ≥2x/<2x). Las líneas calientes
    5x/10x se anexan como feature informativa (no disparan por sí solas) para
    que el modelo de ML aprenda su aporte real."""
    trend = detect_trend_cross_signal(vals)
    if not trend['signal']:
        return None

    hotlines = get_hotline_features(vals)
    features = {
        'tipo_key': 'tendencia_grafico',
        'ultimo_valor': vals[-1],
        'confidence': trend['score'],
        'tendencia_lucky': 'VERDE' if trend['score'] >= 75 else 'AMARILLO',
        'agresiva_condicion': False,
        'ema4': trend['ema4'], 'ema8': trend['ema8'], 'ema20': trend['ema20'], 'ema50': None,
        'momentum': trend['momentum'],
        'cruce_alcista': trend['cruce_alcista'],
        'alineacion_alcista': trend['alineacion_alcista'],
        'linea_5x_activa': hotlines['linea_5x']['fuerte'],
        'linea_5x_fuerza': hotlines['linea_5x']['fuerza_max'],
        'linea_10x_activa': hotlines['linea_10x']['fuerte'],
        'linea_10x_fuerza': hotlines['linea_10x']['fuerza_max'],
        'votos': {}, 'contar_entrar': 0, 'contar_no_entrar': 0, 'risk_score': 0,
        'rsi': None, 'macd': None, 'fuerza': None, 'ia_prob': None,
        'racha_rango_activa': False, 'rango_activo': None,
    }
    prob = predict_prob('tendencia_grafico', features)
    if prob is not None and prob < MODEL_MIN_PROB:
        logger.info(f"[v22] ML probability {prob:.2f} < {MODEL_MIN_PROB}, señal (tendencia) descartada")
        return None

    label = "SEÑAL DE TENDENCIA 📈"
    motivo = (f"Cruce EMA4>EMA8, alineación {'alcista' if trend['alineacion_alcista'] else 'parcial'}, "
              f"score {trend['score']}, momentum {trend['momentum']:.0f}")
    features_json = json.dumps(features, default=str)
    return ('tendencia_grafico', label, motivo, features_json, True)

# ─── FUNCIONES PARA CÁLCULO DE PORCENTAJES DE RANGOS ──────────────────────
def calc_pct_rangos(vals: List[float]) -> tuple:
    if len(vals) < 200:
        return 0.0, 0.0
    ultimas = vals[-200:]
    total = len(ultimas)
    rango1 = sum(1 for v in ultimas if 1.00 <= v < 2.00)
    rango2 = sum(1 for v in ultimas if 2.00 <= v < 5.00)
    return (rango1 / total) * 100, (rango2 / total) * 100

# Umbrales de tendencia favorable/desfavorable (ajustables por env)
# Favorable ⇔ 1.00x-1.99x < 52.51% Y 2.00x-4.99x > 29.00% (últimas 200 rondas)
TREND_RANGO1_MAX = float(os.environ.get("TREND_RANGO1_MAX", "52.51"))
TREND_RANGO2_MIN = float(os.environ.get("TREND_RANGO2_MIN", "27.99"))

def calc_pct_rangos_full(vals: List[float]) -> tuple:
    """Devuelve (conteo_rango1, conteo_rango2, pct_rango1, pct_rango2) sobre las últimas 200 rondas."""
    if len(vals) < 200:
        ultimas = vals
    else:
        ultimas = vals[-200:]
    total = len(ultimas)
    rango1 = sum(1 for v in ultimas if 1.00 <= v < 2.00)
    rango2 = sum(1 for v in ultimas if 2.00 <= v < 5.00)
    pct1 = (rango1 / total) * 100 if total else 0.0
    pct2 = (rango2 / total) * 100 if total else 0.0
    return rango1, rango2, pct1, pct2

# ─── MENSAJES ─────────────────────────────────────────────────────────────────
def build_signal_msg(tipo_label: str, last_value: float, sesion_index: int,
                     pct_rango1: float, pct_rango2: float,
                     ronda_predicha: Optional[int] = None) -> str:
    # Formato fijo de señal — nivel de señal (C1/C2/C3) en vez de contador
    # de sesión; sin línea de ronda predicha por el ML de timing.
    nivel = nivel_senal_label(sesion_index)
    return (
        f"<b>✅✅ ENTRADA CONFIRMADA ✅✅</b>\n\n"
        f"👉 INGRESAR DESPUÉS: {last_value:.2f}x\n"
        f"💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x\n\n"
        f"🧠 NIVEL DE SEÑAL: {nivel}\n"
        f"📈 TENDENCIA 200 RONDAS\n"
        f"🔵 1.00x-1.99x = {pct_rango1:.2f}%\n"
        f"🟢 2.00x-4.99x = {pct_rango2:.2f}%\n\n"
        f"💡 ¡Juegue con Responsabilidad!\n"
        f'🎰 <a href="{GAME_LINK}">Acceder al Spaceman</a>'
    )

def build_win_msg(result: float, intento: int) -> str:
    return (
        "<b>🍀🍀🍀 GANAMOS!!! 🍀🍀🍀</b>\n"
        f"<b>✅ Resultado: {result:.2f}x — INTENTO {intento}</b>"
    )

def build_loss_msg(intento: int, result: float, de: Optional[int] = None) -> str:
    total = de if de is not None else get_max_attempts()
    return (
        f"🧠 <b>INTENTO FALLIDO!!! Resultado: {result:.2f}x</b>\n"
        f"💥 Mantener la calma intento {intento} de {total}"
    )

def build_retry_attempt_msg(nivel_label: str) -> str:
    """Aviso de que el nivel sigue activo: falló el primer intento pero
    queda un segundo intento dentro del mismo nivel (misma entrada, mismo
    retiro). Se guarda su msg_id (sig_retry_msg_id) para borrarlo apenas se
    resuelva el nivel (win o loss del 2do intento)."""
    return (
        f"🔁 <b>REPETIR ENTRADA {CASHOUT_TARGET:.2f}x</b>\n"
        f"🎯 <b>NIVEL DE SEÑAL {nivel_label}</b>"
    )

def build_level_loss_msg(nivel_label: str, siguiente_label: str, resultados: List[float],
                         intento_actual: int, intento_total: int) -> str:
    """Se envía cuando se agotan los intentos de un nivel (C1/C2/C3) sin
    acertar. Borra el mensaje de 'REPETIR ENTRADA' y muestra las cuotas de
    todos los intentos de ese nivel, más el contador acumulado de intentos
    de la sesión (p.ej. 2 de 6: 3 niveles × 2 intentos)."""
    resultados_str = " - ".join(f"{v:.2f}x" for v in resultados)
    return (
        f"🧠 <b>{nivel_label} PERDIDO, ESPERAR SEÑAL {siguiente_label}!!!</b>\n"
        f"❌ <b>Resultados Señal {nivel_label}: {resultados_str}</b>\n"
        f"💥 Mantener la calma intento {intento_actual} de {intento_total}"
    )

def build_win_status_msg(intento: int) -> str:
    return f"✅ WIN INTENTO {intento}"

def build_loss_status_msg(intento: int) -> str:
    return f"❌ LOSS INTENTO {intento}"

def build_trend_status_msg(rango1_count: int, rango2_count: int, pct_rango1: float, pct_rango2: float) -> str:
    hora_ar = datetime.now(ARG_TZ).strftime("%H:%M:%S")
    favorable = pct_rango1 < TREND_RANGO1_MAX and pct_rango2 > TREND_RANGO2_MIN
    estado = "✅ FAVORABLE" if favorable else "❌ DESFAVORABLE"
    return (
        f"{estado} — {hora_ar} (ARG)\n\n"
        "📈 TENDENCIA 200 RONDAS\n"
        f"🔵 (1.00x-1.99x) {rango1_count} — {pct_rango1:.2f}%\n"
        f"🟢 (2.00x-4.99x) {rango2_count} — {pct_rango2:.2f}%"
    )

def build_session_loss_msg(last_result: float) -> str:
    return (
        "<b>❎❎❎ PERDIMOS!!! ❎❎❎</b>\n"
        f"<b>❌ Resultado: {last_result:.2f}x — Sesión Fallida.</b>"
    )

def build_stats_msg() -> str:
    total = daily_wins + daily_losses
    pct = (daily_wins / total * 100) if total > 0 else 0.0
    return (
        f"🚀 <b>Resultado del día ✅ {daily_wins} | ⭕ {daily_losses}</b>\n"
        f"💎 <b>Acertamos el {pct:.2f}% de las Sesiones</b>\n"
        f"🔥 <b>¡{consecutive_signal_wins} Señales ganadas Consecutivas!</b>\n"
        f"📈 <b>¡{consecutive_wins} Sesiones Ganadas Consecutivas!</b>"
    )

# ═══════════════════════════════════════════════════════════════════════════
# SQLITE, ESTADO, ML, etc.
# ═══════════════════════════════════════════════════════════════════════════

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
    CREATE TABLE IF NOT EXISTS hotline_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo        TEXT    NOT NULL,
        activas     INTEGER,
        fuerza_max  INTEGER,
        hits_max    INTEGER,
        fuerte      INTEGER,
        ultimo_valor REAL,
        created     TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    """)
    try:
        cur.execute("ALTER TABLE pattern_stats ADD COLUMN features_json TEXT")
        con.commit()
    except sqlite3.OperationalError:
        pass
    try:
        # Marca si la ronda resuelta llegó a 4x — registro adicional para
        # entrenar el modelo con la señal 2x/4x pedida (retiro sigue en 2x).
        cur.execute("ALTER TABLE pattern_stats ADD COLUMN supero_4x INTEGER DEFAULT 0")
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
        "consecutive_signal_wins": str(consecutive_signal_wins),
        "ml_last_trained_count": str(ml_last_trained_count),
        "timing_last_trained_count": str(timing_last_trained_count),
        "session_signal_count": str(session_signal_count),
        "pending_signal_index": str(pending_signal_index),
        "is_last_signal_of_session": "1" if is_last_signal_of_session else "0",
        "pending_confirmation": "1" if pending_confirmation else "0",
        "pending_confirmation_data": json.dumps(pending_confirmation_data) if pending_confirmation_data else "",
        "is_trained": "1" if is_trained else "0",
        "sig_favorable": "1" if sig_favorable else "0",
        "sig_shadow": "1" if sig_shadow else "0",
        "shadow_results_recent": json.dumps(shadow_results_recent),
        "sig_retry_msg_id": str(sig_retry_msg_id) if sig_retry_msg_id is not None else "",
        "sig_attempt_values": json.dumps(sig_attempt_values),
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global sig_inmediata, sig_emit_attempt, sig_context_json, sig_signal_id, stats_msg_id
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses, consecutive_signal_wins
    global ml_last_trained_count, timing_last_trained_count
    global session_signal_count, pending_signal_index, is_last_signal_of_session
    global pending_confirmation, pending_confirmation_data
    global is_trained
    global sig_favorable, sig_shadow
    global sig_retry_msg_id, sig_attempt_values
    global shadow_results_recent

    d = _load_dict()
    sig_state         = d.get("sig_state", "idle") or "idle"
    if sig_state not in ("idle", "active"):
        sig_state = "idle"
    sig_attempt        = int(d.get("sig_attempt", "0") or "0")
    sig_last_attempt   = int(d.get("sig_last_attempt", str(MAX_ATTEMPTS_NORMAL)) or str(MAX_ATTEMPTS_NORMAL))
    _mid              = d.get("sig_msg_id", "")
    sig_msg_id        = int(_mid) if _mid else None
    sig_tipo          = d.get("sig_tipo", "") or None
    sig_tipo_key      = d.get("sig_tipo_key", "") or None
    sig_features      = d.get("sig_features", "") or None
    sig_inmediata     = (d.get("sig_inmediata", "0") or "0") == "1"
    sig_emit_attempt  = int(d.get("sig_emit_attempt", "1") or "1")
    sig_context_json  = d.get("sig_context_json", "") or None
    sig_signal_id     = d.get("sig_signal_id", "") or None
    _sid              = d.get("stats_msg_id", "")
    stats_msg_id      = int(_sid) if _sid else None
    daily_wins        = int(d.get("daily_wins", "0"))
    daily_losses      = int(d.get("daily_losses", "0"))
    consecutive_wins  = int(d.get("consecutive_wins", "0"))
    consecutive_losses = int(d.get("consecutive_losses", "0"))
    consecutive_signal_wins = int(d.get("consecutive_signal_wins", "0") or "0")
    ml_last_trained_count = int(d.get("ml_last_trained_count", "0") or "0")
    timing_last_trained_count = int(d.get("timing_last_trained_count", "0") or "0")
    session_signal_count = int(d.get("session_signal_count", "0") or "0")
    if session_signal_count < 0 or session_signal_count > SESSION_MAX_SIGNALS_TRAIN:
        session_signal_count = 0
    pending_signal_index = int(d.get("pending_signal_index", "0") or "0")
    is_last_signal_of_session = (d.get("is_last_signal_of_session", "0") or "0") == "1"
    pending_confirmation = (d.get("pending_confirmation", "0") or "0") == "1"
    _pcd = d.get("pending_confirmation_data", "")
    try:
        pending_confirmation_data = json.loads(_pcd) if _pcd else None
    except (TypeError, ValueError):
        pending_confirmation_data = None
    is_trained = (d.get("is_trained", "0") or "0") == "1"
    sig_favorable = (d.get("sig_favorable", "1") or "1") == "1"
    sig_shadow = (d.get("sig_shadow", "0") or "0") == "1"
    try:
        shadow_results_recent = json.loads(d.get("shadow_results_recent", "") or "[]")
        if not isinstance(shadow_results_recent, list):
            shadow_results_recent = []
    except (TypeError, ValueError):
        shadow_results_recent = []
    _rmid = d.get("sig_retry_msg_id", "")
    sig_retry_msg_id = int(_rmid) if _rmid else None
    _sav = d.get("sig_attempt_values", "")
    try:
        sig_attempt_values = json.loads(_sav) if _sav else []
    except (TypeError, ValueError):
        sig_attempt_values = []
    logger.info(
        f"[v21] Estado cargado | estado={sig_state} sesion={session_signal_count} "
        f"esperando_confirmacion={pending_confirmation}"
    )

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

def log_pattern_result(tipo_key: str, tipo_label: str, result: str, value: float,
                       attempt: int = 0, features_json: Optional[str] = None):
    """Registra el resultado de la señal. `supero_4x` queda grabado aparte
    (valor ≥4.00x) para entrenar el modelo con la distinción 2x/4x pedida —
    el objetivo de retiro real de la señal sigue siendo siempre 2x."""
    try:
        con = _db()
        con.execute(
            "INSERT INTO pattern_stats(tipo_key, tipo_label, result, value, attempt, features_json, supero_4x) "
            "VALUES(?,?,?,?,?,?,?)",
            (tipo_key or "desconocido", tipo_label or "", result, value, attempt, features_json,
             int(value >= 4.00))
        )
        con.commit()
        con.close()
    except Exception as e:
        logger.warning(f"Error guardando pattern_stats: {e}")

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
    for key, d in data.items():
        if d["total"] == 0:
            continue
        wins, losses, total = d["wins"], d["losses"], d["total"]
        pct = (wins / total * 100) if total else 0.0
        total_wins   += wins
        total_losses += losses
        lines.append(f"{key}: ✅{wins} ❌{losses} — <b>{pct:.1f}%</b> ({total})")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━")
    grand_total = total_wins + total_losses
    grand_pct   = (total_wins / grand_total * 100) if grand_total else 0.0
    lines.append(f"🌐 <b>TOTAL: ✅{total_wins} ❌{total_losses} — {grand_pct:.1f}%</b>")
    if not data:
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
sig_emit_attempt:  int           = 1
sig_context_json:  Optional[str] = None
sig_signal_id:     Optional[str] = None
stats_msg_id:      Optional[int] = None
daily_wins:        int           = 0
daily_losses:      int           = 0
consecutive_wins:  int           = 0
consecutive_losses: int          = 0
# Racha de señales (niveles C1/C2/C3) ganadas seguidas, independiente de las
# sesiones: cada nivel ganado (en 1er o 2do intento) suma 1; cualquier nivel
# perdido la resetea a 0. Solo cuenta señales favorables (las que se envían
# a Telegram), igual criterio que daily_wins/daily_losses.
consecutive_signal_wins: int      = 0
session_signal_count: int        = 0
pending_signal_index: int        = 0
is_last_signal_of_session: bool  = False
current_session_results: List[bool] = []  # almacena wins/losses de la sesión actual
trend_msg_id: Optional[int] = None
is_trained: bool = False  # False = fase de entrenamiento silenciosa; True = en vivo
# True = la señal activa se emitió con tendencia favorable (pct1<TREND_RANGO1_MAX
# y pct2>TREND_RANGO2_MIN) y por lo tanto SÍ se envía a Telegram. False = se
# procesa igual en 2 planos (log_pattern_result + signal_contexts) para
# entrenar los modelos, pero no se manda ningún mensaje al chat.
sig_favorable: bool = True
# True = la señal activa es "sombra": se emitió con tendencia DESFAVORABLE, así
# que aunque se resuelva (win/loss) NO avanza ni termina la sesión visible
# (C1/C2/C3) — pending_signal_index/session_signal_count quedan congelados en
# el nivel pendiente real. Se sigue registrando en 2 planos para el ML. Al
# volver la tendencia a favorable, la sesión continúa en el mismo nivel donde
# quedó pendiente, sin saltar niveles por señales sombra que el usuario nunca vio.
sig_shadow: bool = False
sig_retry_msg_id: Optional[int] = None  # id del mensaje "REPETIR ENTRADA" del intento 1, para poder borrarlo al resolver el nivel
sig_attempt_values: List[float] = []  # cuotas de cada intento del nivel activo (C1/C2/C3), para "Resultados Señal Cx: ..."

# ─── DASHBOARD HTML — puente de eventos ──────────────────────────────────────
# El panel HTML (servido en '/') ya NO calcula sus propias señales (se quitó
# el motor EMA/tendencia/hotlines client-side): solo dibuja el gráfico con las
# rondas reales y refleja acá, vía polling a /api/state, las señales y
# resultados que el bot YA procesó y mandó a Telegram. Estos contadores le
# permiten al JS del panel detectar "hay un evento nuevo" sin duplicar nada:
# dashboard_session_start_id sube cada vez que arranca una sesión nueva
# (nivel C1, intento 1) y dashboard_resolution_id sube cada vez que se
# resuelve un intento (win o loss), con el detalle en dashboard_last_resolution.
dashboard_session_start_id: int = 0
dashboard_resolution_id: int = 0
dashboard_last_resolution: Optional[dict] = None

def record_dashboard_attempt(win: bool, nivel: int, intento_local: int) -> None:
    global dashboard_resolution_id, dashboard_last_resolution
    dashboard_resolution_id += 1
    dashboard_last_resolution = {
        "id": dashboard_resolution_id,
        "win": win,
        "nivel": nivel,
        "intento_local": intento_local,
        "intento_global": intento_global_actual(nivel, intento_local, get_max_attempts()),
        "ts": datetime.utcnow().isoformat(),
    }

# ─── CONFIRMACIÓN PREVIA AL ENVÍO (nuevo) ────────────────────────────────────
# Si el patrón se detecta pero la última cuota registrada ya fue ≥2x, la señal
# NO se envía todavía: se guarda como pendiente y se espera a la siguiente
# ronda. Recién cuando esa ronda de confirmación resulte <2x se emite la
# señal al Telegram. Ambos caminos (inmediato vs. confirmado por espera)
# quedan registrados en signal_contexts para que el modelo de timing pueda
# aprender cuál conviene mejor.
pending_confirmation: bool = False
pending_confirmation_data: Optional[dict] = None  # {tipo_key, label, motivo, features_json}

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
        logger.warning(f"[v21] send error: {e}")
        return None

async def send_signal_msg(text: str, no_preview: bool = False) -> Optional[int]:
    return await send_msg(text, no_preview=no_preview, thread_id=THREAD_SIGNALS)

async def send_stats_msg(text: str, no_preview: bool = False) -> Optional[int]:
    return await send_msg(text, no_preview=no_preview, thread_id=THREAD_STATS)

async def edit_msg(msg_id: int, text: str, no_preview: bool = False) -> bool:
    try:
        await bot.edit_message_text(
            text, CHAT_ID_BASE, msg_id,
            parse_mode='HTML', disable_web_page_preview=no_preview
        )
        return True
    except Exception as e:
        logger.debug(f"edit error: {e}")
        return False

async def delete_msg(msg_id: int) -> bool:
    try:
        await bot.delete_message(CHAT_ID_BASE, msg_id)
        return True
    except Exception as e:
        logger.debug(f"delete error: {e}")
        return False

async def update_trend_status_msg(vals: List[float], resolved: bool):
    """Actualiza el mensaje de tendencia en el chat de status.
    Si `resolved` es True (se acaba de resolver un intento win/loss), borra el
    mensaje anterior y envía uno nuevo. Si es False (no hubo resolución en este
    tick), simplemente edita el mensaje existente en su lugar."""
    global trend_msg_id
    r1, r2, pct1, pct2 = calc_pct_rangos_full(vals)
    text = build_trend_status_msg(r1, r2, pct1, pct2)

    if resolved:
        if trend_msg_id:
            await delete_msg(trend_msg_id)
        trend_msg_id = await send_stats_msg(text)
        return

    if trend_msg_id:
        ok = await edit_msg(trend_msg_id, text)
        if ok:
            return
    trend_msg_id = await send_stats_msg(text)

# ─── ANÁLISIS DE TENDENCIA (simple) ─────────────────────────────────────────
def get_stats() -> dict:
    total = len(history)
    if total == 0:
        return {"total": 0, "below2": 0, "two_to_five": 0,
                "pct_below2": 0.0, "pct_2to5": 0.0, "favorable": False}
    below2      = sum(1 for v in history if v < 2.00)
    two_to_five = sum(1 for v in history if 2.00 <= v < 5.00)
    pct_below2  = (below2 / total) * 100
    pct_2to5    = (two_to_five / total) * 100
    favorable   = (pct_below2 < 53.51) and (pct_2to5 > 26.99)
    return {
        "total": total, "below2": below2, "two_to_five": two_to_five,
        "pct_below2": pct_below2, "pct_2to5": pct_2to5, "favorable": favorable,
    }

# ═══════════════════════════════════════════════════════════════════════════
# ML — FEATURIZACIÓN E INFERENCIA
# ═══════════════════════════════════════════════════════════════════════════
CATEGORICAL_COLUMNS = [
    'tipo_key', 'tendencia_lucky', 'rango_activo',
    'voto_a1', 'voto_a2', 'voto_a3', 'voto_a4',
    'voto_a5', 'voto_a6', 'voto_a7', 'voto_a8',
]

def flatten_features(tipo_key: str, features: dict) -> dict:
    flat = dict(features)
    votos = flat.pop('votos', None) or {}
    for agente in ('a1','a2','a3','a4','a5','a6','a7','a8'):
        flat[f'voto_{agente}'] = votos.get(agente)
    fuerza = flat.pop('fuerza', None)
    if isinstance(fuerza, dict):
        flat['fuerza_velocidad'] = fuerza.get('velocidad')
        flat['fuerza_tendencia'] = fuerza.get('tendencia')
    else:
        flat['fuerza_velocidad'] = None
        flat['fuerza_tendencia'] = None
    for bool_col in ('agresiva_condicion','racha_rango_activa','emision_inmediata',
                     'cruce_alcista','alineacion_alcista','linea_5x_activa','linea_10x_activa'):
        if bool_col in flat and flat[bool_col] is not None:
            flat[bool_col] = int(bool(flat[bool_col]))
    flat.pop('ml_prob', None)
    flat['tipo_key'] = tipo_key
    return flat

ml_model = None
ml_feature_columns: List[str] = []
ml_categorical_columns: List[str] = []
ml_last_trained_count: int = 0

def load_ml_model():
    global ml_model, ml_feature_columns, ml_categorical_columns
    if not ML_LIBS_OK:
        logger.warning("[ML] joblib/pandas no instalados.")
        return
    if not os.path.exists(MODEL_FILE):
        logger.info(f"[ML] No se encontró '{MODEL_FILE}'.")
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

# ─── ML — TIMING MODEL ────────────────────────────────────────────────────
timing_model = None
timing_feature_columns: List[str] = []
timing_categorical_columns: List[str] = []
timing_last_trained_count: int = 0

def load_timing_model():
    global timing_model, timing_feature_columns, timing_categorical_columns
    if not ML_LIBS_OK:
        logger.warning("[Timing ML] joblib/pandas no instalados.")
        return
    if not os.path.exists(TIMING_MODEL_FILE):
        logger.info(f"[Timing ML] No se encontró '{TIMING_MODEL_FILE}'.")
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
            if cls == 1: result['win_1'] = float(probs[i])
            elif cls == 2: result['win_2'] = float(probs[i])
            elif cls == 3: result['win_3'] = float(probs[i])
            elif cls == 4: result['win_4'] = float(probs[i])
            elif cls == 0: result['loss'] = float(probs[i])
        return result
    except Exception as e:
        logger.warning(f"[Timing ML] Error prediciendo: {e}")
        return None

def elegir_ronda_entrada(timing_pred: Optional[Dict[str, float]]) -> tuple:
    """Aprende, con el modelo de timing (entrenado sobre attempt_when_win de
    signal_contexts), en qué ronda de la sesión conviene más entrar: 1, 2 o 3.
    Sin modelo entrenado todavía, asume ronda 1 (entrada inmediata) con
    confianza neutra, para no bloquear señales antes de tener datos."""
    if not timing_pred:
        return 1, 1.0
    candidatos = {
        1: timing_pred.get('win_1', 0.0),
        2: timing_pred.get('win_2', 0.0),
        3: timing_pred.get('win_3', 0.0),
    }
    mejor_ronda = max(candidatos, key=candidatos.get)
    return mejor_ronda, candidatos[mejor_ronda]

def decide_emit_attempt(timing_pred: Optional[Dict[str, float]], es_inmediata: bool) -> int:
    if timing_pred is None:
        return 1
    loss = timing_pred.get('loss', 0.0)
    if loss > 0.6:
        logger.info(f"[Timing ML] ❌ Alta probabilidad de pérdida ({loss:.2%}) — no emitir")
        return 0
    _, mejor_prob = elegir_ronda_entrada(timing_pred)
    if mejor_prob < TIMING_MIN_PROB:
        logger.info(f"[Timing ML] ⚠️ Probabilidad muy baja ({mejor_prob:.2%}) — no emitir")
        return 0
    return 1

# ─── AUTO-ENTRENAMIENTO ──────────────────────────────────────────────────────
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
    if not AUTO_TRAIN_ENABLED or not ML_LIBS_OK:
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

# ─── FUNCIONES DE CARGA DE DATOS PARA ENTRENAMIENTO ────────────────────────
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
    for row in rows:
        try:
            features = json.loads(row["features_json"])
        except (TypeError, ValueError):
            continue
        flat = flatten_features(row["tipo_key"], features)
        flat["_target"] = 1 if row["result"] == "win" else 0
        registros.append(flat)
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
    for row in rows:
        try:
            context = json.loads(row["context_json"])
        except (TypeError, ValueError):
            continue
        flat = dict(context)
        if row["result"] == "loss":
            flat["_target"] = 0
        else:
            attempt = row["attempt_when_win"]
            if attempt in (1,2,3,4):
                flat["_target"] = attempt
            else:
                flat["_target"] = 0
        registros.append(flat)
    return pd.DataFrame(registros)

def construir_matriz_timing(df):
    y = df["_target"].astype(int)
    x_raw = df.drop(columns=["_target"])
    cat_cols_presentes = [c for c in TIMING_CATEGORICAL_COLUMNS if c in x_raw.columns]
    x = pd.get_dummies(x_raw, columns=cat_cols_presentes, dummy_na=False)
    x = x.select_dtypes(include=["number", "bool"]).astype(float)
    return x, y, cat_cols_presentes

async def maybe_finish_training():
    """Al alcanzar TRAINING_SIGNALS_REQUIRED señales de tiempo resueltas, se
    entrena el modelo de timing con ellas y se pasa a modo EN VIVO (sesión de
    3 niveles C1/C2/C3, 2 intentos por nivel, señales visibles en Telegram)."""
    global is_trained, timing_model, timing_feature_columns, timing_categorical_columns, timing_last_trained_count
    if is_trained:
        return
    total_ctx = count_resolved_contexts()
    if total_ctx < TRAINING_SIGNALS_REQUIRED:
        return
    logger.info(f"[Timing ML] 🎓 {total_ctx} señales de entrenamiento recopiladas — entrenando modelo...")
    if ML_LIBS_OK:
        loop = asyncio.get_running_loop()
        ok, artifact, msg = await loop.run_in_executor(None, train_timing_model_in_thread, TRAINING_SIGNALS_REQUIRED)
        if ok:
            joblib.dump(artifact, TIMING_MODEL_FILE)
            timing_model = artifact["model"]
            timing_feature_columns = artifact["feature_columns"]
            timing_categorical_columns = artifact["categorical_columns"]
            timing_last_trained_count = total_ctx
            logger.info(f"[Timing ML] ✅ Modelo entrenado: {msg}")
        else:
            logger.warning(f"[Timing ML] ⚠️ No se pudo entrenar ({msg}) — se pasa a modo EN VIVO igual")
    else:
        logger.warning("[Timing ML] ⚠️ Librerías de ML no disponibles — se pasa a modo EN VIVO sin modelo entrenado")
    is_trained = True
    save_state()
    logger.info("[Timing ML] 🚀 Modo EN VIVO activado: sesión de 3 niveles (C1/C2/C3), 2 intentos por nivel")
    await send_signal_msg(
        "🎓 <b>Entrenamiento completo</b>\n"
        f"Se recopilaron {total_ctx} señales de entrenamiento.\n"
        "A partir de ahora las señales se envían en vivo — niveles C1/C2/C3, "
        "2 intentos seguidos por nivel."
    )

# ─── MÁQUINA DE ESTADOS ──────────────────────────────────────────────────────
async def resolve_active(value: float, signal_index: int):
    global sig_state, sig_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses, consecutive_signal_wins
    global sig_context_json, sig_signal_id
    global session_signal_count, pending_signal_index, is_last_signal_of_session
    global current_session_results
    global is_trained
    global sig_favorable, sig_shadow
    global sig_retry_msg_id, sig_attempt_values
    global shadow_results_recent

    win = value >= CASHOUT_TRIGGER
    # sig_favorable/sig_shadow quedaron fijados en emit_signal() para ESTA
    # señal activa — favorable decide si sus mensajes van a Telegram; shadow
    # decide si esta señal puede tocar el estado de la sesión visible.
    favorable_actual = sig_favorable
    shadow_actual = sig_shadow
    sig_attempt_values.append(value)

    # Si perdió pero todavía quedan intentos dentro de ESTE nivel (en vivo:
    # 2 intentos seguidos por nivel C1/C2/C3), no se resuelve todavía: se
    # espera la próxima ronda para el siguiente intento, sin avanzar de nivel.
    if not win and sig_attempt < sig_last_attempt:
        if favorable_actual and not shadow_actual:
            record_dashboard_attempt(False, pending_signal_index, sig_attempt)
        sig_attempt += 1
        logger.info(f"[v21] 🔁 Intento {sig_attempt-1}/{sig_last_attempt} fallido — va el intento {sig_attempt} (misma entrada)")
        if favorable_actual:
            nivel_label = nivel_senal_label(pending_signal_index)
            sig_retry_msg_id = await send_signal_msg(build_retry_attempt_msg(nivel_label))
        save_state()
        return

    label = sig_tipo or "Señal"
    key = sig_tipo_key or "desconocido"

    # Reintento de señal de tiempo: si perdió, incrementar fail_count y preparar reintento
    global timing_retry_pred
    if key == 'timing_3x_5x':
        if win:
            timing_retry_pred = None
        else:
            # Buscar la predicción activa y aumentar fail_count
            for r in recorded_times:
                if r['alert_shown']:
                    r['fail_count'] = r.get('fail_count', 0) + 1
                    logger.info(f"[Timing] 🔼 fail_count aumentado a {r['fail_count']} para la predicción")
                    break
            # Reintentar solo si no es la última señal y estamos dentro de la ventana posterior ampliada
            if not is_last_signal_of_session:
                ahora = colombia_now()
                actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
                for r in recorded_times:
                    diff = r['tiempo_seg'] - actual
                    post_window = TIMING_ALERT_WINDOW_SEC + r.get('fail_count', 0) * 2
                    if r['alert_shown'] and diff >= -post_window:
                        timing_retry_pred = r['tiempo_seg']
                        logger.info(f"[Timing] 🔁 Pérdida dentro de ventana — reintento habilitado "
                                    f"(fail_count={r['fail_count']})")
                        break

    attempt_when_win = sig_attempt if win else None
    if sig_signal_id and sig_context_json:
        result = "win" if win else "loss"
        update_signal_context_result(sig_signal_id, attempt_when_win, result)

    if not shadow_actual:
        current_session_results.append(win)

    if win:
        logger.info(f"[v21] ✅ GANAMOS — {value:.2f}x | {label}")
        log_pattern_result(key, label, "win", value, attempt=signal_index, features_json=sig_features)
        if favorable_actual:
            # Señal (nivel C1/C2/C3) ganada — suma a la racha de señales,
            # independiente de si esto además cierra la sesión.
            consecutive_signal_wins += 1
            if not shadow_actual:
                record_dashboard_attempt(True, pending_signal_index, sig_attempt)
            if sig_retry_msg_id:
                await delete_msg(sig_retry_msg_id)
                sig_retry_msg_id = None
            await send_signal_msg(build_win_msg(value, sig_attempt))
            await send_stats_msg(build_win_status_msg(sig_attempt))
    else:
        logger.info(f"[v21] ❌ PERDIMOS — {value:.2f}x | {label}")
        log_pattern_result(key, label, "loss", value, attempt=signal_index, features_json=sig_features)
        if favorable_actual:
            # Señal (nivel C1/C2/C3) perdida — resetea la racha de señales.
            consecutive_signal_wins = 0
            if not shadow_actual:
                record_dashboard_attempt(False, pending_signal_index, sig_attempt)
            if sig_retry_msg_id:
                await delete_msg(sig_retry_msg_id)
                sig_retry_msg_id = None
            nivel_label = nivel_senal_label(pending_signal_index)
            siguiente_index = pending_signal_index + 1 if not is_last_signal_of_session else 1
            siguiente_label = nivel_senal_label(siguiente_index)
            intento_actual = intento_global_actual(pending_signal_index, sig_attempt, sig_last_attempt)
            intento_total = get_session_max_signals() * get_max_attempts()
            await send_signal_msg(build_level_loss_msg(
                nivel_label, siguiente_label, sig_attempt_values, intento_actual, intento_total))
            await send_stats_msg(build_loss_status_msg(sig_attempt))

    if favorable_actual:
        await update_trend_status_msg(list(history), resolved=True)

    # Limpiar estado de señal ANTES de evaluar la sesión: send_stats_update()
    # exige sig_state=="idle" para enviar, así que si se limpia después, el
    # mensaje de estadísticas de sesión nunca sale (bug anterior).
    sig_state = "idle"
    sig_attempt = 0
    sig_msg_id = None
    sig_tipo = None
    sig_tipo_key = None
    sig_features = None
    sig_inmediata = False
    sig_context_json = None
    sig_signal_id = None
    sig_favorable = True
    sig_shadow = False
    sig_retry_msg_id = None
    sig_attempt_values = []

    if shadow_actual:
        # Señal SOMBRA resuelta (tendencia desfavorable): no toca la sesión
        # visible. pending_signal_index se restaura al valor congelado de
        # session_signal_count (el que ya tenía ANTES de esta señal sombra),
        # e is_last_signal_of_session vuelve a False, replicando el mismo
        # invariante de "idle entre niveles" que usa la sesión real. Así,
        # cuando la tendencia vuelva a ser favorable, la próxima señal sigue
        # exactamente en el nivel pendiente real (C1/C2/C3), sin haber
        # avanzado ni terminado la sesión por señales que el usuario nunca vio.
        pending_signal_index = session_signal_count
        is_last_signal_of_session = False
        # Registrar el resultado para la feature de "humedad" del patrón
        # (shadow_win_rate_reciente/shadow_last_result) del modelo de timing.
        shadow_results_recent.append(win)
        if len(shadow_results_recent) > SHADOW_RESULTS_MAX:
            shadow_results_recent.pop(0)
        logger.info(
            f"[v22] 🌒 Señal sombra resuelta ({'win' if win else 'loss'}, tendencia desfavorable) — "
            f"sesión real sigue pendiente en nivel "
            f"{nivel_senal_label(session_signal_count) if session_signal_count else '(sin sesión activa)'}"
        )
        save_state()
        return

    # La sesión termina apenas se gana UN nivel (C1, C2 o C3), o cuando se
    # pierden todos los niveles de la sesión sin ningún acierto.
    session_ends = win or is_last_signal_of_session
    if session_ends:
        session_won = win  # si llegamos acá por is_last_signal_of_session sin ganar, ya perdió todos los niveles
        max_niveles = get_session_max_signals()
        # Solo se contabilizan en las estadísticas del día las sesiones cuya
        # señal SÍ se envió a Telegram (tendencia favorable, ya sea en fase
        # de entrenamiento o en vivo). Las sesiones sombra (tendencia
        # desfavorable) se siguen registrando para el modelo, pero no suman
        # ni restan acá.
        contabiliza = favorable_actual
        if session_won:
            if contabiliza:
                daily_wins += 1
                consecutive_wins += 1
                consecutive_losses = 0
            logger.info(f"[v21] 🏆 Sesión GANADA (acierto en el nivel {pending_signal_index}/{max_niveles})"
                        + ("" if contabiliza else " — no contabilizada (no se envió a Telegram)"))
        else:
            if contabiliza:
                daily_losses += 1
                consecutive_losses += 1
                # La racha de sesiones ganadas consecutivas solo se corta al
                # perder 3 sesiones SEGUIDAS (antes se cortaba con la primera
                # derrota); hasta 2 derrotas seguidas no la afectan.
                if consecutive_losses >= 3:
                    consecutive_wins = 0
                await send_signal_msg(build_session_loss_msg(value))
            racha_txt = ("racha ganada reseteada (3 derrotas seguidas)" if contabiliza and consecutive_losses >= 3
                         else f"racha ganada intacta ({consecutive_losses}/3 derrotas seguidas)" if contabiliza
                         else "racha ganada reseteada")
            logger.info(f"[v22] ❌ Sesión PERDIDA ({max_niveles} niveles fallidos, sin aciertos) — {racha_txt}"
                        + ("" if contabiliza else " — no contabilizada (no se envió a Telegram)"))

        # Resetear estado de sesión
        current_session_results = []
        pending_signal_index = 0
        is_last_signal_of_session = False
        session_signal_count = 0
        save_state()
        # Mensaje de estadísticas de sesión (resultado del día, % acierto,
        # racha) — va al chat de señales, igual que el resto de los avisos.
        if contabiliza:
            await send_stats_update()
        # Chequeo de fin de fase de entrenamiento: se hace entre sesiones,
        # nunca a mitad de una, para no cambiar niveles/intentos con una
        # sesión ya en curso.
        if not is_trained:
            await maybe_finish_training()

    save_state()

# ─── EMISIÓN DE SEÑAL (extraído para reutilizar en el camino directo y en el
#     camino confirmado por espera) ────────────────────────────────────────
async def emit_signal(value: float, tipo_key: str, label: str, motivo: str,
                      features_json: str, confirmada_por_espera: bool):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global sig_emit_attempt, sig_context_json, sig_signal_id
    global session_signal_count, pending_signal_index, is_last_signal_of_session
    global sig_favorable, sig_shadow
    global sig_retry_msg_id, sig_attempt_values
    global dashboard_session_start_id

    signal_id = f"{tipo_key}_{int(datetime.utcnow().timestamp() * 1000)}"
    sig_signal_id = signal_id
    ronda_predicha = None
    sig_retry_msg_id = None
    sig_attempt_values = []

    try:
        features_dict = json.loads(features_json)
        # Se guarda si la señal se emitió directa o recién tras confirmarse
        # con una ronda <2x — así el modelo de timing puede aprender, con
        # datos suficientes, cuál de los dos caminos conviene más.
        features_dict['confirmada_por_espera'] = int(confirmada_por_espera)
        timing_pred = predict_timing(features_dict)
        # Aprende en qué ronda (1, 2 o 3) conviene más entrar, según el
        # historial de attempt_when_win — reemplaza el antiguo bypass fijo.
        ronda_predicha, _prob_ronda = elegir_ronda_entrada(timing_pred)
        features_dict['ronda_predicha'] = ronda_predicha
        emit_attempt = decide_emit_attempt(timing_pred, es_inmediata=False)
        if emit_attempt == 0:
            logger.info(f"[v22] 🛑 ML Timing indica no emitir — señal descartada | {tipo_key}")
            sig_signal_id = None
            return
        sig_emit_attempt = 1
        sig_context_json = json.dumps(features_dict, default=str)
        log_signal_context(signal_id, tipo_key, value, None, "pending", sig_context_json)
    except Exception as e:
        logger.warning(f"[v22] Error en ML timing: {e}")
        sig_emit_attempt = 1
        sig_context_json = None

    # Porcentajes de rangos para mostrar en el mensaje — la tendencia ya se
    # validó como favorable en check_timing_round_trigger antes de llegar
    # acá (única fuente: solo se consideran señales de tendencia favorable,
    # ya no hay señales sombra).
    pct1, pct2 = calc_pct_rangos(list(history))
    sig_favorable = True
    sig_shadow = False

    # Niveles dentro de la sesión: 6 en fase de entrenamiento, 3 —C1/C2/C3— en vivo.
    max_niveles = get_session_max_signals()
    pending_signal_index = session_signal_count + 1
    is_last_signal_of_session = (pending_signal_index == max_niveles)
    if is_last_signal_of_session:
        session_signal_count = 0
    else:
        session_signal_count = pending_signal_index
    if pending_signal_index == 1:
        # Arranca una sesión nueva (C1, intento 1) — el panel HTML dispara
        # window.sStart() en la gestión de dinero real al detectar este id.
        dashboard_session_start_id += 1

    sig_tipo = label
    sig_tipo_key = tipo_key
    sig_features = features_json
    sig_inmediata = True
    sig_state = "active"
    sig_attempt = 1
    sig_last_attempt = get_max_attempts()

    if sig_favorable:
        text = build_signal_msg(label, value, pending_signal_index, pct1, pct2, ronda_predicha=ronda_predicha)
        sig_msg_id = await send_signal_msg(text, no_preview=True)
    else:
        sig_msg_id = None
    save_state()
    origen = "confirmada tras espera <2x" if confirmada_por_espera else "directa"
    fase = "EN VIVO" if is_trained else "ENTRENAMIENTO (silencioso)"
    tendencia = "FAVORABLE" if sig_favorable else "DESFAVORABLE (sombra, sesión congelada, sin Telegram)" if sig_shadow else "DESFAVORABLE (2 planos, sin Telegram)"
    logger.info(f"[v21] ⚡ Señal emitida ({origen}, {fase}, tendencia {tendencia}): {tipo_key} — {motivo} "
                f"(nivel {pending_signal_index}/{max_niveles}, intentos/nivel: {sig_last_attempt}, "
                f"pct1={pct1:.2f}%, pct2={pct2:.2f}%)")

# ─── PROCESAMIENTO CENTRAL — v21 ─────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history
    global sig_state, pending_signal_index

    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]
    save_value(value)
    # Se alimenta siempre, aunque el modo sea silencioso (carga inicial de
    # historial), para que el predictor de tiempo arranque con contexto real
    # desde el primer dato disponible.
    calcular_prediccion_inteligente(value)
    if silent:
        return
    logger.info(f"Nueva cuota: {value:.2f}x | hist:{len(history)} | estado:{sig_state}")

    if sig_state == "active":
        await resolve_active(value, pending_signal_index)
        return

    vals = list(history)
    if is_trained:
        await update_trend_status_msg(vals, resolved=False)

    # Única fuente de señales: el predictor de horario (rebote 3x-5x). Las
    # señales de tendencia (cruce EMA) y líneas calientes 5x/10x se
    # eliminaron — se dejan solamente las señales de tiempo.
    await check_timing_round_trigger()

# ─── CONEXIÓN WEBSOCKET — Pragmatic Play (Spaceman) ──────────────────────────
# Estructura real confirmada con tráfico en vivo (log DEBUG_WS_PAYLOAD): cada
# mensaje del WS trae un HISTORIAL de ~20 rondas (no una ronda nueva por
# mensaje), ordenadas de más nueva a más vieja, cada una con su propio
# "gameId" (numérico, único y creciente) y "time". Ejemplo real:
#   [{"gameId": "17505677920", "result": "15.99", "time": "..."},
#    {"gameId": "17505677820", "result": "1.12",  "time": "..."}, ...]
# Los dos intentos anteriores fallaron porque asumían que el mensaje traía
# una sola ronda nueva y comparaban solo el valor de la cuota (o una ventana
# de tiempo): como el mismo historial se reenvía en cada mensaje, la cuota
# "más reciente" aparecía repetida en mensajes sucesivos y se perdían o
# duplicaban rondas según el enfoque.
# Fix definitivo: se usa el "gameId" real (nunca el valor de la cuota) para
# saber qué rondas son nuevas. En cada mensaje se procesan, en orden
# cronológico, todas las rondas del historial cuyo gameId sea posterior al
# último gameId ya procesado.
ws_conn_status = "disconnected"   # disconnected | connecting | connected | error
ws_conn_detail  = ""
last_game_id: Optional[int] = None

# Log temporal para inspeccionar el payload crudo. DEBUG_WS_PAYLOAD=1 en el
# entorno lo activa; dejar en 0 (default) en producción normal.
DEBUG_WS_PAYLOAD = os.environ.get("DEBUG_WS_PAYLOAD", "0") == "1"

def set_ws_status(state: str, detail: str = ""):
    global ws_conn_status, ws_conn_detail
    ws_conn_status = state
    ws_conn_detail = detail

def _get_val(item: dict) -> Optional[float]:
    v = item.get("result")
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None

def _get_game_id(item: dict) -> Optional[int]:
    v = item.get("gameId")
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None

async def ws_loop():
    global last_game_id
    RECONNECT_DELAY = 5
    set_ws_status("connecting", "🟡 CONECTANDO...")
    while True:
        try:
            logger.info(f"Conectando WebSocket: {WS_URL}")
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
                await ws.send(json.dumps({
                    "type": "subscribe", "casinoId": CASINO_ID,
                    "currency": CURRENCY, "key": [GAME_ID],
                }))
                logger.info(f"Suscrito a game {GAME_ID}")
                set_ws_status("connected", "🟢 CONECTADO — esperando rondas...")
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    game_results = data.get("gameResult", [])
                    if not game_results:
                        continue
                    if DEBUG_WS_PAYLOAD:
                        logger.info(
                            f"[DEBUG_WS] gameResult trae {len(game_results)} elemento(s) | "
                            f"contenido: {json.dumps(game_results, default=str)}"
                        )
                    # Parseamos cada item del historial: (gameId, valor).
                    parsed = []
                    for item in game_results:
                        gid = _get_game_id(item)
                        val = _get_val(item)
                        if gid is None or val is None:
                            continue
                        parsed.append((gid, val))
                    if not parsed:
                        continue

                    if last_game_id is None:
                        # Primer mensaje: fijamos el punto de partida en la
                        # ronda más reciente del historial, SIN procesar las
                        # 19 rondas viejas como si fueran nuevas (mismo
                        # criterio de "sin backfill" que la versión anterior).
                        last_game_id = max(gid for gid, _ in parsed)
                        set_ws_status("connected", f"🟢 CONECTADO — punto de partida: gameId {last_game_id}")
                        logger.info(f"Punto de partida fijado en gameId {last_game_id} (sin backfill de historial)")
                        continue

                    nuevos = [(gid, val) for gid, val in parsed if gid > last_game_id]
                    if nuevos:
                        # El historial viene de más nueva a más vieja: se
                        # ordena ascendente para procesar en orden cronológico
                        # y no invertir la secuencia real de rondas.
                        nuevos.sort(key=lambda x: x[0])
                        for gid, val in nuevos:
                            last_game_id = gid
                            set_ws_status("connected", f"🟢 CONECTADO — nueva ronda: {val:.2f}x")
                            await process_new_value(val, silent=False)
                    else:
                        set_ws_status("connected", "🟢 CONECTADO — sin rondas nuevas")

                    try:
                        await check_timing_predictions()
                    except Exception as e:
                        logger.warning(f"Error en check_timing_predictions: {e}")
        except Exception as e:
            logger.error(f"WS error: {e} — reconectando en {RECONNECT_DELAY}s")
            set_ws_status("error", str(e))
            await asyncio.sleep(RECONNECT_DELAY)

async def timing_predictions_ticker():
    """Limpieza periódica de predicciones de tiempo vencidas (antes se hacía
    en cada tick del polling HTTP; con WebSocket las rondas no llegan a
    intervalo fijo, así que corre en su propio loop independiente)."""
    while True:
        await asyncio.sleep(2)
        try:
            await check_timing_predictions()
        except Exception as e:
            logger.warning(f"Error en check_timing_predictions: {e}")

# ─── DASHBOARD HTML — panel visual servido en '/' ───────────────────────────
# Mismo diseño de AVIATOR_V27.html, con el cálculo propio de señales (EMA/
# tendencia, líneas calientes, predictor de horario) desactivado: el panel
# solo dibuja el gráfico con las rondas reales y refleja, vía polling a
# /api/state, las señales y resultados que este bot YA procesó y mandó a
# Telegram — incluida la gestión de dinero real 2x (columnas C1/C2/C3).
DASHBOARD_HTML = '<!DOCTYPE html>\n<html lang="es">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">\n<title>AVIATOR GENESIS 20.0</title>\n<link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@600;700;900&family=Share+Tech+Mono&display=swap" rel="stylesheet">\n<style>\n*{margin:0;padding:0;box-sizing:border-box;-webkit-tap-highlight-color:transparent}\n:root{\n  --bg:#080e1a;--bg2:#0b1220;--surface:rgba(255,255,255,.04);\n  --border:rgba(255,255,255,.1);--blue:#00d4ff;--green:#00ff88;--red:#ff2d55;\n  --chip-h:32px;\n}\nhtml,body{height:100%;overflow:hidden;background:var(--bg);color:#e8f4ff;font-family:\'Share Tech Mono\',monospace}\n/* safe-area env support */\n@supports(padding-top:env(safe-area-inset-top)){\n  body{\n    padding-top:env(safe-area-inset-top);\n    padding-bottom:env(safe-area-inset-bottom);\n    padding-left:env(safe-area-inset-left);\n    padding-right:env(safe-area-inset-right);\n  }\n}\n/* ── APP SHELL ── */\n#app{display:flex;flex-direction:column;height:100vh;height:calc(var(--vh,1vh)*100);width:100vw;overflow:hidden}\n\n/* ── TOP CHIP BAR ── */\n#topBar{\n  display:flex;align-items:center;gap:6px;\n  padding:6px 8px 5px;\n  background:linear-gradient(180deg,rgba(5,10,22,1) 0%,rgba(8,14,26,.95) 100%);\n  border-bottom:1px solid rgba(255,255,255,.06);\n  flex-shrink:0;min-height:44px;overflow:hidden;\n}\n#chipsScroll{display:flex;gap:5px;overflow-x:auto;flex:1;align-items:center;scrollbar-width:none;padding-bottom:1px}\n#chipsScroll::-webkit-scrollbar{display:none}\n.chip{\n  display:inline-flex;align-items:center;justify-content:center;\n  padding:0 10px;height:var(--chip-h);border-radius:999px;\n  font-family:\'Share Tech Mono\',monospace;font-size:12px;font-weight:700;\n  white-space:nowrap;border:1.5px solid;cursor:default;flex-shrink:0;\n  transition:opacity .2s;letter-spacing:.3px;\n  background:rgba(0,0,0,.55);\n}\n/* chip range colors */\n.chip-low{border-color:rgba(160,160,180,.4);color:rgba(220,220,240,.85)} /* <2x */\n.chip-mid{border-color:rgba(80,160,255,.6);color:rgba(160,210,255,.9)}   /* 2-5x */\n.chip-high{border-color:rgba(255,200,0,.6);color:rgba(255,220,80,.9)}    /* 5-10x */\n.chip-max{border-color:rgba(255,80,80,.7);color:rgba(255,140,120,.9)}    /* >=10x */\n.chip.newest{border-color:var(--blue);color:#fff;box-shadow:0 0 8px rgba(0,212,255,.35);background:rgba(0,60,100,.4)}\n\n/* top right icons */\n.top-icons{display:flex;gap:4px;flex-shrink:0}\n.icon-btn{\n  width:34px;height:34px;border-radius:50%;background:rgba(255,255,255,.06);\n  border:1px solid rgba(255,255,255,.12);display:flex;align-items:center;justify-content:center;\n  cursor:pointer;font-size:15px;color:rgba(255,255,255,.7);transition:all .2s;flex-shrink:0;\n}\n.icon-btn:hover{background:rgba(255,255,255,.12);border-color:rgba(255,255,255,.25);color:#fff}\n\n/* ── CHART AREA ── */\n#chartWrap{\n  position:relative;flex:1;overflow:hidden;\n  background:radial-gradient(ellipse at 50% 40%, rgba(0,40,15,.45) 0%, rgba(5,12,5,.9) 60%, rgba(2,6,2,1) 100%);\n}\n#chart{width:100%;height:100%;display:block;cursor:crosshair}\n.chart-overlay{position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none}\n\n/* Y-AXIS LABELS (right side) */\n#yAxis{position:absolute;top:0;right:0;height:100%;display:flex;flex-direction:column;justify-content:space-between;padding:8px 4px;pointer-events:none;z-index:2}\n.y-label{font-family:\'Share Tech Mono\',monospace;font-size:10px;color:rgba(255,255,255,.3);text-align:right;line-height:1}\n\n\n/* ALERT ROW — fila de señal sobre pctStrip */\n#alertRow{\n  display:none !important;\n  flex-shrink:0;align-items:center;justify-content:center;gap:8px;\n  padding:6px 14px;min-height:36px;text-align:center;\n  background:rgba(5,10,20,.98);border-top:1px solid rgba(255,255,255,.05);\n  font-family:\'Orbitron\',monospace;font-size:11px;font-weight:700;letter-spacing:1.2px;\n  color:rgba(255,255,255,.3);transition:background .3s,border-top-color .3s,color .3s;\n}\n#alertRow.ar-neutral{color:rgba(255,255,255,.3);border-top-color:rgba(255,255,255,.05)}\n#alertRow.ar-bull{color:#ffd700;border-top-color:rgba(255,200,0,.4);background:rgba(18,14,2,.98);box-shadow:0 -2px 12px rgba(255,200,0,.08)}\n#alertRow.ar-win{color:#00ff88;border-top-color:rgba(0,255,136,.35);background:rgba(0,14,8,.98);box-shadow:0 -2px 12px rgba(0,255,136,.08)}\n#alertRow.ar-loss{color:#ff2d55;border-top-color:rgba(255,45,85,.35);background:rgba(14,2,6,.98);box-shadow:0 -2px 12px rgba(255,45,85,.08)}\n#alertRow.ar-so{color:#ff9900;border-top-color:rgba(255,140,0,.35);background:rgba(16,8,0,.98);box-shadow:0 -2px 12px rgba(255,140,0,.08)}\n#alertRow .ar-icon{font-size:14px;font-family:\'Share Tech Mono\',monospace}\n#alertRow .ar-main{font-size:11px;font-weight:700;font-family:\'Orbitron\',monospace;letter-spacing:1.5px}\n#alertRow .ar-sub{font-size:9px;opacity:.6;font-family:\'Share Tech Mono\',monospace;letter-spacing:.5px;margin-left:4px}\n#alertRow .ar-bet{font-size:11px;font-weight:900;font-family:\'Orbitron\',monospace;margin-left:8px;padding:2px 8px;border-radius:6px;background:rgba(255,200,0,.15);border:1px solid rgba(255,200,0,.4);color:#ffd700;letter-spacing:.5px}\n\n/* ALERT CIRCLES — ocultos, reemplazados por alertRow */\n.alert-circle-panel{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);display:none!important;z-index:100;pointer-events:none}\n.alert-circle{position:relative;width:200px;height:200px;border-radius:50%;background:radial-gradient(circle at center,rgba(0,30,70,.92),rgba(0,15,40,.96),rgba(0,5,15,.99));border:2px solid rgba(0,212,255,.4);display:flex;align-items:center;justify-content:center}\n.alert-circle::before{content:"";position:absolute;inset:-7px;border-radius:50%;border:2.5px solid transparent;border-top-color:rgba(0,212,255,.75);border-right-color:rgba(0,212,255,.25);pointer-events:none}\n.alert-circle-content{text-align:center;color:white;width:85%;padding:10px}\n.alert-circle-title{font-size:11px;font-weight:700;margin-bottom:5px;color:#fff;font-family:\'Orbitron\',monospace;letter-spacing:1px}\n.alert-circle-message{font-size:10px;margin-bottom:4px;opacity:.9}\n.alert-circle-emoji{font-size:32px;margin:4px 0;display:block}\n.alert-circle-value{font-size:24px;font-weight:900;margin:4px 0;background:linear-gradient(90deg,#00d4ff,#00ff88);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent;font-family:\'Orbitron\',monospace}\n.alert-circle-info{font-size:9px;opacity:.7;margin-top:3px}\n.alert-circle-panel-small{position:absolute;display:none!important;z-index:100;pointer-events:none;top:10px;left:10px}\n.alert-circle-small{position:relative;width:120px;height:120px;border-radius:50%;background:radial-gradient(circle at center,rgba(60,0,120,.88),rgba(30,0,80,.93));border:2px solid rgba(139,0,255,.45);display:flex;align-items:center;justify-content:center}\n.alert-circle-small .alert-circle-content{width:88%;padding:6px}\n.alert-circle-small .alert-circle-title{font-size:9px;color:#d4aaff}\n.alert-circle-small .alert-circle-message{font-size:8px}\n.alert-circle-small .alert-circle-value{font-size:18px;background:linear-gradient(90deg,#c060ff,#00a8ff);-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}\n.alert-circle-small .alert-circle-info{font-size:8px}\n.alert-circle-small.pre-alert{background:radial-gradient(circle at center,rgba(100,40,0,.88),rgba(60,20,0,.93));border-color:rgba(255,165,0,.45)}\n@keyframes circleBreath{0%,100%{box-shadow:0 0 12px rgba(0,255,255,.08)}50%{box-shadow:0 0 40px rgba(0,255,255,.25)}}\n@keyframes circleBreathBull{0%,100%{box-shadow:0 0 14px rgba(0,212,255,.12)}50%{box-shadow:0 0 22px rgba(0,212,255,.2)}}\n@keyframes circleBreathBear{0%,100%{box-shadow:0 0 14px rgba(255,45,85,.12)}50%{box-shadow:0 0 22px rgba(255,45,85,.2)}}\n@keyframes circleBreathWin{0%,100%{box-shadow:0 0 14px rgba(0,255,136,.14)}50%{box-shadow:0 0 24px rgba(0,255,136,.22)}}\n@keyframes circleBreathLoss{0%,100%{box-shadow:0 0 14px rgba(255,45,85,.14)}50%{box-shadow:0 0 24px rgba(255,45,85,.22)}}\n/* ── PREDICCIÓN INTELIGENTE CIRCLE ── */\n#predCircleOverlay{position:absolute;top:30px;left:30px;z-index:90;pointer-events:none;display:none}\n/* Diseño AMX v20 (.alert-circle-small) */\n.pred-circle{\n  position:relative;width:150px;height:150px;border-radius:50%;\n  background:radial-gradient(circle at center,rgba(60,0,120,.88),rgba(30,0,80,.93),rgba(5,0,15,.98));\n  border:2px solid rgba(139,0,255,.45);\n  display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;\n  color:#fff;text-shadow:0 2px 4px rgba(0,0,0,.7);\n}\n.pred-circle::before{\n  content:\'\';position:absolute;inset:-6px;border-radius:50%;\n  border:2px solid transparent;\n  border-top-color:rgba(139,0,255,.75);\n  border-right-color:rgba(139,0,255,.25);\n  pointer-events:none;\n}\n.pred-circle::after{\n  content:\'\';position:absolute;inset:-12px;border-radius:50%;\n  border:1.5px solid transparent;\n  border-bottom-color:rgba(0,168,255,.5);\n  border-left-color:rgba(0,168,255,.18);\n  pointer-events:none;\n}\n@keyframes predPulse{\n  0%,100%{box-shadow:0 0 12px rgba(139,0,255,.18),0 0 24px rgba(0,168,255,.08),inset 0 0 10px rgba(100,0,200,.05)}\n  50%{box-shadow:0 0 20px rgba(139,0,255,.28),0 0 36px rgba(0,168,255,.14),inset 0 0 16px rgba(100,0,200,.09)}\n}\n.pred-circle.active{animation:predPulse 1.8s ease-in-out infinite}\n.pred-circle-label{font-size:11px;font-weight:700;margin-bottom:4px;color:#d4aaff;font-family:\'Orbitron\',monospace;letter-spacing:.5px}\n.pred-circle-range{font-size:9px;margin-bottom:4px;opacity:.85;font-family:\'Share Tech Mono\',monospace}\n.pred-circle-time{font-size:22px;font-weight:900;margin:4px 0;font-family:\'Orbitron\',monospace;background:linear-gradient(90deg,#c060ff,#00a8ff);-webkit-background-clip:text;background-clip:text;color:transparent;text-shadow:none}\n.pred-circle-info{font-size:9px;opacity:.7;margin-top:3px;font-family:\'Share Tech Mono\',monospace;letter-spacing:.5px}\n/* ── VOZ TOGGLE PILL ── */\n#voiceToggle{position:absolute;bottom:8px;left:8px;z-index:95;background:rgba(0,0,0,.55);border:1px solid rgba(180,0,255,.35);border-radius:20px;padding:4px 10px;font-size:10px;font-family:\'Share Tech Mono\',monospace;color:rgba(180,100,255,.8);cursor:pointer;display:flex;align-items:center;gap:5px}\n#voiceToggle.muted{border-color:rgba(255,255,255,.15);color:rgba(255,255,255,.3)}\n.imi-floating-alert{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);transition:top .2s;background:linear-gradient(135deg,rgba(6,13,30,.94),rgba(3,8,20,.94));border:1px solid rgba(0,212,255,.5);border-radius:14px;padding:12px 18px;min-width:220px;text-align:center;z-index:100;display:none}\n.imi-floating-alert.below-signal{top:calc(50% + 84px)}\n.imi-floating-alert.bearish{border-color:rgba(255,45,85,.7)}.imi-floating-alert.bullish{border-color:rgba(0,255,136,.7)}.imi-floating-alert.momentum{border-color:rgba(255,165,0,.7)}\n\n/* ── ALERTA FLOTANTE MODERADO (azul) ── */\n.mod-floating-alert{\n  position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);\n  background:linear-gradient(135deg,rgba(6,13,30,.96),rgba(3,8,20,.96));\n  border:2px solid rgba(0,140,255,.75);border-radius:14px;\n  padding:13px 22px;width:max-content;min-width:0;max-width:92%;text-align:center;\n  z-index:96;display:none;\n  box-shadow:0 0 24px rgba(0,140,255,.35),inset 0 0 14px rgba(0,140,255,.06);\n}\n.mod-floating-alert.pulse{animation:modAlertPulse 1.4s ease-in-out infinite}\n@keyframes modAlertPulse{\n  0%,100%{box-shadow:0 0 16px rgba(0,140,255,.3),inset 0 0 12px rgba(0,140,255,.05)}\n  50%{box-shadow:0 0 36px rgba(0,140,255,.7),inset 0 0 20px rgba(0,140,255,.1)}\n}\n.mod-alert-icon{font-size:26px;margin-bottom:4px;display:block}\n.mod-alert-won{font-size:12px;font-weight:800;margin-bottom:5px;text-transform:uppercase;letter-spacing:1px;font-family:\'Orbitron\',monospace;color:#00ff88;white-space:nowrap}\n.mod-alert-title{font-size:12px;font-weight:700;margin-bottom:3px;text-transform:uppercase;letter-spacing:1px;font-family:\'Orbitron\',monospace;color:#7fd0ff;white-space:nowrap}\n.mod-alert-detail{font-size:11px;opacity:.95;font-family:\'Share Tech Mono\',monospace;color:#cfeaff;font-weight:700}\n/* ── PILL DE TENDENCIA (debajo de la alerta flotante) ── */\n.mod-trend-pill{\n  position:absolute;bottom:8px;left:50%;transform:translateX(-50%);\n  z-index:95;display:none;padding:6px 16px;border-radius:30px;\n  font-size:10px;font-weight:800;letter-spacing:1px;font-family:\'Orbitron\',monospace;\n  background:rgba(5,15,25,.92);border:1px solid rgba(255,255,255,.15);color:rgba(255,255,255,.6);\n}\n.mod-trend-pill.alcista{color:#00ff88;border-color:rgba(0,255,136,.6);box-shadow:0 0 12px rgba(0,255,136,.25)}\n.mod-trend-pill.bajista{color:#ff2d55;border-color:rgba(255,45,85,.6);box-shadow:0 0 12px rgba(255,45,85,.25)}\n.imi-alert-icon{font-size:24px;margin-bottom:4px;display:block}\n.imi-alert-title{font-size:11px;font-weight:700;margin-bottom:3px;text-transform:uppercase;letter-spacing:1px;font-family:\'Orbitron\',monospace}\n.imi-alert-detail{font-size:10px;opacity:.75}\n.imi-alert-timer{font-size:9px;opacity:.5;margin-top:4px}\n.fractal-arrow-up{position:absolute;width:0;height:0;border-left:12px solid transparent;border-right:12px solid transparent;border-bottom:20px solid #00ff88;filter:drop-shadow(0 0 8px #00ff88);z-index:50}\n.fractal-arrow-down{position:absolute;width:0;height:0;border-left:12px solid transparent;border-right:12px solid transparent;border-top:20px solid #ff2d55;filter:drop-shadow(0 0 8px #ff2d55);z-index:50}\n.tooltip{position:absolute;background:rgba(0,0,0,.92);color:white;padding:6px 10px;border-radius:8px;font-size:11px;pointer-events:none;z-index:200;display:none;border:1px solid rgba(0,212,255,.2)}\n\n/* ── IMI CHART ── */\n#imiWrap{\n  flex-shrink:0;height:64px;position:relative;\n  background:rgba(2,6,12,.95);border-top:1px solid rgba(0,212,255,.06);\n  display:none;\n}\n#imiChart{width:100%;height:100%;display:block}\n#imiToggleBtn{position:absolute;top:2px;right:4px;font-size:9px;color:rgba(0,212,255,.4);cursor:pointer;font-family:\'Share Tech Mono\',monospace;z-index:5;padding:2px 6px;background:rgba(0,212,255,.05);border-radius:4px;border:1px solid rgba(0,212,255,.1)}\n\n/* ── BOTTOM ACTION ROW (Historial | Configuración) ── */\n#bottomRow{\n  display:flex;gap:6px;padding:6px 8px;flex-shrink:0;\n  background:rgba(5,10,20,.98);border-top:1px solid rgba(255,255,255,.06);\n}\n.bottom-btn{\n  flex:1;display:flex;align-items:center;justify-content:center;gap:6px;\n  padding:9px 12px;border-radius:12px;cursor:pointer;\n  border:1px solid rgba(255,255,255,.1);background:rgba(255,255,255,.05);\n  font-family:\'Share Tech Mono\',monospace;font-size:12px;font-weight:700;\n  color:rgba(255,255,255,.7);transition:all .2s;letter-spacing:.5px;\n  -webkit-user-select:none;user-select:none;\n}\n.bottom-btn:hover,.bottom-btn.active{background:rgba(255,255,255,.1);border-color:rgba(255,255,255,.2);color:#fff}\n.bottom-btn.active{border-color:rgba(0,212,255,.4);color:var(--blue)}\n.bottom-btn .btn-icon{font-size:14px}\n\n/* ── INPUT BUTTONS ── */\n#inputSection{\n  flex-shrink:0;padding:6px 8px 8px;\n  padding-bottom:calc(8px + env(safe-area-inset-bottom, 0px));\n  background:rgba(5,10,20,.98);border-top:1px solid rgba(255,255,255,.05);\n}\n.input-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px}\n.val-btn{\n  padding:10px 4px;border-radius:10px;cursor:pointer;\n  font-family:\'Share Tech Mono\',monospace;font-size:11px;font-weight:700;\n  text-align:center;border:1.5px solid;transition:all .15s;letter-spacing:.3px;\n  -webkit-user-select:none;user-select:none;\n}\n.val-btn:active{transform:scale(.93);filter:brightness(1.3)}\n.vb-1{background:rgba(0,180,60,.12);border-color:rgba(0,220,80,.4);color:#00dd55}\n.vb-2{background:rgba(180,200,0,.1);border-color:rgba(220,240,0,.4);color:#d4e800}\n.vb-3{background:rgba(0,130,255,.1);border-color:rgba(80,160,255,.4);color:#5ab4ff}\n.vb-4{background:rgba(130,0,255,.1);border-color:rgba(160,60,255,.4);color:#b060ff}\n.vb-5{background:rgba(255,140,0,.1);border-color:rgba(255,170,0,.4);color:#ffaa00}\n.vb-6{background:rgba(255,0,80,.1);border-color:rgba(255,50,100,.4);color:#ff2d55}\n.vb-del{background:rgba(160,160,160,.08);border-color:rgba(160,160,160,.35);color:#9e9e9e}\n.vb-rst{background:rgba(255,100,0,.08);border-color:rgba(255,100,0,.3);color:#ff6400}\n\n/* ── SIDE PANELS (modal-style) ── */\n.side-panel{\n  position:fixed;inset:0;z-index:1000;display:none;\n}\n.side-panel.open{display:flex}\n.panel-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.7);backdrop-filter:blur(4px)}\n.panel-sheet{\n  position:absolute;bottom:0;left:0;right:0;\n  background:linear-gradient(180deg,rgba(8,14,28,.99),rgba(5,10,20,1));\n  border-top:1px solid rgba(0,212,255,.2);border-radius:20px 20px 0 0;\n  max-height:80vh;overflow-y:auto;padding:16px;\n  animation:slideUp .25s ease-out;\n}\n@keyframes slideUp{from{transform:translateY(100%)}to{transform:translateY(0)}}\n.panel-handle{width:36px;height:4px;border-radius:2px;background:rgba(255,255,255,.2);margin:0 auto 16px}\n.panel-title{font-family:\'Orbitron\',monospace;font-size:12px;color:var(--blue);letter-spacing:2px;text-transform:uppercase;margin-bottom:14px}\n\n/* STATS GRID */\n.stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px}\n.stat-card{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:10px}\n.stat-card-label{font-size:9px;color:rgba(255,255,255,.4);letter-spacing:1px;text-transform:uppercase;margin-bottom:4px}\n.stat-card-name{font-family:\'Orbitron\',monospace;font-size:10px;color:var(--blue);margin-bottom:6px;font-weight:700}\n.stat-row{display:flex;justify-content:space-between;font-size:11px;margin-bottom:2px}\n.stat-row .lbl{color:rgba(255,255,255,.4);font-size:10px}\n.stat-row .val{font-weight:700}\n.val-win{color:#00ff88}.val-loss{color:#ff2d55}.val-tot{color:#00d4ff}\n.eff-bar{margin-top:6px;height:3px;background:rgba(255,255,255,.08);border-radius:2px;overflow:hidden}\n.eff-fill{height:100%;background:linear-gradient(90deg,#00ff88,#00d4ff);border-radius:2px;transition:width .5s}\n\n/* CONFIG PANEL */\n.cfg-section{margin-bottom:14px}\n.cfg-title{font-family:\'Orbitron\',monospace;font-size:9px;color:rgba(0,212,255,.5);letter-spacing:2px;text-transform:uppercase;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid rgba(0,212,255,.08)}\n.cfg-row{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:8px}\n.cfg-btn{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);color:rgba(255,255,255,.5);padding:8px 4px;border-radius:8px;cursor:pointer;font-size:11px;font-weight:600;text-align:center;transition:all .2s;-webkit-user-select:none;user-select:none}\n.cfg-btn.active{background:rgba(0,212,255,.15);color:var(--blue);border-color:rgba(0,212,255,.4)}\n.cfg-btn:active{transform:scale(.95)}\n.chk-row{display:grid;grid-template-columns:1fr 1fr;gap:6px}\n.chk-item{display:flex;align-items:center;gap:7px;padding:7px 10px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:8px;cursor:pointer}\n.chk-item label{font-size:11px;color:rgba(255,255,255,.6);cursor:pointer;flex:1}\n.chk-item input{accent-color:var(--blue);width:14px;height:14px;cursor:pointer;flex-shrink:0}\n\n/* PCT STRIP */\n#pctStrip{\n  display:flex;gap:5px;padding:5px 8px;flex-shrink:0;\n  background:rgba(5,10,20,.98);border-top:1px solid rgba(255,255,255,.04);\n  overflow-x:auto;scrollbar-width:none;\n}\n#pctStrip::-webkit-scrollbar{display:none}\n.pct-item{flex:1;min-width:70px;background:rgba(255,255,255,.03);border-radius:8px;padding:5px 8px;text-align:center;border-left:2px solid transparent}\n.pct-item .p-lbl{font-size:8px;color:rgba(255,255,255,.35);text-transform:uppercase;letter-spacing:.5px}\n.pct-item .p-val{font-family:\'Orbitron\',monospace;font-size:15px;font-weight:900;line-height:1.1}\n.pct-r{border-left-color:rgba(255,45,85,.6)}.pct-r .p-val{color:#ff2d55}\n.pct-y{border-left-color:rgba(255,165,0,.6)}.pct-y .p-val{color:#ffa500}\n.pct-g{border-left-color:rgba(0,255,136,.6)}.pct-g .p-val{color:#00ff88}\n.pct-p{border-left-color:rgba(160,0,255,.6)}.pct-p .p-val{color:#b060ff}\n\n/* STRATEGY, EVAL PANELS (same styles used by JS) */\n.evaluation-panels-container{display:flex;flex-direction:column;gap:6px;margin-top:8px}\n.evaluation-panels-container.hidden{display:none}\n.eval-panel-horizontal{background:rgba(4,8,18,.97);border:1px solid rgba(0,212,255,.12);border-left:3px solid rgba(0,212,255,.5);border-radius:10px;padding:8px 12px}\n.eval-panel-top{display:flex;align-items:center;justify-content:space-between;width:100%}\n.eval-panel-bottom{display:flex;padding-top:4px;margin-top:4px;border-top:1px solid rgba(0,150,255,.15)}\n.eval-panel-horizontal h4{font-size:11px;color:#00d4ff;font-weight:700;margin:0;font-family:\'Orbitron\',monospace}\n.eval-stats-horizontal{display:flex;gap:10px;align-items:center}\n.eval-stat-item{display:flex;flex-direction:column;align-items:center;min-width:36px}\n.eval-stat-label{font-size:9px;color:rgba(200,230,255,.6);text-transform:uppercase;letter-spacing:.3px;margin-bottom:2px}\n.eval-stat-value{font-size:13px;font-weight:700;font-family:\'Orbitron\',monospace}\n.eval-stat-value.total{color:#00d4ff}.eval-stat-value.win{color:#00ff88}.eval-stat-value.loss{color:#ff2d55}\n.eval-win-rate{font-size:11px;font-weight:700;color:#00ff88;font-family:\'Orbitron\',monospace}\n#eval150{border-left-color:rgba(0,255,136,.7)} #eval150 h4{color:#00ff88}\n#eval150SO{border-left-color:rgba(255,230,0,.7)} #eval150SO h4{color:#ffe600}\n#eval200{border-left-color:rgba(255,140,0,.7)} #eval200 h4{color:#ff8c00}\n#eval200SO{border-left-color:rgba(255,45,85,.7)} #eval200SO h4{color:#ff2d55}\n#evalWT20{border-left-color:rgba(160,0,255,.7)} #evalWT20 h4{color:#a000ff}\n#evalMomentum{border-left-color:rgba(255,100,200,.7)} #evalMomentum h4{color:#ff64c8}\n.moderate-alert-panels{display:flex;flex-direction:column;gap:6px;margin-top:8px}\n.moderate-alert-panels.hidden{display:none}\n.moderate-alert-panel{background:rgba(4,8,18,.97);border:1px solid rgba(0,212,255,.1);border-radius:10px;padding:8px 12px}\n.moderate-alert-panel h4{margin:0 0 6px 0;font-size:11px;font-family:\'Orbitron\',monospace;color:var(--blue);font-weight:700}\n.moderate-alert-panel .stat{font-size:11px;margin:2px 0;display:flex;justify-content:space-between}\n.moderate-alert-panel .stat-label{color:rgba(200,230,255,.5);font-size:10px}\n.moderate-alert-panel .stat-value{font-weight:700}\n.moderate-alert-panel .total{color:#00d4ff}.moderate-alert-panel .win{color:#00ff88}.moderate-alert-panel .loss{color:#ff2d55}\n#modPanel150{border-left:3px solid rgba(0,255,136,.7)} #modPanel150 h4{color:#00ff88}\n#modPanel200{border-left:3px solid rgba(255,140,0,.7)} #modPanel200 h4{color:#ff8c00}\n#modPanel150_so{border-left:3px solid rgba(255,230,0,.7)} #modPanel150_so h4{color:#ffe600}\n#modPanel200_so{border-left:3px solid rgba(255,45,85,.7)} #modPanel200_so h4{color:#ff2d55}\n\n/* SECTION LABEL */\n.section-lbl{font-family:\'Orbitron\',monospace;font-size:9px;color:rgba(0,212,255,.4);letter-spacing:2px;text-transform:uppercase;margin:12px 0 6px;padding-bottom:4px;border-bottom:1px solid rgba(0,212,255,.06)}\n.eval-toggle-btn{width:100%;background:rgba(0,212,255,.06);color:var(--blue);border:1px solid rgba(0,212,255,.18);padding:8px 12px;border-radius:10px;cursor:pointer;font-weight:600;font-size:11px;display:flex;align-items:center;justify-content:center;gap:6px;margin-bottom:6px;font-family:\'Orbitron\',monospace;letter-spacing:1px;transition:all .2s;-webkit-user-select:none;user-select:none}\n.eval-toggle-btn .icon{font-size:12px;transition:transform .3s}\n.eval-toggle-btn.active .icon{transform:rotate(180deg)}\n\n/* STRATEGY */\n.strat-wrap{margin-top:12px}\n.strat-header{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:rgba(0,30,70,.6);border:1px solid rgba(0,212,255,.2);border-radius:12px;cursor:pointer;-webkit-user-select:none;user-select:none}\n.strat-header h3{font-family:\'Orbitron\',monospace;font-size:10px;color:var(--blue);letter-spacing:2px;margin:0}\n.strat-header .strat-sub{font-size:9px;color:rgba(241,196,15,.6);margin-top:2px}\n.strat-arrow{font-size:10px;color:rgba(0,212,255,.5);transition:transform .3s}\n.strat-header.sopen .strat-arrow{transform:rotate(180deg)}\n.strat-body{background:rgba(4,8,18,.97);border:1px solid rgba(0,212,255,.12);border-radius:12px;padding:12px;margin-top:6px;display:none}\n.strat-body.sopen{display:block}\n.s-info{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin-bottom:10px}\n.s-box{background:rgba(0,0,0,.4);border:1px solid rgba(0,212,255,.08);border-radius:8px;padding:7px 8px;text-align:center}\n.s-box .s-lbl{font-size:9px;color:rgba(0,212,255,.4);letter-spacing:1px;text-transform:uppercase}\n.s-box .s-val{font-family:\'Orbitron\',monospace;font-size:15px;font-weight:900;margin-top:2px}\n.sv-green{color:#00ff88}.sv-red{color:#ff2d55}.sv-blue{color:#00d4ff}.sv-gold{color:#f1c40f}\n.s-alert{background:rgba(0,0,0,.5);border:1px solid rgba(241,196,15,.2);border-radius:8px;padding:8px 10px;text-align:center;margin:8px 0;font-size:11px;min-height:40px;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:3px;color:rgba(255,255,255,.8);transition:border-color .3s}\n.s-alert.sa-pulse{border-color:rgba(241,196,15,.6);animation:saPulse 1.5s infinite}\n@keyframes saPulse{50%{opacity:.7}}\n.s-dots{display:flex;justify-content:center;gap:3px;margin:7px 0;flex-wrap:wrap}\n.s-dot{width:18px;height:18px;border-radius:50%;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.1);display:flex;align-items:center;justify-content:center;font-size:8px;font-weight:700;color:rgba(255,255,255,.3);transition:all .3s}\n.s-dot.sd-done{background:rgba(0,255,136,.12);border-color:rgba(0,255,136,.4);color:#00ff88}\n.s-dot.sd-active{background:rgba(241,196,15,.18);border-color:rgba(241,196,15,.6);color:#f1c40f}\n.s-cols{display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px;margin:7px 0}\n.s-col{background:rgba(0,0,0,.4);border:1px solid rgba(255,255,255,.06);border-radius:8px;padding:6px 4px;text-align:center}\n.s-col .sc-name{font-family:\'Orbitron\',monospace;font-size:9px;color:rgba(0,212,255,.4)}\n.s-col .sc-st{color:rgba(255,255,255,.3);margin:3px 0;font-size:9px}\n.s-col .sc-bet{color:#f1c40f;font-weight:700;font-size:11px;font-family:\'Orbitron\',monospace}\n.s-col.sc-active{border-color:rgba(0,255,136,.4);background:rgba(0,255,136,.05)}\n.s-col.sc-active .sc-name{color:#00ff88}\n.s-col.sc-done{border-color:rgba(0,212,255,.25);background:rgba(0,212,255,.04)}\n.s-col.sc-done .sc-name{color:#00d4ff}\n.s-btns{display:grid;grid-template-columns:1fr 1fr;gap:6px;margin:8px 0 5px}\n.s-btn{padding:9px;border:none;border-radius:8px;font-size:11px;font-weight:700;cursor:pointer;font-family:\'Orbitron\',monospace;letter-spacing:1px;transition:all .2s;-webkit-user-select:none;user-select:none}\n.s-btn:active{transform:scale(.96)}\n.s-btn-win{background:linear-gradient(135deg,rgba(0,255,136,.18),rgba(0,180,90,.22));color:#00ff88;border:1px solid rgba(0,255,136,.35)}\n.s-btn-loss{background:linear-gradient(135deg,rgba(255,45,85,.15),rgba(180,0,40,.2));color:#ff2d55;border:1px solid rgba(255,45,85,.3)}\n.s-btn-reset{background:linear-gradient(135deg,rgba(241,196,15,.1),rgba(220,110,0,.14));color:#f1c40f;border:1px solid rgba(241,196,15,.25);grid-column:span 2;font-size:10px}\n.s-btn-start{width:100%;padding:10px;background:linear-gradient(135deg,rgba(0,212,255,.12),rgba(0,130,220,.16));color:#00d4ff;border:1px solid rgba(0,212,255,.25);border-radius:8px;font-family:\'Orbitron\',monospace;font-size:11px;font-weight:700;letter-spacing:2px;cursor:pointer;margin-top:4px;-webkit-user-select:none;user-select:none}\n.s-hist{margin-top:8px;font-size:10px;max-height:100px;overflow-y:auto}\n.s-hist table{width:100%;border-collapse:collapse}\n.s-hist th{background:rgba(0,212,255,.07);color:rgba(0,212,255,.5);font-size:9px;letter-spacing:1px;padding:3px;position:sticky;top:0;text-transform:uppercase}\n.s-hist td{padding:3px;border-bottom:1px solid rgba(255,255,255,.03);text-align:center;color:rgba(255,255,255,.5)}\n.sh-win{color:#00ff88!important;font-weight:700}.sh-loss{color:#ff2d55!important;font-weight:700}\n.s-cfg-toggle{text-align:center;font-size:10px;color:rgba(0,212,255,.35);cursor:pointer;margin-top:8px;padding:5px;border-top:1px solid rgba(0,212,255,.07);letter-spacing:1px}\n.s-cfg{background:rgba(0,212,255,.04);border:1px solid rgba(0,212,255,.1);border-radius:8px;padding:10px;margin-top:8px;display:none}\n.s-cfg label{display:block;font-size:9px;color:#f1c40f;letter-spacing:1px;margin:7px 0 3px}\n.s-cfg input{width:100%;padding:7px;background:rgba(0,0,0,.5);border:1px solid rgba(0,212,255,.18);color:#fff;border-radius:6px;text-align:center;font-family:\'Orbitron\',monospace;font-size:13px;font-weight:700;outline:none}\n.s-cfg .s-apply{width:100%;margin-top:8px;padding:8px;background:rgba(0,255,136,.12);border:1px solid rgba(0,255,136,.25);border-radius:6px;color:#00ff88;font-family:\'Orbitron\',monospace;font-weight:700;cursor:pointer;font-size:10px;letter-spacing:1px;-webkit-user-select:none;user-select:none}\n\n/* CASINO */\n.casino-access-panel{position:fixed;bottom:0;right:0;left:0;background:linear-gradient(0deg,rgba(6,13,24,.99),rgba(4,9,18,.99));border-top:1px solid rgba(0,212,255,.2);border-radius:20px 20px 0 0;padding:16px;z-index:900;display:none;transform:translateY(100%);transition:transform .3s ease}\n.casino-access-panel.open{display:block;transform:translateY(0)}\n.casino-btn{background:rgba(255,255,255,.06);color:#fff;text-decoration:none;padding:10px 14px;border-radius:10px;font-size:13px;font-weight:500;display:flex;align-items:center;gap:8px;margin-bottom:8px;border:1px solid rgba(255,255,255,.06)}\n.casino-btn:nth-child(2){border-left:3px solid #00a8ff}\n.casino-btn:nth-child(3){border-left:3px solid #ffbd2e}\n.casino-btn:nth-child(4){border-left:3px solid #ff4d4d}\n.casino-btn:nth-child(5){border-left:3px solid #8b00ff}\n\n/* MODAL */\n.s-modal{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.92);display:flex;align-items:center;justify-content:center;z-index:9000;padding:20px}\n.s-modal-box{background:linear-gradient(160deg,rgba(4,10,22,.99),rgba(6,14,32,.99));padding:24px 20px;border-radius:20px;max-width:340px;width:100%;text-align:center}\n.sm-win-box{border:1px solid rgba(0,255,136,.45)}.sm-loss-box{border:1px solid rgba(255,45,85,.45)}\n.s-modal-box .sm-title{font-family:\'Orbitron\',monospace;font-size:17px;font-weight:900;letter-spacing:3px;margin-bottom:18px;color:#fff}\n.sm-row{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border-radius:10px;margin-bottom:6px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.06)}\n.sm-row .sm-label{font-size:12px;color:rgba(255,255,255,.5);letter-spacing:1px;text-transform:uppercase}\n.sm-row .sm-num{font-family:\'Orbitron\',monospace;font-size:16px;font-weight:700;color:#fff}\n.sm-row.sm-highlight-win{background:rgba(0,255,136,.08);border-color:rgba(0,255,136,.25)}\n.sm-row.sm-highlight-win .sm-label{color:rgba(0,255,136,.7)}\n.sm-row.sm-highlight-win .sm-num{color:#00ff88;font-size:20px}\n.sm-row.sm-highlight-loss{background:rgba(255,45,85,.08);border-color:rgba(255,45,85,.25)}\n.sm-row.sm-highlight-loss .sm-label{color:rgba(255,45,85,.7)}\n.sm-row.sm-highlight-loss .sm-num{color:#ff2d55;font-size:20px}\n.sm-divider{height:1px;background:linear-gradient(90deg,transparent,rgba(0,212,255,.2),transparent);margin:10px 0}\n.sm-btn-row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:14px}\n.sm-btn{padding:11px 8px;border:none;border-radius:10px;font-family:\'Orbitron\',monospace;font-size:10px;font-weight:700;letter-spacing:1px;cursor:pointer;-webkit-user-select:none;user-select:none}\n.sm-btn:active{transform:scale(.96)}\n.sm-btn-new{background:linear-gradient(135deg,rgba(0,255,136,.2),rgba(0,180,90,.25));color:#00ff88;border:1px solid rgba(0,255,136,.4)}\n.sm-btn-retire{background:linear-gradient(135deg,rgba(0,212,255,.12),rgba(0,130,220,.18));color:#00d4ff;border:1px solid rgba(0,212,255,.3)}\n.sm-btn-restart{width:100%;padding:11px;background:linear-gradient(135deg,rgba(241,196,15,.15),rgba(220,110,0,.2));color:#f1c40f;border:1px solid rgba(241,196,15,.3);border-radius:10px;font-family:\'Orbitron\',monospace;font-size:11px;font-weight:700;letter-spacing:1px;cursor:pointer;margin-top:12px;-webkit-user-select:none;user-select:none}\n\n/* HIDDEN COMPAT */\n.alert-item{background:rgba(255,45,85,.08);border-left:3px solid var(--red);padding:7px 10px;margin-bottom:7px;border-radius:8px;font-size:11px}\n.alert-item.success{background:rgba(0,255,136,.08);border-left-color:var(--green)}\n.alert-item-row{flex:1;min-width:45%;background:rgba(255,45,85,.08);border-left:3px solid var(--red);padding:6px 9px;border-radius:7px;font-size:10px}\n.alert-item-row.success{background:rgba(0,255,136,.08);border-left-color:var(--green)}\n.alert-item-row.info{background:rgba(0,212,255,.08);border-left-color:var(--blue)}\n.alert-fila-badge{display:inline-block;background:rgba(0,212,255,.2);color:#00d4ff;padding:1px 5px;border-radius:4px;font-size:9px;font-weight:700;margin-right:4px}\n.alert-row{display:flex;gap:7px;margin-bottom:7px;flex-wrap:wrap}\n.values-panel{position:relative}\n.rh,.rh-tip,.status-bar{display:none}\ninput[type="text"],input[type="number"],input[type="time"],select{background:rgba(0,0,0,.45);border:1px solid rgba(0,212,255,.18);color:#e8f4ff;border-radius:8px;padding:8px 12px;font-family:\'Share Tech Mono\',monospace;font-size:13px;outline:none;width:100%}\ninput:focus,select:focus{border-color:var(--blue)}\n.btn{background:linear-gradient(135deg,rgba(0,255,136,.12),rgba(0,212,255,.12));color:var(--green);border:1px solid rgba(0,255,136,.3);padding:9px 16px;border-radius:10px;cursor:pointer;font-weight:600;width:100%;margin-top:5px;font-size:13px;transition:all .2s;-webkit-user-select:none;user-select:none}\n.prediction-panel{margin-top:8px}\n.prediction-panel h4{font-family:\'Orbitron\',monospace;font-size:10px;color:var(--blue);letter-spacing:1px;margin-bottom:7px}\n.prediction-list{margin-top:7px}\n.prediction-tag{display:inline-block;background:rgba(0,212,255,.12);padding:4px 8px;border-radius:6px;margin-right:4px;margin-bottom:4px;font-size:10px;color:var(--blue);border:1px solid rgba(0,212,255,.2)}\n.prediction-tag.close-btn{background:rgba(255,45,85,.12);color:#ff2d55;cursor:pointer;border-color:rgba(255,45,85,.2)}\n::-webkit-scrollbar{width:3px;height:3px}\n::-webkit-scrollbar-track{background:transparent}\n::-webkit-scrollbar-thumb{background:rgba(0,212,255,.15);border-radius:2px}\n</style>\n</head>\n<body>\n<script>\n// ── MOBILE VIEWPORT FIX ──\n// iOS Safari 100vh includes hidden browser chrome; use window.innerHeight instead\n(function(){\n  function setVh(){\n    document.documentElement.style.setProperty(\'--vh\', (window.innerHeight * 0.01) + \'px\');\n  }\n  setVh();\n  window.addEventListener(\'resize\', setVh);\n  // Also fire on orientationchange (iOS)\n  window.addEventListener(\'orientationchange\', function(){ setTimeout(setVh, 200); });\n})();\n</script>\n<div id="app">\n\n<!-- ══ TOP CHIP BAR ══ -->\n<div id="topBar">\n  <div id="chipsScroll">\n    <span style="font-family:\'Share Tech Mono\',monospace;font-size:9px;color:rgba(255,255,255,.25);letter-spacing:1px;white-space:nowrap">Agrega valores...</span>\n  </div>\n  <div class="top-icons">\n    <div class="icon-btn" id="clockBtn" title="Reloj" style="width:auto;padding:0 10px;gap:5px;font-size:11px;border-radius:20px;">&#128336;<span id="currentTime" style="font-family:\'Share Tech Mono\',monospace;font-size:10px;color:rgba(0,212,255,.8);letter-spacing:.5px;white-space:nowrap;"></span></div>\n    <div class="icon-btn" onclick="toggleCasinoAccess()" title="Casinos">&#127920;</div>\n  </div>\n</div>\n\n<!-- ══ CHART ══ -->\n<div id="chartWrap">\n  <canvas id="chart"></canvas>\n\n  <!-- Y-axis -->\n  <div id="yAxis"></div>\n\n  <!-- Alert overlays -->\n  <div class="tooltip" id="tooltip"></div>\n  <div id="alertCirclePanelSmall" class="alert-circle-panel-small">\n    <div class="alert-circle-small" id="alertCircleSmall">\n      <div class="alert-circle-content">\n        <div class="alert-circle-title" id="alertTitleSmall">&#9200; TIEMPO</div>\n        <div class="alert-circle-message" id="alertMessageSmall">Predicci&#243;n alcanzada</div>\n        <div class="alert-circle-value" id="alertValueSmall">00:00</div>\n        <div class="alert-circle-info" id="alertInfoSmall">Verificar entrada</div>\n      </div>\n    </div>\n  </div>\n\n  <!-- 🔮 CÍRCULO PREDICCIÓN INTELIGENTE 3x–5x -->\n  <div id="predCircleOverlay">\n    <div class="pred-circle" id="predCircle">\n      <span class="pred-circle-label">🔮 PREDICCIÓN</span>\n      <span class="pred-circle-range">Buscando rebote 3x – 5x</span>\n      <span class="pred-circle-time" id="predCircleTime">--:--</span>\n      <span class="pred-circle-info">Verificar entrada</span>\n    </div>\n  </div>\n\n  <!-- 🔊 VOZ TOGGLE -->\n  <div id="voiceToggle" onclick="toggleVoice()">\n    <span id="voiceIcon">🔊</span><span id="voiceLabel">VOZ</span>\n  </div>\n\n  <!-- ══ ALERTA FLOTANTE MODERADO (azul) ══ -->\n  <div id="modFloatingAlert" class="mod-floating-alert">\n    <span class="mod-alert-icon" id="modAlertIcon">&#127919;</span>\n    <div class="mod-alert-won" id="modAlertWon" style="display:none"></div>\n    <div class="mod-alert-title" id="modAlertTitle">SE&#209;AL</div>\n    <div class="mod-alert-detail" id="modAlertDetail"></div>\n  </div>\n  <div class="mod-trend-pill" id="modTrendPill"></div>\n\n  <div id="imiFloatingAlert" class="imi-floating-alert">\n    <div class="imi-alert-title" id="imiAlertTitle">Posible Reversi&#243;n</div>\n    <div class="imi-alert-detail" id="imiAlertDetail">IMI: 75.5</div>\n    <div class="imi-alert-timer">&#9203; Desaparece en 15s</div>\n  </div>\n</div>\n\n<!-- ══ IMI (hidden by default) ══ -->\n<div id="imiWrap">\n  <canvas id="imiChart"></canvas>\n  <span id="imiToggleBtn" onclick="toggleImi()">&#215; cerrar IMI</span>\n</div>\n\n\n<!-- ══ ALERT ROW ══ -->\n<div id="alertRow" class="ar-neutral">\n  <span class="ar-icon">&#128225;</span>\n  <span class="ar-main">ANALIZANDO...</span>\n</div>\n\n<!-- ══ PCT STRIP ══ -->\n<div id="pctStrip">\n  <div class="pct-item pct-r"><div class="p-lbl">&lt; 2.00</div><div class="p-val" id="percentBelow2">0%</div></div>\n  <div class="pct-item pct-y"><div class="p-lbl">2&#8211;4.99</div><div class="p-val" id="percent2to5">0%</div></div>\n  <div class="pct-item pct-g"><div class="p-lbl">5&#8211;9.99</div><div class="p-val" id="percent5to10">0%</div></div>\n  <div class="pct-item pct-p"><div class="p-lbl">&#8805; 10</div><div class="p-val" id="percentAbove10">0%</div></div>\n  <div class="pct-item" style="border-left-color:rgba(255,255,255,.2)"><div class="p-lbl">Total</div><div class="p-val" style="color:rgba(255,255,255,.5);font-size:13px" id="totalValues">0</div></div>\n</div>\n\n<!-- ══ BOTTOM ROW: Historial | Configuración ══ -->\n<div id="bottomRow">\n  <div class="bottom-btn" id="histBtn" onclick="openPanel(\'hist\')">\n    <span class="btn-icon">&#128203;</span>\n    <span>Historial &#10003;</span>\n  </div>\n  <div class="bottom-btn" id="cfgBtn" onclick="openPanel(\'cfg\')">\n    <span>Configuraci&#243;n</span>\n    <span class="btn-icon">&#9881;&#65039;</span>\n  </div>\n</div>\n\n<!-- ══ INPUT BUTTONS ══ -->\n<div id="inputSection">\n  <div class="input-grid">\n    <button class="val-btn vb-1" data-range="1.00-1.49" data-min="1.00" data-max="1.49">1.00&#8211;1.49</button>\n    <button class="val-btn vb-2" data-min="1.50" data-max="1.99" data-range="1.50-1.99">1.50&#8211;1.99</button>\n    <button class="val-btn vb-3" data-min="2.00" data-max="2.99" data-range="2.00-2.99">2.00&#8211;2.99</button>\n    <button class="val-btn vb-4" data-min="3.00" data-max="4.99" data-range="3.00-4.99">3.00&#8211;4.99</button>\n    <button class="val-btn vb-5" data-min="5.00" data-max="9.99" data-range="5.00+">5.00&#8211;9.99</button>\n    <button class="val-btn vb-6" data-min="10.00" data-max="20.00" data-range="10.00+">+10.00</button>\n    <button class="val-btn vb-del" id="btnDeleteLast">&#128465; Borrar</button>\n    <button class="val-btn vb-rst" id="btnResetAll">&#10226; Resetear</button>\n  </div>\n</div>\n\n<!-- hidden compat containers -->\n<div id="alertsContainer" style="display:none"></div>\n<div id="historyList" style="display:none"></div>\n<div id="valuesCount" style="display:none">Valores: 0</div>\n<div id="valuesPanel" style="display:none"><div id="valsResizeCorner"></div><div id="valsResizeRight"></div><div id="valsResizeBottom"></div><div id="valsResizeTip"></div></div>\n\n<!-- ══ HISTORIAL PANEL (bottom sheet) ══ -->\n<div class="side-panel" id="histPanel">\n  <div class="panel-backdrop" onclick="closePanel(\'hist\')"></div>\n  <div class="panel-sheet">\n    <div class="panel-handle"></div>\n    <div class="panel-title">&#128203; Historial &amp; Se&#241;ales</div>\n\n    <div class="hot-entry-stub" style="background:rgba(255,255,255,.04);border-radius:10px;padding:10px;margin-bottom:10px;font-size:11px;color:rgba(255,255,255,.5)">\n      <div style="margin-bottom:4px;color:rgba(0,212,255,.8);font-family:\'Orbitron\',monospace;font-size:10px">ESTADO SISTEMA</div>\n      <div>Estado: <span id="statusText" style="color:rgba(255,255,255,.7)">Normal &#8212; Columna -</span></div>\n    </div>\n\n\n    <button class="eval-toggle-btn" id="moderateEvalToggleBtn" onclick="toggleModerateEval()" style="margin-top:8px">\n      <span class="icon">&#128202;</span><span id="moderateEvalToggleText">Evaluaci&#243;n Moderado</span>\n    </button>\n    <div class="moderate-alert-panels-container hidden" id="moderateEvalContainer">\n      <div class="moderate-alert-panels hidden" id="moderateEvalPanels">\n        <div class="moderate-alert-panel" id="modPanel150"><h4>&#127919; Alerta 1.50</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat150Total">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat150Win">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat150Loss">0</span></div></div>\n        <div class="moderate-alert-panel" id="modPanel200"><h4>&#128142; Alerta 2.00</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat200Total">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat200Win">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat200Loss">0</span></div></div>\n        <div class="moderate-alert-panel" id="modPanel150_so"><h4>&#128260; 1.50 (2&#176; Oport.)</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat150SOTotal">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat150SOWin">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat150SOLoss">0</span></div></div>\n        <div class="moderate-alert-panel" id="modPanel300"><h4>&#128640; Alerta 3.00</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat300Total">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat300Win">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat300Loss">0</span></div></div>\n        <div class="moderate-alert-panel" id="modPanel300_so"><h4>&#128260; 3.00 (2&#176; Oport.)</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat300SOTotal">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat300SOWin">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat300SOLoss">0</span></div></div>\n        <div class="moderate-alert-panel" id="modPanel200_so"><h4>&#128260; 2.00 (2&#176; Oport.)</h4><div class="stat"><span class="stat-label">Total</span><span class="stat-value total" id="modStat200SOTotal">0</span></div><div class="stat"><span class="stat-label">Ganadas</span><span class="stat-value win" id="modStat200SOWin">0</span></div><div class="stat"><span class="stat-label">Perdidas</span><span class="stat-value loss" id="modStat200SOLoss">0</span></div></div>\n      </div>\n    </div>\n\n    <!-- Predictor -->\n    <div class="section-lbl" style="margin-top:14px">&#8987; Predictor Inteligente</div>\n    <input type="time" class="time-input" id="time1" style="margin-bottom:6px">\n    <input type="time" class="time-input" id="time2" style="margin-bottom:6px">\n    <button class="btn" onclick="calcPrediction()">Calcular Predicci&#243;n</button>\n    <div class="prediction-panel">\n      <h4 style="margin-top:10px">&#128204; Predicciones activas</h4>\n      <div class="prediction-list" id="predList"></div>\n    </div>\n    <div id="predictionResult" style="margin-top:6px;color:var(--green);font-size:12px"></div>\n\n    <!-- Strategy -->\n    <div class="strat-wrap">\n      <div class="strat-header" id="stratHeader" onclick="stratToggle()">\n        <div>\n          <h3>&#128176; ESTRATEGIA DINERO REAL</h3>\n          <div class="strat-sub">MARTINGALE 2&#215;3 &#183; ESCALA <span id="sEscLabel">1</span>/10</div>\n        </div>\n        <span class="strat-arrow" id="stratArrow">&#9660;</span>\n      </div>\n      <div class="strat-body" id="stratBody">\n        <div class="s-info">\n          <div class="s-box"><div class="s-lbl">Balance</div><div class="s-val sv-green" id="sBalance">$100</div></div>\n          <div class="s-box"><div class="s-lbl">Margen</div><div class="s-val sv-blue" id="sMargen">$0</div></div>\n          <div class="s-box"><div class="s-lbl">Columna</div><div class="s-val sv-blue" id="sColumna">1</div></div>\n          <div class="s-box"><div class="s-lbl">Apuesta</div><div class="s-val sv-gold" id="sApuesta">$10</div></div>\n        </div>\n        <div class="s-alert" id="sAlerta">&#9203; Esperando inicio...</div>\n        <div class="s-dots" id="sDots"></div>\n        <div class="s-cols">\n          <div class="s-col" id="sC1"><div class="sc-name">C1</div><div class="sc-st" id="sSt1">--</div><div class="sc-bet" id="sBet1"></div></div>\n          <div class="s-col" id="sC2"><div class="sc-name">C2</div><div class="sc-st" id="sSt2">--</div><div class="sc-bet" id="sBet2"></div></div>\n          <div class="s-col" id="sC3"><div class="sc-name">C3</div><div class="sc-st" id="sSt3">--</div><div class="sc-bet" id="sBet3"></div></div>\n        </div>\n        <div style="display:flex;gap:5px;margin-bottom:7px">\n          <button class="s-btn" id="sBtnMod" style="flex:1;font-size:9px;padding:6px;background:rgba(0,212,255,.1);color:rgba(0,212,255,.6);border:1px solid rgba(0,212,255,.2)">MODERADO</button>\n        </div>\n        <div id="sAutoStatus" style="display:none;text-align:center;padding:6px 10px;background:rgba(0,255,136,.06);border:1px solid rgba(0,255,136,.2);border-radius:8px;font-size:10px;color:rgba(0,255,136,.7);letter-spacing:1px;margin-bottom:6px">&#129302; MODO AUTO ACTIVO</div>\n        <div class="s-btns" id="sControls" style="display:none">\n          <button class="s-btn s-btn-reset" onclick="sReset()" style="grid-column:span 2">&#128260; RESET ESTRATEGIA</button>\n        </div>\n        <button class="s-btn-start" id="sBtnStart" onclick="sStart()">&#9889; INICIAR ESTRATEGIA</button>\n        <div class="s-hist" id="sHist" style="display:none">\n          <table><thead><tr><th>#</th><th>Esc</th><th>Col</th><th>Ap</th><th>Res</th><th>Bal</th></tr></thead>\n          <tbody id="sHistBody"><tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:5px">Sin datos</td></tr></tbody></table>\n        </div>\n        <div class="s-cfg-toggle" onclick="sCfgToggle()">&#9881;&#65039; Configurar balance y apuesta</div>\n        <div class="s-cfg" id="sCfg">\n          <label>BALANCE INICIAL</label>\n          <input type="number" id="sCapIn" value="100" min="0.01" step="0.01" oninput="sLiveUpdate()">\n          <label>APUESTA BASE</label>\n          <input type="number" id="sBetIn" value="10" min="0.01" step="0.01" oninput="sLiveUpdate()">\n          <button class="s-apply" onclick="sApply()">&#9989; APLICAR</button>\n        </div>\n      </div>\n    </div>\n\n    <!-- Strategy 3X -->\n    <div class="strat-wrap">\n      <div class="strat-header" id="strat3Header" onclick="strat3Toggle()">\n        <div>\n          <h3>&#128640; GESTI&#211;N DINERO REAL 3X</h3>\n          <div class="strat-sub">MARTINGALE 3X &#183; 2&#215;3 &#183; ESCALA <span id="s3EscLabel">1</span>/10</div>\n        </div>\n        <span class="strat-arrow" id="strat3Arrow">&#9660;</span>\n      </div>\n      <div class="strat-body" id="strat3Body">\n        <div class="s-info">\n          <div class="s-box"><div class="s-lbl">Balance</div><div class="s-val sv-green" id="s3Balance">$100</div></div>\n          <div class="s-box"><div class="s-lbl">Margen</div><div class="s-val sv-blue" id="s3Margen">$0</div></div>\n          <div class="s-box"><div class="s-lbl">Columna</div><div class="s-val sv-blue" id="s3Columna">1</div></div>\n          <div class="s-box"><div class="s-lbl">Apuesta</div><div class="s-val sv-gold" id="s3Apuesta">$10</div></div>\n        </div>\n        <div class="s-alert" id="s3Alerta">&#9203; Esperando inicio...</div>\n        <div class="s-dots" id="s3Dots"></div>\n        <div class="s-cols">\n          <div class="s-col" id="s3C1"><div class="sc-name">C1</div><div class="sc-st" id="s3St1">--</div><div class="sc-bet" id="s3Bet1"></div></div>\n          <div class="s-col" id="s3C2"><div class="sc-name">C2</div><div class="sc-st" id="s3St2">--</div><div class="sc-bet" id="s3Bet2"></div></div>\n          <div class="s-col" id="s3C3"><div class="sc-name">C3</div><div class="sc-st" id="s3St3">--</div><div class="sc-bet" id="s3Bet3"></div></div>\n        </div>\n        <div style="display:flex;gap:5px;margin-bottom:7px">\n          <button class="s-btn" id="s3BtnMod" style="flex:1;font-size:9px;padding:6px;background:rgba(0,212,255,.1);color:rgba(0,212,255,.6);border:1px solid rgba(0,212,255,.2)">MODERADO 3.00x</button>\n        </div>\n        <div id="s3AutoStatus" style="display:none;text-align:center;padding:6px 10px;background:rgba(0,255,136,.06);border:1px solid rgba(0,255,136,.2);border-radius:8px;font-size:10px;color:rgba(0,255,136,.7);letter-spacing:1px;margin-bottom:6px">&#129302; MODO AUTO ACTIVO</div>\n        <div class="s-btns" id="s3Controls" style="display:none">\n          <button class="s-btn s-btn-reset" onclick="s3Reset()" style="grid-column:span 2">&#128260; RESET ESTRATEGIA</button>\n        </div>\n        <button class="s-btn-start" id="s3BtnStart" onclick="s3Start()">&#9889; INICIAR ESTRATEGIA</button>\n        <div class="s-hist" id="s3Hist" style="display:none">\n          <table><thead><tr><th>#</th><th>Esc</th><th>Col</th><th>Ap</th><th>Res</th><th>Bal</th></tr></thead>\n          <tbody id="s3HistBody"><tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:5px">Sin datos</td></tr></tbody></table>\n        </div>\n        <div class="s-cfg-toggle" onclick="s3CfgToggle()">&#9881;&#65039; Configurar balance y apuesta</div>\n        <div class="s-cfg" id="s3Cfg">\n          <label>BALANCE INICIAL</label>\n          <input type="number" id="s3CapIn" value="100" min="0.01" step="0.01" oninput="s3LiveUpdate()">\n          <label>APUESTA BASE</label>\n          <input type="number" id="s3BetIn" value="10" min="0.01" step="0.01" oninput="s3LiveUpdate()">\n          <button class="s-apply" onclick="s3Apply()">&#9989; APLICAR</button>\n        </div>\n      </div>\n    </div>\n\n  </div>\n</div>\n\n<!-- ══ CONFIGURACIÓN PANEL (bottom sheet) ══ -->\n<div class="side-panel" id="cfgPanel">\n  <div class="panel-backdrop" onclick="closePanel(\'cfg\')"></div>\n  <div class="panel-sheet">\n    <div class="panel-handle"></div>\n    <div class="panel-title">&#9881;&#65039; Configuraci&#243;n</div>\n\n    <div class="cfg-section">\n      <div class="cfg-title">Historial</div>\n      <div class="cfg-row">\n        <div class="cfg-btn range-btn active" data-range="50" onclick="selectRange(this,50)">50</div>\n        <div class="cfg-btn range-btn" data-range="100" onclick="selectRange(this,100)">100</div>\n        <div class="cfg-btn range-btn" data-range="150" onclick="selectRange(this,150)">150</div>\n        <div class="cfg-btn range-btn" data-range="250" onclick="selectRange(this,250)">250</div>\n        <div class="cfg-btn range-btn" data-range="350" onclick="selectRange(this,350)">350</div>\n        <div class="cfg-btn range-btn" data-range="600" onclick="selectRange(this,600)">600</div>\n      </div>\n    </div>\n\n    <div class="cfg-section">\n      <div class="cfg-title">Indicadores</div>\n      <div class="chk-row">\n        <div class="chk-item"><input type="checkbox" id="ema20Candles" checked><label for="ema20Candles">EMA 20</label></div>\n        <div class="chk-item"><input type="checkbox" id="support" checked><label for="support">Soporte</label></div>\n        <div class="chk-item"><input type="checkbox" id="resistance" checked><label for="resistance">Resistencia</label></div>\n        <div class="chk-item"><input type="checkbox" id="fibonacci" checked><label for="fibonacci">Fibonacci</label></div>\n        <div class="chk-item"><input type="checkbox" id="fractals" checked><label for="fractals">Fractales</label></div>\n        <div class="chk-item"><input type="checkbox" id="imi" checked><label for="imi">IMI</label></div>\n        <div class="chk-item"><input type="checkbox" id="hotLines5" checked><label for="hotLines5">&#128993; &#8805;5x</label></div>\n        <div class="chk-item"><input type="checkbox" id="hotLines10" checked><label for="hotLines10">&#128995; &#8805;10x</label></div>\n      </div>\n    </div>\n\n    <div class="cfg-section">\n      <div class="cfg-title">EMAs</div>\n      <div class="chk-row">\n        <div class="chk-item"><input type="checkbox" id="emaTrend4" checked><label for="emaTrend4">EMA 4</label></div>\n        <div class="chk-item"><input type="checkbox" id="emaTrend8" checked><label for="emaTrend8">EMA 8</label></div>\n        <div class="chk-item"><input type="checkbox" id="emaTrend20" checked><label for="emaTrend20">EMA 20</label></div>\n        <div class="chk-item"><input type="checkbox" id="emaTrend50" checked><label for="emaTrend50">EMA 50</label></div>\n      </div>\n    </div>\n\n    <div class="cfg-section">\n      <div class="cfg-title">Alertas</div>\n      <div class="chk-row">\n        <div class="chk-item"><input type="checkbox" id="filtroEma50" checked><label for="filtroEma50">Filtro EMA 50</label></div>\n        <div class="chk-item"><input type="checkbox" id="autoPredictor" checked><label for="autoPredictor">Predictor Auto</label></div>\n      </div>\n    </div>\n\n\n\n    <div style="height:16px"></div>\n  </div>\n</div>\n\n<!-- Casino panel -->\n<div class="casino-access-panel" id="casinoAccessPanel">\n  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">\n    <span style="color:#00ff9d;font-weight:bold;font-size:14px">&#127920; Acceso R&#225;pido Casinos</span>\n    <button onclick="toggleCasinoAccess()" style="background:none;border:none;color:#fff;cursor:pointer;font-size:18px">&#215;</button>\n  </div>\n  <a href="https://1xbet.com" target="_blank" class="casino-btn">&#128309; 1XBET</a>\n  <a href="https://1win.com" target="_blank" class="casino-btn">&#128993; 1WIN</a>\n  <a href="https://rusbet.com" target="_blank" class="casino-btn">&#128308; RUSBET</a>\n  <a href="https://88start.com" target="_blank" class="casino-btn">&#128995; 88START</a>\n</div>\n\n<!-- Modals -->\n<div class="s-modal" id="sModalWin" style="display:none">\n  <div class="s-modal-box sm-win-box">\n    <div class="sm-title">&#127881; CICLO COMPLETADO</div>\n    <div class="sm-row"><span class="sm-label">Capital</span><span class="sm-num">$<span id="smCap">100</span></span></div>\n    <div class="sm-row"><span class="sm-label">Balance final</span><span class="sm-num">$<span id="smBal">100</span></span></div>\n    <div class="sm-divider"></div>\n    <div class="sm-row sm-highlight-win"><span class="sm-label">&#9989; Ganancia</span><span class="sm-num">+$<span id="smGain">0</span></span></div>\n    <div class="sm-row"><span class="sm-label">Efectividad</span><span class="sm-num"><span id="smEff">0</span>%</span></div>\n    <div class="sm-btn-row">\n      <button class="sm-btn sm-btn-new" onclick="sNewCycle()">&#128260; NUEVO CICLO</button>\n      <button class="sm-btn sm-btn-retire" onclick="sCloseModal(\'sModalWin\')">&#128176; RETIRAR</button>\n    </div>\n  </div>\n</div>\n<div class="s-modal" id="sModalLoss" style="display:none">\n  <div class="s-modal-box sm-loss-box">\n    <div class="sm-title">&#9888;&#65039; CICLO FALLIDO</div>\n    <div class="sm-row"><span class="sm-label">Escala</span><span class="sm-num"><span id="smFEsc">1</span>/10</span></div>\n    <div class="sm-row"><span class="sm-label">Balance</span><span class="sm-num">$<span id="smFBal">0</span></span></div>\n    <div class="sm-divider"></div>\n    <div class="sm-row sm-highlight-loss"><span class="sm-label">&#10060; P&#233;rdida</span><span class="sm-num">-$<span id="smFLoss">0</span></span></div>\n    <button class="sm-btn-restart" onclick="sReset();sCloseModal(\'sModalLoss\')">&#128260; REINICIAR SESI&#211;N</button>\n  </div>\n</div>\n<div class="s-modal" id="s3ModalWin" style="display:none">\n  <div class="s-modal-box sm-win-box">\n    <div class="sm-title">&#127881; CICLO COMPLETADO</div>\n    <div class="sm-row"><span class="sm-label">Capital</span><span class="sm-num">$<span id="s3mCap">100</span></span></div>\n    <div class="sm-row"><span class="sm-label">Balance final</span><span class="sm-num">$<span id="s3mBal">100</span></span></div>\n    <div class="sm-divider"></div>\n    <div class="sm-row sm-highlight-win"><span class="sm-label">&#9989; Ganancia</span><span class="sm-num">+$<span id="s3mGain">0</span></span></div>\n    <div class="sm-row"><span class="sm-label">Efectividad</span><span class="sm-num"><span id="s3mEff">0</span>%</span></div>\n    <div class="sm-btn-row">\n      <button class="sm-btn sm-btn-new" onclick="s3NewCycle()">&#128260; NUEVO CICLO</button>\n      <button class="sm-btn sm-btn-retire" onclick="s3CloseModal(\'s3ModalWin\')">&#128176; RETIRAR</button>\n    </div>\n  </div>\n</div>\n<div class="s-modal" id="s3ModalLoss" style="display:none">\n  <div class="s-modal-box sm-loss-box">\n    <div class="sm-title">&#9888;&#65039; CICLO FALLIDO</div>\n    <div class="sm-row"><span class="sm-label">Escala</span><span class="sm-num"><span id="s3mFEsc">1</span>/10</span></div>\n    <div class="sm-row"><span class="sm-label">Balance</span><span class="sm-num">$<span id="s3mFBal">0</span></span></div>\n    <div class="sm-divider"></div>\n    <div class="sm-row sm-highlight-loss"><span class="sm-label">&#10060; P&#233;rdida</span><span class="sm-num">-$<span id="s3mFLoss">0</span></span></div>\n    <button class="sm-btn-restart" onclick="s3Reset();s3CloseModal(\'s3ModalLoss\')">&#128260; REINICIAR SESI&#211;N</button>\n  </div>\n</div>\n\n<script>\n// ── PANEL SYSTEM ──\nfunction openPanel(which) {\n  document.getElementById(\'histPanel\').classList.remove(\'open\');\n  document.getElementById(\'cfgPanel\').classList.remove(\'open\');\n  document.getElementById(\'histBtn\').classList.remove(\'active\');\n  document.getElementById(\'cfgBtn\').classList.remove(\'active\');\n  if (which === \'hist\') {\n    document.getElementById(\'histPanel\').classList.add(\'open\');\n    document.getElementById(\'histBtn\').classList.add(\'active\');\n  } else {\n    document.getElementById(\'cfgPanel\').classList.add(\'open\');\n    document.getElementById(\'cfgBtn\').classList.add(\'active\');\n  }\n}\nfunction closePanel(which) {\n  document.getElementById(which === \'hist\' ? \'histPanel\' : \'cfgPanel\').classList.remove(\'open\');\n  document.getElementById(which === \'hist\' ? \'histBtn\' : \'cfgBtn\').classList.remove(\'active\');\n}\n\nfunction selectRange(el, range) {\n  document.querySelectorAll(\'.cfg-btn.range-btn\').forEach(b => b.classList.remove(\'active\'));\n  el.classList.add(\'active\');\n  if (historyData.length > range) { data = historyData.slice(-range); }\n  else { data = [...historyData]; }\n  document.getElementById(\'valuesCount\').textContent = \'Valores: \' + data.length;\n  if (drawRequest) cancelAnimationFrame(drawRequest);\n  drawRequest = requestAnimationFrame(draw);\n}\nfunction toggleImi() {\n  const w = document.getElementById(\'imiWrap\');\n  w.style.display = w.style.display === \'none\' ? \'block\' : \'none\';\n}\nfunction toggleCasinoAccess() {\n  const p = document.getElementById(\'casinoAccessPanel\');\n  p.classList.toggle(\'open\');\n}\n\n// ── SIGNAL BAR UPDATE ──\nfunction updateSignalBar(msg, type) { return; //\n  const bar = document.getElementById(\'signalBar\');\n  // bar.className = \'signal-\' + (type || \'neutral\');\n  bar.innerHTML = \'<span>\' + msg + \'</span>\';\n}\n\n// ── CHIP BAR ──\nfunction updateHistoryTopBar() {\n  const bar = document.getElementById(\'chipsScroll\');\n  if (!bar) return;\n  const last12 = historyData.slice(-12);\n  if (last12.length === 0) {\n    bar.innerHTML = \'<span style="font-family:\\\'Share Tech Mono\\\',monospace;font-size:9px;color:rgba(255,255,255,.25);letter-spacing:1px;white-space:nowrap">Agrega valores...</span>\';\n    return;\n  }\n  let html = \'\';\n  last12.forEach((item, idx) => {\n    const v = item.value;\n    let cls = \'chip-low\';\n    if (v >= 10) cls = \'chip-max\';\n    else if (v >= 5) cls = \'chip-high\';\n    else if (v >= 2) cls = \'chip-mid\';\n    const isNewest = idx === last12.length - 1;\n    html += \'<span class="chip \' + cls + (isNewest ? \' newest\' : \'\') + \'">\' + v.toFixed(2) + \'x</span>\';\n  });\n  bar.innerHTML = html;\n  // scroll to end\n  bar.scrollLeft = bar.scrollWidth;\n}\n\n// ── LEGACY COMPAT ──\nfunction toggleCollapsible(id, hdr) {\n  const c = document.getElementById(id);\n  if (!c) return;\n  const collapsed = c.classList.contains(\'collapsed\');\n  if (collapsed) { c.classList.remove(\'collapsed\'); hdr && hdr.classList.add(\'active\'); }\n  else { c.classList.add(\'collapsed\'); hdr && hdr.classList.remove(\'active\'); }\n}\nfunction toggleConfig() {}\nfunction toggleAmxHistPanel() { openPanel(\'hist\'); }\n\n\nfunction toggleModerateEval() {\n  const c = document.getElementById(\'moderateEvalContainer\');\n  const b = document.getElementById(\'moderateEvalToggleBtn\');\n  const txt = document.getElementById(\'moderateEvalToggleText\');\n  if (!c) return;\n  if (c.classList.contains(\'hidden\')) {\n    c.classList.remove(\'hidden\'); b.classList.add(\'active\');\n    txt.textContent = \'Ocultar Moderado\';\n  } else {\n    c.classList.add(\'hidden\'); b.classList.remove(\'active\');\n    txt.textContent = \'Evaluaci\\u00f3n Moderado\';\n  }\n}\n\n// ── ZOOM (no-op in full-screen mode) ──\nfunction zoomIn(){}; function zoomOut(){}; function zoomReset(){};\nfunction applyZoom(){};\n</script>\n<script>\n// =====================================================\n// ✅ VARIABLES GLOBALES - GRÁFICA TENDENCIA\n// =====================================================\n// =====================================================\n// ✅ VARIABLES GLOBALES - GRÁFICA MODERADO\n// =====================================================\nlet modStats150 = { total: 0, ganadas: 0, perdidas: 0 };\nlet modStats200 = { total: 0, ganadas: 0, perdidas: 0 };\nlet modStats150_so = { total: 0, ganadas: 0, perdidas: 0 };\nlet modStats200_so = { total: 0, ganadas: 0, perdidas: 0 };\nlet modEvaluando150 = false;\nlet modEvaluando200 = false;\nlet modGanadaLock = false; // bloquea reseteo durante los 5s de GANADA\nlet modPendingAlert = null; // alerta detectada durante el lock de GANADA\nlet modSo_150_estado = null;\nlet modSo_200_estado = null;\nlet modStats300 = { total: 0, ganadas: 0, perdidas: 0 };\nlet modStats300_so = { total: 0, ganadas: 0, perdidas: 0 };\nlet modEvaluando300 = false;\nlet modLastWonLabel = null;\nlet mod300CooldownRondas = 0; // tras ganar 3x: esperar 2 rondas antes de nueva señal 3x\nlet modSo_300_estado = null;\nlet modMensajeEmaTemporal = null;\nlet modPrev_ema4_above_ema8 = true;\nlet modLudopataSonidoReproducido = false;\nlet modUltimosPuntos = [];\n// =====================================================\n// ✅ VARIABLES COMPARTIDAS\n// =====================================================\nlet recordedTimes = [];\nlet historialValoresAltos = [];\nlet nivelesSoporte = [];\nlet nivelesResistencia = [];\nlet toquesSoporte = {};\nlet toquesResistencia = {};\nlet soporteRoto = false;\nlet resistenciaRota = false;\nconst MAX_TOQUES_FUERTE = 2;\nconst MAX_TOQUES_DEBIL = 4;\nconst MAX_TOQUES_INVALIDO = 5;\nlet ultimaAlertaSoporte150 = 0; // timestamp para agrupación de alertas\nlet alertasSimultaneas = [];\nlet contadorFilasAlertas = 0;\nconst TIEMPO_AGRUPACION_ALERTAS = 2000;\nlet configOpen = true;\nlet moderateEvalOpen = true;\nlet fractales = [];\nlet imiAlertTimeout = null;\nlet ultimaReversionBajista = 0;\nlet ultimaReversionAlcista = 0;\nlet ultimoCruceMomentum = 0;\nconst TIEMPO_COOLDOWN_IMI = 5000;\nconst TIEMPO_ALERTA_IMI = 15000;\nconst IMI_PERIOD = 14;\nconst IMI_SIGNAL_PERIOD = 7;\nconst IMI_SMA_PERIOD = 15;\nlet imiLine = [];\nlet imiSignalLine = [];\nlet imiSmaLine = [];\nlet imiActualValue = 50;\nlet currentZoom = 100;\nconst minZoom = 50;\nconst maxZoom = 200;\nlet data = [];\nlet historyData = [];\nlet lines = [];\nlet isDrawing = false;\nlet startX, startY;\nlet selectedLine = null;\nlet offsetX, offsetY;\nlet chartType = \'moderate\'; // único gráfico: Moderado\nlet updatePending = false;\nlet drawRequest = null;\n// =====================================================\n// ✅ FUNCIONES DE ZOOM\n// =====================================================\nfunction updateZoomDisplay() { document.getElementById(\'zoomDisplay\').textContent = currentZoom + \'%\'; }\nfunction zoomIn() { if (currentZoom < maxZoom) { currentZoom += 10; applyZoom(); } }\nfunction zoomOut() { if (currentZoom > minZoom) { currentZoom -= 10; applyZoom(); } }\nfunction zoomReset() { currentZoom = 100; applyZoom(); }\nfunction applyZoom() { const wrapper = document.getElementById(\'zoomWrapper\'); wrapper.style.transform = `scale(${currentZoom / 100})`; wrapper.style.width = `${100 / (currentZoom / 100)}%`; updateZoomDisplay(); }\n// =====================================================\n// ✅ FUNCIONES DE SECCIONES COLAPSABLES\n// =====================================================\nfunction toggleCollapsible(sectionId, header) {\nconst content = document.getElementById(sectionId);\nconst isActive = header.classList.contains(\'active\');\nif (isActive) { header.classList.remove(\'active\'); content.classList.add(\'collapsed\'); }\nelse { header.classList.add(\'active\'); content.classList.remove(\'collapsed\'); }\n}\nfunction toggleConfig() {\nconst btn = document.getElementById(\'configToggleBtn\');\nconst content = document.getElementById(\'configContent\');\nconst text = document.getElementById(\'configToggleText\');\nif (content.classList.contains(\'collapsed\')) {\ncontent.classList.remove(\'collapsed\');\nbtn.classList.add(\'active\');\ntext.textContent = \'Ocultar Configuración\';\n} else {\ncontent.classList.add(\'collapsed\');\nbtn.classList.remove(\'active\');\ntext.textContent = \'Mostrar Configuración\';\n}\n}\n// ✅ FUNCIÓN PARA TOGGLAR PANELES MODERADO\nfunction toggleModerateEval() {\nconst btn = document.getElementById(\'moderateEvalToggleBtn\');\nconst container = document.getElementById(\'moderateEvalContainer\');\nconst text = document.getElementById(\'moderateEvalToggleText\');\nif (container.classList.contains(\'hidden\')) {\ncontainer.classList.remove(\'hidden\');\nbtn.classList.add(\'active\');\ntext.textContent = \'Ocultar Evaluación Moderado\';\nmoderateEvalOpen = true;\n} else {\ncontainer.classList.add(\'hidden\');\nbtn.classList.remove(\'active\');\ntext.textContent = \'Mostrar Evaluación Moderado\';\nmoderateEvalOpen = false;\n}\n}\n// =====================================================\n// ✅ FUNCIONES DE CASINOS\n// =====================================================\nfunction toggleCasinoAccess() {\nconst panel = document.getElementById(\'casinoAccessPanel\');\nconst btnText = document.getElementById(\'casinoToggleText\');\nif (panel.style.display === \'none\' || panel.style.display === \'\') { panel.style.display = \'block\'; btnText.textContent = \'Cerrar\'; }\nelse { panel.style.display = \'none\'; btnText.textContent = \'Casinos\'; }\n}\n// =====================================================\n// ✅ FUNCIÓN SONIDOS\n// =====================================================\nfunction playSound(type) {\ntry {\nconst audioCtx = new (window.AudioContext || window.webkitAudioContext)();\nconst oscillator = audioCtx.createOscillator();\nconst gainNode = audioCtx.createGain();\noscillator.connect(gainNode); gainNode.connect(audioCtx.destination);\nlet frequency = 800, duration = 300, typeWave = \'sine\';\nswitch(type) {\ncase \'alert\': frequency = 900; duration = 400; typeWave = \'square\'; break;\ncase \'win\': frequency = [600, 800, 1000]; duration = 200; typeWave = \'sine\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.1, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 100); }); return;\ncase \'loss\': frequency = [1000, 800, 600]; duration = 200; typeWave = \'sine\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.1, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 100); }); return;\ncase \'otra_oportunidad\': frequency = 600; duration = 500; typeWave = \'sine\'; break;\ncase \'momentum\': frequency = [500, 700, 900, 1100]; duration = 100; typeWave = \'square\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.15, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 80); }); return;\ncase \'soporte\': frequency = [700, 900, 1100]; duration = 200; typeWave = \'sine\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.15, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 100); }); return;\ncase \'time\': frequency = [800, 1000, 1200]; duration = 250; typeWave = \'sine\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.15, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 100); }); return;\ncase \'time_warning\': frequency = [600, 700, 800]; duration = 200; typeWave = \'sine\';\nfrequency.forEach((freq, i) => { setTimeout(() => { const osc = audioCtx.createOscillator(); const gn = audioCtx.createGain(); osc.type = typeWave; osc.frequency.value = freq; gn.gain.setValueAtTime(0.12, audioCtx.currentTime); gn.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000); osc.connect(gn); gn.connect(audioCtx.destination); osc.start(); osc.stop(audioCtx.currentTime + duration/1000); }, i * 80); }); return;\n}\noscillator.type = typeWave; oscillator.frequency.value = frequency;\ngainNode.gain.setValueAtTime(0.1, audioCtx.currentTime);\ngainNode.gain.exponentialRampToValueAtTime(0.01, audioCtx.currentTime + duration/1000);\noscillator.start(); oscillator.stop(audioCtx.currentTime + duration/1000);\n} catch(e) { console.log("Sonido no soportado"); }\n}\n// =====================================================\n// ✅ VOZ — Sistema de Cola con Prioridad (iOS/Android)\n//    PRIORIDAD 1 (SEÑAL)  : alertas y resultados de señal\n//    PRIORIDAD 2 (MERCADO): IMI sobrecompra / sobreventa\n// =====================================================\nlet voiceEnabled = true;\nlet _voiceUnlocked = false;\n\n// Cola: [ { text, priority } ]  priority 1 = señal (alta), 2 = mercado (baja)\nlet _voiceQueue = [];\nlet _voicePlaying = false;\nlet _currentPriority = 0;   // prioridad del mensaje que está sonando ahora\n\n// Pre-carga voces al cargar la página\nif (window.speechSynthesis) {\n  window.speechSynthesis.getVoices();\n  if (window.speechSynthesis.onvoiceschanged !== undefined) {\n    window.speechSynthesis.onvoiceschanged = () => window.speechSynthesis.getVoices();\n  }\n}\n\nfunction _unlockSpeech() {\n  if (_voiceUnlocked || !window.speechSynthesis) return;\n  const silent = new SpeechSynthesisUtterance(\' \');\n  silent.volume = 0; silent.lang = \'es-ES\';\n  window.speechSynthesis.speak(silent);\n  _voiceUnlocked = true;\n}\n\nfunction _getEsVoice() {\n  try {\n    const voices = window.speechSynthesis.getVoices();\n    return voices.find(v => v.lang === \'es-ES\') ||\n           voices.find(v => v.lang && v.lang.startsWith(\'es\')) ||\n           null;\n  } catch(e) { return null; }\n}\n\nfunction _playNext() {\n  if (!voiceEnabled || _voiceQueue.length === 0) {\n    _voicePlaying = false;\n    _currentPriority = 0;\n    return;\n  }\n  _voicePlaying = true;\n  const item = _voiceQueue.shift();\n  _currentPriority = item.priority;\n  const utt = new SpeechSynthesisUtterance(item.text);\n  utt.lang = \'es-ES\';\n  utt.rate = 0.95;\n  utt.pitch = 1.0;\n  utt.volume = 1.0;\n  const esVoice = _getEsVoice();\n  if (esVoice) utt.voice = esVoice;\n  utt.onend = () => { _voicePlaying = false; _currentPriority = 0; _playNext(); };\n  utt.onerror = () => { _voicePlaying = false; _currentPriority = 0; _playNext(); };\n  try { window.speechSynthesis.speak(utt); } catch(e) {\n    _voicePlaying = false; _currentPriority = 0; _playNext();\n  }\n}\n\n// priority: 1 = señal (alta), 2 = mercado (baja)\nfunction speakVoice(text, priority = 1) {\n  if (!voiceEnabled || !window.speechSynthesis) return;\n\n  if (priority === 1) {\n    // Señal: cancelar todo, limpiar cola de mercado, hablar ya\n    _voiceQueue = _voiceQueue.filter(i => i.priority === 1); // mantener solo señales pendientes\n    try { window.speechSynthesis.cancel(); } catch(e) {}\n    _voicePlaying = false;\n    _currentPriority = 0;\n    _voiceQueue.unshift({ text, priority }); // meter al frente\n    _playNext();\n  } else {\n    // Mercado: solo encolar si no hay señal en curso ni pendiente\n    const haySenal = _voicePlaying && _currentPriority === 1;\n    const hayPendiente = _voiceQueue.some(i => i.priority === 1);\n    if (haySenal || hayPendiente) return; // ignorar mensaje de mercado\n    // Reemplazar cualquier mensaje de mercado anterior (no acumular)\n    _voiceQueue = _voiceQueue.filter(i => i.priority !== 2);\n    _voiceQueue.push({ text, priority: 2 });\n    if (!_voicePlaying) _playNext();\n  }\n}\n\nfunction toggleVoice() {\n  voiceEnabled = !voiceEnabled;\n  if (!voiceEnabled) {\n    try { window.speechSynthesis.cancel(); } catch(e) {}\n    _voiceQueue = []; _voicePlaying = false; _currentPriority = 0;\n  }\n  document.getElementById(\'voiceIcon\').textContent = voiceEnabled ? \'🔊\' : \'🔇\';\n  document.getElementById(\'voiceLabel\').textContent = voiceEnabled ? \'VOZ\' : \'MUTE\';\n  document.getElementById(\'voiceToggle\').className = voiceEnabled ? \'\' : \'muted\';\n  if (voiceEnabled) speakVoice(\'Voz activada\', 1);\n}\n// =====================================================\n// ✅ ACTUALIZAR PANELES DE ESTADÍSTICAS - TENDENCIA\n// =====================================================\n\n// =====================================================\n// ✅ ACTUALIZAR PANELES DE ESTADÍSTICAS - MODERADO\n// =====================================================\nfunction updateModerateStatsPanels() {\ndocument.getElementById(\'modStat150Total\').textContent = modStats150.total;\ndocument.getElementById(\'modStat150Win\').textContent = modStats150.ganadas;\ndocument.getElementById(\'modStat150Loss\').textContent = modStats150.perdidas;\ndocument.getElementById(\'modStat200Total\').textContent = modStats200.total;\ndocument.getElementById(\'modStat200Win\').textContent = modStats200.ganadas;\ndocument.getElementById(\'modStat200Loss\').textContent = modStats200.perdidas;\ndocument.getElementById(\'modStat150SOTotal\').textContent = modStats150_so.total;\ndocument.getElementById(\'modStat150SOWin\').textContent = modStats150_so.ganadas;\ndocument.getElementById(\'modStat150SOLoss\').textContent = modStats150_so.perdidas;\ndocument.getElementById(\'modStat300Total\').textContent = modStats300.total;\ndocument.getElementById(\'modStat300Win\').textContent = modStats300.ganadas;\ndocument.getElementById(\'modStat300Loss\').textContent = modStats300.perdidas;\ndocument.getElementById(\'modStat300SOTotal\').textContent = modStats300_so.total;\ndocument.getElementById(\'modStat300SOWin\').textContent = modStats300_so.ganadas;\ndocument.getElementById(\'modStat300SOLoss\').textContent = modStats300_so.perdidas;\ndocument.getElementById(\'modStat200SOTotal\').textContent = modStats200_so.total;\ndocument.getElementById(\'modStat200SOWin\').textContent = modStats200_so.ganadas;\ndocument.getElementById(\'modStat200SOLoss\').textContent = modStats200_so.perdidas;\n}\n// =====================================================\n// ✅ ALERTA → alertRow (reemplaza círculo grande)\n// =====================================================\n// ✅ AUDIO POR GRÁFICA: solo suena si su gráfico está seleccionado\n\n\nfunction playSoundMod(type) { if (chartType === \'moderate\') playSound(type); }\nfunction speakVoiceMod(text, priority = 1) { if (chartType === \'moderate\') speakVoice(text, priority); }\n\n// =====================================================\n// ✅ ALERTA CÍRCULO PEQUEÑO (TIEMPO)\n// =====================================================\n// =====================================================\n// ✅ ALERTA TIEMPO (predictor) → signalBar\n// =====================================================\nfunction showAlertCircleSmall(time, message, info, isPreAlert = false) {\n  // signalBar hidden — no-op\n}\n// =====================================================\n// ✅ ACTUALIZAR EMA CIRCLE MODERADO\n// =====================================================\n// =====================================================\n// ✅ ALERT ROW — función central\n// =====================================================\nfunction setAlertRow(icon, main, cls, sub) {\n  const row = document.getElementById(\'alertRow\');\n  if (!row) return;\n  row.className = \'ar-\' + (cls || \'neutral\');\n  const subHtml = sub ? `<span class="ar-sub">${sub}</span>` : \'\';\n  const betBadge = (typeof window._sActive === \'function\' && window._sActive() && typeof window._sCurBet === \'function\')\n    ? `<span class="ar-bet">$${window._sCurBet()}</span>` : \'\';\n  row.innerHTML = `<span class="ar-icon">${icon}</span><span class="ar-main">${main}</span>${subHtml}${betBadge}`;\n}\n// compat stub — ya no actualiza el círculo\n// Helper: texto y voz del monto de apuesta activo\nfunction _getBetLabel() {\n  if (typeof window._sActive === \'function\' && window._sActive() && typeof window._sCurBet === \'function\') {\n    return \' · APOSTAR $\' + window._sCurBet();\n  }\n  return \'\';\n}\nfunction _getBetVoice() {\n  if (typeof window._sActive === \'function\' && window._sActive() && typeof window._sCurBet === \'function\') {\n    return \', apuesta \' + window._sCurBet();\n  }\n  return \'\';\n}\n// Anuncia el monto de apuesta con delay de 2s (separado del anuncio de señal)\n// Captura col/att EN EL MOMENTO de la señal para usar el cache correcto\nfunction speakBetAfterDelay(ms = 2000) {\n  if (typeof window._sActive !== \'function\' || !window._sActive()) return;\n  // Captura inmediata del estado actual (antes del delay)\n  const snapCol   = typeof window._sCol  === \'function\' ? window._sCol()  : null;\n  const snapAtt   = typeof window._sAtt  === \'function\' ? window._sAtt()  : null;\n  const snapCache = typeof window._sBetCache === \'function\' ? window._sBetCache() : null;\n  setTimeout(() => {\n    if (snapCol && snapAtt && snapCache) {\n      const key = \'C\' + snapCol + \'A\' + snapAtt;\n      const amount = snapCache[key];\n      if (amount !== undefined) { speakVoice(\'apostar \' + amount, 1); return; }\n    }\n    // fallback: usa apuesta actual si el cache no está disponible\n    if (typeof window._sCurBet === \'function\') {\n      speakVoice(\'apostar \' + window._sCurBet(), 1);\n    }\n  }, ms);\n}\n// =====================================================\n// ✅ ACTUALIZAR EMA CIRCLE MODERADO → alertRow\n// =====================================================\n// ✅ ALERTA FLOTANTE MODERADO (azul)\nfunction modFloatShow(icon, title, detail, pulse = true) {\n  const el = document.getElementById(\'modFloatingAlert\'); if (!el) return;\n  const wn = document.getElementById(\'modAlertWon\');\n  if (wn) { wn.textContent = \'\'; wn.style.display = \'none\'; }\n  const ic = document.getElementById(\'modAlertIcon\');\n  ic.textContent = \'\'; ic.style.display = \'none\'; // sin fila de emoji en ninguna señal\n  document.getElementById(\'modAlertTitle\').textContent = title;\n  document.getElementById(\'modAlertDetail\').textContent = detail || \'\';\n  el.classList.toggle(\'pulse\', pulse);\n  el.style.display = \'block\';\n}\n// ✅ Monto de apuesta según la señal: 1.50/2.00 → gestión 2x · 3.00 → gestión 3x\nfunction _getBetFor(kind) {\n  try {\n    if (kind === \'3.00\') {\n      if (typeof window._s3Active === \'function\' && window._s3Active() && typeof window._s3CurBet === \'function\')\n        return \'Apuesta: $\' + Number(window._s3CurBet()).toFixed(2);\n    } else {\n      if (typeof window._sActive === \'function\' && window._sActive() && typeof window._sCurBet === \'function\')\n        return \'Apuesta: $\' + Number(window._sCurBet()).toFixed(2);\n    }\n  } catch(e) {}\n  return \'esperando resultado\';\n}\n// ✅ Pill de tendencia bajo la alerta (EMA4 vs EMA8 del Moderado)\nfunction updateTrendPill(positions, ema4, ema8) {\n  const el = document.getElementById(\'modTrendPill\'); if (!el) return;\n  if (!positions || positions.length < 5 || chartType !== \'moderate\') { el.style.display = \'none\'; return; }\n  const e4 = ema4 && ema4.length ? ema4[ema4.length - 1] : 0;\n  const e8 = ema8 && ema8.length ? ema8[ema8.length - 1] : 0;\n  el.style.display = \'block\';\n  if (e4 > e8) { el.textContent = \'📈 TENDENCIA ALCISTA\'; el.className = \'mod-trend-pill alcista\'; }\n  else if (e4 < e8) { el.textContent = \'📉 TENDENCIA BAJISTA\'; el.className = \'mod-trend-pill bajista\'; }\n  else { el.textContent = \'➖ TENDENCIA NEUTRAL\'; el.className = \'mod-trend-pill\'; }\n}\nfunction modFloatHide() { const el = document.getElementById(\'modFloatingAlert\'); if (el) el.style.display = \'none\'; }\n// ✅ Resultado GANADO + NUEVA SEÑAL en la misma ronda (3 líneas)\nfunction modFloatShowGanadaNueva(wonLabel, newLabel) {\n  const el = document.getElementById(\'modFloatingAlert\'); if (!el) return;\n  const ic = document.getElementById(\'modAlertIcon\');\n  ic.textContent = \'\'; ic.style.display = \'none\';\n  const wn = document.getElementById(\'modAlertWon\');\n  if (wn) { wn.textContent = \'✅ ¡SEÑAL GANADA \' + wonLabel + \'x! ✅\'; wn.style.display = \'block\'; }\n  document.getElementById(\'modAlertTitle\').textContent = \'🎯 SEÑAL A \' + newLabel + \'x 🎯\';\n  document.getElementById(\'modAlertDetail\').textContent = _getBetFor(newLabel);\n  el.classList.add(\'pulse\');\n  el.style.display = \'block\';\n}\nfunction updateModerateEmaCircle() {\nif (modGanadaLock) return;\nif (chartType !== \'moderate\') { return; }\nif (modSo_150_estado === \'espera\' || modSo_200_estado === \'espera\' || modSo_300_estado === \'espera\') {\n  const kindSO = modSo_300_estado === \'espera\' ? \'3.00\' : \'2.00\';\n  modFloatShow(\'🔄\', \'2ª OPORTUNIDAD\', _getBetFor(kindSO));\n  return;\n}\nconst sigs = [];\nif (modEvaluando150) sigs.push(\'1.50\');\nif (modEvaluando200) sigs.push(\'2.00\');\nif (modEvaluando300) sigs.push(\'3.00\');\nif (sigs.length > 0) {\n  const title = \'🎯 SEÑAL A \' + sigs.map(s => s + \'x\').join(\' + \') + \' 🎯\';\n  const kind = (sigs.length === 1 && sigs[0] === \'3.00\') ? \'3.00\' : \'2.00\';\n  modFloatShow(\'\', title, _getBetFor(kind));\n} else {\n  modFloatHide();\n}\n}\n// =====================================================\n// ✅ EVALUAR ALERTA - GRÁFICA MODERADO\n// =====================================================\nfunction showGanada(label) {\n  modGanadaLock = true;\n  const base = String(label).replace(\' SO\', \'\');\n  modLastWonLabel = base;\n  if (chartType === \'moderate\') modFloatShow(\'\', \'✅ ¡SEÑAL GANADA \' + base + \'x! ✅\', \'\');\n  let ganadaTimer = setTimeout(() => finalizarGanada(), 5000);\n  window._ganadaTimer = ganadaTimer;\n  window._ganadaFinish = finalizarGanada;\n  function finalizarGanada() {\n    clearTimeout(window._ganadaTimer);\n    modGanadaLock = false;\n    if (modPendingAlert) {\n      modPendingAlert = null;\n      updateModerateEmaCircle();\n    } else {\n      updateModerateEmaCircle();\n    }\n  }\n}\nfunction evaluateModerateAlertResult(valor) {\n// ── HOOK GESTIÓN 3X: SO 3.00 ──\nif (modSo_300_estado === \'espera\' && typeof window._s3Active === \'function\' && window._s3Active() && window._s3WaitingSO()) {\n  const win300so = valor >= 3.00;\n  setTimeout(() => window.s3AutoResult(win300so), 50);\n}\n// ── HOOK ESTRATEGIA: Moderado SO 2.00 ──\nif (modSo_200_estado === \'espera\' && typeof window._sActive === \'function\' && window._sActive() && window._sChart() === \'moderate\' && window._sWaitingSO()) {\n  const win200so = valor >= 2.00;\n  setTimeout(() => window.sAutoResult(win200so), 50);\n}\nif (modSo_150_estado === \'espera\') {\nif (valor >= 1.50) { modStats150_so.ganadas++; showGanada(\'1.50 SO\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó señal a uno punto cincuenta\'); modSo_150_estado = null; updateModerateStatsPanels(); return; }\nelse { modStats150_so.perdidas++; playSoundMod(\'loss\'); speakVoiceMod(\'Se perdió señal a uno punto cincuenta\'); modSo_150_estado = null; updateModerateStatsPanels(); if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 1.50x — PERDIDA\', \'\', false); setTimeout(() => updateModerateEmaCircle(), 5000); return; }\n}\nif (modSo_200_estado === \'espera\') {\nif (valor >= 2.00) { modStats200_so.ganadas++; showGanada(\'2.00 SO\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó dos equis\'); modSo_200_estado = null; updateModerateStatsPanels(); return; }\nelse { modStats200_so.perdidas++; playSoundMod(\'loss\'); speakVoiceMod(\'Se perdió dos equis\'); modSo_200_estado = null; updateModerateStatsPanels(); if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 2.00x — PERDIDA\', \'\', false); setTimeout(() => updateModerateEmaCircle(), 5000); return; }\n}\nif (modSo_300_estado === \'espera\') {\nif (valor >= 3.00) { modStats300_so.ganadas++; mod300CooldownRondas = 2; showGanada(\'3.00 SO\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó tres equis\'); modSo_300_estado = null; updateModerateStatsPanels(); return; }\nelse { modStats300_so.perdidas++; playSoundMod(\'loss\'); speakVoiceMod(\'Se perdió tres equis\'); modSo_300_estado = null; updateModerateStatsPanels(); if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 3.00x — PERDIDA\', \'\', false); setTimeout(() => updateModerateEmaCircle(), 5000); return; }\n}\nif(modEvaluando150) {\nif(valor >= 1.50) { modStats150.ganadas++; modStats150_so.ganadas++; showGanada(\'1.50\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó señal a uno punto cincuenta\'); modEvaluando150 = false; updateModerateStatsPanels(); }\nelse { modStats150.perdidas++; modSo_150_estado = \'espera\'; if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 1.50x — PERDIDA\', \'segunda oportunidad en camino\', false); playSoundMod(\'otra_oportunidad\'); speakVoiceMod(\'Segunda Oportunidad uno punto cincuenta\'); setTimeout(() => { modMensajeEmaTemporal = null; updateModerateEmaCircle(); }, 3000); modEvaluando150 = false; updateModerateStatsPanels(); }\n}\nif(modEvaluando200) {\nconst win200 = valor >= 2.00;\nif(win200) { modStats200.ganadas++; modStats200_so.ganadas++; showGanada(\'2.00\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó dos equis\'); modEvaluando200 = false; updateModerateStatsPanels(); }\nelse { modStats200.perdidas++; modSo_200_estado = \'espera\'; if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 2.00x — PERDIDA\', \'segunda oportunidad en camino\', false); playSoundMod(\'otra_oportunidad\'); setTimeout(() => speakVoiceMod(\'Segunda Oportunidad dos equis\'), 120); setTimeout(() => { modMensajeEmaTemporal = null; updateModerateEmaCircle(); }, 3000); modEvaluando200 = false; updateModerateStatsPanels(); }\n// ── HOOK ESTRATEGIA: Moderado 2.00 principal ──\nif (typeof window._sActive === \'function\' && window._sActive() && window._sChart() === \'moderate\' && !window._sWaitingSO()) {\n  setTimeout(() => window.sAutoResult(win200), 50);\n}\n}\nif(modEvaluando300) {\nconst win300 = valor >= 3.00;\nif(win300) { modStats300.ganadas++; modStats300_so.ganadas++; mod300CooldownRondas = 2; showGanada(\'3.00\'); playSoundMod(\'win\'); speakVoiceMod(\'Se ganó tres equis\'); modEvaluando300 = false; updateModerateStatsPanels(); }\nelse { modStats300.perdidas++; modSo_300_estado = \'espera\'; if (chartType === \'moderate\') modFloatShow(\'😢\', \'SEÑAL A 3.00x — PERDIDA\', \'segunda oportunidad en camino\', false); playSoundMod(\'otra_oportunidad\'); setTimeout(() => speakVoiceMod(\'Segunda Oportunidad tres equis\'), 600); modEvaluando300 = false; updateModerateStatsPanels(); }\n// ── HOOK GESTIÓN 3X: 3.00 principal ──\nif (typeof window._s3Active === \'function\' && window._s3Active() && !window._s3WaitingSO()) {\n  setTimeout(() => window.s3AutoResult(win300), 50);\n}\n}\n}\n// =====================================================\n// ✅ EVALUAR RESULTADO DE ALERTA - GRÁFICA TENDENCIA\n// =====================================================\n\n\n// =====================================================\n// ✅ DETECTAR ALERTAS - GRÁFICA MODERADO\n// =====================================================\nfunction checkModerateAlerts(positions, ema4, ema8, ema20, data) {\n// DESACTIVADO — el panel ya no calcula sus propias señales (EMA/tendencia).\n// Las señales reales vienen del bot (Telegram) vía el puente realBridge más\n// abajo, que llama a window.sStart()/window.sResult() directamente.\nreturn;\nif (data.length < 4) return;\nconst currentPos = positions[positions.length - 1];\nconst currentEma4 = ema4.length ? ema4[ema4.length - 1] : currentPos;\nconst currentEma8 = ema8.length ? ema8[ema8.length - 1] : currentPos;\nconst currentEma20 = ema20.length ? ema20[ema20.length - 1] : currentPos;\nconst prevEma4 = ema4.length > 1 ? ema4[ema4.length - 2] : currentEma4;\nconst prevEma8 = ema8.length > 1 ? ema8[ema8.length - 2] : currentEma8;\nconst prevEma20 = ema20.length > 1 ? ema20[ema20.length - 2] : currentEma20;\nlet alerta150 = false;\nif(data.length >= 4 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) {\nconst ultimosCambios = data.slice(-4).map(x => x.value >= 2 ? 1 : -1);\nconst [c1, c2, c3, c4] = ultimosCambios;\nif(c1 === -1 && c2 === -1 && c3 === -1 && c4 === 1 && currentPos <= currentEma4 && currentPos <= currentEma8) { alerta150 = true; }\n}\nif(ema4.length >= 2 && prevEma4 <= prevEma8 && currentEma4 > currentEma8 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta150 = true; }\nconst soporte = positions.length >= 20 ? Math.min(...positions.slice(-20)) : Math.min(...positions);\nif(currentPos <= soporte * 1.01 && currentPos > currentEma4 && currentPos > currentEma8 && currentPos > currentEma20 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta150 = true; }\nlet alerta200 = false;\nif(ema8.length >= 2 && prevEma8 <= prevEma20 && currentEma8 > currentEma20 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta200 = true; }\nmodUltimosPuntos = positions.slice(-3);\nif(modUltimosPuntos.length === 3 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) {\nconst [a, b, c] = modUltimosPuntos;\nif(Math.abs(a - c) <= 1 && b > a && currentPos > currentEma4 && currentPos > currentEma8 && currentPos > currentEma20) { alerta200 = true; }\n}\nif (data.length >= 2 && data[data.length - 1].value >= 2.00 && data[data.length - 2].value >= 2.00 && currentEma4 > currentEma8 && currentEma8 > currentEma20 && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) {\nconst beforePrev = data.length >= 3 ? data[data.length - 3] : null;\nif (beforePrev === null || beforePrev.value < 2.00) { alerta200 = true; }\n}\nlet alerta300 = false;\n// Impulso fuerte: EMAs alineadas al alza con pendiente positiva y racha de 3 valores >= 2.00\nif (currentEma4 > currentEma8 && currentEma8 > currentEma20 && currentEma4 > prevEma4 && data.length >= 3\n    && data[data.length-1].value >= 2.00 && data[data.length-2].value >= 2.00 && data[data.length-3].value >= 2.00\n    && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta300 = true; }\n// Cruce fuerte: EMA4 cruza por encima de EMA20 con precio sobre EMA8, pendiente positiva y último valor >= 2.00\nif (ema4.length >= 2 && ema20.length >= 2 && prevEma4 <= prevEma20 && currentEma4 > currentEma20 && currentPos > currentEma8\n    && currentEma4 > prevEma4 && data[data.length - 1].value >= 2.00\n    && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta300 = true; }\n// Continuación de impulso: valor actual >= 3.00 tras uno >= 2.00 con EMA4 subiendo\nif (data.length >= 2 && data[data.length - 1].value >= 3.00 && data[data.length - 2].value >= 2.00\n    && currentEma4 > prevEma4 && currentEma4 > currentEma8\n    && !modEvaluando150 && !modEvaluando200 && !modEvaluando300) { alerta300 = true; }\nif (!filtroEma50OK(positions)) { alerta150 = false; alerta200 = false; alerta300 = false; }\nif (alerta300 && !filtro3xColumnaOK(currentPos, currentEma4, currentEma8, currentEma20, data)) { alerta300 = false; }\nif (mod300CooldownRondas > 0) { alerta300 = false; mod300CooldownRondas--; }\nif(alerta150 && !modEvaluando150) {\n  modStats150.total++; modStats150_so.total++; modEvaluando150 = true;\n  if (modGanadaLock) {\n    modPendingAlert = \'1.50\';\n    if (chartType === \'moderate\') modFloatShowGanadaNueva(modLastWonLabel || \'1.50\', \'1.50\');\n    clearTimeout(window._ganadaTimer);\n    window._ganadaTimer = setTimeout(() => { if(window._ganadaFinish) window._ganadaFinish(); }, 3000);\n  } else { updateModerateEmaCircle(); }\n  playSoundMod(\'alert\'); speakVoiceMod(\'Señal uno punto cincuenta\'); updateModerateStatsPanels();\n}\nelse if(alerta200 && !modEvaluando200) {\n  modStats200.total++; modStats200_so.total++; modEvaluando200 = true;\n  if (modGanadaLock) {\n    modPendingAlert = \'2.00\';\n    if (chartType === \'moderate\') modFloatShowGanadaNueva(modLastWonLabel || \'2.00\', \'2.00\');\n    clearTimeout(window._ganadaTimer);\n    window._ganadaTimer = setTimeout(() => { if(window._ganadaFinish) window._ganadaFinish(); }, 3000);\n  } else { updateModerateEmaCircle(); }\n  playSoundMod(\'alert\'); speakVoiceMod(\'Señal dos equis\'); updateModerateStatsPanels();\n}\nelse if(alerta300 && !modEvaluando300) {\n  modStats300.total++; modStats300_so.total++; modEvaluando300 = true;\n  if (modGanadaLock) {\n    modPendingAlert = \'3.00\';\n    if (chartType === \'moderate\') modFloatShowGanadaNueva(modLastWonLabel || \'3.00\', \'3.00\');\n    clearTimeout(window._ganadaTimer);\n    window._ganadaTimer = setTimeout(() => { if(window._ganadaFinish) window._ganadaFinish(); }, 3000);\n  } else { updateModerateEmaCircle(); }\n  playSoundMod(\'alert\'); speakVoiceMod(\'Señal tres equis\'); updateModerateStatsPanels();\n}\n}\n// =====================================================\n// ✅ DETECTAR ALERTAS - GRÁFICA TENDENCIA\n// =====================================================\n\nfunction agregarAlertaEnFila(mensaje, tipo = "info") {\nconst ahora = Date.now();\nif (ahora - ultimaAlertaSoporte150 > TIEMPO_AGRUPACION_ALERTAS || alertasSimultaneas.length === 0) { contadorFilasAlertas++; alertasSimultaneas = []; }\nalertasSimultaneas.push({ mensaje, tipo, timestamp: ahora });\nrenderizarAlertasEnFilas();\n}\nfunction renderizarAlertasEnFilas() {\nconst container = document.getElementById("alertsContainer"); container.innerHTML = \'\';\nconst filas = []; let filaActual = [];\nalertasSimultaneas.forEach((alerta, index) => {\nfilaActual.push(alerta);\nif (filaActual.length >= 2 || index === alertasSimultaneas.length - 1) { filas.push([...filaActual]); filaActual = []; }\n});\nfilas.forEach((fila, filaIndex) => {\nconst rowDiv = document.createElement(\'div\'); rowDiv.className = \'alert-row\';\nfila.forEach((alerta, alertaIndex) => {\nconst alertDiv = document.createElement(\'div\');\nalertDiv.className = `alert-item-row ${alerta.tipo === "success" ? "success" : (alerta.tipo === "info" ? "info" : "")}`;\nconst badgeFila = fila.length > 1 ? `<span class="alert-fila-badge">F#${filaIndex + 1}-${alertaIndex + 1}</span>` : \'\';\nalertDiv.innerHTML = `${badgeFila}<strong>${new Date().toLocaleTimeString()}</strong> - ${alerta.mensaje}`;\nrowDiv.appendChild(alertDiv);\n});\ncontainer.appendChild(rowDiv);\n});\nif (container.children.length > 10) { container.removeChild(container.firstChild); }\n}\nfunction addAlert(message, type = "info") {\nconst container = document.getElementById("alertsContainer");\nconst alertDiv = document.createElement("div");\nalertDiv.className = `alert-item ${type === "success" ? "success" : ""}`;\nalertDiv.innerHTML = `<strong>${new Date().toLocaleTimeString()}</strong> - ${message}`;\ncontainer.insertBefore(alertDiv, container.firstChild);\nif (container.children.length > 10) { container.removeChild(container.lastChild); }\n}\n// =====================================================\n// ✅ FUNCIONES WT2.0, MOMENTUM, ETC.\n// =====================================================\n\n\n\n\n\n\n// =====================================================\n// ✅ FUNCIONES COMPARTIDAS\n// =====================================================\nfunction timeToSeconds(timeStr) { if (!timeStr) return null; const [h, m, s] = timeStr.split(\':\').map(Number); return h * 3600 + m * 60 + (s || 0); }\nfunction secondsToTime(seconds) { seconds = Math.round(seconds); const h = Math.floor(seconds / 3600) % 24; const m = Math.floor((seconds % 3600) / 60); const s = seconds % 60; return `${h.toString().padStart(2,\'0\')}:${m.toString().padStart(2,\'0\')}:${s.toString().padStart(2,\'0\')}`; }\nfunction calcPrediction() { const t1 = document.getElementById("time1").value; const t2 = document.getElementById("time2").value; if (!t1 || !t2) { document.getElementById("predictionResult").textContent = "⚠️ Ingresa dos tiempos válidos."; return; } const s1 = timeToSeconds(t1); const s2 = timeToSeconds(t2); if (s1 === null || s2 === null || s1 >= s2) { document.getElementById("predictionResult").textContent = "⚠️ Tiempo 1 debe ser menor que Tiempo 2."; return; } const diff = s2 - s1; const nextTime = s2 + diff; const prediction = secondsToTime(nextTime); recordedTimes.push({ time: prediction, active: true, tipo: \'manual\', timestamp: Date.now(), preAlertShown: false, alertShown: false }); document.getElementById("predictionResult").textContent = `⏱️ Próximo: ${prediction}`; updatePredList(); addAlert(`🔮 Predicción manual: ${prediction}`, "info"); }\nfunction updatePredList() { const list = document.getElementById("predList"); list.innerHTML = ""; const now = new Date(); const currentSeconds = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds(); recordedTimes = recordedTimes.filter(pred => { const predSeconds = timeToSeconds(pred.time); return predSeconds > currentSeconds; }); recordedTimes.forEach((item, i) => { const div = document.createElement("div"); div.className = "prediction-tag"; div.innerHTML = `⏱️ ${item.time} <span class="close-btn" onclick="removePrediction(${i})">×</span>`; list.appendChild(div); }); }\nfunction removePrediction(index) { recordedTimes.splice(index, 1); updatePredList(); }\nfunction checkAutoPredictions() { if (!document.getElementById(\'autoPredictor\').checked) return; const now = new Date(); const currentSeconds = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds(); recordedTimes.forEach((pred, i) => { const predSeconds = timeToSeconds(pred.time); const diff = predSeconds - currentSeconds; if (diff >= 9 && diff <= 10 && !pred.preAlertShown) { pred.preAlertShown = true; addAlert(`⏳ PRE-ALERTA: ${pred.time} - Preparando entrada`, "info"); showAlertCircleSmall(pred.time, \'Preparando entrada\', \'Buscando rebote 3x - 5x\', true); } if (diff >= -1 && diff <= 1 && !pred.alertShown) { pred.alertShown = true; addAlert(`⏰ PREDICCIÓN ALCANZADA: ${pred.time}`, "success"); showAlertCircleSmall(pred.time, \'Tiempo alcanzado\', \'Verificar entrada\', false); } }); updatePredList(); }\nfunction calcularPrediccionInteligente(valor) { return; /* DESACTIVADO — predicción real de horario viene del bot vía realBridge */ if (valor < 3.00) return; const ahora = new Date(); const tiempoActual = ahora.getHours() * 3600 + ahora.getMinutes() * 60 + ahora.getSeconds(); historialValoresAltos.push({ valor: valor, tiempo: tiempoActual, timestamp: Date.now() }); if (historialValoresAltos.length > 15) { historialValoresAltos.shift(); } if (historialValoresAltos.length >= 2) { const ultimosValores = historialValoresAltos.slice(-Math.min(5, historialValoresAltos.length)); let diferencias = []; for (let i = 1; i < ultimosValores.length; i++) { const diff = ultimosValores[i].tiempo - ultimosValores[i-1].tiempo; diferencias.push(diff); } const promedio = diferencias.reduce((a, b) => a + b, 0) / diferencias.length; const ultimoTiempo = ultimosValores[ultimosValores.length - 1].tiempo; let tiempoPredicho = ultimoTiempo + promedio; const prediction = secondsToTime(tiempoPredicho); const prediccionExistente = recordedTimes.find(t => { if (!t.tipo || t.tipo !== \'inteligente\') return false; const diff = Math.abs(timeToSeconds(t.time) - tiempoPredicho); return diff < 15; }); if (!prediccionExistente) { recordedTimes.push({ time: prediction, active: true, tipo: \'inteligente\', timestamp: Date.now(), alerted: false, preAlertShown: false, alertShown: false }); updatePredList(); addAlert(`🔮 Predicción Inteligente: ${prediction}`, "info");\n// ── Mostrar círculo predicción en el gráfico ──\nmostrarPredCircle(prediction);\n} } }\n// =====================================================\n// ✅ CÍRCULO PREDICCIÓN 3x–5x EN GRÁFICO\n// =====================================================\nlet _predCircleTimer = null;\nfunction mostrarPredCircle(time) {\n  const overlay = document.getElementById(\'predCircleOverlay\');\n  const circle  = document.getElementById(\'predCircle\');\n  const timeEl  = document.getElementById(\'predCircleTime\');\n  if (!overlay) return;\n  timeEl.textContent = time;\n  circle.classList.add(\'active\');\n  overlay.style.display = \'block\';\n  if (_predCircleTimer) clearTimeout(_predCircleTimer);\n  // Countdown visual: update label every second\n  const targetSec = timeToSeconds(time);\n  function updateCountdown() {\n    if (!voiceEnabled && overlay.style.display === \'none\') return;\n    const now = new Date();\n    const nowSec = now.getHours() * 3600 + now.getMinutes() * 60 + now.getSeconds();\n    const diff = targetSec - nowSec;\n    if (diff <= 0) {\n      timeEl.textContent = time;\n      return;\n    }\n    timeEl.textContent = diff + \'s\';\n    if (overlay.style.display !== \'none\') setTimeout(updateCountdown, 1000);\n  }\n  updateCountdown();\n  _predCircleTimer = setTimeout(() => {\n    circle.classList.remove(\'active\');\n    overlay.style.display = \'none\';\n  }, 30000);\n}\nfunction calculateEMAForTrend(positions, period) { if (positions.length < period) return []; const emaResult = []; const k = 2 / (period + 1); let sum = 0; for (let i = 0; i < period; i++) { sum += positions[i]; } let emaValue = sum / period; emaResult.push(emaValue); for (let i = period; i < positions.length; i++) { emaValue = (positions[i] * k) + (emaValue * (1 - k)); emaResult.push(emaValue); } return emaResult; }\n// ✅ NUEVA EMA 50 — filtro de confirmación de patrones (bloquea señales alcistas bajo la EMA 50)\n// ✅ FILTRO POR COLUMNA 3X: con la gestión 3X encendida, exige más confirmación en columnas altas\nfunction filtro3xColumnaOK(currentPos, currentEma4, currentEma8, currentEma20, data) {\n  if (typeof window._s3Active !== \'function\' || !window._s3Active()) return true; // gestión apagada: sin filtro extra\n  const col = (typeof window._s3Col === \'function\') ? window._s3Col() : 1;\n  if (col <= 1) return true; // Columna 1: condiciones normales\n  if (col === 2) return currentEma4 > currentEma8 && currentPos > currentEma8; // Columna 2: tendencia corta alcista\n  // Columna 3: máxima exigencia — EMAs alineadas, precio arriba y último valor >= 2.00\n  return currentEma4 > currentEma8 && currentEma8 > currentEma20 && currentPos > currentEma4\n      && data.length >= 1 && data[data.length - 1].value >= 2.00;\n}\nfunction filtroEma50OK(positions) {\nconst chk = document.getElementById(\'filtroEma50\');\nif (!chk || !chk.checked) return true;\nconst ema50 = calculateEMAForTrend(positions, 50);\nif (ema50.length === 0) return true; // pocos datos: no bloquear\nreturn positions[positions.length - 1] >= ema50[ema50.length - 1];\n}\nfunction drawEMAForTrend(ctx, positions, padding, width, height, minPos, maxPos, period, color, lineWidth = 2) { const emaCheckbox = period === 20 ? document.getElementById(\'ema20Candles\') : document.getElementById(`emaTrend${period}`); if (!emaCheckbox || !emaCheckbox.checked) return; const emaResult = calculateEMAForTrend(positions, period); if (emaResult.length === 0) return; const step = (width - padding * 2) / Math.max(1, positions.length - 1); const posRange = Math.max(1, maxPos - minPos); ctx.beginPath(); ctx.strokeStyle = color; ctx.lineWidth = lineWidth; ctx.lineCap = \'round\'; ctx.lineJoin = \'round\'; emaResult.forEach((posValue, i) => { const dataIndex = period - 1 + i; const x = padding + dataIndex * step; const y = padding + ((maxPos - posValue) / posRange) * (height - padding * 2); if (i === 0) { ctx.moveTo(x, y); } else { ctx.lineTo(x, y); } }); ctx.stroke(); }\n\n// ✅ MODIFICACIÓN: NUEVOS COLORES DE PUNTOS\nconst rangeColors = { \n    \'1.00-1.49\': { fill: \'rgba(0, 100, 0, 1.0)\', stroke: \'rgba(0, 150, 0, 0.9)\' },\n    \'1.50-1.99\': { fill: \'rgba(0, 120, 0, 1.0)\', stroke: \'rgba(0, 170, 0, 0.9)\' },\n    \'2.00-2.99\': { fill: \'rgba(138, 43, 226, 1.0)\', stroke: \'rgba(186, 85, 211, 0.9)\' },\n    \'3.00-4.99\': { fill: \'rgba(70, 130, 180, 1.0)\', stroke: \'rgba(100, 149, 237, 0.9)\' },\n    \'5.00+\': { fill: \'rgba(255, 215, 0, 1.0)\', stroke: \'rgba(255, 223, 0, 0.9)\' },\n    \'10.00+\': { fill: \'rgba(255, 0, 100, 1.0)\', stroke: \'rgba(255, 20, 147, 0.9)\' }\n};\n\nconst candleConfig = { \'1.00-1.49\': { body: 36, wick: 18, dir: -1, color: \'rgba(255, 0, 0, 1.0)\', stroke: \'rgba(255, 50, 50, 0.9)\' }, \'1.50-1.99\': { body: 24, wick: 12, dir: -1, color: \'rgba(255, 0, 0, 1.0)\', stroke: \'rgba(255, 50, 50, 0.9)\' }, \'2.00-2.99\': { body: 18, wick: 10, dir: 1, color: \'rgba(255, 165, 0, 1.0)\', stroke: \'rgba(255, 165, 0, 0.8)\' }, \'3.00-4.99\': { body: 24, wick: 12, dir: 1, color: \'rgba(0, 128, 0, 1.0)\', stroke: \'rgba(0, 200, 0, 0.9)\' }, \'5.00+\': { body: 36, wick: 18, dir: 1, color: \'rgba(0, 128, 0, 1.0)\', stroke: \'rgba(0, 200, 0, 0.9)\' }, \'10.00+\': { body: 48, wick: 24, dir: 1, color: \'rgba(139, 0, 255, 1.0)\', stroke: \'rgba(139, 0, 255, 0.8)\' } };\nconst canvas = document.getElementById(\'chart\'); const ctx = canvas.getContext(\'2d\');\nconst imiCanvas = document.getElementById(\'imiChart\'); const imiCtx = imiCanvas.getContext(\'2d\');\nconst tooltip = document.getElementById(\'tooltip\');\n\n// ✅ FUNCIÓN OPTIMIZADA PARA AGREGAR VALORES\nfunction addValueManually(rangeKey, minValue, maxValue) {\nif (updatePending) return;\n_unlockSpeech(); // desbloquear síntesis de voz en iOS (requiere gesto del usuario)\nupdatePending = true;\nconst timestamp = new Date();\nconst randomValue = parseFloat((Math.random() * (maxValue - minValue) + minValue).toFixed(2));\ndata.push({ timestamp: timestamp, value: randomValue, range: rangeKey });\nhistoryData.push({ timestamp: timestamp, value: randomValue, range: rangeKey });\nif (historyData.length > 600) historyData.shift();\ndocument.getElementById(\'valuesCount\').textContent = `Valores: ${data.length}`;\ndocument.getElementById(\'totalValues\').textContent = data.length;\nconst _ctEl = document.getElementById(\'currentTime\'); if (_ctEl) _ctEl.textContent = timestamp.toLocaleTimeString();\nevaluateModerateAlertResult(randomValue); // ✅ FIX: resolver señales activas ANTES de detectar nuevas\nprocessNewValue(randomValue);\naddAlert(`Valor agregado: ${randomValue.toFixed(2)}x`, "success");\nupdatePercentages();\n// ✅ OPTIMIZACIÓN: Usar requestAnimationFrame para dibujar\nupdateHistoryTopBar();\nif (drawRequest) cancelAnimationFrame(drawRequest);\ndrawRequest = requestAnimationFrame(() => { draw(); updatePending = false; });\n}\n\nfunction processNewValue(valor) {\nif (valor >= 3.00) { calcularPrediccionInteligente(valor); }\nconst positions = [0]; let currentPos = 0;\nfor (let i = 1; i < data.length; i++) { if (data[i].value >= 2.00) currentPos += 1; else currentPos -= 1; positions.push(currentPos); }\nconst ema4 = calculateEMAForTrend(positions, 4);\nconst ema8 = calculateEMAForTrend(positions, 8);\nconst ema20 = calculateEMAForTrend(positions, 20);\ndetectarFractales(positions, data);\nupdateTrendPill(positions, ema4, ema8);\ncheckModerateAlerts(positions, ema4, ema8, ema20, data);\nconst col = data.length > 0 ? data.length : "-";\ndocument.getElementById(\'statusText\').textContent = `Normal — Columna ${col}`;\nupdateModerateEmaCircle();\n}\nfunction deleteLastValue() { if (data.length === 0) { addAlert("No hay valores para eliminar", "info"); return; } data.pop(); historyData.pop(); document.getElementById(\'valuesCount\').textContent = `Valores: ${data.length}`; document.getElementById(\'totalValues\').textContent = data.length; updatePercentages(); updateHistoryPanel(); if (drawRequest) cancelAnimationFrame(drawRequest); drawRequest = requestAnimationFrame(() => { draw(); }); addAlert("Último valor eliminado", "info"); }\nfunction resetAllValues() {\nconst count = data.length; data = []; historyData = [];\ndocument.getElementById(\'valuesCount\').textContent = `Valores: 0`; document.getElementById(\'totalValues\').textContent = \'0\';\nmodStats150 = { total: 0, ganadas: 0, perdidas: 0 }; modStats200 = { total: 0, ganadas: 0, perdidas: 0 };\nmodStats150_so = { total: 0, ganadas: 0, perdidas: 0 }; modStats200_so = { total: 0, ganadas: 0, perdidas: 0 };\nmodEvaluando150 = false; modEvaluando200 = false;\nmodSo_150_estado = null; modSo_200_estado = null;\nmodStats300 = { total: 0, ganadas: 0, perdidas: 0 }; modStats300_so = { total: 0, ganadas: 0, perdidas: 0 };\nmodEvaluando300 = false; modSo_300_estado = null; modLastWonLabel = null; mod300CooldownRondas = 0;\nmodMensajeEmaTemporal = null; modPrev_ema4_above_ema8 = true; modLudopataSonidoReproducido = false; modUltimosPuntos = [];\nhistorialValoresAltos = []; recordedTimes = [];\nnivelesSoporte = []; nivelesResistencia = [];\ntoquesSoporte = {}; toquesResistencia = {}; soporteRoto = false; resistenciaRota = false;\nalertasSimultaneas = []; contadorFilasAlertas = 0; fractales = [];\nimiLine = []; imiSignalLine = []; imiSmaLine = [];\nif (imiAlertTimeout) { clearTimeout(imiAlertTimeout); imiAlertTimeout = null; }\ndocument.getElementById(\'imiFloatingAlert\').style.display = \'none\';\nupdatePercentages(); updateHistoryPanel(); updateHistoryTopBar(); updateModerateStatsPanels(); updatePredList(); updateModerateEmaCircle();\nif (drawRequest) cancelAnimationFrame(drawRequest);\ndrawRequest = requestAnimationFrame(() => { draw(); });\naddAlert(`¡${count} valores reseteados!`, "success");\nconst sb2 = document.getElementById(\'signalBar\');\n// signalBar hidden\n}\nfunction updatePercentages() { if (data.length === 0) { document.getElementById(\'percentBelow2\').textContent = \'0%\'; document.getElementById(\'percent2to5\').textContent = \'0%\'; document.getElementById(\'percent5to10\').textContent = \'0%\'; document.getElementById(\'percentAbove10\').textContent = \'0%\'; return; } let below2 = 0, twoToFive = 0, fiveToTen = 0, aboveTen = 0; data.forEach(item => { if (item.value < 2.00) below2++; else if (item.value < 5.00) twoToFive++; else if (item.value < 10.00) fiveToTen++; else aboveTen++; }); const total = data.length; document.getElementById(\'percentBelow2\').textContent = `${((below2 / total) * 100).toFixed(1)}%`; document.getElementById(\'percent2to5\').textContent = `${((twoToFive / total) * 100).toFixed(1)}%`; document.getElementById(\'percent5to10\').textContent = `${((fiveToTen / total) * 100).toFixed(1)}%`; document.getElementById(\'percentAbove10\').textContent = `${((aboveTen / total) * 100).toFixed(1)}%`; }\nfunction updateHistoryPanel() { const container = document.getElementById(\'historyList\'); container.innerHTML = \'\'; historyData.slice().reverse().forEach(item => { const div = document.createElement(\'div\'); div.className = \'history-item\'; const color = rangeColors[item.range]?.fill || \'white\'; div.innerHTML = `<span style="color: ${color}">${item.timestamp.toLocaleTimeString()}</span> - ${item.value.toFixed(2)}x <small>(${item.range})</small>`; container.appendChild(div); }); }\n// =====================================================\n// ✅ FUNCIONES DE DIBUJADO OPTIMIZADAS\n// =====================================================\n// ══════════════════════════════════════════════\n//  LÍNEAS CALIENTES — ≥5x amarilla · ≥10x morada\n//  Lógica:\n//  · Se traza desde el 1er valor que cumple\n//  · Cada nuevo hit en esa línea la REFUERZA\n//  · Después de 6 datos consecutivos SIN hit\n//    la línea se DEBILITA (-1 fuerza por ronda)\n//  · Si vuelve a cumplir se REFUERZA de nuevo\n//  · Si fuerza llega a 0 → desaparece\n// ══════════════════════════════════════════════\nfunction drawHotLines(ctx, data, positions, padding, width, height, minPos, maxPos, posRange, step, useMapY, mapYFn) {\n  if (data.length === 0) return;\n\n  const show5  = document.getElementById(\'hotLines5\')?.checked ?? true;\n  const show10 = document.getElementById(\'hotLines10\')?.checked ?? true;\n  if (!show5 && !show10) return;\n\n  const TOL = 0.5; // tolerancia para agrupar niveles cercanos\n\n  // Construir niveles con dinámica de fuerza\n  // fuerza: empieza en 1, +1 por cada hit, -1 por cada 6 rondas sin hit\n  // si fuerza <= 0 → desaparece\n  function buildLevels(minValue) {\n    const levels = {}; // key → { pos, fuerza, hits, sinHit }\n\n    data.forEach((d, i) => {\n      const pos = positions[i];\n      const key = Math.round(pos * 2) / 2;\n\n      if (d.value >= minValue) {\n        // HIT — refuerza o crea nivel\n        let found = null;\n        for (const k in levels) {\n          if (Math.abs(parseFloat(k) - key) <= TOL) { found = k; break; }\n        }\n        if (found !== null) {\n          levels[found].fuerza++;\n          levels[found].hits++;\n          levels[found].sinHit = 0;\n        } else {\n          levels[key] = { pos, fuerza: 1, hits: 1, sinHit: 0 };\n        }\n      } else {\n        // NO hit — solo debilita si el dato cae EN esa misma línea (posición cercana)\n        // Bajistas en otra posición NO debilitan\n        for (const k in levels) {\n          if (Math.abs(parseFloat(k) - key) <= TOL) {\n            levels[k].sinHit++;\n            if (levels[k].sinHit >= 6) {\n              levels[k].fuerza = Math.max(0, levels[k].fuerza - 1);\n              levels[k].sinHit = 0;\n            }\n          }\n        }\n      }\n    });\n\n    return Object.values(levels).filter(lv => lv.fuerza > 0);\n  }\n\n  function getY(pos) {\n    if (useMapY && mapYFn) return mapYFn(pos);\n    return padding + ((maxPos - pos) / posRange) * (height - padding * 2);\n  }\n\n  // ── Líneas ≥5x (amarillo) ──\n  if (show5) {\n    buildLevels(5.00).forEach(lv => {\n      const y     = getY(lv.pos);\n      const f     = lv.fuerza;\n      const h     = lv.hits;\n      // Desde el 2do hit (h>=2) → FUERTE. Se mantiene fuerte mientras f sea alto\n      const strong = h >= 2 && f >= 2;\n      const opacity = strong ? Math.min(0.75 + Math.min(f,6) * 0.04, 0.95) : 0.30;\n      const lineW   = strong ? Math.min(1.8 + Math.min(f,6) * 0.25, 4.0) : 1.0;\n      const dashOn  = strong ? (f >= 4 ? 10 : 7) : 5;\n      const dashOff = strong ? 3 : 6;\n      const glowOp  = strong ? Math.min(0.18 + f * 0.06, 0.45) : 0.05;\n\n      ctx.save();\n      if (glowOp > 0) {\n        ctx.strokeStyle = `rgba(255,210,0,${glowOp})`;\n        ctx.lineWidth = lineW + 6;\n        ctx.setLineDash([]);\n        ctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke();\n      }\n      ctx.strokeStyle = `rgba(255,215,0,${opacity})`;\n      ctx.lineWidth = lineW;\n      ctx.setLineDash([dashOn, dashOff]);\n      ctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke();\n      ctx.setLineDash([]);\n      const ico = f >= 5 ? \'🔥\' : strong ? \'🟡\' : \'🌫️\';\n      ctx.fillStyle = `rgba(255,220,0,${strong ? Math.min(opacity+0.05,1) : 0.4})`;\n      ctx.font = `bold ${strong ? 11 : 10}px Arial`;\n      ctx.textAlign = \'right\';\n      ctx.fillText(`${ico} ≥5x (${h}hit · f${f})`, width - padding - 4, y - 4);\n      ctx.restore();\n    });\n  }\n\n  // ── Líneas ≥10x (morado) — encima de las amarillas ──\n  if (show10) {\n    buildLevels(10.00).forEach(lv => {\n      const y     = getY(lv.pos);\n      const f     = lv.fuerza;\n      const h     = lv.hits;\n      const strong = h >= 2 && f >= 2;\n      const opacity = strong ? Math.min(0.78 + Math.min(f,6) * 0.04, 0.97) : 0.32;\n      const lineW   = strong ? Math.min(2.0 + Math.min(f,6) * 0.28, 4.5) : 1.0;\n      const dashOn  = strong ? (f >= 4 ? 11 : 8) : 5;\n      const dashOff = strong ? 3 : 6;\n      const glowOp  = strong ? Math.min(0.20 + f * 0.07, 0.50) : 0.06;\n\n      ctx.save();\n      if (glowOp > 0) {\n        ctx.strokeStyle = `rgba(180,0,255,${glowOp})`;\n        ctx.lineWidth = lineW + 7;\n        ctx.setLineDash([]);\n        ctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke();\n      }\n      ctx.strokeStyle = `rgba(190,0,255,${opacity})`;\n      ctx.lineWidth = lineW;\n      ctx.setLineDash([dashOn, dashOff]);\n      ctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke();\n      ctx.setLineDash([]);\n      const ico = f >= 5 ? \'🔥\' : strong ? \'🟣\' : \'🌫️\';\n      ctx.fillStyle = `rgba(200,80,255,${strong ? Math.min(opacity+0.05,1) : 0.4})`;\n      ctx.font = `bold ${strong ? 11 : 10}px Arial`;\n      ctx.textAlign = \'right\';\n      ctx.fillText(`${ico} ≥10x (${h}hit · f${f})`, width - padding - 4, y - 4);\n      ctx.restore();\n    });\n  }\n}\n\n\nfunction drawModerate(ctx, data, padding, width, height) {\nif (data.length === 0) { ctx.fillStyle = \'rgba(200,230,255,0.3)\'; ctx.font = \'24px Arial\'; ctx.textAlign = \'center\'; ctx.fillText(\'👆 Presiona un botón para agregar valores\', width / 2, height / 2); return; }\nconst positions = [0]; let currentPos = 0;\nfor (let i = 1; i < data.length; i++) { currentPos += (data[i].value >= 2.00) ? 1 : -1; positions.push(currentPos); }\nconst step = (width - padding * 2) / Math.max(1, data.length - 1);\nlet min = Math.min(...positions); let max = Math.max(...positions);\nif (min === max) { min -= 0.5; max += 0.5; }\nconst chartTop = padding + 10; const chartBottom = height - padding;\nconst scale = (chartBottom - chartTop) / (max - min);\nconst mapY = v => chartBottom - (v - min) * scale;\n\n/* ── fondo degradado oscuro ── */\nconst bgGrad = ctx.createLinearGradient(0, chartTop, 0, chartBottom);\nbgGrad.addColorStop(0, \'rgba(0,20,50,.35)\');\nbgGrad.addColorStop(1, \'rgba(0,5,15,.6)\');\nctx.fillStyle = bgGrad;\nctx.fillRect(padding, chartTop, width - padding*2, chartBottom - chartTop);\n\n/* ── líneas de cuadrícula sutiles ── */\nconst gridStep = Math.ceil((max - min) / 6);\nfor (let gv = Math.ceil(min); gv <= max; gv += Math.max(1, gridStep)) {\n  const gy = mapY(gv);\n  ctx.strokeStyle = \'rgba(0,212,255,.06)\'; ctx.lineWidth = 1; ctx.setLineDash([4,8]);\n  ctx.beginPath(); ctx.moveTo(padding, gy); ctx.lineTo(width-padding, gy); ctx.stroke();\n}\nctx.setLineDash([]);\n\n/* ── línea de precio con gradiente oscuro ── */\nconst lineGrad = ctx.createLinearGradient(padding, 0, width-padding, 0);\nlineGrad.addColorStop(0,   \'rgba(0,180,255,.25)\');\nlineGrad.addColorStop(0.5, \'rgba(0,220,255,.55)\');\nlineGrad.addColorStop(1,   \'rgba(0,255,180,.35)\');\nctx.beginPath();\npositions.forEach((p, i) => { const x = padding + i * step; const y = mapY(p); if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); });\nctx.strokeStyle = lineGrad; ctx.lineWidth = 1.8; ctx.stroke();\n\n/* ── relleno bajo la línea ── */\nconst fillGrad = ctx.createLinearGradient(0, chartTop, 0, chartBottom);\nfillGrad.addColorStop(0, \'rgba(0,200,255,.07)\');\nfillGrad.addColorStop(1, \'rgba(0,200,255,.0)\');\nctx.beginPath();\npositions.forEach((p, i) => { const x = padding + i * step; const y = mapY(p); if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y); });\nctx.lineTo(padding + (positions.length-1)*step, chartBottom);\nctx.lineTo(padding, chartBottom); ctx.closePath();\nctx.fillStyle = fillGrad; ctx.fill();\n\n/* ── puntos ── */\npositions.forEach((p, i) => {\nconst x = padding + i * step; const y = mapY(p); const d = data[i];\nconst color = rangeColors[d.range] || rangeColors[\'2.00-2.99\'];\n/* anillo exterior oscuro */\nctx.beginPath(); ctx.arc(x, y, 7, 0, Math.PI*2);\nctx.fillStyle = \'rgba(2,6,14,.85)\'; ctx.fill();\nctx.strokeStyle = color.stroke + \'bb\'; ctx.lineWidth = 1.5; ctx.stroke();\n/* núcleo */\nctx.beginPath(); ctx.arc(x, y, 4, 0, Math.PI*2);\nctx.fillStyle = color.fill; ctx.fill();\n/* etiqueta */\nctx.fillStyle = \'rgba(200,230,255,.75)\'; ctx.font = \'9px "Share Tech Mono",monospace\'; ctx.textAlign = \'center\';\nctx.fillText(d.value.toFixed(2), x, y - 12);\n});\nconst show4 = document.getElementById(\'emaTrend4\')?.checked ?? true;\nconst show8 = document.getElementById(\'emaTrend8\')?.checked ?? true;\nconst show20 = document.getElementById(\'emaTrend20\')?.checked ?? true;\nif (show4) drawEMAForTrend(ctx, positions, padding, width, height, min, max, 4, \'rgba(255,255,100,0.95)\', 2);\nif (show8) drawEMAForTrend(ctx, positions, padding, width, height, min, max, 8, \'rgba(0,255,166,0.9)\', 2);\nif (show20) drawEMAForTrend(ctx, positions, padding, width, height, min, max, 20, \'rgba(255,105,180,0.9)\', 2);\nconst show50 = document.getElementById(\'emaTrend50\')?.checked ?? true;\nif (show50) drawEMAForTrend(ctx, positions, padding, width, height, min, max, 50, \'rgba(0,168,255,0.95)\', 2.4);\nif (document.getElementById(\'support\')?.checked) {\nconst { soportes } = detectarSoportesResistencias(positions, data);\nsoportes.forEach(s => {\nconst y = mapY(s.nivel);\nctx.strokeStyle = "rgba(0,255,100,0.8)"; ctx.lineWidth = 2; ctx.setLineDash([6, 4]);\nctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke(); ctx.setLineDash([]);\nctx.fillStyle = "rgba(0,255,100,0.9)"; ctx.font = "12px Arial"; ctx.fillText("SOPORTE", padding + 5, y - 5);\n});\n}\nif (document.getElementById(\'resistance\')?.checked) {\nconst { resistencias } = detectarSoportesResistencias(positions, data);\nresistencias.forEach(r => {\nconst y = mapY(r.nivel);\nctx.strokeStyle = "rgba(255,80,80,0.8)"; ctx.lineWidth = 2; ctx.setLineDash([6, 4]);\nctx.beginPath(); ctx.moveTo(padding, y); ctx.lineTo(width - padding, y); ctx.stroke(); ctx.setLineDash([]);\nctx.fillStyle = "rgba(255,80,80,0.9)"; ctx.font = "12px Arial"; ctx.fillText("RESISTENCIA", padding + 5, y - 5);\n});\n}\nif (document.getElementById(\'fractals\')?.checked) {\nconst fractalesDetectados = detectarFractales(positions, data);\nfractalesDetectados.forEach(f => {\nconst x = padding + f.index * step; const y = mapY(f.valor); const size = 12;\nif (f.tipo === \'up\') {\nctx.fillStyle = "rgba(46, 204, 113, 0.9)"; ctx.beginPath();\nctx.moveTo(x, y + 25); ctx.lineTo(x - size, y + 25 + size * 1.5); ctx.lineTo(x + size, y + 25 + size * 1.5); ctx.closePath(); ctx.fill();\nctx.strokeStyle = "#2ecc71"; ctx.lineWidth = 2; ctx.stroke();\n} else {\nctx.fillStyle = "rgba(231, 76, 60, 0.9)"; ctx.beginPath();\nctx.moveTo(x, y - 25); ctx.lineTo(x - size, y - 25 - size * 1.5); ctx.lineTo(x + size, y - 25 - size * 1.5); ctx.closePath(); ctx.fill();\nctx.strokeStyle = "#e74c3c"; ctx.lineWidth = 2; ctx.stroke();\n}\n});\n}\n// ── LÍNEAS CALIENTES ≥5x (amarilla) y ≥10x (morada) ──\ndrawHotLines(ctx, data, positions, padding, width, height, Math.min(...positions), Math.max(...positions),\n  Math.max(1, Math.max(...positions) - Math.min(...positions)), step, true, mapY);\n}\n\n// ✅ OPTIMIZACIÓN: IMI con menos efectos de sombra\nfunction drawIMI(ctx, padding, width, height) {\nif (!document.getElementById(\'imi\').checked || imiLine.filter(v => v !== null).length === 0) return;\nconst imiRange = 100; const step = (width - padding * 2) / Math.max(1, imiLine.length - 1);\nctx.lineWidth = 2; ctx.setLineDash([6, 3]); ctx.shadowBlur = 0;\nconst y70 = padding + ((100 - 70) / imiRange) * (height - padding * 2);\nctx.strokeStyle = \'rgba(255, 50, 50, 0.9)\'; ctx.beginPath(); ctx.moveTo(padding, y70); ctx.lineTo(width - padding, y70); ctx.stroke();\nconst y50 = padding + ((100 - 50) / imiRange) * (height - padding * 2);\nif (imiActualValue < 50) { ctx.strokeStyle = \'rgba(255, 200, 200, 0.95)\'; }\nelse { ctx.strokeStyle = \'rgba(200, 255, 220, 0.95)\'; }\nctx.beginPath(); ctx.moveTo(padding, y50); ctx.lineTo(width - padding, y50); ctx.stroke();\nconst y30 = padding + ((100 - 30) / imiRange) * (height - padding * 2);\nctx.strokeStyle = \'rgba(50, 255, 100, 0.9)\'; ctx.beginPath(); ctx.moveTo(padding, y30); ctx.lineTo(width - padding, y30); ctx.stroke();\nctx.setLineDash([]); ctx.beginPath(); ctx.strokeStyle = \'rgba(0, 255, 255, 0.9)\'; ctx.lineWidth = 2;\nlet started = false;\nimiLine.forEach((value, i) => { if (value === null) return; const x = padding + i * step; const y = padding + ((100 - value) / imiRange) * (height - padding * 2); if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); } });\nctx.stroke();\nctx.beginPath(); ctx.strokeStyle = \'rgba(255, 165, 0, 0.9)\'; ctx.lineWidth = 2; started = false;\nimiSignalLine.forEach((value, i) => { if (value === null) return; const x = padding + i * step; const y = padding + ((100 - value) / imiRange) * (height - padding * 2); if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); } });\nctx.stroke();\nctx.beginPath(); ctx.strokeStyle = \'rgba(139, 0, 255, 0.9)\'; ctx.lineWidth = 2; started = false;\nimiSmaLine.forEach((value, i) => { if (value === null) return; const x = padding + i * step; const y = padding + ((100 - value) / imiRange) * (height - padding * 2); if (!started) { ctx.moveTo(x, y); started = true; } else { ctx.lineTo(x, y); } });\nctx.stroke();\nctx.fillStyle = \'rgba(0, 255, 255, 0.9)\'; ctx.font = \'11px Arial\';\nctx.fillText(\'IMI\', padding + 5, padding + 12);\nctx.fillStyle = \'rgba(255, 165, 0, 0.9)\'; ctx.fillText(\'Señal\', padding + 35, padding + 12);\nctx.fillStyle = \'rgba(139, 0, 255, 0.9)\'; ctx.fillText(\'SMA 15\', padding + 75, padding + 12);\n}\nfunction calculateIMI(data) {\nif (data.length < IMI_PERIOD) return;\nimiLine = []; imiSignalLine = []; imiSmaLine = [];\nfor (let i = 0; i < data.length; i++) {\nif (i < IMI_PERIOD - 1) { imiLine.push(null); continue; }\nlet upCloses = 0; let downCloses = 0;\nfor (let j = i - IMI_PERIOD + 1; j <= i; j++) {\nif (j > 0 && data[j].value >= 2.00) { upCloses++; }\nelse if (j > 0 && data[j].value < 2.00) { downCloses++; }\n}\nconst total = upCloses + downCloses;\nconst imi = total > 0 ? (upCloses / total) * 100 : 50;\nimiLine.push(imi); imiActualValue = imi;\nif (imiLine.filter(v => v !== null).length >= IMI_SIGNAL_PERIOD) {\nconst validImi = imiLine.filter(v => v !== null);\nconst k = 2 / (IMI_SIGNAL_PERIOD + 1);\nlet emaValue = validImi.slice(0, IMI_SIGNAL_PERIOD).reduce((a, b) => a + b, 0) / IMI_SIGNAL_PERIOD;\nfor (let m = IMI_SIGNAL_PERIOD; m < validImi.length; m++) { emaValue = (validImi[m] * k) + (emaValue * (1 - k)); }\nimiSignalLine.push(emaValue);\n} else { imiSignalLine.push(null); }\nconst validImiForSma = imiLine.filter(v => v !== null);\nif (validImiForSma.length >= IMI_SMA_PERIOD) {\nconst smaSlice = validImiForSma.slice(-IMI_SMA_PERIOD);\nconst smaValue = smaSlice.reduce((a, b) => a + b, 0) / IMI_SMA_PERIOD;\nimiSmaLine.push(smaValue);\n} else { imiSmaLine.push(null); }\n}\ndetectarAlertasIMI();\n}\nfunction detectarAlertasIMI() {\nif (!document.getElementById(\'imi\')?.checked) return;\nif (imiLine.length < 2) return;\nconst ahora = Date.now();\nconst imiActual = imiLine[imiLine.length - 1];\nconst imiAnterior = imiLine[imiLine.length - 2];\nconst smaActual = imiSmaLine[imiSmaLine.length - 1];\nconst smaAnterior = imiSmaLine[imiSmaLine.length - 2];\nif (imiActual === null || smaActual === null) return;\nif (imiActual > 70 && ahora - ultimaReversionBajista > TIEMPO_COOLDOWN_IMI) {\nultimaReversionBajista = ahora;\nmostrarAlertaFlotanteIMI(\'bearish\', \'Posible Reversión Bajista\', `IMI: ${imiActual.toFixed(1)} (Sobrecompra > 70)`);\nplaySound(\'alert\');\nspeakVoice(\'Sobrecompra detectada, posible reversión bajista\', 2);\n}\nif (imiActual < 30 && ahora - ultimaReversionAlcista > TIEMPO_COOLDOWN_IMI) {\nultimaReversionAlcista = ahora;\nmostrarAlertaFlotanteIMI(\'bullish\', \'Posible Reversión Alcista\', `IMI: ${imiActual.toFixed(1)} (Sobreventa < 30)`);\nplaySound(\'alert\');\nspeakVoice(\'Sobreventa detectada, posible reversión alcista\', 2);\n}\nif (smaAnterior !== null && imiAnterior !== null) {\nconst cruceAlcista = imiAnterior < smaAnterior && imiActual > smaActual;\nconst cruceBajista = imiAnterior > smaAnterior && imiActual < smaActual;\nif ((cruceAlcista || cruceBajista) && ahora - ultimoCruceMomentum > TIEMPO_COOLDOWN_IMI) {\nultimoCruceMomentum = ahora;\nconst tipoCruce = cruceAlcista ? \'🟡 Momentum Alcista\' : \'🟡 Momentum Bajista\';\nconst detalleCruce = cruceAlcista ? \'IMI cruza SMA 15 hacia arriba\' : \'IMI cruza SMA 15 hacia abajo\';\nmostrarAlertaFlotanteIMI(\'momentum\', tipoCruce, detalleCruce);\nplaySound(\'alert\');\n}\n}\n}\nfunction mostrarAlertaFlotanteIMI(tipo, titulo, detalle) {\nconst alertEl = document.getElementById(\'imiFloatingAlert\');\nconst titleEl = document.getElementById(\'imiAlertTitle\');\nconst detailEl = document.getElementById(\'imiAlertDetail\');\nif (imiAlertTimeout) { clearTimeout(imiAlertTimeout); }\n// Posición: centro sin señal activa · debajo de la alerta de señal cuando hay una activa\nconst sigEl = document.getElementById(\'modFloatingAlert\');\nconst sigVisible = sigEl && sigEl.style.display === \'block\';\nalertEl.className = `imi-floating-alert ${tipo}` + (sigVisible ? \' below-signal\' : \'\');\nalertEl.style.display = \'block\';\ntitleEl.textContent = titulo; detailEl.textContent = detalle;\nimiAlertTimeout = setTimeout(() => { alertEl.style.display = \'none\'; }, TIEMPO_ALERTA_IMI);\n}\nfunction detectarFractales(puntos, data) {\nfractales = []; if (puntos.length < 5) return [];\nfor (let i = 2; i < puntos.length - 2; i++) {\nif (puntos[i] < puntos[i-1] && puntos[i] < puntos[i-2] && puntos[i] < puntos[i+1] && puntos[i] < puntos[i+2]) {\nconst esAlcista = i >= 5 && puntos[i] > puntos[i-5];\nif (esAlcista) { fractales.push({ index: i, tipo: \'up\', valor: puntos[i], precio: data[i] ? data[i].value : 0 }); }\n}\nif (puntos[i] > puntos[i-1] && puntos[i] > puntos[i-2] && puntos[i] > puntos[i+1] && puntos[i] > puntos[i+2]) {\nconst esBajista = i >= 5 && puntos[i] < puntos[i-5];\nif (esBajista) { fractales.push({ index: i, tipo: \'down\', valor: puntos[i], precio: data[i] ? data[i].value : 0 }); }\n}\n}\nreturn fractales;\n}\nfunction detectarSoportesResistencias(puntos, valores) {\nif (puntos.length < 30) return { soportes: [], resistencias: [] };\nconst soportes = []; const resistencias = []; const tolerancia = 0.5; const minRebotes = 2;\nfor (let i = 5; i < puntos.length - 5; i++) {\nconst ventana = puntos.slice(i - 5, i + 6);\nconst minimo = Math.min(...ventana); const indiceMinimo = ventana.indexOf(minimo) + (i - 5);\nif (indiceMinimo === i && puntos[i] < puntos[i-1] && puntos[i] < puntos[i+1]) {\nconst nivel = puntos[i]; const fuerzaRebote = Math.abs(puntos[i-1] - puntos[i]) + Math.abs(puntos[i+1] - puntos[i]);\nconst existe = soportes.find(s => Math.abs(s.nivel - nivel) <= tolerancia);\nif (existe) { existe.rebotes++; existe.fuerzaTotal += fuerzaRebote; existe.ultimosIndices.push(i); if (nivel > existe.nivel) { existe.nivel = nivel; } }\nelse { soportes.push({ nivel: nivel, rebotes: 1, fuerzaTotal: fuerzaRebote, ultimosIndices: [i], esFuerte: false }); }\n}\n}\nfor (let i = 5; i < puntos.length - 5; i++) {\nconst ventana = puntos.slice(i - 5, i + 6);\nconst maximo = Math.max(...ventana); const indiceMaximo = ventana.indexOf(maximo) + (i - 5);\nif (indiceMaximo === i && puntos[i] > puntos[i-1] && puntos[i] > puntos[i+1]) {\nconst nivel = puntos[i]; const fuerzaRebote = Math.abs(puntos[i-1] - puntos[i]) + Math.abs(puntos[i+1] - puntos[i]);\nconst existe = resistencias.find(r => Math.abs(r.nivel - nivel) <= tolerancia);\nif (existe) { existe.rebotes++; existe.fuerzaTotal += fuerzaRebote; existe.ultimosIndices.push(i); if (nivel > existe.nivel) { existe.nivel = nivel; } }\nelse { resistencias.push({ nivel: nivel, rebotes: 1, fuerzaTotal: fuerzaRebote, ultimosIndices: [i], esFuerte: false }); }\n}\n}\nconst soportesValidos = soportes.filter(s => s.rebotes >= minRebotes);\nconst resistenciasValidas = resistencias.filter(r => r.rebotes >= minRebotes);\nsoportesValidos.forEach(s => { if (s.rebotes >= 3 || s.fuerzaTotal >= 4) { s.esFuerte = true; } });\nresistenciasValidas.forEach(r => { if (r.rebotes >= 3 || r.fuerzaTotal >= 4) { r.esFuerte = true; } });\nconst soportesFinales = soportesValidos.sort((a, b) => b.ultimosIndices[b.ultimosIndices.length - 1] - a.ultimosIndices[a.ultimosIndices.length - 1]).slice(0, 3);\nconst resistenciasFinales = resistenciasValidas.sort((a, b) => b.ultimosIndices[b.ultimosIndices.length - 1] - a.ultimosIndices[a.ultimosIndices.length - 1]).slice(0, 3);\nreturn { soportes: soportesFinales, resistencias: resistenciasFinales };\n}\n// =====================================================\n// ✅ FUNCIÓN PRINCIPAL DE DIBUJADO OPTIMIZADA\n// =====================================================\nfunction draw() {\nconst width = canvas.width; const height = canvas.height; const padding = 40;\nctx.clearRect(0, 0, width, height);\nif (data.length === 0) { ctx.fillStyle = \'rgba(255, 255, 255, 0.5)\'; ctx.font = \'24px Arial\'; ctx.textAlign = \'center\'; ctx.fillText(\'👆 Presiona un botón para agregar valores\', width / 2, height / 2); return; }\nconst values = data.map(d => d.value);\nlet min = Math.min(...values) * 0.995; let max = Math.max(...values) * 1.005;\ndrawModerate(ctx, data, padding, width, height);\ncalculateIMI(data);\nconst imiWidth = imiCanvas.width; const imiHeight = imiCanvas.height;\nimiCtx.clearRect(0, 0, imiWidth, imiHeight);\ndrawIMI(imiCtx, padding, imiWidth, imiHeight);\nif (data.length > 0) {\nconst last = data[data.length - 1]; const color = rangeColors[last.range] || rangeColors[\'2.00-2.99\'];\nctx.fillStyle = color.fill; ctx.font = \'bold 26px Arial\'; ctx.textAlign = \'right\';\nctx.fillText(`${last.value.toFixed(2)}x`, width - padding, padding - 10);\n}\n// ✅ ALTERNAR PANELES DE EVALUACIÓN AUTOMÁTICAMENTE\nconst moderatePanels = document.getElementById(\'moderateEvalPanels\');\nconst moderateContainer = document.getElementById(\'moderateEvalContainer\');\nmoderatePanels.classList.remove(\'hidden\');\nmoderatePanels.style.display = \'flex\';\nmoderateContainer.style.display = \'block\';\n// ✅ ACTUALIZAR ALERTROW SEGÚN CHARTTYPE\nupdateModerateEmaCircle();\n}\n\nfunction updateHistoryTopBar() {\n  const bar = document.getElementById(\'historyTopBar\');\n  if (!bar) return;\n  const last10 = historyData.slice(-10);\n  if (last10.length === 0) {\n    bar.innerHTML = \'<span class="hist-chip-label">HISTORIAL</span><span style="font-family: Share Tech Mono,monospace;font-size:9px;color:rgba(0,212,255,.3)">Agrega valores...</span>\';\n    return;\n  }\n  const clsMap = {\'1.00-1.49\':\'hc-1\',\'1.50-1.99\':\'hc-2\',\'2.00-2.99\':\'hc-3\',\'3.00-4.99\':\'hc-4\',\'5.00+\':\'hc-5\',\'10.00+\':\'hc-6\'};\n  let html = \'<span class="hist-chip-label">ULTIMOS</span>\';\n  last10.forEach((item, idx) => {\n    const cls = clsMap[item.range] || \'hc-3\';\n    const op = (0.35 + (idx / last10.length) * 0.65).toFixed(2);\n    html += \'<span class="hist-chip \' + cls + \'" style="opacity:\' + op + \'">\' + item.value.toFixed(2) + \'x</span>\';\n  });\n  bar.innerHTML = html;\n}\n\n// =====================================================\n// INICIALIZACIÓN\n// =====================================================\nfunction init() {\nconst container = canvas.parentElement;\ncanvas.width = container.clientWidth * 2;\ncanvas.height = container.clientHeight * 2;\n// Build Y-axis labels\nconst yAxisEl = document.getElementById(\'yAxis\');\nif (yAxisEl) {\n  yAxisEl.innerHTML = \'\';\n  for (let v = 1; v >= -8; v--) {\n    const lbl = document.createElement(\'span\');\n    lbl.className = \'y-label\';\n    lbl.textContent = (v > 0 ? \'+\' : \'\') + v;\n    yAxisEl.appendChild(lbl);\n  }\n}\nimiCanvas.width = container.clientWidth * 2; imiCanvas.height = 120 * 2;\ndocument.querySelectorAll(\'.val-btn[data-range]\').forEach(btn => {\nbtn.addEventListener(\'click\', function() {\nconst rangeKey = this.dataset.range; const minValue = parseFloat(this.dataset.min); const maxValue = parseFloat(this.dataset.max);\naddValueManually(rangeKey, minValue, maxValue);\n});\n});\ndocument.getElementById(\'btnDeleteLast\').addEventListener(\'click\', deleteLastValue);\ndocument.getElementById(\'btnResetAll\').addEventListener(\'click\', resetAllValues);\ndocument.querySelectorAll(\'.range-btn\').forEach(btn => {\nbtn.addEventListener(\'click\', () => {\ndocument.querySelectorAll(\'.range-btn\').forEach(b => b.classList.remove(\'active\'));\nbtn.classList.add(\'active\'); const range = parseInt(btn.dataset.range);\nif (historyData.length > range) { data = historyData.slice(-range); } else { data = [...historyData]; }\ndocument.getElementById(\'valuesCount\').textContent = `Valores: ${data.length}`; draw();\n});\n});\nsetInterval(() => { const _ct = document.getElementById(\'currentTime\'); if (_ct) _ct.textContent = new Date().toLocaleTimeString(); checkAutoPredictions(); }, 1000);\nupdateModerateStatsPanels(); updateModerateEmaCircle(); draw();\naddAlert("✅ AVIATOR V20.0 Activado - Evaluación Dual Independiente", "success");\n}\nwindow.addEventListener(\'load\', () => { setTimeout(init, 100); });\nwindow.addEventListener(\'resize\', () => {\nconst container = canvas.parentElement;\ncanvas.width = container.clientWidth * 2;\ncanvas.height = container.clientHeight * 2;\n// Build Y-axis labels\nconst yAxisEl = document.getElementById(\'yAxis\');\nif (yAxisEl) {\n  yAxisEl.innerHTML = \'\';\n  for (let v = 1; v >= -8; v--) {\n    const lbl = document.createElement(\'span\');\n    lbl.className = \'y-label\';\n    lbl.textContent = (v > 0 ? \'+\' : \'\') + v;\n    yAxisEl.appendChild(lbl);\n  }\n}\nimiCanvas.width = container.clientWidth * 2; imiCanvas.height = 120 * 2;\ndraw();\n});\n\n// ══════════════════════════════════════════════\n//  RESIZE HANDLES — canvas principal, IMI, botones\n// ══════════════════════════════════════════════\nfunction makeResizable(cfg) {\n  const el = document.getElementById(cfg.id); if (!el) return;\n  const tip = document.getElementById(cfg.tipId);\n  const MIN_W = cfg.minW||220, MIN_H = cfg.minH||80;\n  let active=false, rType=null, sx=0, sy=0, sw=0, sh=0;\n  function getXY(e){ return { x:e.clientX??(e.touches?.[0]?.clientX??0), y:e.clientY??(e.touches?.[0]?.clientY??0) }; }\n  function start(e,type){\n    active=true; rType=type; const p=getXY(e); sx=p.x; sy=p.y;\n    sw=el.offsetWidth; sh=el.offsetHeight;\n    document.body.style.cursor=type===\'right\'?\'ew-resize\':type===\'bottom\'?\'ns-resize\':\'nwse-resize\';\n    document.body.style.userSelect=\'none\'; e.preventDefault();\n  }\n  function move(e){\n    if(!active) return;\n    const p=getXY(e), dx=p.x-sx, dy=p.y-sy;\n    if(rType===\'corner\'||rType===\'right\') el.style.width=Math.max(MIN_W,sw+dx)+\'px\';\n    if(rType===\'corner\'||rType===\'bottom\') el.style.height=Math.max(MIN_H,sh+dy)+\'px\';\n    if(tip){ tip.textContent=Math.round(el.offsetWidth)+\'×\'+Math.round(el.offsetHeight); tip.style.opacity=\'1\'; }\n    if(cfg.innerCanvas){ const c=document.getElementById(cfg.innerCanvas); if(c) c.style.height=Math.max(40,el.offsetHeight-24)+\'px\'; }\n    if(cfg.redraw&&typeof draw===\'function\') draw();\n    e.preventDefault();\n  }\n  function stop(){\n    if(!active) return; active=false; rType=null;\n    document.body.style.cursor=\'\'; document.body.style.userSelect=\'\';\n    if(tip) setTimeout(()=>tip.style.opacity=\'0\',2000);\n    if(cfg.redraw&&typeof draw===\'function\') setTimeout(draw,30);\n  }\n  const corner=document.getElementById(cfg.cornerId);\n  const right=document.getElementById(cfg.rightId);\n  const bottom=document.getElementById(cfg.bottomId);\n  if(corner){\n    corner.addEventListener(\'mousedown\', e=>start(e,\'corner\'));\n    corner.addEventListener(\'touchstart\', e=>start(e,\'corner\'),{passive:false});\n    corner.addEventListener(\'dblclick\', ()=>{\n      el.style.width=\'\'; el.style.height=\'\';\n      if(cfg.innerCanvas){ const c=document.getElementById(cfg.innerCanvas); if(c) c.style.height=(cfg.defaultH||140)+\'px\'; }\n      if(tip){ tip.textContent=\'⤡ reset\'; tip.style.opacity=\'1\'; setTimeout(()=>tip.style.opacity=\'0\',2000); }\n      if(cfg.redraw&&typeof draw===\'function\') setTimeout(draw,30);\n    });\n  }\n  if(right){ right.addEventListener(\'mousedown\',e=>start(e,\'right\')); right.addEventListener(\'touchstart\',e=>start(e,\'right\'),{passive:false}); }\n  if(bottom){ bottom.addEventListener(\'mousedown\',e=>start(e,\'bottom\')); bottom.addEventListener(\'touchstart\',e=>start(e,\'bottom\'),{passive:false}); }\n  document.addEventListener(\'mousemove\',move);\n  document.addEventListener(\'touchmove\',move,{passive:false});\n  document.addEventListener(\'mouseup\',stop);\n  document.addEventListener(\'touchend\',stop);\n  if(tip){ tip.textContent=\'⤡ arrastra para redimensionar\'; tip.style.opacity=\'1\'; setTimeout(()=>tip.style.opacity=\'0\',3500); }\n}\nsetTimeout(()=>{\n  makeResizable({id:\'chartContainer\',cornerId:\'chartResizeCorner\',rightId:\'chartResizeRight\',bottomId:\'chartResizeBottom\',tipId:\'chartResizeTip\',minW:300,minH:200,defaultH:500,redraw:true});\n  makeResizable({id:\'imiContainer\',cornerId:\'imiResizeCorner\',rightId:\'imiResizeRight\',bottomId:\'imiResizeBottom\',tipId:\'imiResizeTip\',minW:220,minH:50,innerCanvas:\'imiChart\',defaultH:140,redraw:true});\n  makeResizable({id:\'valuesPanel\',cornerId:\'valsResizeCorner\',rightId:\'valsResizeRight\',bottomId:\'valsResizeBottom\',tipId:\'valsResizeTip\',minW:220,minH:180,redraw:false});\n},400);\n\n// ══════════════════════════════════════════\n//  SCROLL DRAG BUTTON\n// ══════════════════════════════════════════\n(function(){\n  const btn = document.getElementById(\'scrollDragBtn\');\n  const pct = document.getElementById(\'scrollPct\');\n  if(!btn) return;\n  let dragging=false, startY=0, startScroll=0;\n  const SPEED=2.5;\n  function getMax(){ return document.documentElement.scrollHeight - window.innerHeight; }\n  function updatePct(){\n    const max=getMax(); if(max<=0){pct.textContent=\'0%\';return;}\n    const p=Math.round((window.scrollY/max)*100);\n    pct.textContent=p+\'%\';\n    btn.style.top=(10+(p/100)*80)+\'%\';\n    btn.style.transform=\'translateY(-50%)\';\n  }\n  function onStart(e){\n    dragging=true;\n    startY=e.clientY??(e.touches?.[0]?.clientY??0);\n    startScroll=window.scrollY;\n    btn.style.background=\'rgba(0,212,255,.15)\';\n    btn.style.borderColor=\'rgba(0,212,255,.8)\';\n    document.body.style.userSelect=\'none\';\n    e.preventDefault();\n  }\n  function onMove(e){\n    if(!dragging) return;\n    const cy=e.clientY??(e.touches?.[0]?.clientY??startY);\n    window.scrollTo({top:Math.max(0,Math.min(getMax(),startScroll+(cy-startY)*SPEED)),behavior:\'instant\'});\n    updatePct(); e.preventDefault();\n  }\n  function onEnd(){\n    if(!dragging)return; dragging=false;\n    btn.style.background=\'\'; btn.style.borderColor=\'\';\n    document.body.style.userSelect=\'\';\n  }\n  btn.addEventListener(\'mousedown\',onStart);\n  document.addEventListener(\'mousemove\',onMove);\n  document.addEventListener(\'mouseup\',onEnd);\n  btn.addEventListener(\'touchstart\',onStart,{passive:false});\n  document.addEventListener(\'touchmove\',onMove,{passive:false});\n  document.addEventListener(\'touchend\',onEnd);\n  btn.addEventListener(\'click\',function(e){\n    if(dragging)return;\n    const r=btn.getBoundingClientRect();\n    window.scrollBy({top:e.clientY<r.top+r.height/2?-200:200,behavior:\'smooth\'});\n  });\n  window.addEventListener(\'scroll\',updatePct,{passive:true});\n  window.addEventListener(\'resize\',updatePct,{passive:true});\n  setTimeout(updatePct,600);\n})();\n\n// ══════════════════════════════════════════════\n//  ESTRATEGIA DINERO REAL — MARTINGALE 2×3 AUTO\n// ══════════════════════════════════════════════\n(function(){\n  // Estado\n  let sCapital=100, sBalance=100, sBaseBet=10;\n  let sActive=false, sScale=1, sCol=1, sAttempt=1, sLost=0, sCurBet=10;\n  const S_MAX_ATT=2;\n  let sEntries=0, sWins=0, sLosses=0;\n  // Auto: cuál gráfica sigue y si está esperando SO\n  let sChart=\'moderate\'; // única gráfica: Moderado\n  let sWaitingSO=false;  // true = esperando resultado SO, false = esperando alerta principal\n\n  // Cache de apuestas pre-calculadas al iniciar\n  let sBetCache = {};\n\n  // Exponer estado para que las gráficas lo consulten\n  window._sActive    = ()=> sActive;\n  window._sChart     = ()=> sChart;\n  window._sWaitingSO = ()=> sWaitingSO;\n  window._sCurBet    = ()=> sCurBet;\n  window._sCol       = ()=> sCol;\n  window._sAtt       = ()=> sAttempt;\n  window._sBetCache  = ()=> sBetCache;\n  // Punto de entrada automático desde las gráficas\n  window.sAutoResult = function(win){ sResult(win); };\n  window.sAutoSetWaiting = function(v){ sWaitingSO = v; };\n\n  // Preview en tiempo real mientras escribe (antes de aplicar)\n  window.sLiveUpdate = function(){\n    if(sActive) return; // Solo en inactivo\n    const cap = parseFloat(document.getElementById(\'sCapIn\').value)||0;\n    const bet = parseFloat(document.getElementById(\'sBetIn\').value)||0;\n    if(cap>0) document.getElementById(\'sBalance\').textContent=\'$\'+cap.toFixed(2);\n    if(bet>0) document.getElementById(\'sApuesta\').textContent=\'$\'+bet.toFixed(2);\n    const diff = cap - sCapital;\n    document.getElementById(\'sBalance\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n  };\n\n  // Selector gráfica\n  window.sSetChart = function(c){\n    sChart = \'moderate\'; // única gráfica disponible\n    const bMod = document.getElementById(\'sBtnMod\');\n    if(bMod){ bMod.style.background=\'rgba(0,212,255,.25)\'; bMod.style.color=\'#00d4ff\'; bMod.style.borderColor=\'rgba(0,212,255,.5)\'; }\n    if(!sActive) sSetAlert(\'⚡ Gráfica Moderado — sigue alerta 2.00\');\n  };\n\n  // Toggle panel\n  window.stratToggle = function(){\n    const body=document.getElementById(\'stratBody\');\n    const hdr=document.getElementById(\'stratHeader\');\n    const arr=document.getElementById(\'stratArrow\');\n    const isOpen = !body.classList.contains(\'sopen\');\n    if(isOpen){ body.classList.add(\'sopen\'); body.style.display=\'block\'; }\n    else { body.classList.remove(\'sopen\'); body.style.display=\'\'; }\n    hdr.classList.toggle(\'sopen\',isOpen);\n    arr.style.transform=isOpen?\'rotate(180deg)\':\'\';\n  };\n\n  // Config\n  window.sCfgToggle=function(){ const c=document.getElementById(\'sCfg\'); c.style.display=c.style.display===\'block\'?\'none\':\'block\'; };\n  window.sApply=function(){\n    const newCap = Math.max(0.01, parseFloat(document.getElementById(\'sCapIn\').value)||100);\n    const newBet = Math.max(0.01, parseFloat(document.getElementById(\'sBetIn\').value)||10);\n    sCapital = newCap;\n    sBaseBet = newBet;\n    if(!sActive){\n      sBalance = sCapital;\n      sCurBet  = sBaseBet;\n      sBuildBetCache(sBaseBet);\n      sUpdateUI();\n      sSetAlert(\'✅ Balance $\'+sCapital+\' · Apuesta base $\'+sBaseBet);\n    } else {\n      // Sesión activa: actualiza capital y apuesta base sin reiniciar\n      // La apuesta actual se recalcula si no hay pérdidas acumuladas\n      if(sLost === 0) sCurBet = sBaseBet;\n      sUpdateUI();\n      sSetAlert(\'⚡ Base actualizada: $\'+sBaseBet+\' · Balance ajustado a $\'+sCapital, true);\n    }\n    document.getElementById(\'sCfg\').style.display=\'none\';\n  };\n\n  // ── PRE-CALCULA Y CALIENTA TTS para todas las apuestas C1A1..C3A2 ──\n  function sBuildBetCache(baseBet) {\n    let lost = 0;\n    sBetCache = {};\n    for (let col = 1; col <= 3; col++) {\n      for (let att = 1; att <= S_MAX_ATT; att++) {\n        const bet = (col === 1 && att === 1) ? baseBet : lost + baseBet;\n        sBetCache[\'C\' + col + \'A\' + att] = bet;\n        lost += bet;\n      }\n    }\n    // Pre-calentar motor TTS: habla cada monto en silencio para que el navegador\n    // ya tenga el audio sintetizado cuando llegue la señal real\n    if (window.speechSynthesis) {\n      const keys = Object.keys(sBetCache);\n      keys.forEach(function(k, i) {\n        setTimeout(function() {\n          try {\n            const utt = new SpeechSynthesisUtterance(\'apostar \' + sBetCache[k]);\n            utt.lang = \'es-ES\'; utt.volume = 0; utt.rate = 0.95; utt.pitch = 1.0;\n            const voices = window.speechSynthesis.getVoices();\n            const esVoice = voices.find(v => v.lang === \'es-ES\') || voices.find(v => v.lang && v.lang.startsWith(\'es\')) || null;\n            if (esVoice) utt.voice = esVoice;\n            window.speechSynthesis.speak(utt);\n          } catch(e) {}\n        }, 400 + i * 200);\n      });\n    }\n  }\n\n  // Iniciar\n  window.sStart=function(){\n    if(sActive) return;\n    sActive=true; sBalance=sCapital; sScale=1; sCol=1; sAttempt=1; sLost=0; sCurBet=sBaseBet;\n    sEntries=sWins=sLosses=0; sWaitingSO=false;\n    sBuildBetCache(sBaseBet);\n    document.getElementById(\'sControls\').style.display=\'grid\';\n    document.getElementById(\'sHist\').style.display=\'block\';\n    document.getElementById(\'sBtnStart\').style.display=\'none\';\n    document.getElementById(\'sAutoStatus\').style.display=\'block\';\n    document.getElementById(\'sHistBody\').innerHTML=\'<tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:6px">Sin datos</td></tr>\';\n    const grafLabel = \'Moderado (alerta 2.00)\';\n    sSetAlert(\'🤖 AUTO · Esc \'+sScale+\' · Esperando alerta en \'+grafLabel);\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // Resultado — llamado automáticamente desde las gráficas\n  window.sResult=function(win){\n    if(!sActive) return;\n    sEntries++;\n    if(win){\n      const net=sCurBet-sLost;\n      sBalance+=net; sWins++;\n      sSetAlert(\'✅ +$\'+net+\' neto · Esc \'+sScale+\' ✓ — Esperando nueva alerta\',false);\n      sAddHist(sScale,sCol,sAttempt,sCurBet,win);\n      sScale++; sLost=0; sCurBet=sBaseBet; sWaitingSO=false;\n      if(sScale>10){ sEndCycle(true); return; }\n      sCol=1; sAttempt=1;\n    } else {\n      sLost+=sCurBet; sLosses++;\n      sCurBet=sLost+sBaseBet; sAttempt++;\n      sAddHist(sScale,sCol,sAttempt-1,sCurBet,win);\n      if(sAttempt>S_MAX_ATT){\n        sAttempt=1; sCol++; sWaitingSO=false;\n        if(sCol>3){ sSetAlert(\'⚠️ 3 columnas fallidas · Esc \'+sScale,true); sEndCycle(false); return; }\n        sCurBet=sLost+sBaseBet;\n        sSetAlert(\'📍 Col \'+sCol+\' · $\'+sCurBet+\' · Esperando próxima alerta 2.00\',true);\n      } else {\n        sWaitingSO=true;\n        sSetAlert(\'❌ Perdida · $\'+sCurBet+\' · 🔄 Esperando 2ª Oportunidad\',true);\n      }\n    }\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // UI\n  function sUpdateUI(){\n    document.getElementById(\'sBalance\').textContent=\'$\'+sBalance.toFixed(2);\n    document.getElementById(\'sMargen\').textContent=\'$\'+(sBalance-sCapital).toFixed(2);\n    document.getElementById(\'sColumna\').textContent=sCol;\n    document.getElementById(\'sApuesta\').textContent=\'$\'+sCurBet;\n    document.getElementById(\'sEscLabel\').textContent=sScale;\n    const diff=sBalance-sCapital;\n    document.getElementById(\'sBalance\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n    document.getElementById(\'sMargen\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n  }\n  function sUpdateCols(){\n    for(let i=1;i<=3;i++){\n      const el=document.getElementById(\'sC\'+i);\n      const st=document.getElementById(\'sSt\'+i);\n      const bt=document.getElementById(\'sBet\'+i);\n      el.classList.remove(\'sc-active\',\'sc-done\');\n      if(i===sCol&&sActive){ el.classList.add(\'sc-active\'); st.textContent=\'Intento \'+sAttempt+\'/\'+S_MAX_ATT; st.style.color=\'#f1c40f\'; bt.textContent=\'$\'+sCurBet; }\n      else if(i<sCol){ el.classList.add(\'sc-done\'); st.textContent=\'✅ OK\'; st.style.color=\'#00ff88\'; bt.textContent=\'\'; }\n      else { st.textContent=\'--\'; st.style.color=\'rgba(255,255,255,.3)\'; bt.textContent=\'\'; }\n    }\n  }\n  function sUpdateDots(){\n    const cont=document.getElementById(\'sDots\'); cont.innerHTML=\'\';\n    for(let i=1;i<=10;i++){ const d=document.createElement(\'div\'); d.className=\'s-dot\'+(i<sScale?\' sd-done\':i===sScale?\' sd-active\':\'\'); d.textContent=i; cont.appendChild(d); }\n  }\n  function sSetAlert(msg,pulse=false){ const el=document.getElementById(\'sAlerta\'); el.innerHTML=msg; el.classList.toggle(\'sa-pulse\',pulse); }\n  function sAddHist(sc,cl,att,bet,win){\n    const tb=document.getElementById(\'sHistBody\');\n    if(tb.querySelector(\'[colspan]\')) tb.innerHTML=\'\';\n    tb.insertAdjacentHTML(\'afterbegin\',`<tr><td>${sEntries}</td><td>${sc}</td><td>${cl}</td><td style="color:#f1c40f">$${bet}</td><td class="${win?\'sh-win\':\'sh-loss\'}">${win?\'WIN\':\'LOSS\'}</td><td>$${sBalance.toFixed(2)}</td></tr>`);\n  }\n\n  // Ciclo\n  function sEndCycle(ok){\n    sActive=false; sWaitingSO=false;\n    document.getElementById(\'sControls\').style.display=\'none\';\n    document.getElementById(\'sBtnStart\').style.display=\'block\';\n    document.getElementById(\'sAutoStatus\').style.display=\'none\';\n    if(ok){\n      document.getElementById(\'smCap\').textContent=sCapital;\n      document.getElementById(\'smBal\').textContent=sBalance.toFixed(2);\n      document.getElementById(\'smGain\').textContent=(sBalance-sCapital).toFixed(2);\n      document.getElementById(\'smEff\').textContent=sEntries?Math.round(sWins/sEntries*100):0;\n      document.getElementById(\'sModalWin\').style.display=\'flex\';\n    } else {\n      document.getElementById(\'smFEsc\').textContent=sScale;\n      document.getElementById(\'smFBal\').textContent=sBalance.toFixed(2);\n      document.getElementById(\'smFLoss\').textContent=(sCapital-sBalance).toFixed(2);\n      document.getElementById(\'sModalLoss\').style.display=\'flex\';\n    }\n  }\n  window.sCloseModal=function(id){ document.getElementById(id).style.display=\'none\'; };\n  window.sNewCycle=function(){ sCloseModal(\'sModalWin\'); sStart(); };\n  window.sReset=function(){\n    sActive=false; sBalance=sCapital; sScale=1; sCol=1; sAttempt=1;\n    sLost=0; sCurBet=sBaseBet; sEntries=sWins=sLosses=0; sWaitingSO=false;\n    document.getElementById(\'sControls\').style.display=\'none\';\n    document.getElementById(\'sHist\').style.display=\'none\';\n    document.getElementById(\'sBtnStart\').style.display=\'block\';\n    document.getElementById(\'sAutoStatus\').style.display=\'none\';\n    document.getElementById(\'sAlerta\').innerHTML=\'⏳ Esperando inicio...\';\n    document.getElementById(\'sAlerta\').classList.remove(\'sa-pulse\');\n    document.getElementById(\'sHistBody\').innerHTML=\'<tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:6px">Sin datos</td></tr>\';\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // Init\n  sSetChart(\'moderate\');\n  sUpdateUI(); sUpdateDots();\n})();\n\n// ══════════════════════════════════════════════\n//  GESTIÓN DINERO REAL 3X — MARTINGALE 2×3 (pago 3x)\n// ══════════════════════════════════════════════\n(function(){\n  // Apuesta de recuperación a pago 3x: recupera lo perdido y gana la base\n  // ✅ TABLA DE FICHAS 3X por columna/intento: C1[1,1] · C2[2,2.5] · C3[4.5,6] (múltiplos de la apuesta base)\n  const S3_FICHAS = { 1:[1,1], 2:[2,2.5], 3:[4.5,6] };\n  function s3BetFor(col, att, base){ return Math.round(S3_FICHAS[col][att-1] * base * 100) / 100; }\n  // Estado\n  let sCapital=100, s3Balance=100, sBaseBet=10;\n  let sActive=false, sScale=1, sCol=1, sAttempt=1, sLost=0, sCurBet=10;\n  const S_MAX_ATT=2;\n  let sEntries=0, sWins=0, sLosses=0;\n  // Auto: cuál gráfica sigue y si está esperando SO\n  let sChart=\'moderate\'; // única gráfica: Moderado\n  let sWaitingSO=false;  // true = esperando resultado SO, false = esperando alerta principal 3.00\n\n  // Cache de apuestas pre-calculadas al iniciar\n  let sBetCache = {};\n\n  // Exponer estado para que las gráficas lo consulten\n  window._s3Active    = ()=> sActive;\n  window._s3Chart     = ()=> sChart;\n  window._s3WaitingSO = ()=> sWaitingSO;\n  window._s3CurBet    = ()=> sCurBet;\n  window._s3Col       = ()=> sCol;\n  window._s3Att       = ()=> sAttempt;\n  window._s3BetCache  = ()=> sBetCache;\n  // Punto de entrada automático desde las gráficas\n  window.s3AutoResult = function(win){ s3Result(win); };\n  window.s3AutoSetWaiting = function(v){ sWaitingSO = v; };\n\n  // Preview en tiempo real mientras escribe (antes de aplicar)\n  window.s3LiveUpdate = function(){\n    if(sActive) return; // Solo en inactivo\n    const cap = parseFloat(document.getElementById(\'s3CapIn\').value)||0;\n    const bet = parseFloat(document.getElementById(\'s3BetIn\').value)||0;\n    if(cap>0) document.getElementById(\'s3Balance\').textContent=\'$\'+cap.toFixed(2);\n    if(bet>0) document.getElementById(\'s3Apuesta\').textContent=\'$\'+bet.toFixed(2);\n    const diff = cap - sCapital;\n    document.getElementById(\'s3Balance\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n  };\n\n  // Selector gráfica\n  window.s3SetChart = function(c){\n    sChart = \'moderate\'; // única gráfica disponible\n    const bMod = document.getElementById(\'s3BtnMod\');\n    if(bMod){ bMod.style.background=\'rgba(0,212,255,.25)\'; bMod.style.color=\'#00d4ff\'; bMod.style.borderColor=\'rgba(0,212,255,.5)\'; }\n    if(!sActive) sSetAlert(\'⚡ Gráfica Moderado — sigue alerta 3.00\');\n  };\n\n  // Toggle panel\n  window.strat3Toggle = function(){\n    const body=document.getElementById(\'strat3Body\');\n    const hdr=document.getElementById(\'strat3Header\');\n    const arr=document.getElementById(\'strat3Arrow\');\n    const isOpen = !body.classList.contains(\'sopen\');\n    if(isOpen){ body.classList.add(\'sopen\'); body.style.display=\'block\'; }\n    else { body.classList.remove(\'sopen\'); body.style.display=\'\'; }\n    hdr.classList.toggle(\'sopen\',isOpen);\n    arr.style.transform=isOpen?\'rotate(180deg)\':\'\';\n  };\n\n  // Config\n  window.s3CfgToggle=function(){ const c=document.getElementById(\'s3Cfg\'); c.style.display=c.style.display===\'block\'?\'none\':\'block\'; };\n  window.s3Apply=function(){\n    const newCap = Math.max(0.01, parseFloat(document.getElementById(\'s3CapIn\').value)||100);\n    const newBet = Math.max(0.01, parseFloat(document.getElementById(\'s3BetIn\').value)||10);\n    sCapital = newCap;\n    sBaseBet = newBet;\n    if(!sActive){\n      s3Balance = sCapital;\n      sCurBet  = sBaseBet;\n      sBuildBetCache(sBaseBet);\n      sUpdateUI();\n      sSetAlert(\'✅ Balance $\'+sCapital+\' · Apuesta base $\'+sBaseBet);\n    } else {\n      // Sesión activa: actualiza capital y apuesta base sin reiniciar\n      // La apuesta actual se recalcula si no hay pérdidas acumuladas\n      if(sLost === 0) sCurBet = sBaseBet;\n      sUpdateUI();\n      sSetAlert(\'⚡ Base actualizada: $\'+sBaseBet+\' · Balance ajustado a $\'+sCapital, true);\n    }\n    document.getElementById(\'s3Cfg\').style.display=\'none\';\n  };\n\n  // ── PRE-CALCULA Y CALIENTA TTS para todas las apuestas C1A1..C3A2 ──\n  function sBuildBetCache(baseBet) {\n    sBetCache = {};\n    for (let col = 1; col <= 3; col++) {\n      for (let att = 1; att <= S_MAX_ATT; att++) {\n        sBetCache[\'C\' + col + \'A\' + att] = s3BetFor(col, att, baseBet);\n      }\n    }\n    // Pre-calentar motor TTS: habla cada monto en silencio para que el navegador\n    // ya tenga el audio sintetizado cuando llegue la señal real\n    if (window.speechSynthesis) {\n      const keys = Object.keys(sBetCache);\n      keys.forEach(function(k, i) {\n        setTimeout(function() {\n          try {\n            const utt = new SpeechSynthesisUtterance(\'apostar \' + sBetCache[k]);\n            utt.lang = \'es-ES\'; utt.volume = 0; utt.rate = 0.95; utt.pitch = 1.0;\n            const voices = window.speechSynthesis.getVoices();\n            const esVoice = voices.find(v => v.lang === \'es-ES\') || voices.find(v => v.lang && v.lang.startsWith(\'es\')) || null;\n            if (esVoice) utt.voice = esVoice;\n            window.speechSynthesis.speak(utt);\n          } catch(e) {}\n        }, 400 + i * 200);\n      });\n    }\n  }\n\n  // Iniciar\n  window.s3Start=function(){\n    if(sActive) return;\n    sActive=true; s3Balance=sCapital; sScale=1; sCol=1; sAttempt=1; sLost=0; sCurBet=s3BetFor(1,1,sBaseBet);\n    sEntries=sWins=sLosses=0; sWaitingSO=false;\n    sBuildBetCache(sBaseBet);\n    document.getElementById(\'s3Controls\').style.display=\'grid\';\n    document.getElementById(\'s3Hist\').style.display=\'block\';\n    document.getElementById(\'s3BtnStart\').style.display=\'none\';\n    document.getElementById(\'s3AutoStatus\').style.display=\'block\';\n    document.getElementById(\'s3HistBody\').innerHTML=\'<tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:6px">Sin datos</td></tr>\';\n    const grafLabel = \'Moderado (Señal 3.00x)\';\n    sSetAlert(\'🤖 AUTO · Esc \'+sScale+\' · Esperando alerta en \'+grafLabel);\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // Resultado — llamado automáticamente desde las gráficas\n  window.s3Result=function(win){\n    if(!sActive) return;\n    sEntries++;\n    if(win){\n      const net=Math.round((sCurBet*2-sLost)*100)/100;\n      s3Balance+=net; sWins++;\n      sSetAlert(\'✅ +$\'+net+\' neto · Esc \'+sScale+\' ✓ — Esperando nueva alerta\',false);\n      sAddHist(sScale,sCol,sAttempt,sCurBet,win);\n      sScale++; sLost=0; sCurBet=s3BetFor(1,1,sBaseBet); sWaitingSO=false;\n      if(sScale>10){ sEndCycle(true); return; }\n      sCol=1; sAttempt=1;\n    } else {\n      sLost=Math.round((sLost+sCurBet)*100)/100; sLosses++;\n      sAttempt++;\n      if(sAttempt<=S_MAX_ATT) sCurBet=s3BetFor(sCol,sAttempt,sBaseBet);\n      sAddHist(sScale,sCol,sAttempt-1,sCurBet,win);\n      if(sAttempt>S_MAX_ATT){\n        sAttempt=1; sCol++; sWaitingSO=false;\n        if(sCol>3){ sSetAlert(\'⚠️ 3 columnas fallidas · Esc \'+sScale,true); sEndCycle(false); return; }\n        sCurBet=s3BetFor(sCol,1,sBaseBet);\n        sSetAlert(\'📍 Col \'+sCol+\' · $\'+sCurBet+\' · Esperando próxima alerta 3.00\',true);\n      } else {\n        sWaitingSO=true;\n        sSetAlert(\'❌ Perdida · $\'+sCurBet+\' · 🔄 Esperando 2ª Oportunidad\',true);\n      }\n    }\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // UI\n  function sUpdateUI(){\n    document.getElementById(\'s3Balance\').textContent=\'$\'+s3Balance.toFixed(2);\n    document.getElementById(\'s3Margen\').textContent=\'$\'+(s3Balance-sCapital).toFixed(2);\n    document.getElementById(\'s3Columna\').textContent=sCol;\n    document.getElementById(\'s3Apuesta\').textContent=\'$\'+sCurBet;\n    document.getElementById(\'s3EscLabel\').textContent=sScale;\n    const diff=s3Balance-sCapital;\n    document.getElementById(\'s3Balance\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n    document.getElementById(\'s3Margen\').className=\'s-val \'+(diff>=0?\'sv-green\':\'sv-red\');\n  }\n  function sUpdateCols(){\n    for(let i=1;i<=3;i++){\n      const el=document.getElementById(\'s3C\'+i);\n      const st=document.getElementById(\'s3St\'+i);\n      const bt=document.getElementById(\'s3Bet\'+i);\n      el.classList.remove(\'sc-active\',\'sc-done\');\n      if(i===sCol&&sActive){ el.classList.add(\'sc-active\'); st.textContent=\'Intento \'+sAttempt+\'/\'+S_MAX_ATT; st.style.color=\'#f1c40f\'; bt.textContent=\'$\'+sCurBet; }\n      else if(i<sCol){ el.classList.add(\'sc-done\'); st.textContent=\'✅ OK\'; st.style.color=\'#00ff88\'; bt.textContent=\'\'; }\n      else { st.textContent=\'--\'; st.style.color=\'rgba(255,255,255,.3)\'; bt.textContent=\'\'; }\n    }\n  }\n  function sUpdateDots(){\n    const cont=document.getElementById(\'s3Dots\'); cont.innerHTML=\'\';\n    for(let i=1;i<=10;i++){ const d=document.createElement(\'div\'); d.className=\'s-dot\'+(i<sScale?\' sd-done\':i===sScale?\' sd-active\':\'\'); d.textContent=i; cont.appendChild(d); }\n  }\n  function sSetAlert(msg,pulse=false){ const el=document.getElementById(\'s3Alerta\'); el.innerHTML=msg; el.classList.toggle(\'sa-pulse\',pulse); }\n  function sAddHist(sc,cl,att,bet,win){\n    const tb=document.getElementById(\'s3HistBody\');\n    if(tb.querySelector(\'[colspan]\')) tb.innerHTML=\'\';\n    tb.insertAdjacentHTML(\'afterbegin\',`<tr><td>${sEntries}</td><td>${sc}</td><td>${cl}</td><td style="color:#f1c40f">$${bet}</td><td class="${win?\'sh-win\':\'sh-loss\'}">${win?\'WIN\':\'LOSS\'}</td><td>$${s3Balance.toFixed(2)}</td></tr>`);\n  }\n\n  // Ciclo\n  function sEndCycle(ok){\n    sActive=false; sWaitingSO=false;\n    document.getElementById(\'s3Controls\').style.display=\'none\';\n    document.getElementById(\'s3BtnStart\').style.display=\'block\';\n    document.getElementById(\'s3AutoStatus\').style.display=\'none\';\n    if(ok){\n      document.getElementById(\'s3mCap\').textContent=sCapital;\n      document.getElementById(\'s3mBal\').textContent=s3Balance.toFixed(2);\n      document.getElementById(\'s3mGain\').textContent=(s3Balance-sCapital).toFixed(2);\n      document.getElementById(\'s3mEff\').textContent=sEntries?Math.round(sWins/sEntries*100):0;\n      document.getElementById(\'s3ModalWin\').style.display=\'flex\';\n    } else {\n      document.getElementById(\'s3mFEsc\').textContent=sScale;\n      document.getElementById(\'s3mFBal\').textContent=s3Balance.toFixed(2);\n      document.getElementById(\'s3mFLoss\').textContent=(sCapital-s3Balance).toFixed(2);\n      document.getElementById(\'s3ModalLoss\').style.display=\'flex\';\n    }\n  }\n  window.s3CloseModal=function(id){ document.getElementById(id).style.display=\'none\'; };\n  window.s3NewCycle=function(){ s3CloseModal(\'s3ModalWin\'); s3Start(); };\n  window.s3Reset=function(){\n    sActive=false; s3Balance=sCapital; sScale=1; sCol=1; sAttempt=1;\n    sLost=0; sCurBet=s3BetFor(1,1,sBaseBet); sEntries=sWins=sLosses=0; sWaitingSO=false;\n    document.getElementById(\'s3Controls\').style.display=\'none\';\n    document.getElementById(\'s3Hist\').style.display=\'none\';\n    document.getElementById(\'s3BtnStart\').style.display=\'block\';\n    document.getElementById(\'s3AutoStatus\').style.display=\'none\';\n    document.getElementById(\'s3Alerta\').innerHTML=\'⏳ Esperando inicio...\';\n    document.getElementById(\'s3Alerta\').classList.remove(\'sa-pulse\');\n    document.getElementById(\'s3HistBody\').innerHTML=\'<tr><td colspan="6" style="color:rgba(255,255,255,.25);padding:6px">Sin datos</td></tr>\';\n    sUpdateUI(); sUpdateCols(); sUpdateDots();\n  };\n\n  // Init\n  s3SetChart(\'moderate\');\n  sUpdateUI(); sUpdateDots();\n})();\n</script>\n\n<script>\n// ══════════════════════════════════════════════\n//  PUENTE CON EL BOT REAL — realBridge\n//  El panel ya NO calcula sus propias señales (se desactivaron\n//  checkModerateAlerts y calcularPrediccionInteligente más arriba). Todo lo\n//  que se ve acá — gráfico, fila de alerta y la gestión de dinero real 2x —\n//  refleja lo que el bot de Telegram YA procesó, vía polling a /api/state.\n// ══════════════════════════════════════════════\n(function(){\n  let lastHistoryCount = 0;\n  let lastSessionStartId = 0;\n  let lastResolutionId = 0;\n  let firstPoll = true;\n\n  function rangeKeyFor(v){\n    if (v < 1.50) return \'1.00-1.49\';\n    if (v < 2.00) return \'1.50-1.99\';\n    if (v < 3.00) return \'2.00-2.99\';\n    if (v < 5.00) return \'3.00-4.99\';\n    if (v < 10.00) return \'5.00+\';\n    return \'10.00+\';\n  }\n\n  function pushRealValue(value){\n    if (updatePending) return;\n    updatePending = true;\n    const timestamp = new Date();\n    const rangeKey = rangeKeyFor(value);\n    data.push({ timestamp: timestamp, value: value, range: rangeKey });\n    historyData.push({ timestamp: timestamp, value: value, range: rangeKey });\n    if (historyData.length > 600) historyData.shift();\n    const vc = document.getElementById(\'valuesCount\'); if (vc) vc.textContent = `Valores: ${data.length}`;\n    const tv = document.getElementById(\'totalValues\'); if (tv) tv.textContent = data.length;\n    const ctEl = document.getElementById(\'currentTime\'); if (ctEl) ctEl.textContent = timestamp.toLocaleTimeString();\n    processNewValue(value); // solo visual: el cálculo de señales ya está desactivado\n    updatePercentages();\n    updateHistoryTopBar();\n    if (drawRequest) cancelAnimationFrame(drawRequest);\n    drawRequest = requestAnimationFrame(() => { draw(); updatePending = false; });\n  }\n\n  function applySignalState(sig){\n    if (!sig || sig.state !== \'active\') {\n      setAlertRow(\'📡\', \'ESPERANDO SEÑAL DEL BOT\', \'neutral\', \'\');\n      return;\n    }\n    const nivelLabel = \'C\' + sig.nivel;\n    setAlertRow(\'⚡\', `SEÑAL ${nivelLabel} — INTENTO ${sig.intento_global}/${sig.intento_total}`, \'bull\', sig.tipo || \'\');\n  }\n\n  async function poll(){\n    try {\n      const res = await fetch(\'/api/state\', { cache: \'no-store\' });\n      if (!res.ok) return;\n      const st = await res.json();\n\n      // 1) Rondas nuevas del bot — solo actualizan gráfico/historial (visual).\n      const hist = st.history || [];\n      if (firstPoll) {\n        hist.forEach(v => pushRealValue(v));\n        lastHistoryCount = hist.length;\n        firstPoll = false;\n      } else if (hist.length > lastHistoryCount) {\n        hist.slice(lastHistoryCount).forEach(v => pushRealValue(v));\n        lastHistoryCount = hist.length;\n      } else if (hist.length < lastHistoryCount) {\n        lastHistoryCount = hist.length; // el bot rotó su historial\n      }\n\n      // 2) Señal activa real (nivel C1/C2/C3, intento X/6) — solo texto.\n      applySignalState(st.signal);\n\n      // 3) Nueva sesión real (arranca en C1, intento 1) → gestión 2x en marcha.\n      if (st.dashboard_session_start_id > lastSessionStartId) {\n        lastSessionStartId = st.dashboard_session_start_id;\n        if (typeof window._sActive === \'function\' && !window._sActive() && typeof window.sStart === \'function\') {\n          window.sStart();\n        }\n      }\n\n      // 4) Resolución real de un intento (win/loss) → avanza columna/apuesta.\n      const lr = st.last_resolution;\n      if (lr && lr.id > lastResolutionId) {\n        lastResolutionId = lr.id;\n        if (typeof window._sActive === \'function\' && window._sActive() && typeof window.sAutoResult === \'function\') {\n          window.sAutoResult(lr.win);\n        }\n      }\n    } catch (e) {\n      // silencioso — se reintenta en el siguiente ciclo de polling\n    }\n  }\n\n  setInterval(poll, 1500);\n  poll();\n})();\n</script>\n\n</body>\n</html>'

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    return DASHBOARD_HTML, 200

@flask_app.route('/status')
def status_text():
    stats = get_stats()
    return (
        f"🤖 SpacemanBot v22 (Pragmatic WS) | hist:{len(history)} "
        f"| conexión:{ws_conn_status} "
        f"| señal:{sig_state}{'(INM)' if sig_inmediata else ''} "
        f"| tend:{'🟢' if stats['favorable'] else '🔴'}"
    ), 200

@flask_app.route('/api/state')
def api_state():
    """Estado en vivo para el dashboard HTML (panel servido en '/'). El panel
    ya no calcula sus propias señales: solo consulta acá lo que este bot YA
    procesó y mandó a Telegram (incluida la gestión de dinero real 2x)."""
    intento_total = get_session_max_signals() * get_max_attempts()
    signal_info = {"state": "idle"}
    if sig_state == "active":
        signal_info = {
            "state": "active",
            "nivel": pending_signal_index,
            "intento_local": sig_attempt,
            "intento_global": intento_global_actual(pending_signal_index, sig_attempt, sig_last_attempt),
            "intento_total": intento_total,
            "tipo": sig_tipo,
        }
    stats = get_stats()
    return {
        "history": list(history)[-HISTORY_MAX:],
        "signal": signal_info,
        "dashboard_session_start_id": dashboard_session_start_id,
        "last_resolution": dashboard_last_resolution,
        "favorable": stats["favorable"],
        "is_trained": is_trained,
    }, 200

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
        "connection": {"state": ws_conn_status, "detail": ws_conn_detail},
        "signal": {"state": sig_state, "attempt": sig_attempt, "tipo": sig_tipo, "inmediata": sig_inmediata},
        "favorable": stats["favorable"],
        "pct_below2": round(stats["pct_below2"], 2),
        "pct_2to5":   round(stats["pct_2to5"], 2),
    }, 200

@flask_app.route('/ping')
def ping():
    return 'pong', 200

# ─── STATS UPDATE ────────────────────────────────────────────────────────────
async def send_stats_update():
    global stats_msg_id
    if sig_state != "idle":
        return
    if stats_msg_id:
        await delete_msg(stats_msg_id)
    stats_msg_id = await send_signal_msg(build_stats_msg())
    save_state()

# ─── TELEGRAM COMMANDS ────────────────────────────────────────────────────────
@bot.message_handler(commands=['chatid'])
async def cmd_chatid(message):
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
        "🤖 <b>SPACEMAN BOT v23 — SOLO SEÑALES DE TIEMPO</b>\n"
        f"💰 <b>Retiro único: {CASHOUT_TARGET:.2f}x</b>\n"
        "⏰ <b>Única señal: predictor de horario (rebote 3x-5x)</b>\n"
        f"🎓 <b>Entrenamiento: {TRAINING_SIGNALS_REQUIRED} señales silenciosas (no van a Telegram)</b>\n"
        "🧠 <b>En vivo: sesión de 3 niveles C1/C2/C3 — 2 intentos por nivel</b>\n"
        "📈 <b>Estadísticas por sesión (no por señal)</b>\n"
        "📡 <b>Fuente: Spaceman — Pragmatic Play (WebSocket)</b>\n"
        f"📈 <b>Estado Actual</b>\n"
        f"   Historial: {len(history)} cuotas\n"
        f"   Favorable: {'✅' if stats['favorable'] else '❌'}\n"
        f"   Fase: {'🟢 EN VIVO' if is_trained else f'🎓 ENTRENANDO ({count_resolved_contexts()}/{TRAINING_SIGNALS_REQUIRED})'}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode='HTML')

@bot.message_handler(commands=['stats'])
async def cmd_stats(message):
    stats   = get_stats()
    hora    = colombia_time()
    if sig_state == "idle":
        sig_txt = "Idle"
    else:
        sig_txt = f"{sig_tipo} (activa)"
    total   = daily_wins + daily_losses
    pct     = (daily_wins / total * 100) if total > 0 else 0.0
    sesion_actual = pending_signal_index if pending_signal_index > 0 else 0
    await bot.reply_to(message,
        f"📊 <b>ESTADÍSTICAS — {hora}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 Historial: <code>{stats['total']}</code> cuotas\n"
        f"🔵 &lt;2x: {stats['below2']} ({stats['pct_below2']:.1f}%)\n"
        f"🟡 2-5x: {stats['two_to_five']} ({stats['pct_2to5']:.1f}%)\n"
        f"📈 Tendencia: {'🟢 FAVORABLE' if stats['favorable'] else '🔴 DESFAVORABLE'}\n"
        f"📡 Señal: <code>{sig_txt}</code>\n"
        f"✅ Sesiones Ganadas: {daily_wins} | ❌ Sesiones Perdidas: {daily_losses}\n"
        f"💎 Acierto de Sesiones: {pct:.1f}%\n"
        f"📈 Racha de Sesiones Ganadas: {consecutive_wins}\n"
        f"🔥 Racha de Señales Ganadas: {consecutive_signal_wins}\n"
        f"🧠 Sesión actual: {sesion_actual}/{get_session_max_signals()}\n"
        f"🎓 Fase: {'EN VIVO' if is_trained else f'ENTRENANDO ({count_resolved_contexts()}/{TRAINING_SIGNALS_REQUIRED})'}\n"
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
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses, consecutive_signal_wins
    while True:
        now = colombia_now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())
        await send_stats_msg("🤑 <b>Resultados del día</b>\n" + build_stats_msg())
        daily_wins = daily_losses = consecutive_wins = consecutive_losses = consecutive_signal_wins = 0
        save_state()
        logger.info("🔄 Estadísticas reiniciadas — 00:00 Colombia")

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main_async():
    global _main_loop
    global current_session_results
    current_session_results = []

    _main_loop = asyncio.get_running_loop()
    logger.info("🤖 Iniciando SPACEMAN Bot v22 — WebSocket Pragmatic Play + estrategias de tendencia/líneas calientes/timing")
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
    asyncio.create_task(ws_loop())
    asyncio.create_task(timing_predictions_ticker())
    asyncio.create_task(self_ping_loop())
    asyncio.create_task(daily_reset_loop())
    if AUTO_TRAIN_ENABLED and ML_LIBS_OK:
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

if __name__ == '__main__':
    threading.Thread(target=run_flask, daemon=True).start()
    asyncio.run(main_async())
