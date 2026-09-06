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
"""
import asyncio
import sqlite3
import sys
import threading
import json
import logging
import os
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
# El horario predicho es el momento estimado en que caerá la próxima ronda
# 3x-5x (el "rebote"). La señal de ENTRADA SOLO se procesa si una ronda real
# cae dentro de la franja de 15 a 30 segundos ANTES de ese horario (es decir,
# una ronda debe caer en esos 15s de ventana). No basta con que el reloj
# entre en la franja: tiene que llegar una ronda nueva estando dentro de ella.
TIMING_PREALERT_MIN_SEC = int(os.environ.get("TIMING_PREALERT_MIN_SEC", "15"))
TIMING_PREALERT_MAX_SEC = int(os.environ.get("TIMING_PREALERT_MAX_SEC", "30"))
TIMING_ALERT_WINDOW_SEC = int(os.environ.get("TIMING_ALERT_WINDOW_SEC", "10"))
TIMING_DEDUPE_SEC       = int(os.environ.get("TIMING_DEDUPE_SEC", "15"))

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
MAX_ATTEMPTS_NORMAL = 1   # solo un intento por señal
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

async def check_timing_round_trigger():
    """Dispara la señal de tiempo con ventanas dinámicas según fallos acumulados.
    Cada fallo reduce el preaviso mínimo/máximo y amplía la ventana posterior."""
    global recorded_times, timing_retry_pred
    if sig_state != "idle" or pending_confirmation:
        return
    ahora = colombia_now()
    actual = ahora.hour * 3600 + ahora.minute * 60 + ahora.second
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
            r['alert_shown'] = True
            timing_retry_pred = None
            if reintento:
                logger.info(f"[Timing] 🔁 Reintento de señal de tiempo (fail_count={fail_count}, diff={diff:.0f}s)")
            else:
                logger.info(f"[Timing] ⏰ Enviando señal {diff:.0f}s antes del rebote (fail_count={fail_count})")
            await emit_timing_signal()
            break

async def emit_timing_signal():
    """Emite la señal de tiempo (ventana 3x-5x) reutilizando el mismo pipeline
    de sesión/ML que la señal de tendencia — el objetivo de retiro sigue
    siendo 2x. Ya no se bloquea por tendencia general."""
    ultimo_valor = history[-1] if history else 0.0
    features = {
        'tipo_key': 'timing_3x_5x',
        'ultimo_valor': ultimo_valor,
        'confidence': 60,
        'tendencia_lucky': 'AMARILLO',
        'agresiva_condicion': False,
        'ema4': None, 'ema8': None, 'ema20': None, 'ema50': None,
        'votos': {}, 'contar_entrar': 0, 'contar_no_entrar': 0, 'risk_score': 0,
        'rsi': None, 'macd': None, 'fuerza': None, 'ia_prob': None,
        'racha_rango_activa': False, 'rango_activo': "3.00x-5.00x",
    }
    features_json = json.dumps(features, default=str)
    label = "SEÑAL DE TIEMPO 3x-5x ⏰"
    motivo = "Horario predicho de rebote 3x-5x alcanzado — objetivo de retiro 2.00x"
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
TREND_RANGO1_MAX = float(os.environ.get("TREND_RANGO1_MAX", "54"))
TREND_RANGO2_MIN = float(os.environ.get("TREND_RANGO2_MIN", "28"))

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
    # Formato fijo de señal — sin línea de ronda predicha por el ML de timing.
    return (
        f"<b>✅✅ ENTRADA CONFIRMADA ✅✅</b>\n\n"
        f"👉 INGRESAR DESPUÉS: {last_value:.2f}x\n"
        f"💰 RETIRAR EN: {CASHOUT_TARGET:.2f}x\n\n"
        f"🧠 GESTIÓN MASSANIELLO: {sesion_index}/5\n"
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

def build_loss_msg(intento: int, result: float) -> str:
    return (
        f"🧠 <b>INTENTO FALLIDO!!! Resultado: {result:.2f}x</b>\n"
        f"💥 Mantener la calma intento {intento} de 5"
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
        "ml_last_trained_count": str(ml_last_trained_count),
        "timing_last_trained_count": str(timing_last_trained_count),
        "session_signal_count": str(session_signal_count),
        "pending_signal_index": str(pending_signal_index),
        "is_last_signal_of_session": "1" if is_last_signal_of_session else "0",
        "pending_confirmation": "1" if pending_confirmation else "0",
        "pending_confirmation_data": json.dumps(pending_confirmation_data) if pending_confirmation_data else "",
    }
    _save_dict(values)

def load_state():
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features
    global sig_inmediata, sig_emit_attempt, sig_context_json, sig_signal_id, stats_msg_id
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    global ml_last_trained_count, timing_last_trained_count
    global session_signal_count, pending_signal_index, is_last_signal_of_session
    global pending_confirmation, pending_confirmation_data

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
    ml_last_trained_count = int(d.get("ml_last_trained_count", "0") or "0")
    timing_last_trained_count = int(d.get("timing_last_trained_count", "0") or "0")
    session_signal_count = int(d.get("session_signal_count", "0") or "0")
    if session_signal_count < 0 or session_signal_count > 5:
        session_signal_count = 0
    pending_signal_index = int(d.get("pending_signal_index", "0") or "0")
    is_last_signal_of_session = (d.get("is_last_signal_of_session", "0") or "0") == "1"
    pending_confirmation = (d.get("pending_confirmation", "0") or "0") == "1"
    _pcd = d.get("pending_confirmation_data", "")
    try:
        pending_confirmation_data = json.loads(_pcd) if _pcd else None
    except (TypeError, ValueError):
        pending_confirmation_data = None
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
session_signal_count: int        = 0
pending_signal_index: int        = 0
is_last_signal_of_session: bool  = False
current_session_results: List[bool] = []  # almacena wins/losses de la sesión actual
trend_msg_id: Optional[int] = None

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

# ─── MÁQUINA DE ESTADOS ──────────────────────────────────────────────────────
async def resolve_active(value: float, signal_index: int):
    global sig_state, sig_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global daily_wins, daily_losses, consecutive_wins, consecutive_losses
    global sig_context_json, sig_signal_id
    global session_signal_count, pending_signal_index, is_last_signal_of_session
    global current_session_results

    win = value >= CASHOUT_TRIGGER
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

    if sig_signal_id and sig_context_json:
        attempt_when_win = signal_index if win else None
        result = "win" if win else "loss"
        update_signal_context_result(sig_signal_id, attempt_when_win, result)

    current_session_results.append(win)

    if win:
        logger.info(f"[v21] ✅ GANAMOS — {value:.2f}x | {label}")
        log_pattern_result(key, label, "win", value, attempt=signal_index, features_json=sig_features)
        await send_signal_msg(build_win_msg(value, signal_index))
        await send_stats_msg(build_win_status_msg(signal_index))
    else:
        logger.info(f"[v21] ❌ PERDIMOS — {value:.2f}x | {label}")
        log_pattern_result(key, label, "loss", value, attempt=signal_index, features_json=sig_features)
        await send_signal_msg(build_loss_msg(signal_index, value))
        await send_stats_msg(build_loss_status_msg(signal_index))

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

    # La sesión termina apenas se gana UNA señal (en cualquier intento del 1 al 5)
    # — no hace falta llegar a la 5ta —, o cuando se pierden las 5 seguidas sin
    # ningún acierto.
    session_ends = win or is_last_signal_of_session
    if session_ends:
        session_won = win  # si llegamos acá por is_last_signal_of_session sin ganar, ya perdió las 5
        if session_won:
            daily_wins += 1
            consecutive_wins += 1
            consecutive_losses = 0
            logger.info(f"[v21] 🏆 Sesión GANADA (acierto en el intento {signal_index}/5)")
        else:
            daily_losses += 1
            consecutive_losses += 1
            # Cada sesión perdida resetea la racha de victorias consecutivas.
            consecutive_wins = 0
            # Enviar mensaje de sesión fallida con el último valor
            await send_signal_msg(build_session_loss_msg(value))
            logger.info(f"[v22] ❌ Sesión PERDIDA (5 intentos fallidos, sin aciertos) — racha ganada reseteada")

        # Resetear estado de sesión
        current_session_results = []
        pending_signal_index = 0
        is_last_signal_of_session = False
        session_signal_count = 0
        save_state()
        # Mensaje de estadísticas de sesión (resultado del día, % acierto,
        # racha) — va al chat de señales, igual que el resto de los avisos.
        await send_stats_update()

    save_state()

# ─── EMISIÓN DE SEÑAL (extraído para reutilizar en el camino directo y en el
#     camino confirmado por espera) ────────────────────────────────────────
async def emit_signal(value: float, tipo_key: str, label: str, motivo: str,
                      features_json: str, confirmada_por_espera: bool):
    global sig_state, sig_attempt, sig_last_attempt, sig_msg_id, sig_tipo, sig_tipo_key, sig_features, sig_inmediata
    global sig_emit_attempt, sig_context_json, sig_signal_id
    global session_signal_count, pending_signal_index, is_last_signal_of_session

    signal_id = f"{tipo_key}_{int(datetime.utcnow().timestamp() * 1000)}"
    sig_signal_id = signal_id
    ronda_predicha = None

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

    # Incrementar contador de sesión
    pending_signal_index = session_signal_count + 1
    is_last_signal_of_session = (pending_signal_index == 5)
    if is_last_signal_of_session:
        session_signal_count = 0
    else:
        session_signal_count = pending_signal_index

    # Calcular porcentajes de rangos sobre las últimas 200 rondas
    pct1, pct2 = calc_pct_rangos(list(history))

    sig_tipo = label
    sig_tipo_key = tipo_key
    sig_features = features_json
    sig_inmediata = True
    sig_state = "active"
    sig_attempt = 1
    sig_last_attempt = MAX_ATTEMPTS_NORMAL

    text = build_signal_msg(label, value, pending_signal_index, pct1, pct2, ronda_predicha=ronda_predicha)
    sig_msg_id = await send_signal_msg(text, no_preview=True)
    save_state()
    origen = "confirmada tras espera <2x" if confirmada_por_espera else "directa"
    logger.info(f"[v21] ⚡ Señal emitida ({origen}): {tipo_key} — {motivo} (señal {pending_signal_index}/5)")

# ─── PROCESAMIENTO CENTRAL — v21 ─────────────────────────────────────────────
async def process_new_value(value: float, silent: bool = False):
    global last_result, history
    global sig_state, pending_signal_index
    global pending_confirmation, pending_confirmation_data

    history.append(value)
    if len(history) > HISTORY_MAX:
        history = history[-HISTORY_MAX:]
    save_value(value)
    # Se alimentan siempre, aunque el modo sea silencioso (carga inicial de
    # historial), para que el predictor de tiempo y las líneas calientes
    # arranquen con contexto real desde el primer dato disponible.
    calcular_prediccion_inteligente(value)
    if silent:
        return
    log_hotline_snapshot(list(history))
    logger.info(f"Nueva cuota: {value:.2f}x | hist:{len(history)} | estado:{sig_state}")

    if sig_state == "active":
        await resolve_active(value, pending_signal_index)
        return

    # Confirmación previa al envío: si hay una señal pendiente de una ronda
    # anterior (patrón detectado con la última cuota ≥ CONFIRM_BELOW), se
    # espera a que esta ronda resulte < CONFIRM_BELOW para recién emitirla.
    if pending_confirmation:
        if value < CONFIRM_BELOW:
            data = pending_confirmation_data
            pending_confirmation = False
            pending_confirmation_data = None
            save_state()
            if data:
                await emit_signal(value, data["tipo_key"], data["label"], data["motivo"],
                                   data["features_json"], confirmada_por_espera=True)
            return
        else:
            # Sigue ≥ CONFIRM_BELOW: se mantiene pendiente, no se evalúan
            # nuevos patrones mientras se espera la confirmación.
            logger.info(f"[v21] ⏳ Confirmación pendiente — cuota {value:.2f}x aún ≥ {CONFIRM_BELOW:.2f}x")
            return

    vals = list(history)
    await update_trend_status_msg(vals, resolved=False)

    # Señal de tiempo: SOLO si ESTA ronda cayó dentro de la franja de 15-30s
    # antes del horario predicho del rebote 3x-5x, dispara la entrada.
    # Si dispara, termina acá.
    await check_timing_round_trigger()
    if sig_state == "active":
        return

    resultado = evaluate_signal(vals)
    if not resultado:
        return

    # Solo se consideran señales cuando la tendencia general (200 rondas) es
    # favorable — mismo criterio que get_stats()/pct_below2/pct_2to5.
    if not get_stats()["favorable"]:
        logger.info("[v22] ⏸️ Tendencia NO favorable — señal detectada pero descartada")
        return

    tipo_key, label, motivo, features_json, es_inmediata = resultado

    if value >= CONFIRM_BELOW:
        pending_confirmation = True
        pending_confirmation_data = {
            "tipo_key": tipo_key, "label": label, "motivo": motivo, "features_json": features_json,
        }
        save_state()
        logger.info(f"[v21] ⏸️ Patrón detectado con cuota {value:.2f}x ≥ {CONFIRM_BELOW:.2f}x — esperando confirmación")
        return

    await emit_signal(value, tipo_key, label, motivo, features_json, confirmada_por_espera=False)

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

# ─── FLASK ROUTES ─────────────────────────────────────────────────────────────
@flask_app.route('/')
def home():
    stats = get_stats()
    return (
        f"🤖 SpacemanBot v22 (Pragmatic WS) | hist:{len(history)} "
        f"| conexión:{ws_conn_status} "
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
        "🤖 <b>SPACEMAN BOT v22 — SEÑALES DEL GRÁFICO DE TENDENCIA</b>\n"
        f"💰 <b>Retiro único: {CASHOUT_TARGET:.2f}x</b>\n"
        "📈 <b>Señal: cruce EMA4/8/20 del gráfico de tendencia</b>\n"
        "⏰ <b>Señal de tiempo: rebote 3x-5x (predictor de horario)</b>\n"
        "🟡🟣 <b>Líneas calientes 5x/10x: feature de ML, no disparan solas</b>\n"
        "🧠 <b>Gestión Massaniello: hasta 5 señales por sesión (corta al primer acierto)</b>\n"
        "📈 <b>Estadísticas por sesión (no por señal)</b>\n"
        "📡 <b>Fuente: Spaceman — Pragmatic Play (WebSocket)</b>\n"
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
        f"🧠 Sesión actual: {sesion_actual}/5\n"
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
        now = colombia_now()
        next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        await asyncio.sleep((next_midnight - now).total_seconds())
        await send_stats_msg("🤑 <b>Resultados del día</b>\n" + build_stats_msg())
        daily_wins = daily_losses = consecutive_wins = consecutive_losses = 0
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
