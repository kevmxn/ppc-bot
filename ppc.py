#!/usr/bin/env python3
"""
Speed Roulette 2 — Bot con Polling HTTP cada 1 segundo
===========================================================================
CAMBIOS v27 (FIXED):
  - Cada señal = 1 intento (sin gale inmediato)
  - Al perder señal → envía "🚨 ANALIZANDO 2° OPORTUNIDAD 🚨" y espera
    a que se detecte una NUEVA señal natural (≥80%) para usarla como 2da oportunidad
  - Al confirmar nueva señal → borra mensaje de análisis y envía "🚨 SEGUNDA OPORTUNIDAD 🚨"
  - MIN_PROB = 0.80 (ambas oportunidades deben superar 80%)

CORRECCIONES:
  - Cero: se apuesta 10% de la ficha. Al ganar D/C se descuenta zero_bet.
    Al ganar el cero se descuentan las apuestas D/C. (igual lógica v26)
  - Stats: sólo se registra el resultado FINAL de la señal (la 1ra pérdida
    no se cuenta como LOSS, solo se espera la 2da oportunidad), igual a v26
  - registrar_perdida_senal() y subida de nivel: SOLO cuando falla la 2da oportunidad
  - Análisis de predicciones adaptado al nuevo servidor HTTP (Render)
  - Umbral 80% en ambas oportunidades
"""

import asyncio
import json
import logging
import math
import os
import sqlite3
import threading
import time
import urllib.request
from collections import deque, defaultdict
from typing import Optional, Dict

import numpy as np
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier

import telebot
import aiohttp
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [Speed2DC] %(levelname)s %(message)s')
logger = logging.getLogger("Speed2DC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── CREDENCIALES ─────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID = -1003630680656

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))

try:
    bot = telebot.TeleBot(TOKEN, threaded=False)
    bot.session = _session
    logger.info("✅ Telegram bot initialized")
except Exception as e:
    logger.error(f"❌ Failed to initialize Telegram bot: {e}")
    exit(1)

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
STATS_URL       = "https://ruletasbot-rjce.onrender.com"
TARGET_ROULETTE = "SPEED2"
POLL_INTERVAL   = 1       # segundos
LIVE_DB         = "speed2_live.db"

BASE_BET        = 0.50
MAX_NIVEL       = 6
WARMUP_SPINS    = 25
MIN_PROB        = 0.80    # umbral 80% — aplica a ambas oportunidades
TRAIN_INTERVAL  = 100

PF_W_NORM  = 0.65; PH_W_NORM  = 0.35
BASE_W_NORM = 0.50; ML_W_NORM = 0.50
PF_W_GALE1 = 0.30; PH_W_GALE1 = 0.70
BASE_W_GALE1 = 0.65; ML_W_GALE1 = 0.35
MIN_PROB_GALE1 = 0.70

CURRENCY_MULTIPLIERS = {"USD": 1.0, "MXN": 20.0, "PEN": 5.0, "COP": 5000.0, "ARS": 1500.0, "CLP": 1000.0}
CURRENCY_SYMBOLS     = {"USD": "$", "MXN": "$", "PEN": "S/.", "COP": "$", "ARS": "$", "CLP": "$"}
CURRENCY_FLAGS       = {"USD": "🇺🇲", "MXN": "🇲🇽", "PEN": "🇵🇪", "COP": "🇨🇴", "ARS": "🇦🇷", "CLP": "🇨🇱"}
CURRENCY_DECIMALS    = {"USD": 2, "MXN": 2, "PEN": 2, "COP": 0, "ARS": 0, "CLP": 0}

REAL_COLOR_MAP: dict[int, str] = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}

def get_dozen(n: int) -> int:
    if n == 0: return 0
    return (n - 1) // 12 + 1

def get_column(n: int) -> int:
    if n == 0: return 0
    return ((n - 1) % 3) + 1

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(LIVE_DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS live_spins (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        number INTEGER NOT NULL,
        ts INTEGER NOT NULL
    )""")
    conn.commit()
    return conn

_TG_RETRIES = 12
def _tg_call(fn, *a, **kw):
    """Retry logic para llamadas a Telegram con backoff exponencial"""
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try:
            return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            logger.debug(f"TG attempt {attempt}/{_TG_RETRIES}: {err}")
            if "retry after" in err.lower():
                try:
                    wait = int(''.join(filter(str.isdigit, err))) + 1
                except:
                    wait = 30
                logger.warning(f"⏳ Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if attempt == _TG_RETRIES:
                logger.error(f"❌ TG call failed after {_TG_RETRIES} attempts: {err}")
                return None
            time.sleep(delay)
            delay = min(delay * 2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    """Send message to Telegram with retry logic"""
    if not text:
        logger.warning("⚠️ Empty message to send")
        return None
    try:
        msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
        if msg:
            logger.info(f"✅ Message sent (ID: {msg.message_id})")
            return msg.message_id
        else:
            logger.error(f"❌ Failed to send message to {CHAT_ID}")
            return None
    except Exception as e:
        logger.error(f"❌ Exception in tg_send: {e}")
        return None

def tg_delete(chat_id: int, message_id: int):
    """Delete message from Telegram"""
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Message deleted (ID: {message_id})")
    except Exception as e:
        logger.warning(f"⚠️ Failed to delete message: {e}")

# ─── CLIENTE STATS (HTTP POLLING — nuevo servidor Render) ────────────────────
class StatsClient:
    """
    Recibe stats del servidor Render via HTTP polling.
    El endpoint /latest/SPEED2 devuelve:
      {
        "last_20":      [ {"game_id": "...", "number": N}, ... ],
        "stats_dozen":  { "NUM": {"total": T, "1": p1, "2": p2, "3": p3} },
        "stats_column": { ... },
        "total_spins":  N
      }
    Estos datos se usan en _get_ph() para el análisis de predicciones de señales.
    """
    def __init__(self):
        self.stats_dozen  = {}
        self.stats_column = {}
        self.last_20      = []
        self.total_spins  = 0
        self.connected    = False
        self.poll_count   = 0
        self.last_poll_ok = 0.0
        self.last_error   = None

    def update(self, data: dict):
        try:
            self.last_20      = data.get("last_20",      self.last_20)
            self.stats_dozen  = data.get("stats_dozen",  self.stats_dozen)
            self.stats_column = data.get("stats_column", self.stats_column)
            self.total_spins  = data.get("total_spins",  self.total_spins)
            self.connected    = True
            self.poll_count  += 1
            self.last_poll_ok = time.time()
            self.last_error   = None
        except Exception as e:
            logger.error(f"❌ Error updating stats: {e}")
            self.last_error = str(e)

    def get_ph_from_stats(self, number: int, cat_type: str) -> Optional[Dict]:
        """
        Obtiene probabilidades históricas (PH) desde el servidor Render.
        Busca qué par de docenas/columnas aparece más seguido después del número dado.
        Adapta la misma lógica de v26 al nuevo servidor HTTP.
        """
        stats   = self.stats_column if cat_type == "COLUMNA" else self.stats_dozen
        num_key = str(number)
        if num_key not in stats:
            return None
        data = stats[num_key]
        if data.get("total", 0) < 10:
            return None
        probs        = {1: data.get("1", 0), 2: data.get("2", 0), 3: data.get("3", 0)}
        sorted_probs = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        if sorted_probs[0][1] == 0:
            return None
        pair    = tuple(sorted([sorted_probs[0][0], sorted_probs[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        prob    = (sorted_probs[0][1] + sorted_probs[1][1]) / 100.0
        return {"pair": pair, "missing": missing, "prob": prob}

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data, period):
    if len(data) < period: return [None] * len(data)
    mult = 2 / (period + 1)
    out  = [None] * (period - 1)
    prev = sum(data[:period]) / period
    out.append(prev)
    for v in data[period:]:
        prev = v * mult + prev * (1 - mult)
        out.append(prev)
    return out

def ema_signal(levels, mode="moderado"):
    if len(levels) < 20: return False
    e4, e8, e20 = calc_ema(levels, 4), calc_ema(levels, 8), calc_ema(levels, 20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li], e8[li], e20[li]]): return False
    cur  = levels[li]
    ce4, ce8, ce20 = e4[li], e8[li], e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return (pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20)
    else:
        vp = False
        if len(levels) >= 3:
            a, b, c = levels[-3], levels[-2], levels[-1]
            vp = (b < a) and (b < c) and (c > a)
        return (pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or \
               (cur > ce4 and cur > ce8) or vp

# ─── MARKOV ───────────────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window=60, order=2):
        self.window            = window
        self.order             = order
        self.transition_counts = {}

    def update(self, sequence):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            self.transition_counts[tuple(recent[i:i+self.order])][recent[i+self.order]] += 1

    def predict(self, sequence):
        if len(sequence) < self.order: return None
        counts = dict(self.transition_counts.get(tuple(sequence[-self.order:]), {}))
        total  = sum(counts.values())
        if total < 10: return None
        alpha = 2.0
        vs    = 3
        probs = {k: (v + alpha) / (total + alpha * vs) for k, v in counts.items()}
        for c in [1, 2, 3]:
            if c not in probs: probs[c] = alpha / (total + alpha * vs)
        return probs

# ─── ENSEMBLE ML ──────────────────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW  = 5
    CLASSES = [1, 2, 3]

    def __init__(self):
        self.mnb     = MultinomialNB(alpha=2.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd     = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.005,
                                     penalty='l2', alpha=0.01, epsilon=0.2)
        self.trained = False

    def _extract_features(self, hist_d, hist_c, pf_pd, ph_pd, pf_pc, ph_pc):
        if len(hist_d) < self.WINDOW or len(hist_c) < self.WINDOW: return None
        features = []
        for i in range(1, self.WINDOW + 1):
            d   = hist_d[-i]
            c   = hist_c[-i]
            vec = [0] * 9
            vec[(d - 1) * 3 + (c - 1)] = 1
            features.extend(vec)
        for pair in (pf_pd, ph_pd, pf_pc, ph_pc):
            vec = [0, 0, 0]
            [vec.__setitem__(x - 1, 1) for x in pair]
            features.extend(vec)
        return features

    def partial_train(self, hist_d, hist_c, target, pf_d, ph_d, pf_c, ph_c):
        feats = self._extract_features(hist_d[:-1], hist_c[:-1], pf_d, ph_d, pf_c, ph_c)
        if feats is None: return
        X = np.array(feats).reshape(1, -1)
        y = np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES)
            self.sgd.partial_fit(X, y, classes=self.CLASSES)
            self.trained = True
        else:
            self.mnb.partial_fit(X, y)
            self.sgd.partial_fit(X, y)

    def predict(self, hist_d, hist_c, pf_d, ph_d, pf_c, ph_c):
        if not self.trained: return None
        feats = self._extract_features(hist_d, hist_c, pf_d, ph_d, pf_c, ph_c)
        if feats is None: return None
        X = np.array(feats).reshape(1, -1)
        try:
            return {c + 1: float(p) for c, p in enumerate(
                0.5 * self.mnb.predict_proba(X)[0] + 0.5 * self.sgd.predict_proba(X)[0])}
        except:
            return None

# ─── GESTOR DOCENAS ───────────────────────────────────────────────────────────
class GestorDocenas:
    def __init__(self):
        self.nivel       = 1
        self.oportunidad = 1
        self.b0          = 0.0
        self.debt_stack  = []

    def iniciar_senal(self, balance):
        self.b0          = balance
        self.oportunidad = 1

    def get_target(self):
        # La ganancia neta de un ciclo completo al descontar la apuesta del cero (10%)
        net_base = BASE_BET * 0.9
        return (self.debt_stack[-1] + net_base) if self.debt_stack else (self.b0 + net_base)

    def apostar_por_docena(self, balance):
        return self.nivel * BASE_BET if self.oportunidad == 1 else 3 * self.nivel * BASE_BET

    def registrar_perdida_senal(self):
        """Se llama SOLO cuando fallan AMBAS oportunidades (igual que v26)."""
        self.debt_stack.append(self.b0)
        self.nivel = self.nivel + 1 if self.nivel < MAX_NIVEL else 1
        logger.info(f"[Speed2DC] 📋 Deuda: B0={self.b0:.2f} | Pila: {len(self.debt_stack)} | Nivel→{self.nivel}")

    def verificar_recuperacion(self, balance):
        while self.debt_stack:
            if balance >= self.debt_stack[-1] + BASE_BET * 0.9:
                self.debt_stack.pop()
            else:
                break
        if not self.debt_stack:
            self.nivel = 1

# ─── STATS ────────────────────────────────────────────────────────────────────
class DetailedStats:
    """
    Registra el RESULTADO FINAL de cada señal global (igual a v26):
      - WIN_PAR  : gana la docena/columna en 1ra o 2da oportunidad
      - WIN_ZERO : gana el cero en 1ra o 2da oportunidad
      - LOSS     : pierden AMBAS oportunidades

    La 1ra oportunidad perdida NO se registra aquí (sólo genera waiting_second_opp).
    Cada entrada en last_20 muestra en qué oportunidad fue el resultado final.
    """
    def __init__(self):
        self.wins_pair           = 0
        self.wins_zero           = 0
        self.losses              = 0
        self.consecutive         = 0
        self.last_20             = deque(maxlen=20)
        self.signals_processed   = 0
        self.last_report_signals = 0

    def record(self, result_type, oportunidad, number, val, type_str, bankroll):
        """
        result_type : 'WIN_PAR' | 'WIN_ZERO' | 'LOSS'
        oportunidad : 1 = primera señal  |  2 = segunda oportunidad
        """
        self.signals_processed += 1
        if result_type == 'WIN_PAR':
            self.wins_pair   += 1
            self.consecutive += 1
        elif result_type == 'WIN_ZERO':
            self.wins_zero   += 1
            self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses      += 1
            self.consecutive  = 0
        self.last_20.append({"result": result_type, "oportunidad": oportunidad,
                              "number": number, "val": val,
                              "type": type_str, "balance": bankroll})

    def should_send(self):
        return (self.signals_processed - self.last_report_signals) >= 20

    def mark_sent(self):
        self.last_report_signals = self.signals_processed

    def get_stats_text(self, bankroll):
        total_wins = self.wins_pair + self.wins_zero
        total      = total_wins + self.losses
        eff        = (total_wins / total * 100) if total > 0 else 0.0
        text = (f"📊 RESUMEN 📊\n"
                f"► ✅{total_wins}(🟢{self.wins_zero} cero) | 🚫{self.losses}\n"
                f"► Consecutivas = {self.consecutive}\n"
                f"► Assert = {eff:.2f}%\n"
                f"► Balance: 💰 ${bankroll:.2f} USD\n"
                f"► Total señales: {total}\n\n"
                f"📌 Últimas 20 📌\n")
        for s in reversed(list(self.last_20)):
            opp = "1ra" if s['oportunidad'] == 1 else "2da"
            b   = f"💰${s['balance']:.2f}"
            v   = f"{'D' if s['type']=='DOCENA' else 'C'}{s['val']}"
            r   = s['result']
            if r == 'WIN_PAR':
                text += f"✅ WIN #{s['number']} {s['type']} {v} | {opp} | {b}\n"
            elif r == 'WIN_ZERO':
                text += f"🟢 WIN CERO #0 | {opp} | {b}\n"
            else:
                text += f"🚫 LOSS #{s['number']} {s['type']} {v} | {opp} | {b}\n"
        return text

# ─── ENGINE ───────────────────────────────────────────────────────────────────
class Speed2RouletteEngine:
    def __init__(self, stats_client: StatsClient):
        self.stats_client         = stats_client
        self.spin_history         = []
        self.dozen_seq            = []
        self.column_seq           = []
        self.d_levels             = {1: [], 2: [], 3: []}
        self.c_levels             = {1: [], 2: [], 3: []}
        self.markov_d             = SmoothedMarkovPredictor()
        self.markov_c             = SmoothedMarkovPredictor()
        self.ensemble_d           = OnlineEnsemblePredictor()
        self.ensemble_c           = OnlineEnsemblePredictor()
        self.after_number_dozen   = defaultdict(lambda: defaultdict(int))
        self.after_number_column  = defaultdict(lambda: defaultdict(int))

        # ── Estado señal ──────────────────────────────────────────────────────
        self.signal_active        = False
        self.active_type          = None
        self.active_pair          = ()
        self.active_missing       = ""
        self.gestor               = GestorDocenas()
        self.total_signal_loss    = 0.0   # acumula pérdidas de AMBAS oportunidades
        self.active_signal_msg_id = None

        # ── Estado 2da oportunidad ────────────────────────────────────────────
        # waiting_second_opp: True cuando se perdió la 1ra señal y
        # se espera que el modelo detecte una NUEVA señal natural ≥80%
        self.waiting_second_opp   = False
        self.analyzing_msg_id     = None   # ID del mensaje "🚨 ANALIZANDO 2°..."

        # ── Resto ─────────────────────────────────────────────────────────────
        self.bankroll             = 100.0  # BANKROLL INICIAL (USD)
        self.stats                = DetailedStats()
        self._db                  = _get_db()
        self.spins_since_train    = 0
        self.processed_game_ids: set = set()
        self.MAX_PROCESSED_IDS    = 300

        live_loaded      = self._load_live_history()
        self.ws_count    = live_loaded
        self.warmup_done = live_loaded >= WARMUP_SPINS
        logger.info(f"[Speed2DC] 📦 Pre-cargados: {live_loaded} | Warmup: {'✅' if self.warmup_done else '⏳'}")

    # ── DB ────────────────────────────────────────────────────────────────────
    def _load_live_history(self):
        try:
            rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except:
            return 0
        for (n,) in rows:
            self._update_state(n, persist=False, train_model=False)
        if rows:
            self._train_models()
        return len(rows)

    def _persist(self, number):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)",
                             (number, int(time.time())))
            self._db.commit()
        except Exception as e:
            logger.debug(f"⚠️ DB persist error: {e}")

    def _train_models(self):
        self.markov_d.update(self.dozen_seq)
        self.markov_c.update(self.column_seq)

    # ── Estado interno ────────────────────────────────────────────────────────
    def _update_state(self, number, persist=True, train_model=True):
        d = get_dozen(number)
        c = get_column(number)
        if number != 0 and self.spin_history:
            prev = self.spin_history[-1]["number"]
            if prev != 0:
                self.after_number_dozen[prev][d]  += 1
                self.after_number_column[prev][c] += 1
        self.spin_history.append({"number": number, "color": REAL_COLOR_MAP.get(number, "VERDE")})
        if d != 0:
            self.dozen_seq.append(d)
            for dd in (1, 2, 3):
                prev = self.d_levels[dd][-1] if self.d_levels[dd] else 0
                self.d_levels[dd].append(prev + (1 if d == dd else -1))
        if c != 0:
            self.column_seq.append(c)
            for cc in (1, 2, 3):
                prev = self.c_levels[cc][-1] if self.c_levels[cc] else 0
                self.c_levels[cc].append(prev + (1 if c == cc else -1))
        if train_model and d != 0 and c != 0 and len(self.dozen_seq) > 5:
            pf_d, ph_d = self._get_pf("DOCENA"),  self._get_ph("DOCENA")
            pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
            if pf_d and ph_d and pf_c and ph_c:
                self.ensemble_d.partial_train(self.dozen_seq, self.column_seq, d,
                                              pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
                self.ensemble_c.partial_train(self.dozen_seq, self.column_seq, c,
                                              pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models()
                self.spins_since_train = 0
        if persist:
            self._persist(number)

    def _get_pf(self, cat_type):
        """
        PF — Frecuencia reciente (últimos 5 giros).
        Igual lógica que v26, adaptada al engine v27.
        """
        if len(self.spin_history) < 5: return None
        counts = {1: 0, 2: 0, 3: 0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0:
                val = get_dozen(n) if cat_type == "DOCENA" else get_column(n)
                counts[val] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2: return None
        return {"pair": tuple(sorted(active)),
                "missing": list({1, 2, 3} - set(active))[0],
                "prob": sum(counts[a] for a in active) / 5.0}

    def _get_ph(self, cat_type):
        """
        PH — Histórico post-número.
        Primero consulta el servidor Render (stats del nuevo servidor HTTP),
        si no hay datos suficientes usa histórico local.
        Misma lógica que v26 pero adaptada al nuevo servidor.
        """
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        # 1) Intenta obtener PH desde el nuevo servidor HTTP (Render)
        server_ph = self.stats_client.get_ph_from_stats(last_num, cat_type)
        if server_ph: return server_ph
        # 2) Fallback: histórico local (igual que v26)
        counts = (self.after_number_dozen.get(last_num, {})
                  if cat_type == "DOCENA"
                  else self.after_number_column.get(last_num, {}))
        total = sum(counts.values())
        if total < 10: return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        return {"pair": tuple(sorted([sc[0][0], sc[1][0]])),
                "missing": list({1, 2, 3} - {sc[0][0], sc[1][0]})[0],
                "prob": (sc[0][1] + sc[1][1]) / total}

    def _predict_pair_ml(self, cat_type, missing_num):
        """
        ML — Predicción combinada Markov + Ensemble.
        Misma lógica que v26, adaptada al engine v27.
        """
        mk     = self.markov_d if cat_type == "DOCENA" else self.markov_c
        hist   = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])
        mk_pred   = mk.predict(hist)
        m_p_miss  = mk_pred.get(missing_num, 1 / 3) if mk_pred else 1 / 3
        pf_d, ph_d = self._get_pf("DOCENA"),  self._get_ph("DOCENA")
        pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
        ens_p_miss = 1 / 3
        if pf_d and ph_d and pf_c and ph_c:
            ens = (self.ensemble_d.predict(hist, self.column_seq,
                                           pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
                   if cat_type == "DOCENA"
                   else self.ensemble_c.predict(self.dozen_seq, hist,
                                                pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"]))
            if ens: ens_p_miss = ens.get(missing_num, 1 / 3)
        ml_miss = 0.4 * m_p_miss + 0.6 * ens_p_miss
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    # ── Detección de señal (igual a v26, adaptada al engine v27) ─────────────
    def _detect_signal(self):
        """
        Detecta señal con probabilidad ≥ MIN_PROB (80%).
        Combina PF + PH + ML igual que v26.
        Se usa tanto para señal normal (1ra oportunidad) como para la 2da.
        Adaptado al nuevo servidor HTTP (PH usa stats del servidor Render).
        """
        pf_d = self._get_pf("DOCENA")
        pf_c = self._get_pf("COLUMNA")
        if not pf_d and not pf_c: return None
        ph_d = self._get_ph("DOCENA")
        ph_c = self._get_ph("COLUMNA")
        candidates = []
        if pf_d and ph_d and set(pf_d["pair"]) == set(ph_d["pair"]):
            base = PF_W_NORM * pf_d["prob"] + PH_W_NORM * ph_d["prob"]
            ml   = self._predict_pair_ml("DOCENA", pf_d["missing"])
            prob = BASE_W_NORM * base + ML_W_NORM * ml
            logger.info(f"[Speed2DC] D base:{base:.0%} ml:{ml:.0%} final:{prob:.0%}")
            if prob >= MIN_PROB:
                candidates.append({"type": "DOCENA",
                                    "pair": tuple(f"D{x}" for x in sorted(pf_d["pair"])),
                                    "missing": f"D{pf_d['missing']}", "prob": prob})
        if pf_c and ph_c and set(pf_c["pair"]) == set(ph_c["pair"]):
            base = PF_W_NORM * pf_c["prob"] + PH_W_NORM * ph_c["prob"]
            ml   = self._predict_pair_ml("COLUMNA", pf_c["missing"])
            prob = BASE_W_NORM * base + ML_W_NORM * ml
            logger.info(f"[Speed2DC] C base:{base:.0%} ml:{ml:.0%} final:{prob:.0%}")
            if prob >= MIN_PROB:
                candidates.append({"type": "COLUMNA",
                                    "pair": tuple(f"C{x}" for x in sorted(pf_c["pair"])),
                                    "missing": f"C{pf_c['missing']}", "prob": prob})
        return max(candidates, key=lambda x: x["prob"]) if candidates else None

    # ── Formato de señal ──────────────────────────────────────────────────────
    def _format_bets(self, bet_usd, type_str):
        """
        Muestra apuesta por país con el cero incluido (10% de la apuesta).
        Correcto: apuesta D/C + apuesta cero (10%) separadas.
        """
        lines = []
        for curr in ["USD", "MXN", "PEN", "COP", "ARS", "CLP"]:
            sym      = CURRENCY_SYMBOLS[curr]
            mult     = CURRENCY_MULTIPLIERS[curr]
            dec      = CURRENCY_DECIMALS[curr]
            flag     = CURRENCY_FLAGS[curr]
            bet_loc  = bet_usd * mult
            zero_loc = bet_usd * 0.1 * mult
            lines.append(
                f"{flag} {curr}: {sym}{bet_loc:.{dec}f} x {type_str} "
                f"+ Cero: {sym}{zero_loc:.{dec}f}"
            )
        return "\n".join(lines)

    def _build_signal_text(self):
        bet_usd    = self.gestor.apostar_por_docena(self.bankroll)
        nums       = sorted([p[1:] for p in self.active_pair])
        prefix     = "D" if self.active_type == "DOCENA" else "C"
        pair_disp  = f"{prefix}{nums[0]} y {prefix}{nums[1]}"
        cat_label  = "Docenas"  if self.active_type == "DOCENA" else "Columnas"
        bet_label  = "Docena"   if self.active_type == "DOCENA" else "Columna"
        header     = ("✅✅ ENTRADA CONFIRMADA ✅✅"
                      if self.gestor.oportunidad == 1
                      else "🚨 SEGUNDA OPORTUNIDAD 🚨")
        return (f"{header}\n\n"
                f"🕹️ SPEED ROULETTE 2\n"
                f"🎯 {cat_label}: {pair_disp}\n"
                f"⚔️ Cubrir el CERO 🟢\n\n"
                f"🚨 MONTO DE APUESTA POR PAIS:\n"
                f"{self._format_bets(bet_usd, bet_label)}")

    def _send_signal(self):
        msg_id = tg_send(self._build_signal_text())
        if msg_id:
            self.active_signal_msg_id = msg_id

    # ── Resolución ────────────────────────────────────────────────────────────
    def _resolve(self, number, color):
        """
        Resuelve el giro activo considerando la apuesta al cero (10% de la ficha).

        Lógica financiera (igual que v26):
          spin_investment = (2 × bet_usd) + zero_bet
          WIN D/C  : dozen_payout (3×bet) - spin_investment  → net = bet - zero_bet
          WIN CERO : zero_payout (36×zero_bet) - spin_investment
          LOSS     : -spin_investment

        Stats: se registra solo el resultado FINAL del ciclo completo (igual a v26).
          - 1ra oportunidad perdida → NO se registra, se activa waiting_second_opp
          - nivel/debt solo sube cuando fallan AMBAS oportunidades
        """
        d        = get_dozen(number)
        c        = get_column(number)
        type_str = self.active_type
        val_num  = d if type_str == "DOCENA" else c
        opp_num  = self.gestor.oportunidad     # 1 = primera, 2 = segunda
        val_prefix  = "D" if type_str == "DOCENA" else "C"
        val_display = f"{val_prefix}{val_num}"
        bet_usd  = self.gestor.apostar_por_docena(self.bankroll)

        # Apuesta al cero: 10% de la ficha (igual que v26)
        zero_bet       = round(0.1 * bet_usd, 2)
        spin_investment = round((2 * bet_usd) + zero_bet, 2)

        # ── ZERO ──────────────────────────────────────────────────────────────
        if number == 0:
            # Gana el cero (36x la ficha del cero), se pierden las 2 apuestas D/C
            zero_payout  = round(zero_bet * 36, 2)
            spin_profit  = round(zero_payout - spin_investment, 2)
            self.bankroll = round(self.bankroll + spin_profit, 2)
            signal_profit = round(spin_profit - self.total_signal_loss, 2)

            self.gestor.verificar_recuperacion(self.bankroll)
            sign = "+" if signal_profit >= 0 else ""
            tg_send(f"🟢 CERO — Ganó el cero — Op. #{opp_num}\n"
                    f"🎉 {sign}{signal_profit:.2f} USD 🎉\n"
                    f"💰 Balance: ${self.bankroll:.2f} USD")
            self.stats.record('WIN_ZERO', opp_num, 0, 0, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()
            return

        won = ((type_str == "DOCENA"  and d != 0 and f"D{d}" in self.active_pair) or
               (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair))

        if won:
            # ── WIN D/C: paga 3× la ficha, se descuenta la apuesta del cero
            dozen_payout  = round(3 * bet_usd, 2)
            spin_profit   = round(dozen_payout - spin_investment, 2)  # = bet_usd - zero_bet
            self.bankroll = round(self.bankroll + spin_profit, 2)
            signal_profit = round(spin_profit - self.total_signal_loss, 2)

            self.gestor.verificar_recuperacion(self.bankroll)
            sign = "+" if signal_profit >= 0 else ""
            tg_send(f"✅ WIN {number} — {type_str} {val_display} — Op. #{opp_num}\n"
                    f"🎉 {sign}{signal_profit:.2f} USD 🎉\n"
                    f"💰 Balance: ${self.bankroll:.2f} USD")
            self.stats.record('WIN_PAR', opp_num, number, val_num, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()

        else:
            # ── LOSS: se pierde la inversión completa (D/C × 2 + cero)
            self.bankroll          = round(self.bankroll - spin_investment, 2)
            self.total_signal_loss = round(self.total_signal_loss + spin_investment, 2)

            if opp_num == 1:
                # ── 1ra señal perdida: borrar mensaje, esperar 2da oportunidad
                # NO se registra en stats (igual a v26 donde gale#0 no se contaba)
                # NO se sube de nivel (igual a v26)
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None

                self._reset_signal()   # limpia estado de señal

                # Enviar mensaje de análisis y activar modo espera
                msg_id = tg_send("🚨 ANALIZANDO 2° OPORTUNIDAD 🚨")
                self.analyzing_msg_id   = msg_id
                self.waiting_second_opp = True
                logger.info("[Speed2DC] ⏳ Esperando 2da oportunidad (nueva señal ≥80%)...")

            else:
                # ── 2da oportunidad perdida: LOSS final del ciclo completo
                # Ahora sí se registra en stats y se sube de nivel (igual a v26 gale#1)
                tg_send(f"❌ LOSS {number} — {type_str} {val_display}\n"
                        f"🚨 -{self.total_signal_loss:.2f} USD 🚨\n"
                        f"💰 Balance: ${self.bankroll:.2f} USD")
                self.stats.record('LOSS', opp_num, number, val_num, type_str, self.bankroll)
                self.gestor.registrar_perdida_senal()
                self._check_stats()
                self._reset_signal()

    def _reset_signal(self):
        """Resetea completamente el estado de señal y 2da oportunidad."""
        self.signal_active        = False
        self.active_pair          = ()
        self.active_type          = None
        self.total_signal_loss    = 0.0
        self.active_signal_msg_id = None
        self.waiting_second_opp   = False
        self.analyzing_msg_id     = None

    def _check_stats(self):
        if not self.stats.should_send(): return
        tg_send(self.stats.get_stats_text(self.bankroll))
        self.stats.mark_sent()

    # ── Loop principal ────────────────────────────────────────────────────────
    def process_batch(self, batch):
        """
        Procesa los giros recibidos desde el nuevo servidor HTTP (Render).
        Formato esperado: [{"game_id": "...", "number": N}, ...]
        Igual deduplicación que v27 original, adaptada al formato del nuevo servidor.
        """
        new_spins = []
        for spin in reversed(batch):
            gid = spin.get("game_id")
            if not gid or gid in self.processed_game_ids: continue
            new_spins.append(spin)
        if not new_spins: return
        for spin in new_spins:
            gid    = spin["game_id"]
            number = spin["number"]
            self.processed_game_ids.add(gid)
            if 0 <= number <= 36:
                try:
                    self._process_inner(number)
                except Exception as e:
                    logger.error(f"Error processing spin: {e}", exc_info=True)
                    self._reset_signal()
        if len(self.processed_game_ids) > self.MAX_PROCESSED_IDS:
            for gid in list(self.processed_game_ids)[:150]:
                self.processed_game_ids.discard(gid)

    def _process_inner(self, number):
        d = get_dozen(number)
        c = get_column(number)
        logger.info(f"[Speed2DC] 🎰 #{len(self.spin_history)+1}: {number} D{d} C{c}")
        self._update_state(number)

        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send("🟢 <b>Speed Roulette 2 DC</b> — Listo.")

        if self.signal_active:
            # Hay señal activa → resolver el giro
            self._resolve(number, REAL_COLOR_MAP.get(number, "VERDE"))

        else:
            # No hay señal activa → intentar detectar una nueva señal ≥80%
            sig = self._detect_signal()
            if sig:
                if self.waiting_second_opp:
                    # ── Confirmar como 2da oportunidad ───────────────────────
                    if self.analyzing_msg_id:
                        tg_delete(CHAT_ID, self.analyzing_msg_id)
                        self.analyzing_msg_id = None
                    self.waiting_second_opp   = False

                    self.signal_active        = True
                    self.active_type          = sig["type"]
                    self.active_pair          = sig["pair"]
                    self.active_missing       = sig["missing"]
                    self.gestor.iniciar_senal(self.bankroll)
                    self.gestor.oportunidad   = 2          # ← segunda oportunidad
                    # total_signal_loss ya acumuló la pérdida de la 1ra oportunidad
                    self._send_signal()
                    logger.info(f"[Speed2DC] 🎯 2DA OPP {sig['type']}: {sig['pair']} ({sig['prob']:.0%})")

                else:
                    # ── Señal nueva normal (1ra oportunidad) ─────────────────
                    self.signal_active        = True
                    self.active_type          = sig["type"]
                    self.active_pair          = sig["pair"]
                    self.active_missing       = sig["missing"]
                    self.gestor.iniciar_senal(self.bankroll)
                    self.total_signal_loss    = 0.0
                    self._send_signal()
                    logger.info(f"[Speed2DC] 🎯 SEÑAL {sig['type']}: {sig['pair']} ({sig['prob']:.0%})")

    # ── HTTP Polling al nuevo servidor Render ─────────────────────────────────
    async def poll_loop(self):
        """
        Polling HTTP cada 1s al endpoint del nuevo servidor Render.
        Endpoint: GET /latest/SPEED2
        Respuesta: { last_20: [{game_id, number},...], stats_dozen, stats_column, total_spins }
        """
        url = f"{STATS_URL}/latest/{TARGET_ROULETTE}"
        logger.info(f"[Speed2DC] 🔄 Iniciando polling cada {POLL_INTERVAL}s → {url}")
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        if resp.status == 200:
                            data    = await resp.json()
                            self.stats_client.update(data)
                            last_20 = data.get("last_20", [])
                            if isinstance(last_20, list) and last_20 and isinstance(last_20[0], dict):
                                self.process_batch(last_20)
                        else:
                            self.stats_client.connected = False
                            logger.warning(f"[Speed2DC] ⚠️ Poll status: {resp.status}")
                except Exception as e:
                    self.stats_client.connected = False
                    logger.debug(f"[Speed2DC] Poll error: {e}")
                await asyncio.sleep(POLL_INTERVAL)

# ─── FLASK ────────────────────────────────────────────────────────────────────
app    = Flask(__name__)
engine: Optional[Speed2RouletteEngine] = None

@app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Speed 2 DC", "mode": "HTTP polling"})

@app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})

@app.route("/health")
def health():
    if not engine:
        return jsonify({"status": "not_ready"}), 503
    return jsonify({
        "warmup":          engine.warmup_done,
        "spins":           len(engine.spin_history),
        "balance":         f"${engine.bankroll:.2f} USD",
        "stats_connected": engine.stats_client.connected,
        "polls":           engine.stats_client.poll_count,
        "signal_active":   engine.signal_active,
        "waiting_2nd_opp": engine.waiting_second_opp,
        "nivel":           engine.gestor.nivel,
        "debt_count":      len(engine.gestor.debt_stack),
    })

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url or "localhost" in url: return
    await asyncio.sleep(30)
    while True:
        try:
            urllib.request.urlopen(f"{url}/ping", timeout=15)
        except:
            pass
        await asyncio.sleep(240)

@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Speed Roulette 2 DC</b>\n\n"
        "Polling HTTP cada 1s | Umbral 80%\n"
        "Apuesta: 0.50 USD + Cero 10% (0.05 USD)\n"
        "2 oportunidades por señal global\n\n"
        "/status /stats /reset",
        parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado", parse_mode="HTML")
        return
    if engine.signal_active:
        opp_label = "1ra Señal" if engine.gestor.oportunidad == 1 else "2da Oportunidad"
        st = f"🟢 {engine.active_pair} — {opp_label}"
    elif engine.waiting_second_opp:
        st = "⏳ Esperando 2da oportunidad..."
    else:
        st = "⚪ Idle"
    conn = "🟢 Conectado" if engine.stats_client.connected else "🔴 Desconectado"
    ago  = (time.time() - engine.stats_client.last_poll_ok
            if engine.stats_client.last_poll_ok > 0 else 0)
    bot.reply_to(m,
        f"<b>Estado:</b> {st}\n"
        f"<b>Giros:</b> {len(engine.spin_history)}\n"
        f"<b>Balance:</b> ${engine.bankroll:.2f} USD\n"
        f"<b>Nivel:</b> {engine.gestor.nivel}\n"
        f"<b>Deudas:</b> {len(engine.gestor.debt_stack)}\n"
        f"<b>Servidor:</b> {conn} ({engine.stats_client.poll_count} polls, "
        f"último hace {ago:.0f}s)\n"
        f"<b>IDs procesados:</b> {len(engine.processed_game_ids)}",
        parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not engine:
        bot.reply_to(m, "❌ Engine no inicializado", parse_mode="HTML")
        return
    bot.reply_to(m, engine.stats.get_stats_text(engine.bankroll), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    if engine:
        engine.stats                = DetailedStats()
        engine.bankroll             = 100.0
        engine.gestor.nivel         = 1
        engine.gestor.debt_stack    = []
        engine.processed_game_ids.clear()
        engine._reset_signal()
    bot.reply_to(m, f"🔄 <b>Resetado — Balance: ${engine.bankroll:.2f} USD</b>",
                 parse_mode="HTML")

def run_flask():
    app.run(host="0.0.0.0", port=10003, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    stats_client = StatsClient()
    engine       = Speed2RouletteEngine(stats_client)
    threading.Thread(
        target=lambda: bot.polling(none_stop=True, interval=1, timeout=30),
        daemon=True
    ).start()
    logger.info("[Speed2DC] 🎰 Bot Speed 2 — HTTP Polling cada 1s | Umbral 80% | Cero 10%")
    await asyncio.gather(
        asyncio.create_task(engine.poll_loop()),
        asyncio.create_task(self_ping_loop())
    )

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido.")
