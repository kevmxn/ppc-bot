#!/usr/bin/env python3
"""
Russian Roulette — Bot DC con Polling HTTP cada 1 segundo
===========================================================================
CAMBIOS v28 (RUSSIAN + PHF):
  - TARGET_ROULETTE = "RUSSIAN" (key=221 en el servidor Render)
  - PHF = PHTML(65%) + PH(35%) ponderado por docena
    * PHTML: señales predefinidas de la tabla del HTML (por número)
    * PH: histórico del servidor Render (post-número)
    * PHF blendea ambas y calcula las probabilidades de las 3 docenas
  - Predicción final: PF + PHF + ML  (PHF reemplaza a PH en DOCENA)
  - COLUMNA sigue usando PH directamente (tabla HTML solo tiene docenas)
  - Si PHF par coincide con PF par → se analiza la señal
  - PHTML_W = 0.65, PH_W_COMBINE = 0.35 (configurables arriba)
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
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiohttp
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [RussianDC] %(levelname)s %(message)s')
logger = logging.getLogger("RussianDC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── CREDENCIALES ─────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID = -1003630680656

# ─── URL RULETA RUSSIAN ───────────────────────────────────────────────────────
RUSSIAN_URL = "https://stake1039.com/es/casino/games/pragmatic-play-russian-roulette"

def russian_keyboard() -> InlineKeyboardMarkup:
    """Teclado inline con el botón de acceso directo a Russian Roulette."""
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👆🏻 ACCEDER a RUSSIAN ROULETTE", url=RUSSIAN_URL))
    return kb

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
TARGET_ROULETTE = "RUSSIAN"           # ← Ruleta Rusa (key=221 en el servidor)
POLL_INTERVAL   = 1                   # segundos
LIVE_DB         = "russian_live.db"   # DB local específica de Russian

BASE_BET        = 0.50
MAX_NIVEL       = 6
WARMUP_SPINS    = 25
MIN_PROB        = 0.80
TRAIN_INTERVAL  = 100

# Pesos para PHF = PHTML(65%) + PH(35%)
PHTML_W         = 0.65    # peso de la tabla HTML predefinida
PH_W_COMBINE    = 0.35    # peso del histórico del servidor

# Pesos del cálculo de señal
PF_W_NORM   = 0.65; PH_W_NORM   = 0.35   # PF vs PHF en base
BASE_W_NORM = 0.50; ML_W_NORM   = 0.50   # base vs ML en prob final
PF_W_GALE1  = 0.30; PH_W_GALE1  = 0.70
BASE_W_GALE1= 0.65; ML_W_GALE1  = 0.35
MIN_PROB_GALE1 = 0.70

CURRENCY_MULTIPLIERS = {"USD":1.0,"MXN":20.0,"PEN":5.0,"COP":5000.0,"ARS":1500.0,"CLP":1000.0}
CURRENCY_SYMBOLS     = {"USD":"$","MXN":"$","PEN":"S/.","COP":"$","ARS":"$","CLP":"$"}
CURRENCY_FLAGS       = {"USD":"🇺🇲","MXN":"🇲🇽","PEN":"🇵🇪","COP":"🇨🇴","ARS":"🇦🇷","CLP":"🇨🇱"}
CURRENCY_DECIMALS    = {"USD":2,"MXN":2,"PEN":2,"COP":0,"ARS":0,"CLP":0}

REAL_COLOR_MAP: dict[int, str] = {
    0:"VERDE",1:"ROJO",2:"NEGRO",3:"ROJO",4:"NEGRO",5:"ROJO",6:"NEGRO",
    7:"ROJO",8:"NEGRO",9:"ROJO",10:"NEGRO",11:"NEGRO",12:"ROJO",13:"NEGRO",
    14:"ROJO",15:"NEGRO",16:"ROJO",17:"NEGRO",18:"ROJO",19:"ROJO",20:"NEGRO",
    21:"ROJO",22:"NEGRO",23:"ROJO",24:"NEGRO",25:"ROJO",26:"NEGRO",27:"ROJO",
    28:"NEGRO",29:"NEGRO",30:"ROJO",31:"NEGRO",32:"ROJO",33:"NEGRO",34:"ROJO",
    35:"NEGRO",36:"ROJO",
}

# ─── TABLA PHTML (extraída del HTML Russian-Roulette-Docena.html) ─────────────
# Probabilidades predefinidas por número (d1, d2, d3 en porcentaje).
# Son las señales fijas de la tabla que se ponderan en PHF.
# id=0 → "NO APOSTAR" (tratado como ausencia de señal PHTML)
DOZEN_TABLE: Dict[int, Dict[str, int]] = {
    0:  {"d1": 32, "d2": 32, "d3": 32},   # NO APOSTAR
    1:  {"d1": 28, "d2": 32, "d3": 36},
    2:  {"d1": 36, "d2": 28, "d3": 32},
    3:  {"d1": 24, "d2": 32, "d3": 36},
    4:  {"d1": 32, "d2": 40, "d3": 24},
    5:  {"d1": 40, "d2": 24, "d3": 36},
    6:  {"d1": 32, "d2": 24, "d3": 40},
    7:  {"d1": 36, "d2": 24, "d3": 40},
    8:  {"d1": 32, "d2": 36, "d3": 28},
    9:  {"d1": 28, "d2": 36, "d3": 32},
    10: {"d1": 40, "d2": 32, "d3": 28},
    11: {"d1": 36, "d2": 24, "d3": 36},
    12: {"d1": 32, "d2": 28, "d3": 36},
    13: {"d1": 32, "d2": 28, "d3": 36},
    14: {"d1": 16, "d2": 48, "d3": 32},
    15: {"d1": 36, "d2": 28, "d3": 32},
    16: {"d1": 28, "d2": 32, "d3": 36},
    17: {"d1": 20, "d2": 44, "d3": 32},
    18: {"d1": 32, "d2": 28, "d3": 36},
    19: {"d1": 36, "d2": 28, "d3": 32},
    20: {"d1": 36, "d2": 36, "d3": 28},
    21: {"d1": 24, "d2": 44, "d3": 28},
    22: {"d1": 36, "d2": 36, "d3": 28},
    23: {"d1": 24, "d2": 32, "d3": 40},
    24: {"d1": 44, "d2": 32, "d3": 24},
    25: {"d1": 36, "d2": 24, "d3": 36},
    26: {"d1": 40, "d2": 28, "d3": 32},
    27: {"d1": 32, "d2": 28, "d3": 36},
    28: {"d1": 36, "d2": 28, "d3": 32},
    29: {"d1": 32, "d2": 24, "d3": 40},
    30: {"d1": 36, "d2": 36, "d3": 28},
    31: {"d1": 32, "d2": 36, "d3": 24},
    32: {"d1": 32, "d2": 36, "d3": 28},
    33: {"d1": 28, "d2": 32, "d3": 36},
    34: {"d1": 36, "d2": 28, "d3": 32},
    35: {"d1": 36, "d2": 32, "d3": 24},
    36: {"d1": 28, "d2": 36, "d3": 32},
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

def tg_send(text: str, markup: InlineKeyboardMarkup = None) -> Optional[int]:
    if not text:
        return None
    try:
        msg = _tg_call(
            bot.send_message,
            chat_id=CHAT_ID,
            text=text,
            parse_mode="HTML",
            reply_markup=markup,
        )
        if msg:
            logger.info(f"✅ Message sent (ID: {msg.message_id})")
            return msg.message_id
        return None
    except Exception as e:
        logger.error(f"❌ Exception in tg_send: {e}")
        return None

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
        logger.info(f"✅ Message deleted (ID: {message_id})")
    except Exception as e:
        logger.warning(f"⚠️ Failed to delete message: {e}")

# ─── CLIENTE STATS (HTTP POLLING) ────────────────────────────────────────────
class StatsClient:
    """
    Recibe stats del servidor Render via HTTP polling.
    GET /latest/RUSSIAN  →
      {
        "last_20":      [{game_id, number}, ...],
        "stats_dozen":  {"NUM": {"total": T, "1": p1, "2": p2, "3": p3}},
        "stats_column": {...},
        "total_spins":  N
      }
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
        PH clásico: retorna el par más frecuente post-número desde el servidor.
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

    def get_ph_probs_raw(self, number: int, cat_type: str) -> Optional[Dict]:
        """
        Retorna probabilidades individuales de las 3 docenas/columnas (en rango 0-1)
        desde el servidor Render.  Usado para construir PHF.
        """
        stats   = self.stats_column if cat_type == "COLUMNA" else self.stats_dozen
        num_key = str(number)
        if num_key not in stats:
            return None
        data = stats[num_key]
        if data.get("total", 0) < 10:
            return None
        return {
            1: data.get("1", 0) / 100.0,
            2: data.get("2", 0) / 100.0,
            3: data.get("3", 0) / 100.0,
        }

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
        net_base = BASE_BET * 0.9
        return (self.debt_stack[-1] + net_base) if self.debt_stack else (self.b0 + net_base)

    def apostar_por_docena(self, balance):
        return self.nivel * BASE_BET if self.oportunidad == 1 else 3 * self.nivel * BASE_BET

    def registrar_perdida_senal(self):
        self.debt_stack.append(self.b0)
        self.nivel = self.nivel + 1 if self.nivel < MAX_NIVEL else 1
        logger.info(f"[RussianDC] 📋 Deuda: B0={self.b0:.2f} | Pila: {len(self.debt_stack)} | Nivel→{self.nivel}")

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
    def __init__(self):
        self.wins_pair           = 0
        self.wins_zero           = 0
        self.losses              = 0
        self.consecutive         = 0
        self.last_20             = deque(maxlen=20)
        self.signals_processed   = 0
        self.last_report_signals = 0

    def record(self, result_type, oportunidad, number, val, type_str, bankroll):
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
class RussianRouletteEngine:
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

        self.signal_active        = False
        self.active_type          = None
        self.active_pair          = ()
        self.active_missing       = ""
        self.gestor               = GestorDocenas()
        self.total_signal_loss    = 0.0
        self.active_signal_msg_id = None

        self.waiting_second_opp   = False
        self.analyzing_msg_id     = None

        self.bankroll             = 100.0
        self.stats                = DetailedStats()
        self._db                  = _get_db()
        self.spins_since_train    = 0
        self.processed_game_ids: set = set()
        self.MAX_PROCESSED_IDS    = 300

        live_loaded      = self._load_live_history()
        self.ws_count    = live_loaded
        self.warmup_done = live_loaded >= WARMUP_SPINS
        logger.info(f"[RussianDC] 📦 Pre-cargados: {live_loaded} | Warmup: {'✅' if self.warmup_done else '⏳'}")

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

    # ── PF — Frecuencia reciente ──────────────────────────────────────────────
    def _get_pf(self, cat_type):
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

    # ── PH — Histórico post-número (par clásico) ──────────────────────────────
    def _get_ph(self, cat_type):
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None
        # 1) Servidor Render
        server_ph = self.stats_client.get_ph_from_stats(last_num, cat_type)
        if server_ph: return server_ph
        # 2) Fallback local
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

    # ── PHTML — Probabilidades fijas de la tabla HTML ─────────────────────────
    def _get_phtml_probs(self, number: int) -> Optional[Dict]:
        """
        Retorna las probabilidades predefinidas de la tabla HTML (PHTML)
        para el número dado, normalizadas como fracciones {1: p1, 2: p2, 3: p3}.
        Retorna None para el número 0 (NO APOSTAR).
        """
        if number == 0:
            return None
        entry = DOZEN_TABLE.get(number)
        if entry is None:
            return None
        d1, d2, d3 = entry["d1"], entry["d2"], entry["d3"]
        total = d1 + d2 + d3
        if total == 0:
            return None
        return {1: d1 / total, 2: d2 / total, 3: d3 / total}

    # ── PH Probs Raw — Probabilidades individuales del servidor ───────────────
    def _get_ph_probs_raw(self, number: int, cat_type: str) -> Optional[Dict]:
        """
        Retorna probabilidades individuales de las 3 docenas desde el servidor
        (en rango 0-1).  Usado internamente para construir PHF.
        Fallback a histórico local si no hay suficientes datos del servidor.
        """
        if number == 0:
            return None
        # 1) Servidor Render
        probs = self.stats_client.get_ph_probs_raw(number, cat_type)
        if probs is not None:
            return probs
        # 2) Fallback local
        counts = (self.after_number_dozen.get(number, {})
                  if cat_type == "DOCENA"
                  else self.after_number_column.get(number, {}))
        total = sum(counts.values())
        if total < 10:
            return None
        return {1: counts.get(1, 0) / total,
                2: counts.get(2, 0) / total,
                3: counts.get(3, 0) / total}

    # ── PHF — Probabilidad combinada PHTML(65%) + PH(35%) ────────────────────
    def _get_phf(self, number: int) -> Optional[Dict]:
        """
        PHF = PHTML(65%) + PH(35%) ponderado para DOCENA.

        1. Obtiene PHTML (probabilidades fijas de la tabla HTML) para el número.
        2. Obtiene PH (probabilidades históricas del servidor/local).
        3. Combina: PHF[d] = PHTML_W * PHTML[d] + PH_W_COMBINE * PH[d]
        4. Normaliza y determina el par de docenas con mayor probabilidad.
        5. Retorna {"pair", "missing", "prob", "probs"}.

        Si no hay datos PH, usa solo PHTML.
        Retorna None si no hay PHTML disponible (número 0 o sin datos).
        """
        if number == 0:
            return None

        phtml = self._get_phtml_probs(number)
        if phtml is None:
            return None

        ph = self._get_ph_probs_raw(number, "DOCENA")

        if ph is not None:
            # Blend ponderado
            phf_raw = {d: PHTML_W * phtml[d] + PH_W_COMBINE * ph[d] for d in [1, 2, 3]}
        else:
            # Sin datos PH: usar solo PHTML
            logger.debug(f"[RussianDC] PHF: sin PH para #{number}, usando solo PHTML")
            phf_raw = phtml.copy()

        # Normalizar
        total = sum(phf_raw.values())
        if total == 0:
            return None
        phf = {d: v / total for d, v in phf_raw.items()}

        # Obtener top 2 docenas por PHF
        sorted_d = sorted(phf.items(), key=lambda x: x[1], reverse=True)
        pair     = tuple(sorted([sorted_d[0][0], sorted_d[1][0]]))
        missing  = list({1, 2, 3} - set(pair))[0]
        prob     = phf[sorted_d[0][0]] + phf[sorted_d[1][0]]

        logger.debug(
            f"[RussianDC] PHF #{number}: "
            f"D1={phf[1]:.1%} D2={phf[2]:.1%} D3={phf[3]:.1%} "
            f"→ par={pair} prob={prob:.0%}"
        )
        return {"pair": pair, "missing": missing, "prob": prob, "probs": phf}

    # ── ML — Predicción combinada Markov + Ensemble ───────────────────────────
    def _predict_pair_ml(self, cat_type, missing_num):
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

    # ── Detección de señal con PHF ────────────────────────────────────────────
    def _detect_signal(self):
        """
        Detecta señal con probabilidad ≥ MIN_PROB (80%).

        DOCENA  → usa PHF = PHTML(65%) + PH(35%)
          • Si PHF par == PF par → analiza señal con PF + PHF + ML
        COLUMNA → usa PH directo (sin PHTML, la tabla solo tiene docenas)
          • Si PH par == PF par → analiza señal con PF + PH + ML
        """
        pf_d = self._get_pf("DOCENA")
        pf_c = self._get_pf("COLUMNA")
        if not pf_d and not pf_c:
            return None

        last_num = self.spin_history[-1]["number"] if self.spin_history else None
        if last_num is None:
            return None

        candidates = []

        # ── DOCENA: PHF = PHTML(65%) + PH(35%) ───────────────────────────────
        if pf_d and last_num != 0:
            phf_d = self._get_phf(last_num)
            if phf_d and set(pf_d["pair"]) == set(phf_d["pair"]):
                # PF par coincide con PHF par → analizar señal
                base = PF_W_NORM * pf_d["prob"] + PH_W_NORM * phf_d["prob"]
                ml   = self._predict_pair_ml("DOCENA", pf_d["missing"])
                prob = BASE_W_NORM * base + ML_W_NORM * ml
                logger.info(
                    f"[RussianDC] D | PF:{pf_d['prob']:.0%} "
                    f"PHF:{phf_d['prob']:.0%} "
                    f"(PHTML:{phf_d['probs'][1]:.1%}/{phf_d['probs'][2]:.1%}/{phf_d['probs'][3]:.1%}) "
                    f"base:{base:.0%} ml:{ml:.0%} → final:{prob:.0%}"
                )
                if prob >= MIN_PROB:
                    candidates.append({
                        "type": "DOCENA",
                        "pair": tuple(f"D{x}" for x in sorted(pf_d["pair"])),
                        "missing": f"D{pf_d['missing']}",
                        "prob": prob
                    })
            else:
                if phf_d:
                    logger.debug(
                        f"[RussianDC] D | PF par {pf_d['pair']} ≠ PHF par {phf_d['pair']} → sin señal"
                    )

        # ── COLUMNA: PH directo (tabla HTML no cubre columnas) ────────────────
        ph_c = self._get_ph("COLUMNA")
        if pf_c and ph_c and set(pf_c["pair"]) == set(ph_c["pair"]):
            base = PF_W_NORM * pf_c["prob"] + PH_W_NORM * ph_c["prob"]
            ml   = self._predict_pair_ml("COLUMNA", pf_c["missing"])
            prob = BASE_W_NORM * base + ML_W_NORM * ml
            logger.info(f"[RussianDC] C | PF:{pf_c['prob']:.0%} PH:{ph_c['prob']:.0%} base:{base:.0%} ml:{ml:.0%} → final:{prob:.0%}")
            if prob >= MIN_PROB:
                candidates.append({
                    "type": "COLUMNA",
                    "pair": tuple(f"C{x}" for x in sorted(pf_c["pair"])),
                    "missing": f"C{pf_c['missing']}",
                    "prob": prob
                })

        return max(candidates, key=lambda x: x["prob"]) if candidates else None

    # ── Formato de señal ──────────────────────────────────────────────────────
    def _format_bets(self, bet_usd, type_str):
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
                f"🕹️ RUSSIAN ROULETTE\n"
                f"🎯 {cat_label}: {pair_disp}\n"
                f"⚔️ Cubrir el CERO 🟢\n\n"
                f"🚨 MONTO DE APUESTA POR PAIS:\n"
                f"{self._format_bets(bet_usd, bet_label)}")

    def _send_signal(self):
        msg_id = tg_send(self._build_signal_text(), markup=russian_keyboard())
        if msg_id:
            self.active_signal_msg_id = msg_id

    # ── Resolución ────────────────────────────────────────────────────────────
    def _resolve(self, number, color):
        d        = get_dozen(number)
        c        = get_column(number)
        type_str = self.active_type
        val_num  = d if type_str == "DOCENA" else c
        opp_num  = self.gestor.oportunidad
        val_prefix  = "D" if type_str == "DOCENA" else "C"
        val_display = f"{val_prefix}{val_num}"
        bet_usd  = self.gestor.apostar_por_docena(self.bankroll)

        zero_bet        = round(0.1 * bet_usd, 2)
        spin_investment = round((2 * bet_usd) + zero_bet, 2)

        if number == 0:
            zero_payout  = round(zero_bet * 36, 2)
            spin_profit  = round(zero_payout - spin_investment, 2)
            self.bankroll = round(self.bankroll + spin_profit, 2)
            signal_profit = round(spin_profit - self.total_signal_loss, 2)
            self.gestor.verificar_recuperacion(self.bankroll)
            sign = "+" if signal_profit >= 0 else ""
            tg_send(f"🟢 CERO — Ganó el cero — Op. #{opp_num}\n"
                    f"🎉 {sign}{signal_profit:.2f} USD 🎉\n"
                    f"💰 Balance: ${self.bankroll:.2f} USD",
                    markup=russian_keyboard())
            self.stats.record('WIN_ZERO', opp_num, 0, 0, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()
            return

        won = ((type_str == "DOCENA"  and d != 0 and f"D{d}" in self.active_pair) or
               (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair))

        if won:
            dozen_payout  = round(3 * bet_usd, 2)
            spin_profit   = round(dozen_payout - spin_investment, 2)
            self.bankroll = round(self.bankroll + spin_profit, 2)
            signal_profit = round(spin_profit - self.total_signal_loss, 2)
            self.gestor.verificar_recuperacion(self.bankroll)
            sign = "+" if signal_profit >= 0 else ""
            tg_send(f"✅ WIN {number} — {type_str} {val_display} — Op. #{opp_num}\n"
                    f"🎉 {sign}{signal_profit:.2f} USD 🎉\n"
                    f"💰 Balance: ${self.bankroll:.2f} USD",
                    markup=russian_keyboard())
            self.stats.record('WIN_PAR', opp_num, number, val_num, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()

        else:
            self.bankroll          = round(self.bankroll - spin_investment, 2)
            self.total_signal_loss = round(self.total_signal_loss + spin_investment, 2)

            if opp_num == 1:
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None
                self._reset_signal()
                msg_id = tg_send("🚨 ANALIZANDO 2° OPORTUNIDAD 🚨")
                self.analyzing_msg_id   = msg_id
                self.waiting_second_opp = True
                logger.info("[RussianDC] ⏳ Esperando 2da oportunidad (nueva señal ≥80%)...")
            else:
                tg_send(f"❌ LOSS {number} — {type_str} {val_display}\n"
                        f"🚨 -{self.total_signal_loss:.2f} USD 🚨\n"
                        f"💰 Balance: ${self.bankroll:.2f} USD",
                        markup=russian_keyboard())
                self.stats.record('LOSS', opp_num, number, val_num, type_str, self.bankroll)
                self.gestor.registrar_perdida_senal()
                self._check_stats()
                self._reset_signal()

    def _reset_signal(self):
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
        logger.info(f"[RussianDC] 🎰 #{len(self.spin_history)+1}: {number} D{d} C{c}")
        self._update_state(number)

        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send("🟢 <b>Russian Roulette DC</b> — Listo. (PHF activo)")

        if self.signal_active:
            self._resolve(number, REAL_COLOR_MAP.get(number, "VERDE"))
        else:
            sig = self._detect_signal()
            if sig:
                if self.waiting_second_opp:
                    if self.analyzing_msg_id:
                        tg_delete(CHAT_ID, self.analyzing_msg_id)
                        self.analyzing_msg_id = None
                    self.waiting_second_opp   = False
                    self.signal_active        = True
                    self.active_type          = sig["type"]
                    self.active_pair          = sig["pair"]
                    self.active_missing       = sig["missing"]
                    self.gestor.iniciar_senal(self.bankroll)
                    self.gestor.oportunidad   = 2
                    self._send_signal()
                    logger.info(f"[RussianDC] 🎯 2DA OPP {sig['type']}: {sig['pair']} ({sig['prob']:.0%})")
                else:
                    self.signal_active        = True
                    self.active_type          = sig["type"]
                    self.active_pair          = sig["pair"]
                    self.active_missing       = sig["missing"]
                    self.gestor.iniciar_senal(self.bankroll)
                    self.total_signal_loss    = 0.0
                    self._send_signal()
                    logger.info(f"[RussianDC] 🎯 SEÑAL {sig['type']}: {sig['pair']} ({sig['prob']:.0%})")

    # ── HTTP Polling al servidor Render ───────────────────────────────────────
    async def poll_loop(self):
        """
        Polling HTTP cada 1s → GET /latest/RUSSIAN
        Respuesta: {last_20, stats_dozen, stats_column, total_spins}
        """
        url = f"{STATS_URL}/latest/{TARGET_ROULETTE}"
        logger.info(f"[RussianDC] 🔄 Iniciando polling cada {POLL_INTERVAL}s → {url}")
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
                            logger.warning(f"[RussianDC] ⚠️ Poll status: {resp.status}")
                except Exception as e:
                    self.stats_client.connected = False
                    logger.debug(f"[RussianDC] Poll error: {e}")
                await asyncio.sleep(POLL_INTERVAL)

# ─── FLASK ────────────────────────────────────────────────────────────────────
app    = Flask(__name__)
engine: Optional[RussianRouletteEngine] = None

@app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Russian Roulette DC", "mode": "HTTP polling + PHF"})

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
        "phf_active":      True,
        "phtml_w":         PHTML_W,
        "ph_w_combine":    PH_W_COMBINE,
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

# ─── COMANDOS TELEGRAM ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m):
    bot.reply_to(m,
        "<b>🎰 Russian Roulette DC</b>\n\n"
        "Polling HTTP cada 1s | Umbral 80%\n"
        "PHF = PHTML(65%) + PH(35%)\n"
        "Apuesta: 0.50 USD + Cero 10% (0.05 USD)\n"
        "2 oportunidades por señal global\n\n"
        "/status /stats /reset /phf",
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
        f"<b>PHF:</b> PHTML {PHTML_W:.0%} + PH {PH_W_COMBINE:.0%}",
        parse_mode="HTML")

@bot.message_handler(commands=['phf'])
def cmd_phf(m):
    """Muestra el PHF del último número procesado."""
    if not engine or not engine.spin_history:
        bot.reply_to(m, "❌ Sin giros procesados aún", parse_mode="HTML")
        return
    last = engine.spin_history[-1]["number"]
    phtml = engine._get_phtml_probs(last)
    ph    = engine._get_ph_probs_raw(last, "DOCENA")
    phf   = engine._get_phf(last)
    pf    = engine._get_pf("DOCENA")

    def fmt(d): return f"D1:{d[1]:.1%} D2:{d[2]:.1%} D3:{d[3]:.1%}" if d else "N/A"

    coincide = "✅ SÍ" if (pf and phf and set(pf["pair"]) == set(phf["pair"])) else "❌ NO"

    bot.reply_to(m,
        f"<b>🔬 PHF para #{last}</b>\n\n"
        f"<b>PHTML ({PHTML_W:.0%}):</b> {fmt(phtml)}\n"
        f"<b>PH ({PH_W_COMBINE:.0%}):</b> {fmt(ph)}\n"
        f"<b>PHF:</b> {fmt(phf['probs']) if phf else 'N/A'}\n"
        f"  → par={phf['pair']} prob={phf['prob']:.0%}\n\n"
        f"<b>PF par:</b> {pf['pair'] if pf else 'N/A'}\n"
        f"<b>PHF par:</b> {phf['pair'] if phf else 'N/A'}\n"
        f"<b>Coinciden:</b> {coincide}",
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
    app.run(host="0.0.0.0", port=10005, debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    stats_client = StatsClient()
    engine       = RussianRouletteEngine(stats_client)
    threading.Thread(
        target=lambda: bot.polling(none_stop=True, interval=1, timeout=30),
        daemon=True
    ).start()
    logger.info(
        f"[RussianDC] 🎰 Russian Roulette DC — "
        f"HTTP Polling 1s | Umbral {MIN_PROB:.0%} | "
        f"PHF=PHTML({PHTML_W:.0%})+PH({PH_W_COMBINE:.0%}) | Cero 10%"
    )
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
