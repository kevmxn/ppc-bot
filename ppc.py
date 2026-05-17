#!/usr/bin/env python3
"""
Speed Roulette 2 — Bot de señales para Docenas y Columnas
Conectado al Stats Server para datos históricos en tiempo real.

  - Formato de señales con montos por país (USD, MXN, PEN, COP, ARS, CLP).
  - Cero se calcula al 10% de la apuesta base por docena/columna.
  - Segunda oportunidad header: "🚨 SEGUNDA OPORTUNIDAD 🚨".
  - Estadísticas globales en USD.
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
import websockets
from flask import Flask, jsonify
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s [Speed2DC] %(levelname)s %(message)s')
logger = logging.getLogger("Speed2DC")
for _ln in ['werkzeug', 'flask.app', 'flask', 'urllib3']:
    logging.getLogger(_ln).setLevel(logging.ERROR)

# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
TOKEN   = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID = -1003630680656

_session = requests.Session()
_retry = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
               allowed_methods=["GET", "POST"], raise_on_status=False)
_session.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
_session.mount("http://",  HTTPAdapter(max_retries=_retry, pool_connections=10, pool_maxsize=20))
bot = telebot.TeleBot(TOKEN, threaded=False)
bot.session = _session

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
STATS_WS_URL = "wss://ruletasbot-rjce.onrender.com/ws"
TARGET_ROULETTE = "SPEED2"
LIVE_DB     = "speed2_live.db"

BASE_BET       = 0.50
MAX_NIVEL      = 6
WARMUP_SPINS   = 25
MIN_PROB       = 0.78
TRAIN_INTERVAL = 100

PF_W_NORM  = 0.65; PH_W_NORM  = 0.35
BASE_W_NORM = 0.50; ML_W_NORM   = 0.50

PF_W_GALE1  = 0.30; PH_W_GALE1  = 0.70
BASE_W_GALE1 = 0.65; ML_W_GALE1   = 0.35
MIN_PROB_GALE1 = 0.70

# ─── MULTIPLICADORES DE MONEDA (Relativos a USD 1.00) ────────────────────────
CURRENCY_MULTIPLIERS = {
    "USD": 1.0,
    "MXN": 20.0,
    "PEN": 5.0,
    "COP": 5000.0,
    "ARS": 1500.0,
    "CLP": 1000.0
}
CURRENCY_SYMBOLS = {
    "USD": "$", "MXN": "$", "PEN": "S/.", "COP": "$", "ARS": "$", "CLP": "$"
}
CURRENCY_FLAGS = {
    "USD": "🇺🇲", "MXN": "🇲🇽", "PEN": "🇵🇪", "COP": "🇨🇴", "ARS": "🇦🇷", "CLP": "🇨🇱"
}
CURRENCY_DECIMALS = {
    "USD": 2, "MXN": 2, "PEN": 2, "COP": 0, "ARS": 0, "CLP": 0
}

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
    delay = 2.0
    for attempt in range(1, _TG_RETRIES + 1):
        try: return fn(*a, **kw)
        except Exception as e:
            err = str(e)
            if "retry after" in err.lower():
                try: wait = int(''.join(filter(str.isdigit, err))) + 1
                except: wait = 30
                time.sleep(wait); continue
            if attempt == _TG_RETRIES: return None
            time.sleep(delay); delay = min(delay * 2, 60)
    return None

def tg_send(text: str) -> Optional[int]:
    msg = _tg_call(bot.send_message, chat_id=CHAT_ID, text=text, parse_mode="HTML")
    return msg.message_id if msg else None

def tg_delete(chat_id: int, message_id: int):
    try:
        _tg_call(bot.delete_message, chat_id=chat_id, message_id=message_id)
    except:
        pass

# ─── CLIENTE DEL STATS SERVER ─────────────────────────────────────────────────
class StatsClient:
    def __init__(self):
        self.stats_dozen = {}
        self.stats_column = {}
        self.last_20 = []
        self.last_50 = []
        self.total_spins = 0
        self.connected = False

    def update_from_server(self, data: dict):
        self.last_20 = data.get("last_20", self.last_20)
        self.last_50 = data.get("last_50", self.last_50)
        self.stats_dozen = data.get("stats_dozen", self.stats_dozen)
        self.stats_column = data.get("stats_column", self.stats_column)
        self.total_spins = data.get("total_spins", self.total_spins)

    def get_ph_from_stats(self, number: int, cat_type: str) -> Optional[Dict]:
        stats = self.stats_column if cat_type == "COLUMNA" else self.stats_dozen
        num_key = str(number)
        if num_key not in stats: return None
        data = stats[num_key]
        if data.get("total", 0) < 10: return None
        probs = {1: data.get("1", 0), 2: data.get("2", 0), 3: data.get("3", 0)}
        sorted_probs = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        if sorted_probs[0][1] == 0: return None
        pair = tuple(sorted([sorted_probs[0][0], sorted_probs[1][0]]))
        missing = list({1, 2, 3} - set(pair))[0]
        prob = (sorted_probs[0][1] + sorted_probs[1][1]) / 100.0
        return {"pair": pair, "missing": missing, "prob": prob}

# ─── EMA ──────────────────────────────────────────────────────────────────────
def calc_ema(data: list, period: int) -> list:
    if len(data) < period: return [None] * len(data)
    mult = 2 / (period + 1)
    out = [None] * (period - 1)
    prev = sum(data[:period]) / period
    out.append(prev)
    for v in data[period:]:
        prev = v * mult + prev * (1 - mult)
        out.append(prev)
    return out

def ema_signal(levels: list, mode: str = "moderado") -> bool:
    if len(levels) < 20: return False
    e4, e8, e20 = calc_ema(levels, 4), calc_ema(levels, 8), calc_ema(levels, 20)
    li = len(levels) - 1
    if any(v is None for v in [e4[li], e8[li], e20[li]]): return False
    cur = levels[li]; ce4, ce8, ce20 = e4[li], e8[li], e20[li]
    pe4  = e4[li-1]  if li > 0 and e4[li-1]  is not None else ce4
    pe8  = e8[li-1]  if li > 0 and e8[li-1]  is not None else ce8
    pe20 = e20[li-1] if li > 0 and e20[li-1] is not None else ce20
    if mode == "tendencia":
        return (pe4 <= pe20 and ce4 > ce20) or (cur > ce4 and cur > ce8 and cur > ce20)
    else:
        v_pattern = False
        if len(levels) >= 3:
            a, b, c = levels[-3], levels[-2], levels[-1]
            v_pattern = (b < a) and (b < c) and (c > a)
        return (pe4 <= pe8 and ce4 > ce8) or (pe8 <= pe20 and ce8 > ce20) or (cur > ce4 and cur > ce8) or v_pattern

# ─── MARKOV SUAVIZADO ─────────────────────────────────────────────────────────
class SmoothedMarkovPredictor:
    def __init__(self, window: int = 60, order: int = 2):
        self.window = window; self.order = order
        self.transition_counts: dict = {}

    def update(self, sequence: list):
        self.transition_counts = defaultdict(lambda: defaultdict(int))
        recent = sequence[-self.window:]
        if len(recent) < self.order + 1: return
        for i in range(len(recent) - self.order):
            state = tuple(recent[i:i + self.order])
            nxt = recent[i + self.order]
            self.transition_counts[state][nxt] += 1

    def predict(self, sequence: list) -> Optional[dict]:
        if len(sequence) < self.order: return None
        state = tuple(sequence[-self.order:])
        counts = dict(self.transition_counts.get(state, {}))
        total = sum(counts.values())
        if total < 10: return None
        alpha = 2.0; vocab_size = 3
        probs = {k: (v + alpha) / (total + alpha * vocab_size) for k, v in counts.items()}
        for c in [1, 2, 3]:
            if c not in probs: probs[c] = alpha / (total + alpha * vocab_size)
        return probs

# ─── ENSEMBLE ML CRUZADO ──────────────────────────────────────────────────────
class OnlineEnsemblePredictor:
    WINDOW = 5; CLASSES = [1, 2, 3]

    def __init__(self):
        self.mnb = MultinomialNB(alpha=2.0, class_prior=[0.333, 0.333, 0.333])
        self.sgd = SGDClassifier(loss='log_loss', learning_rate='adaptive', eta0=0.005,
                                 penalty='l2', alpha=0.01, epsilon=0.2)
        self.trained = False; self.sample_count = 0

    def _extract_features(self, hist_d, hist_c, pf_pair_d, ph_pair_d, pf_pair_c, ph_pair_c) -> Optional[list]:
        if len(hist_d) < self.WINDOW or len(hist_c) < self.WINDOW: return None
        features = []
        for i in range(1, self.WINDOW + 1):
            d = hist_d[-i]; c = hist_c[-i]
            vec = [0] * 9; vec[(d - 1) * 3 + (c - 1)] = 1
            features.extend(vec)
        for pair in (pf_pair_d, ph_pair_d, pf_pair_c, ph_pair_c):
            vec = [0, 0, 0]
            for x in pair: vec[x - 1] = 1
            features.extend(vec)
        return features

    def partial_train(self, hist_d, hist_c, target, pf_d, ph_d, pf_c, ph_c):
        feats = self._extract_features(hist_d[:-1], hist_c[:-1], pf_d, ph_d, pf_c, ph_c)
        if feats is None: return
        X = np.array(feats).reshape(1, -1); y = np.array([target])
        if not self.trained:
            self.mnb.partial_fit(X, y, classes=self.CLASSES)
            self.sgd.partial_fit(X, y, classes=self.CLASSES)
            self.trained = True
        else:
            self.mnb.partial_fit(X, y); self.sgd.partial_fit(X, y)
        self.sample_count += 1

    def predict(self, hist_d, hist_c, pf_d, ph_d, pf_c, ph_c) -> Optional[dict]:
        if not self.trained: return None
        feats = self._extract_features(hist_d, hist_c, pf_d, ph_d, pf_c, ph_c)
        if feats is None: return None
        X = np.array(feats).reshape(1, -1)
        try:
            nb_p = self.mnb.predict_proba(X)[0]
            sg_p = self.sgd.predict_proba(X)[0]
            final = 0.5 * nb_p + 0.5 * sg_p
            return {c + 1: float(p) for c, p in enumerate(final)}
        except: return None

# ─── GESTOR DOCENAS ───────────────────────────────────────────────────────────
class GestorDocenas:
    def __init__(self):
        self.nivel = 1
        self.oportunidad = 1
        self.max_nivel = MAX_NIVEL
        self.b0 = 0.0
        self.debt_stack: list[float] = []

    def iniciar_senal(self, balance_actual: float):
        self.b0 = balance_actual
        self.oportunidad = 1

    def get_target(self) -> float:
        if self.debt_stack:
            return self.debt_stack[-1] + BASE_BET
        return self.b0 + BASE_BET

    def apostar_por_docena(self, balance_actual: float) -> float:
        if self.oportunidad == 1:
            return self.nivel * BASE_BET
        else:
            return 3 * self.nivel * BASE_BET

    def registrar_perdida_senal(self):
        self.debt_stack.append(self.b0)
        if self.nivel < self.max_nivel:
            self.nivel += 1
        else:
            self.nivel = 1
        logger.info(f"[Speed2DC] 📋 Deuda registrada: B0={self.b0:.2f} | Pila: {len(self.debt_stack)} | Nivel→{self.nivel}")

    def verificar_recuperacion(self, balance_actual: float):
        while self.debt_stack:
            target = self.debt_stack[-1] + BASE_BET
            if balance_actual >= target:
                self.debt_stack.pop()
            else:
                break
        if not self.debt_stack:
            self.nivel = 1

# ─── STATS ────────────────────────────────────────────────────────────────────
class DetailedStats:
    def __init__(self):
        self.wins = 0; self.zeros = 0; self.losses = 0
        self.consecutive = 0
        self.last_20 = deque(maxlen=20)
        self.signals_processed = 0
        self.last_report_signals = 0

    def record(self, result_type, attempt, number, val, type_str, bankroll):
        self.signals_processed += 1
        if result_type == 'WIN':
            self.wins += 1; self.consecutive += 1
        elif result_type == 'LOSS':
            self.losses += 1; self.consecutive = 0
        elif result_type == 'EMPATE':
            self.zeros += 1
        self.last_20.append({"result": result_type, "attempt": attempt,
                              "number": number, "val": val, "type": type_str, "balance": bankroll})

    def should_send(self) -> bool:
        return (self.signals_processed - self.last_report_signals) >= 20

    def mark_sent(self):
        self.last_report_signals = self.signals_processed

    def get_stats_text(self, bankroll: float) -> str:
        total = self.wins + self.zeros + self.losses
        eff = ((self.wins + self.zeros) / total * 100) if total > 0 else 0.0
        text  = "📊 RESUMEN DE SEÑALES 📊\n"
        text += f"► PLACAR = ✅{self.wins} | 🟠{self.zeros} | 🚫{self.losses}\n"
        text += f"► Consecutivas = {self.consecutive}\n"
        text += f"► Assertividade = {eff:.2f}%\n"
        text += f"► Balance actual: 💰 ${bankroll:.2f} USD\n"
        text += f"► Total señales procesadas: {total}\n\n"
        text += "📌 Últimas 20 SEÑALES 📌\n"
        for s in reversed(list(self.last_20)):
            a_str = f"🔄 GALE #{s['attempt']}"
            b_str = f"💰 ${s['balance']:.2f} USD"
            val_str = f"D{s['val']}" if s['type'] == 'DOCENA' else f"C{s['val']}"
            if s['result'] == 'WIN':
                text += f"✅ WIN #{s['number']} {s['type']} {val_str} | {a_str} | {b_str}\n"
            elif s['result'] == 'EMPATE':
                text += f"🟠 EMPATE ZERO | {a_str} | {b_str}\n"
            else:
                text += f"🚫 LOSS #{s['number']} {s['type']} {val_str} | {a_str} | {b_str}\n"
        return text

# ─── ENGINE ───────────────────────────────────────────────────────────────────
class Speed2RouletteEngine:
    def __init__(self, stats_client: StatsClient):
        self.stats_client = stats_client

        self.spin_history: list = []
        self.dozen_seq: list = []; self.column_seq: list = []
        self.d_levels: dict = {1: [], 2: [], 3: []}
        self.c_levels: dict = {1: [], 2: [], 3: []}
        self.markov_d = SmoothedMarkovPredictor()
        self.markov_c = SmoothedMarkovPredictor()
        self.ensemble_d = OnlineEnsemblePredictor()
        self.ensemble_c = OnlineEnsemblePredictor()
        self.after_number_dozen: dict = defaultdict(lambda: defaultdict(int))
        self.after_number_column: dict = defaultdict(lambda: defaultdict(int))

        self.signal_active = False; self.active_type = None
        self.active_pair: tuple = (); self.active_missing = ""
        self.gestor = GestorDocenas()
        self.total_signal_loss = 0.0
        self.bankroll: float = 0.0
        self.stats = DetailedStats()
        self._db = _get_db()
        self.spins_since_train = 0
        self.active_signal_msg_id = None

        self.gale1_changed = False
        self.gale1_change_desc = ""
        self.original_pair = ()

        live_loaded  = self._load_live_history()
        total = live_loaded
        self.ws_count = total
        self.warmup_done = total >= WARMUP_SPINS
        logger.info(f"[Speed2DC] 📦 Pre-cargados: {total} (Live) | Warmup: {'✅' if self.warmup_done else '⏳'}")

    def _load_live_history(self) -> int:
        try: rows = self._db.execute("SELECT number FROM live_spins ORDER BY id ASC").fetchall()
        except: return 0
        for (n,) in rows: self._update_state(n, persist=False, train_model=False)
        if rows: self._train_models()
        return len(rows)

    def _persist(self, number: int):
        try:
            self._db.execute("INSERT INTO live_spins(number,ts) VALUES(?,?)", (number, int(time.time())))
            self._db.commit()
        except: pass

    def _train_models(self):
        self.markov_d.update(self.dozen_seq)
        self.markov_c.update(self.column_seq)

    def _update_state(self, number: int, persist=True, train_model=True):
        color = REAL_COLOR_MAP.get(number, "VERDE")
        d = get_dozen(number); c = get_column(number)
        if number != 0 and self.spin_history:
            prev = self.spin_history[-1]["number"]
            if prev != 0:
                self.after_number_dozen[prev][d] += 1
                self.after_number_column[prev][c] += 1
        self.spin_history.append({"number": number, "color": color})
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
            pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
            pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
            if pf_d and ph_d and pf_c and ph_c:
                self.ensemble_d.partial_train(self.dozen_seq, self.column_seq, d, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
                self.ensemble_c.partial_train(self.dozen_seq, self.column_seq, c, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            self.spins_since_train += 1
            if self.spins_since_train >= TRAIN_INTERVAL:
                self._train_models(); self.spins_since_train = 0
                logger.info("[Speed2DC] 🧠 Modelos re-entrenados (c/100 giros)")
        if persist: self._persist(number)

    def _get_pf(self, cat_type: str) -> Optional[Dict]:
        if len(self.spin_history) < 5: return None
        counts = {1: 0, 2: 0, 3: 0}
        for s in self.spin_history[-5:]:
            n = s["number"]
            if n != 0:
                val = get_dozen(n) if cat_type == "DOCENA" else get_column(n)
                counts[val] += 1
        active = [k for k, v in counts.items() if v > 0]
        if len(active) != 2: return None
        missing = list({1, 2, 3} - set(active))[0]
        return {"pair": tuple(sorted(active)), "missing": missing, "prob": sum(counts[a] for a in active) / 5.0}

    def _get_ph(self, cat_type: str) -> Optional[Dict]:
        if not self.spin_history: return None
        last_num = self.spin_history[-1]["number"]
        if last_num == 0: return None

        server_ph = self.stats_client.get_ph_from_stats(last_num, cat_type)
        if server_ph:
            return server_ph

        counts = self.after_number_dozen.get(last_num, {}) if cat_type == "DOCENA" else self.after_number_column.get(last_num, {})
        total = sum(counts.values())
        if total < 10: return None
        sc = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        if len(sc) < 2: return None
        missing = list({1, 2, 3} - {sc[0][0], sc[1][0]})[0]
        return {"pair": tuple(sorted([sc[0][0], sc[1][0]])), "missing": missing, "prob": (sc[0][1] + sc[1][1]) / total}

    def _predict_pair_ml(self, cat_type: str, missing_num: int) -> float:
        mk   = self.markov_d if cat_type == "DOCENA" else self.markov_c
        hist = self.dozen_seq if cat_type == "DOCENA" else self.column_seq
        levels = (self.d_levels if cat_type == "DOCENA" else self.c_levels).get(missing_num, [])
        mk_pred = mk.predict(hist)
        m_p_miss = mk_pred.get(missing_num, 1/3) if mk_pred else 1/3
        pf_d, ph_d = self._get_pf("DOCENA"), self._get_ph("DOCENA")
        pf_c, ph_c = self._get_pf("COLUMNA"), self._get_ph("COLUMNA")
        ens_p_miss = 1/3
        if pf_d and ph_d and pf_c and ph_c:
            ens = self.ensemble_d.predict(hist, self.column_seq, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"]) \
                  if cat_type == "DOCENA" else \
                  self.ensemble_c.predict(self.dozen_seq, hist, pf_d["pair"], ph_d["pair"], pf_c["pair"], ph_c["pair"])
            if ens: ens_p_miss = ens.get(missing_num, 1/3)
        ml_miss = 0.4 * m_p_miss + 0.6 * ens_p_miss
        if len(levels) >= 20:
            if ema_signal(levels, "tendencia"): ml_miss *= 0.85
            elif ema_signal(levels, "moderado"): ml_miss *= 0.92
        return 1.0 - ml_miss

    def _detect_signal(self) -> Optional[dict]:
        pf_d = self._get_pf("DOCENA"); pf_c = self._get_pf("COLUMNA")
        if not pf_d and not pf_c: return None
        ph_d = self._get_ph("DOCENA"); ph_c = self._get_ph("COLUMNA")
        candidates = []
        if pf_d and ph_d and set(pf_d["pair"]) == set(ph_d["pair"]):
            base = PF_W_NORM * pf_d["prob"] + PH_W_NORM * ph_d["prob"]
            ml   = self._predict_pair_ml("DOCENA", pf_d["missing"])
            prob = BASE_W_NORM * base + ML_W_NORM * ml
            logger.info(f"[Speed2DC] D base:{base:.0%} ml:{ml:.0%} final:{prob:.0%}")
            if prob >= MIN_PROB:
                candidates.append({"type":"DOCENA","pair":tuple(f"D{x}" for x in sorted(pf_d["pair"])),"missing":f"D{pf_d['missing']}","prob":prob})
        if pf_c and ph_c and set(pf_c["pair"]) == set(ph_c["pair"]):
            base = PF_W_NORM * pf_c["prob"] + PH_W_NORM * ph_c["prob"]
            ml   = self._predict_pair_ml("COLUMNA", pf_c["missing"])
            prob = BASE_W_NORM * base + ML_W_NORM * ml
            logger.info(f"[Speed2DC] C base:{base:.0%} ml:{ml:.0%} final:{prob:.0%}")
            if prob >= MIN_PROB:
                candidates.append({"type":"COLUMNA","pair":tuple(f"C{x}" for x in sorted(pf_c["pair"])),"missing":f"C{pf_c['missing']}","prob":prob})
        return max(candidates, key=lambda x: x["prob"]) if candidates else None

    # ═══ RE-ANÁLISIS GALE #1 ════════════════════════════════════════════════
    def _reanalyze_for_gale1(self, original_type: str, original_pair: tuple) -> dict:
        pf = self._get_pf(original_type)
        ph = self._get_ph(original_type)
        prefix = "D" if original_type == "DOCENA" else "C"
        cat_str = "docenas" if original_type == "DOCENA" else "columnas"

        if not pf or not ph:
            reason = "PF no disponible" if not pf else "PH no disponible"
            logger.info(f"[Speed2DC] 🔄 Re-análisis: {reason} → mantener original")
            return {"pair": original_pair, "missing": "", "prob": 0.0, "changed": False, "change_desc": f"{reason}, se mantiene señal original"}

        if set(pf["pair"]) != set(ph["pair"]):
            logger.info(f"[Speed2DC] 🔄 Re-análisis: PF y PH no coinciden → mantener original")
            return {"pair": original_pair, "missing": "", "prob": 0.0, "changed": False, "change_desc": "PF y PH no coinciden, se mantiene señal original"}

        base = PF_W_GALE1 * pf["prob"] + PH_W_GALE1 * ph["prob"]
        ml   = self._predict_pair_ml(original_type, pf["missing"])
        prob = BASE_W_GALE1 * base + ML_W_GALE1 * ml

        new_pair = tuple(f"{prefix}{x}" for x in sorted(pf["pair"]))
        same_as_original = set(new_pair) == set(original_pair)

        logger.info(f"[Speed2DC] 🔍 Re-análisis {original_type} | PF={pf['prob']:.0%} PH={ph['prob']:.0%} base={base:.0%} ml={ml:.0%} final={prob:.0%}")

        if prob < MIN_PROB_GALE1:
            return {"pair": original_pair, "missing": "", "prob": prob, "changed": False, "change_desc": f"Prob {prob:.0%} insuficiente, se mantiene señal original"}

        if same_as_original:
            return {"pair": new_pair, "missing": f"{prefix}{pf['missing']}", "prob": prob, "changed": False, "change_desc": f"Señal confirmada por re-análisis (prob {prob:.0%})"}

        orig_nums = sorted([p[1:] for p in original_pair])
        new_nums  = sorted([p[1:] for p in new_pair])
        desc = f"Cambio en {cat_str}: {prefix}{orig_nums[0]} y {prefix}{orig_nums[1]} → {prefix}{new_nums[0]} y {prefix}{new_nums[1]} (prob {prob:.0%})"
        return {"pair": new_pair, "missing": f"{prefix}{pf['missing']}", "prob": prob, "changed": True, "change_desc": desc}

    # ═══ FORMATO DE MONTO POR PAÍS ═════════════════════════════════════════
    def _format_bets(self, bet_usd: float, type_str: str) -> str:
        """Formatear montos de apuesta por país. Cero = 10% de la apuesta."""
        zero_usd = bet_usd * 0.10
        lines = []
        for curr in ["USD", "MXN", "PEN", "COP", "ARS", "CLP"]:
            flag = CURRENCY_FLAGS[curr]
            sym = CURRENCY_SYMBOLS[curr]
            mult = CURRENCY_MULTIPLIERS[curr]
            dec = CURRENCY_DECIMALS[curr]
            
            main_val = bet_usd * mult
            zero_val = zero_usd * mult
            
            lines.append(f"{flag} {curr}: {sym}{main_val:.{dec}f} x {type_str} + Cero: {sym}{zero_val:.{dec}f}")
        
        return "\n".join(lines)

    # ═══ SEÑALES Y RESOLUCIÓN ══════════════════════════════════════════════
    def _build_signal_text(self) -> str:
        bet_usd = self.gestor.apostar_por_docena(self.bankroll)
        nums = sorted([p[1:] for p in self.active_pair])
        prefix = "D" if self.active_type == "DOCENA" else "C"
        pair_disp = f"{prefix}{nums[0]} y {prefix}{nums[1]}"
        
        if self.active_type == "DOCENA":
            cat_label = "Docenas"
            bet_label = "Docena"
        else:
            cat_label = "Columnas"
            bet_label = "Columna"

        # Header según oportunidad
        if self.gestor.oportunidad == 1:
            header = "✅✅ ENTRADA CONFIRMADA ✅✅"
        else:
            header = "🚨 SEGUNDA OPORTUNIDAD 🚨"

        # Detalle de cambio si es Gale #1
        change_info = ""
        if self.gestor.oportunidad == 2:
            if self.gale1_changed:
                orig_nums = sorted([p[1:] for p in self.original_pair])
                orig_prefix = "D" if self.active_type == "DOCENA" else "C"
                orig_disp = f"{orig_prefix}{orig_nums[0]} y {orig_prefix}{orig_nums[1]}"
                orig_cat = "Docenas" if self.active_type == "DOCENA" else "Columnas"
                change_info = (f"\n⚡ Antes: {orig_cat} {orig_disp}"
                               f"\n📍 Ahora: {cat_label} {pair_disp}"
                               f"\n📝 {self.gale1_change_desc}\n")
            else:
                change_info = f"\n📝 {self.gale1_change_desc}\n"

        # Formatear montos
        bets_text = self._format_bets(bet_usd, bet_label)

        return (f"{header}\n\n"
                f"🕹️ SPEED ROULETTE 2\n"
                f"🎯 {cat_label}: {pair_disp}\n"
                f"⚔️ Cubrir el CERO 🟢\n\n"
                f"🚨 MONTO DE APUESTA POR PAIS:\n"
                f"{bets_text}"
                f"{change_info}")

    def _send_signal(self):
        msg_id = tg_send(self._build_signal_text())
        if msg_id:
            self.active_signal_msg_id = msg_id

    def _resolve(self, number: int, color: str):
        d, c = get_dozen(number), get_column(number)
        type_str = self.active_type
        val_num  = d if type_str == "DOCENA" else c
        gale_num = self.gestor.oportunidad - 1
        val_prefix = "D" if type_str == "DOCENA" else "C"
        val_display = f"{val_prefix}{val_num}"

        bet_usd = self.gestor.apostar_por_docena(self.bankroll)

        if number == 0:
            tg_send(f"🟠 EMPATE {number} — ZERO — 🔄 GALE #{gale_num}\n"
                    f"💰 Balance actual: ${self.bankroll:.2f} USD")
            self.stats.record('EMPATE', gale_num, 0, 0, type_str, self.bankroll)
            self._check_stats(); self._reset_signal()
            return

        won = (type_str == "DOCENA" and d != 0 and f"D{d}" in self.active_pair) or \
              (type_str == "COLUMNA" and c != 0 and f"C{c}" in self.active_pair)

        if won:
            profit = bet_usd
            self.bankroll = round(self.bankroll + profit, 2)
            self.gestor.verificar_recuperacion(self.bankroll)

            extra = ""
            if gale_num == 1 and self.gale1_changed:
                orig_nums = sorted([p[1:] for p in self.original_pair])
                orig_prefix = "D" if type_str == "DOCENA" else "C"
                extra = f" (par actualizado de {orig_prefix}{orig_nums[0]} y {orig_prefix}{orig_nums[1]})"

            tg_send(f"✅ WIN {number} — {type_str} {val_display} — 🔄 GALE #{gale_num}{extra}\n"
                    f"🎉 Felicidades has ganado ${profit:.2f} USD 🎉\n"
                    f"💰 Balance actual: ${self.bankroll:.2f} USD")

            self.stats.record('WIN', gale_num, number, val_num, type_str, self.bankroll)
            self._check_stats()
            self._reset_signal()
        else:
            loss = bet_usd * 2
            self.bankroll = round(self.bankroll - loss, 2)
            self.total_signal_loss = round(self.total_signal_loss + loss, 2)

            if gale_num == 0:
                if self.active_signal_msg_id:
                    tg_delete(CHAT_ID, self.active_signal_msg_id)
                    self.active_signal_msg_id = None

                self.original_pair = self.active_pair
                reanalysis = self._reanalyze_for_gale1(self.active_type, self.active_pair)

                self.active_pair = reanalysis["pair"]
                self.active_missing = reanalysis.get("missing", "")
                self.gale1_changed = reanalysis["changed"]
                self.gale1_change_desc = reanalysis["change_desc"]

                cat_str = "docenas" if self.active_type == "DOCENA" else "columnas"
                nums = sorted([p[1:] for p in self.active_pair])
                prefix = "D" if self.active_type == "DOCENA" else "C"

                if self.gale1_changed:
                    orig_nums = sorted([p[1:] for p in self.original_pair])
                    orig_prefix = "D" if self.active_type == "DOCENA" else "C"
                    tg_send(f"🔄 <b>RE-ANÁLISIS GALE #1</b> — Par actualizado\n\n"
                            f"❌ Pérdida en Gale #0 con #{number}\n"
                            f"📍 Antes: {cat_str} {orig_prefix}{orig_nums[0]} y {orig_prefix}{orig_nums[1]}\n"
                            f"✅ Ahora: {cat_str} {prefix}{nums[0]} y {prefix}{nums[1]}\n"
                            f"📝 {self.gale1_change_desc}")
                else:
                    tg_send(f"🔄 <b>RE-ANÁLISIS GALE #1</b> — Mantenida señal original\n\n"
                            f"❌ Pérdida en Gale #0 con #{number}\n"
                            f"📌 {cat_str} {prefix}{nums[0]} y {prefix}{nums[1]}\n"
                            f"📝 {self.gale1_change_desc}")

                self.gestor.oportunidad = 2
                self._send_signal()
            else:
                tg_send(f"❌ LOSS {number} — {type_str} {val_display} — 🔄 GALE #1\n"
                        f"🚨 Señal perdida. Monto perdido: -${self.total_signal_loss:.2f} USD 🚨\n"
                        f"💰 Balance actual: ${self.bankroll:.2f} USD")

                self.stats.record('LOSS', 1, number, val_num, type_str, self.bankroll)
                self.gestor.registrar_perdida_senal()
                self._check_stats()
                self._reset_signal()

    def _reset_signal(self):
        self.signal_active = False
        self.active_pair = ()
        self.active_type = None
        self.total_signal_loss = 0.0
        self.active_signal_msg_id = None
        self.gale1_changed = False
        self.gale1_change_desc = ""
        self.original_pair = ()

    def _check_stats(self):
        if not self.stats.should_send(): return
        tg_send(self.stats.get_stats_text(self.bankroll))
        self.stats.mark_sent()

    def process_number(self, number: int):
        try: self._process_inner(number)
        except Exception as e: logger.error(f"Error: {e}", exc_info=True); self._reset_signal()

    def _process_inner(self, number: int):
        color = REAL_COLOR_MAP.get(number, "VERDE")
        d = get_dozen(number); c = get_column(number)
        logger.info(f"[Speed2DC] 🎰 #{len(self.spin_history)+1}: {number} {color} D{d} C{c}")
        self._update_state(number)
        if not self.warmup_done:
            self.ws_count += 1
            if self.ws_count < WARMUP_SPINS: return
            self.warmup_done = True
            tg_send("🟢 <b>Speed Roulette 2 DC</b> — Sistema PF+PH+ML Listo.")
            logger.info("[Speed2DC] ✅ WARMUP COMPLETADO")
        if self.signal_active:
            self._resolve(number, color)
        else:
            sig = self._detect_signal()
            if sig:
                self.signal_active = True; self.active_type = sig["type"]
                self.active_pair = sig["pair"]; self.active_missing = sig["missing"]
                self.gestor.iniciar_senal(self.bankroll)
                self.total_signal_loss = 0.0
                self.gale1_changed = False
                self.gale1_change_desc = ""
                self.original_pair = sig["pair"]
                self._send_signal()
                logger.info(f"[Speed2DC] 🎯 SEÑAL {sig['type']}: {sig['pair']} ({sig['prob']:.0%})")

    # ═══ CONEXIÓN AL STATS SERVER ══════════════════════════════════════════
    async def run_ws(self):
        reconnect_delay = 5
        while True:
            try:
                async with websockets.connect(STATS_WS_URL, ping_interval=30, ping_timeout=60) as ws:
                    await ws.send(json.dumps({"type": "subscribe", "roulette": TARGET_ROULETTE}))
                    logger.info(f"[Speed2DC] ✅ Conectado al Stats Server — Suscrito a {TARGET_ROULETTE}")
                    reconnect_delay = 5
                    self.stats_client.connected = True
                    
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                            msg_type = data.get("type")
                            
                            if msg_type == "welcome":
                                logger.info(f"[Speed2DC] Servidor disponible: {list(data.get('available_roulettes', {}).keys())}")
                            
                            elif msg_type == "full_state":
                                self.stats_client.update_from_server(data["data"])
                                logger.info(f"[Speed2DC] 📊 Estado completo: {data['data'].get('total_spins', 0)} giros históricos")
                            
                            elif msg_type == "new_spin":
                                spin_data = data["data"]
                                n = spin_data.get("number")
                                self.stats_client.update_from_server(spin_data)
                                if n is not None and 0 <= n <= 36:
                                    self.process_number(n)
                                    
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                self.stats_client.connected = False
                logger.warning(f"[Speed2DC] Stats Server desconectado: {e}. Recon en {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)


# ─── FLASK ────────────────────────────────────────────────────────────────────
app = Flask(__name__)
engine: Optional[Speed2RouletteEngine] = None

@app.route("/")
def home(): return jsonify({"status": "ok", "bot": "Speed 2 DC", "stats_server": STATS_WS_URL})
@app.route("/ping")
def ping(): return jsonify({"status": "pong", "ts": time.time()})
@app.route("/health")
def health(): return jsonify({
    "warmup": engine.warmup_done if engine else False,
    "spins": len(engine.spin_history) if engine else 0,
    "balance": f"${engine.bankroll:.2f} USD" if engine else 0,
    "stats_connected": engine.stats_client.connected if engine else False
})

async def self_ping_loop():
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url: return
    await asyncio.sleep(30)
    while True:
        try: urllib.request.urlopen(f"{url}/ping", timeout=15)
        except: pass
        await asyncio.sleep(240)

# ─── TELEGRAM COMMANDS ────────────────────────────────────────────────────────
@bot.message_handler(commands=['start', 'help'])
def cmd_start(m): bot.reply_to(m, "<b>🎰 Speed Roulette 2 DC</b>\n\nApuesta: 0.50 USD base\nCapital: 0\nServidor: Stats Server\n\n/status /stats /reset", parse_mode="HTML")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if not engine: return
    st = f"🟢 {engine.active_pair}" if engine.signal_active else "⚪ Idle"
    conn = "🟢 Conectado" if engine.stats_client.connected else "🔴 Desconectado"
    extra = ""
    if engine.signal_active and engine.gestor.oportunidad == 2:
        extra = f" (Gale #1{' ✏️' if engine.gale1_changed else ' 📌'})"
    bot.reply_to(m, f"<b>Estado:</b> {st}{extra}\n<b>Giros:</b> {len(engine.spin_history)}\n<b>Balance:</b> ${engine.bankroll:.2f} USD\n<b>Nivel:</b> {engine.gestor.nivel}\n<b>Stats Server:</b> {conn}", parse_mode="HTML")

@bot.message_handler(commands=['stats'])
def cmd_stats(m):
    if not engine: return
    bot.reply_to(m, engine.stats.get_stats_text(engine.bankroll), parse_mode="HTML")

@bot.message_handler(commands=['reset'])
def cmd_reset(m):
    if engine: engine.stats = DetailedStats(); engine.bankroll = 0.0; engine.gestor.nivel = 1; engine.gestor.debt_stack = []
    bot.reply_to(m, "🔄 <b>Resetado — Balance: $0.00 USD</b>", parse_mode="HTML")

def run_flask():
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10003)), debug=False, use_reloader=False)

# ─── MAIN ─────────────────────────────────────────────────────────────────────
async def main():
    global engine
    stats_client = StatsClient()
    engine = Speed2RouletteEngine(stats_client)
    
    threading.Thread(target=lambda: bot.polling(none_stop=True, interval=1, timeout=30), daemon=True).start()
    logger.info("[Speed2DC] 🎰 Bot Speed 2 iniciado — Conectado a Stats Server")
    
    await asyncio.gather(asyncio.create_task(engine.run_ws()), asyncio.create_task(self_ping_loop()))

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Bot detenido.")
