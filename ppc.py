"""
╔══════════════════════════════════════════════════════════════╗
║   BOT BACCARAT — PRAGMATIC PLAY (Baccarat 1, 2, 3, 5, 6, 9)  ║
║   - Patrón 🔵🔴🔵🔴🔵 + confirmación 🔴🔴 → Banker           ║
║          🔴🔵🔴🔵🔴 + confirmación 🔵🔵 → Player           ║
║     buscado en los 4 gráficos (círculo, círculo vacío,       ║
║     círculo lleno y línea /)                                 ║
║   - Si falla la confirmación 1 de 2 → pasa a 2 de 2 al revés ║
║   - Cada aparición se guarda con sus 5 rondas y las cartas   ║
║     (biblioteca compartida) y alimenta las predicciones      ║
║   - ML + Labouchere · Telegram: señales / estadísticas       ║
║   - WebSocket Pragmatic, HTTP /health, self-ping, /status    ║
╚══════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import math
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import websockets
from aiohttp import web, ClientSession, ClientTimeout

try:
    from telebot.async_telebot import AsyncTeleBot
    TELEBOT_OK = True
except ImportError:
    AsyncTeleBot = None
    TELEBOT_OK = False

# ──────────────────────────────────────────────
#  CONFIGURACIÓN
# ──────────────────────────────────────────────
WS_URL        = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID     = "ppcdk00000005349"
CURRENCY_ID   = "BRL"
PING_INTERVAL = 240
SAVE_INTERVAL = 30

# Mesas "Baccarat N" del HTML (key de Pragmatic -> nombre mostrado)
BACCARAT_TABLES = {
    401: "Baccarat 1",
    404: "Baccarat 2",
    422: "Baccarat 3",
    411: "Baccarat 5",
    413: "Baccarat 6",
    436: "Baccarat 9",
}
PROVIDER_NAME = "Pragmatic Play"

# ── Telegram ──
# El token se lee SOLO de la variable de entorno BOT_TOKEN (no lo dejes escrito en el código).
BOT_TOKEN       = os.environ.get("BOT_TOKEN", "9024770820:AAF6c-tZ2vsFA1OyX1IKIC4TX3QFfxPTUMw")
CHANNEL_ID      = os.environ.get("CHANNEL_ID", "-1003926001959")
CHAT_ID         = int(CHANNEL_ID) if CHANNEL_ID.strip() else 0
# Compatibilidad interna: estos valores ya NO representan temas de Telegram.
THREAD_SIGNALS  = 0
THREAD_STATS    = 0
TG_SEND_RETRIES = 8      # reintentos por mensaje hasta que Telegram confirme
TG_MIN_GAP      = 0.5    # segundos entre mensajes (evita límites de Telegram)

# ── Señales / gestión ──
MAX_ENTRIES = 3          # compatibilidad con estadísticas antiguas
HK_SHOE_TARGET = int(os.environ.get("HK_SHOE_TARGET", "10"))  # unidades netas objetivo por zapato
LAB_UNIT    = int(os.environ.get("LAB_UNIT", "100"))      # valor de 1 unidad de Labouchere
LAB_START   = [1, 1, 1]                                   # secuencia inicial de Labouchere
TIE_AMOUNT  = int(os.environ.get("TIE_AMOUNT", "100"))    # apuesta opcional a Tie
STATS_FILE  = os.environ.get("STATS_FILE", "baccarat_stats.json")
LAB_FILE    = os.environ.get("LAB_FILE", "baccarat_labouchere.json")
HANDS_FILE  = os.environ.get("HANDS_FILE", "baccarat_hands.jsonl")   # manos aprendidas (sobreviven a reinicios)
SHOE_MAX_HANDS = int(os.environ.get("SHOE_MAX_HANDS", "110"))   # 8 mazos ≈ 84-104 manos: más = faltó el aviso de barajado
PATTERNS_FILE = os.environ.get("PATTERNS_FILE", "baccarat_patterns.jsonl")   # biblioteca de apariciones del patrón (5 rondas + cartas)
HANDS_KEEP  = int(os.environ.get("HANDS_KEEP", "5000"))             # manos que se recuerdan por mesa
SIGNALS_FILE = os.environ.get("SIGNALS_FILE", "baccarat_signals.jsonl")   # todas las señales (análisis futuro)

# ── Modelos estadísticos (ML) ──
ML_MIN_ROWS   = int(os.environ.get("ML_MIN_ROWS", "30"))      # intentos resueltos mínimos para confiar en el ML
ML_MARGIN     = float(os.environ.get("ML_MARGIN", "0.02"))    # ventaja mínima sobre 50% para decidir
ML_REPORT_EVERY = int(os.environ.get("ML_REPORT_EVERY", "5"))  # reporte Telegram cada N señales de precalentamiento
SIM_MIN_CASES = int(os.environ.get("SIM_MIN_CASES", "5"))     # casos mínimos para "casos parecidos"

# ── Confiabilidad: el bot solo ENVÍA señales a Telegram cuando se cumplen las dos condiciones.
#    Mientras tanto trabaja en modo entrenamiento (las señales se simulan y se registran, sin enviar nada).
MIN_SIGNALS_TRAIN   = int(os.environ.get("MIN_SIGNALS_TRAIN", "100"))      # señales resueltas mínimas (reales + simuladas)
RELIABLE_WINDOW     = int(os.environ.get("RELIABLE_WINDOW", "200"))        # últimas N predicciones del modelo
RELIABLE_MIN_PRED   = int(os.environ.get("RELIABLE_MIN_PRED", "100"))      # predicciones mínimas evaluadas
RELIABLE_MIN_ACC    = float(os.environ.get("RELIABLE_MIN_ACC", "0.56"))    # acierto mínimo de mantener/cambiar (azar = 0.50)

# ──────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════
#  PARSER BACCARAT (idéntico a normalizeBaccarat del HTML)
# ══════════════════════════════════════════════
def pick(o, keys):
    if not isinstance(o, dict):
        return None
    for k in keys:
        if o.get(k) is not None:
            return o[k]
    return None

def to_num(v) -> Optional[int]:
    if v is None or isinstance(v, bool):
        return None
    m = re.match(r"\s*[-+]?\d+", str(v))
    return int(m.group(0)) if m else None

def norm_winner(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().upper()
    if s in ("P", "PLAYER", "PLAYERWIN", "PLAYER_WIN", "1"):
        return "P"
    if s in ("B", "BANKER", "BANKERWIN", "BANKER_WIN", "2"):
        return "B"
    if s in ("T", "TIE", "TIEWIN", "TIE_WIN", "0", "3"):
        return "T"
    if s and s[0] in ("P", "B", "T"):
        return s[0]
    return None

def _rank_value(tok: str) -> Optional[int]:
    if tok in ("A", "ACE"):
        return 1
    if tok in ("J", "Q", "K", "T", "10", "JACK", "QUEEN", "KING", "TEN"):
        return 0
    if tok.isdigit() and len(tok) == 1 and tok != "0":
        return int(tok)
    return None

def _norm_card_value(v) -> Optional[int]:
    """Convierte una carta a su valor Baccarat (A=1, 2-9, 10/J/Q/K=0).
    Acepta números (1-13), rangos ("K", "10"), cartas con palo ("KH", "10S", "A♠", "H5") y nombres ("KING OF HEARTS")."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, dict):
        v = pick(v, ["value", "rank", "cardValue", "number", "card", "name"])
        if v is None or isinstance(v, bool):
            return None
    if isinstance(v, (int, float)):
        n = int(v)
        return n if 1 <= n <= 9 else (0 if 10 <= n <= 13 else None)
    s = str(v).strip().upper()
    for ch in "♠♥♦♣":
        s = s.replace(ch, "")
    for word, val in (("ACE", 1), ("KING", 0), ("QUEEN", 0), ("JACK", 0), ("TEN", 0)):
        if s.startswith(word):
            return val
    s = re.sub(r"[^0-9A-Z]", "", s)
    if not s:
        return None
    if s.isdigit():
        n = int(s)
        return n if 1 <= n <= 9 else (0 if 10 <= n <= 13 else None)
    m = re.match(r"^(10|[2-9]|A|J|Q|K|T)", s) or re.match(r"^[SHDC](10|[2-9]|A|J|Q|K|T)", s)
    return _rank_value(m.group(1)) if m else None

_SUIT_SYMBOLS = {"S": "♠", "H": "♥", "D": "♦", "C": "♣"}
_SUIT_WORDS = {"SPADE": "♠", "HEART": "♥", "DIAMOND": "♦", "CLUB": "♣"}
_RANK_TEXT = {1: "A", 11: "J", 12: "Q", 13: "K"}

def _card_label(v) -> Optional[str]:
    """Carta legible con palo: 'KH' -> 'K♥', '10S' -> '10♠', 'A♠' -> 'A♠', 5 -> '5' (sin palo si no viene)."""
    if v is None or isinstance(v, bool):
        return None
    suit = ""
    if isinstance(v, dict):
        st = pick(v, ["suit", "suitName", "palo"])
        rk = pick(v, ["rank", "value", "cardValue", "number", "card", "name"])
        if st is not None:
            u = str(st).strip().upper()
            suit = _SUIT_SYMBOLS.get(u[:1], "") if len(u) == 1 else next((sy for w, sy in _SUIT_WORDS.items() if u.startswith(w)), "")
            if u in "♠♥♦♣" and u:
                suit = u
        v = rk
        if v is None or isinstance(v, bool):
            return None
    if isinstance(v, (int, float)):
        n = int(v)
        return (_RANK_TEXT.get(n) or str(n)) + suit if 1 <= n <= 13 else None
    s = str(v).strip().upper()
    for ch in "♠♥♦♣":
        if ch in s:
            suit = ch
            s = s.replace(ch, "")
    for w, sy in _SUIT_WORDS.items():
        if w in s:
            suit = suit or sy
    for word, txt in (("ACE", "A"), ("KING", "K"), ("QUEEN", "Q"), ("JACK", "J"), ("TEN", "10")):
        if s.startswith(word):
            return txt + suit
    s2 = re.sub(r"[^0-9A-Z]", "", s)
    if not s2:
        return None
    if s2.isdigit():
        n = int(s2)
        return (_RANK_TEXT.get(n) or str(n)) + suit if 1 <= n <= 13 else None
    m = re.match(r"^(10|[2-9]|A|J|Q|K|T)([SHDC])?$", s2) or re.match(r"^([SHDC])(10|[2-9]|A|J|Q|K|T)$", s2)
    if not m:
        return None
    g = m.groups()
    if g[0] in ("S", "H", "D", "C") and len(g) == 2 and g[1]:
        rank, st = g[1], g[0]
    else:
        rank, st = g[0], g[1] if len(g) > 1 else None
    if rank == "T":
        rank = "10"
    return rank + (suit or _SUIT_SYMBOLS.get(st or "", ""))

_CARD_KEYS_P = ("playerCards", "pCards", "player_cards", "cardsPlayer", "cardsP", "playerHand", "pHand")
_CARD_KEYS_B = ("bankerCards", "bCards", "banker_cards", "cardsBanker", "cardsB", "bankerHand", "bHand")

def _extract_card_list(raw, keys):
    """Busca listas de cartas en las claves habituales y también un nivel anidado."""
    if not isinstance(raw, dict):
        return []
    for k in keys:
        v = raw.get(k)
        if isinstance(v, list):
            return v[:3]
        if isinstance(v, dict):
            for kk in ("cards", "hand", "values", "results"):
                if isinstance(v.get(kk), list):
                    return v[kk][:3]
    # búsqueda recursiva limitada: evita recorrer historiales enormes
    for v in raw.values():
        if isinstance(v, dict):
            got = _extract_card_list(v, keys)
            if got:
                return got
    return []

def parse_baccarat_cards(raw) -> dict:
    """Extrae contexto de cartas sin asumir un único formato del WebSocket."""
    pc = _extract_card_list(raw, _CARD_KEYS_P)
    bc = _extract_card_list(raw, _CARD_KEYS_B)
    pv = [x for x in (_norm_card_value(c) for c in pc) if x is not None]
    bv = [x for x in (_norm_card_value(c) for c in bc) if x is not None]
    p = sum(pv) % 10 if (pv and len(pv) == len(pc)) else to_num(pick(raw, ["playerScore", "pScore", "playerPoints", "pPoints", "player", "pointsPlayer", "playerTotal"]))
    b = sum(bv) % 10 if (bv and len(bv) == len(bc)) else to_num(pick(raw, ["bankerScore", "bScore", "bankerPoints", "bPoints", "banker", "pointsBanker", "bankerTotal"]))
    return {
        "player_cards": pv, "banker_cards": bv,
        "player_labels": [x for x in (_card_label(c) for c in pc) if x is not None],
        "banker_labels": [x for x in (_card_label(c) for c in bc) if x is not None],
        "player_ncards": len(pv), "banker_ncards": len(bv),
        "player_total": p, "banker_total": b,
        "player_third": pv[2] if len(pv) >= 3 else None,
        "banker_third": bv[2] if len(bv) >= 3 else None,
        "player_third_card": len(pv) >= 3,
        "banker_third_card": len(bv) >= 3,
        "total_diff": (abs(p - b) if p is not None and b is not None else None),
    }

def normalize_baccarat(raw) -> Optional[str]:
    """Devuelve 'P' | 'B' | 'T' o None si no se puede determinar el ganador."""
    cards = parse_baccarat_cards(raw)
    p, b = cards["player_total"], cards["banker_total"]
    r = norm_winner(pick(raw, ["result", "winner", "gameResult", "outcome", "win", "status"]))
    if not r and p is not None and b is not None:
        r = "P" if p > b else "B" if b > p else "T"
    return r

P_KEYS = ["playerScore", "pScore", "playerPoints", "pPoints", "player", "pointsPlayer", "playerTotal"]
B_KEYS = ["bankerScore", "bScore", "bankerPoints", "bPoints", "banker", "pointsBanker", "bankerTotal"]

def slim_raw(raw) -> dict:
    out = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            if isinstance(v, (str, int, float, bool)) or v is None:
                out[k] = v
            elif len(json.dumps(v, default=str)) < 400:
                out[k] = v
        except Exception:
            pass
    return out

def make_hand(raw, gid, r) -> dict:
    cards = parse_baccarat_cards(raw)
    return {"gid": gid, "r": r,
            "p": cards["player_total"], "b": cards["banker_total"],
            "cards": cards, "raw": slim_raw(raw)}


# ══════════════════════════════════════════════
#  NUEVA ESTRATEGIA — 4 ROADS + CARTAS + HISTORIAL
# ══════════════════════════════════════════════
# Big Road: P/B reales (Banker = rojo, Player = azul).
# Derived Roads (Big Eye, Small, Cockroach): solo hay ROJO o AZUL, no Banker/Player. Para buscar los patrones
# se usa el mismo alfabeto: rojo = "B" y azul = "P" (igual que en la Big Road). La señal de esos gráficos es
# un COLOR; para apostar se traduce a la mano (Banker/Player) que hace aparecer ese color en la próxima entrada.
ROAD_DEFS = {
    "big": {"name": "Big Road", "symbol": "círculo", "lookback": 0},
    "big_eye": {"name": "Big Eye Boy", "symbol": "círculo vacío", "lookback": 1},
    "small": {"name": "Small Road", "symbol": "círculo lleno", "lookback": 2},
    "cockroach": {"name": "Cockroach Pig", "symbol": "/", "lookback": 3},
}
# rojo = B, azul = P.
#   🔵🔴🔵🔴🔵 (PBPBP) + confirmación 🔴🔴 -> sigue Rojo (Banker)
#   🔴🔵🔴🔵🔴 (BPBPB) + confirmación 🔵🔵 -> sigue Azul (Player)
# Las 2 confirmaciones son las 2 marcas siguientes del mismo color que el patrón anticipa; después vienen las entradas.
PATTERN_TARGETS = {"PBPBP": "B", "BPBPB": "P"}
PATTERN_LEN = 5
INVERT_LOW_PROB = os.environ.get("INVERT_LOW_PROB", "0") == "1"   # (en desuso: ahora decide el modelo cuando es confiable)
ML_CTX_MIN = int(os.environ.get("ML_CTX_MIN", "10"))   # resultados previos de un contexto (gráfico+patrón+fase+intento) para que el modelo opine


def other_side(side):
    return "B" if side == "P" else "P"


def _derived_marks(sides, lookback):
    """Camino derivado estándar (lookback 1=Big Eye, 2=Small, 3=Cockroach). Símbolos: 'B'=rojo, 'P'=azul.

    Regla 1 (columna nueva): rojo si las columnas c-1 y c-1-lookback tienen el mismo largo.
    Regla 2 (continúa la columna): se mira la columna c-lookback en esa fila:
      con casilla → rojo · le falta exactamente una → azul · le faltan 2 o más → rojo.
    Las columnas son lógicas (sin tope de 6 filas): el desborde horizontal del Big Road no cambia la lógica.
    """
    cols = []
    out = []
    for i, side in enumerate(sides):
        if cols and cols[-1][0] == side:
            cols[-1].append(side)
        else:
            cols.append([side])
        c, row = len(cols) - 1, len(cols[-1]) - 1
        if c < lookback or (c == lookback and row == 0):
            continue
        if row == 0:
            red = len(cols[c - 1]) == len(cols[c - 1 - lookback])
        else:
            red = len(cols[c - lookback]) != row
        out.append({"symbol": "B" if red else "P", "source_index": i, "big_col": c, "big_row": row})
    return out


def _road_sequence(sides, road_key):
    if road_key == "big":
        return [{"symbol": x, "source_index": i} for i, x in enumerate(sides)]
    return _derived_marks(sides, ROAD_DEFS[road_key]["lookback"])


def _pattern_hits(seq, pattern):
    hits = []
    if len(seq) < len(pattern):
        return hits
    for i in range(len(pattern) - 1, len(seq)):
        got = "".join(x["symbol"] for x in seq[i - len(pattern) + 1:i + 1])
        if got == pattern:
            hits.append((i, seq[i]["source_index"]))
    return hits


def _entry_symbol(sides, road_key, hand):
    """Símbolo (lado o color) que aparecería en el gráfico si la próxima mano fuera `hand`. None = sin entrada."""
    if road_key == "big":
        return hand
    lb = ROAD_DEFS[road_key]["lookback"]
    before = _derived_marks(sides, lb)
    after = _derived_marks(sides + [hand], lb)
    return after[-1]["symbol"] if len(after) == len(before) + 1 else None


def hand_for_color(sides, road_key, color):
    """Mano (Banker/Player) que hace que el gráfico muestre `color` (en la Big Road es el propio lado).
    Solo si es única: si ambas manos o ninguna dan ese color no hay entrada definida → None."""
    ok = [h for h in ("B", "P") if _entry_symbol(sides, road_key, h) == color]
    return ok[0] if len(ok) == 1 else None


def banker_third_reason(player_total, player_third, banker_total):
    """Regla estándar de tercera carta del Banker, incluyendo la carta 3 del Player."""
    if banker_total is None:
        return "desconocido"
    if banker_total <= 2:
        return "Banker 0-2: roba siempre"
    if banker_total == 3:
        if player_third == 8:
            return "Banker 3: se planta si Player tercera=8"
        return "Banker 3: roba salvo Player tercera=8"
    if banker_total == 4:
        return "Banker 4: roba si Player tercera=2-7"
    if banker_total == 5:
        return "Banker 5: roba si Player tercera=4-7"
    if banker_total == 6:
        return "Banker 6: roba si Player tercera=6-7"
    return f"Banker {banker_total}: se planta"


def _hand_signature(hand):
    c = hand.get("cards") or {}
    ptotal, p3, btotal = c.get("player_total"), c.get("player_third"), c.get("banker_total")
    return {
        "p_cards": tuple(c.get("player_cards", [])),
        "b_cards": tuple(c.get("banker_cards", [])),
        "p_n": c.get("player_ncards", 0), "b_n": c.get("banker_ncards", 0),
        "p_total": c.get("player_total"), "b_total": c.get("banker_total"),
        "p3": c.get("player_third"), "b3": c.get("banker_third"),
        "diff": c.get("total_diff"),
        "banker_third_reason": banker_third_reason(ptotal, p3, btotal),
    }


def _card_similarity(a, b):
    """0..1; compara composición real de las dos manos, no solo el ganador."""
    if not a or not b:
        return 0.0
    score = 0.0; weight = 0.0
    for key, w in (("p_n",1.5),("b_n",1.5),("p_total",1.0),("b_total",1.0),("p3",1.2),("b3",1.2),("diff",0.8), ("banker_third_reason",0.7)):
        av, bv = a.get(key), b.get(key)
        if av is not None and bv is not None:
            score += w * (1.0 if av == bv else (0.5 if key == "diff" and abs(av-bv) <= 2 else 0.0))
            weight += w
    for key, w in (("p_cards",1.8),("b_cards",1.8)):
        av, bv = tuple(a.get(key, ())), tuple(b.get(key, ()))
        if av and bv:
            inter = len(set(av) & set(bv))
            score += w * (inter / max(len(set(av) | set(bv)), 1))
            weight += w
    return score / weight if weight else 0.0


def _similar_context_score(pattern_hands, hist_hands):
    if not pattern_hands or not hist_hands:
        return 0.0
    n = min(5, len(pattern_hands), len(hist_hands))
    vals = [_card_similarity(_hand_signature(pattern_hands[-j]), _hand_signature(hist_hands[-j])) for j in range(1, n+1)]
    return sum(vals) / len(vals) if vals else 0.0


# ══════════════════════════════════════════════
#  BIBLIOTECA DE PATRONES (🔵🔴🔵🔴🔵 y 🔴🔵🔴🔵🔴 en cualquiera de los 4 gráficos)
#  Cada aparición guarda sus 5 rondas con las cartas de Player y Banker y qué salió después.
#  Es compartida por las 6 mesas, se guarda en disco y alimenta las predicciones futuras.
# ══════════════════════════════════════════════
PATTERN_LIB: dict = {}   # (gráfico, patrón) -> {clave: registro}

def _lib_save(rec: dict):
    try:
        with open(PATTERNS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning(f"Error guardando patrón: {e}")

def lib_load() -> int:
    """Carga la biblioteca guardada (si una aparición tiene varias líneas, vale la última)."""
    n = 0
    if not os.path.exists(PATTERNS_FILE):
        return 0
    try:
        with open(PATTERNS_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                    last_gid = ((rec.get("rounds") or [{}])[-1] or {}).get("gid")
                    if last_gid is not None:      # registros viejos usaban el índice: se normaliza (colapsa duplicados)
                        rec["k"] = f"{rec.get('t')}:{rec.get('sid')}:{last_gid}"
                    PATTERN_LIB.setdefault((rec["road"], rec["pattern"]), {})[rec["k"]] = rec
                except Exception:
                    continue
        n = sum(len(v) for v in PATTERN_LIB.values())
    except Exception as e:
        log.warning(f"Error cargando biblioteca de patrones: {e}")
    return n

def lib_counts() -> tuple:
    total = sum(len(v) for v in PATTERN_LIB.values())
    done = sum(1 for v in PATTERN_LIB.values() for r in v.values() if r.get("outcome") is not None)
    return total, done

def _lib_round(h: dict) -> dict:
    c = h.get("cards") or {}
    return {"r": h.get("r"), "gid": h.get("gid"),
            "cards": {"player_cards": list(c.get("player_cards") or []), "banker_cards": list(c.get("banker_cards") or []),
                      "player_total": c.get("player_total"), "banker_total": c.get("banker_total"),
                      "player_labels": list(c.get("player_labels") or []), "banker_labels": list(c.get("banker_labels") or [])}}

def lib_scan_shoe(table_key, sid, shoe_hands) -> int:
    """Busca el patrón en los 4 gráficos del zapato; registra las apariciones nuevas (5 rondas + cartas) y
    completa el resultado de las que estaban pendientes. Devuelve cuántas apariciones nuevas guardó."""
    valid = [h for h in shoe_hands if h.get("r") in ("P", "B")]
    sides = [h["r"] for h in valid]
    if len(sides) < PATTERN_LEN:
        return 0
    new = 0
    for road_key in ROAD_DEFS:
        sq = _road_sequence(sides, road_key)
        for pattern, target in PATTERN_TARGETS.items():
            for pos, src in _pattern_hits(sq, pattern):
                if road_key == "big":
                    outcome = sides[src + 1] if src + 1 < len(sides) else None
                else:
                    outcome = sq[pos + 1]["symbol"] if pos + 1 < len(sq) else None
                bucket = PATTERN_LIB.setdefault((road_key, pattern), {})
                gid_src = valid[src].get("gid")      # clave estable: el id de la mano donde se completa el patrón
                k = f"{table_key}:{sid}:{gid_src if gid_src is not None else 'i' + str(src)}"
                rec = bucket.get(k)
                if rec is None:
                    idx = [sq[j]["source_index"] for j in range(pos - PATTERN_LEN + 1, pos + 1)]
                    rec = {"k": k, "t": table_key, "sid": sid, "road": road_key, "pattern": pattern,
                           "target": target, "outcome": outcome, "ts": int(time.time()),
                           "rounds": [_lib_round(valid[i]) for i in idx]}
                    bucket[k] = rec
                    _lib_save(rec)
                    new += 1
                elif rec.get("outcome") is None and outcome is not None:
                    rec["outcome"] = outcome
                    _lib_save(rec)
    return new

def lib_scan_hands(table_key, hands) -> int:
    groups: dict = {}
    for h in hands:
        groups.setdefault(h.get("shoe_id", 0), []).append(h)
    return sum(lib_scan_shoe(table_key, sid, gh) for sid, gh in groups.items())


CARD_MIN_CASES = int(os.environ.get("CARD_MIN_CASES", "1"))        # casos históricos parecidos necesarios
CARD_ROUNDS_REQUIRED = int(os.environ.get("CARD_ROUNDS_REQUIRED", "2"))  # rondas (de las 5 del patrón) que deben parecerse


def _round_cards(hand):
    """Cartas de una ronda: valores en orden (A=1, 2-9, 10/J/Q/K=0) de Player y de Banker (2 o 3 cartas cada uno)."""
    c = (hand or {}).get("cards") or {}
    return tuple(c.get("player_cards") or ()), tuple(c.get("banker_cards") or ())


def _round_match(cur_hand, hist_hand):
    """Compara UNA ronda del patrón actual con la misma ronda de un caso histórico.
    'exact' = mismas cartas (valor y posición, Player y Banker).
    'near'  = mínima variación: mismo número de cartas por lado y (una sola carta distinta, o mismos totales).
    None    = distinta (2 vs 3 cartas, otros totales) o sin datos de cartas."""
    cp, cb = _round_cards(cur_hand)
    hp, hb = _round_cards(hist_hand)
    if not (cp and cb and hp and hb):
        return None
    if len(cp) != len(hp) or len(cb) != len(hb):
        return None
    diff = sum(a != b for a, b in zip(cp, hp)) + sum(a != b for a, b in zip(cb, hb))
    if diff == 0:
        return "exact"
    if diff == 1:
        return "near"
    cc, hc = (cur_hand or {}).get("cards") or {}, (hist_hand or {}).get("cards") or {}
    if (cc.get("player_total") is not None and cc.get("player_total") == hc.get("player_total")
            and cc.get("banker_total") is not None and cc.get("banker_total") == hc.get("banker_total")):
        return "near"
    return None


def _pattern_card_match(cur_hands, hist_hands):
    """Compara las 5 rondas que formaron el patrón (ronda 1 con ronda 1, ... ronda 5 con ronda 5).
    Se necesitan al menos CARD_ROUNDS_REQUIRED rondas parecidas. 'exact' si todas las parecidas tienen las mismas
    cartas, 'near' si hay mínima variación en alguna, None si no alcanza."""
    if len(cur_hands) != len(hist_hands) or not cur_hands:
        return None
    kinds = [_round_match(a, b) for a, b in zip(cur_hands, hist_hands)]
    ok = [k for k in kinds if k]
    if len(ok) < min(CARD_ROUNDS_REQUIRED, len(cur_hands)):
        return None
    return "exact" if all(k == "exact" for k in ok) and len(ok) == len(cur_hands) else "near"


def analyze_road_candidates(hands, current_sides=None, library=None):
    """Analiza las 4 carreteras y devuelve candidatos para PBPBP/BPBPB.
    · Big Road: el resultado es el lado de la mano siguiente.
    · Gráficos 2-4: el resultado es el COLOR de la siguiente entrada del gráfico (no el ganador de la mano);
      la mano a apostar se traduce con hand_for_color y queda en `bet_side`.
    El historial se limita a manos anteriores al patrón actual para evitar leakage.
    """
    groups = {}
    for h in hands:
        sid = h.get("shoe_id", 0)
        groups.setdefault(sid, []).append(h)
    if not groups:
        return []
    current_sid = max(groups)
    current_hands = groups[current_sid]
    valid = [(i, h) for i, h in enumerate(current_hands) if h.get("r") in ("P", "B")]
    sides = [h["r"] for _, h in valid]
    if len(sides) < PATTERN_LEN:
        return []
    out = []
    for road_key, rd in ROAD_DEFS.items():
        seq = _road_sequence(sides, road_key)
        for pattern, target in PATTERN_TARGETS.items():
            hits = _pattern_hits(seq, pattern)
            if not hits:
                continue
            cur_pos, source_idx = hits[-1]
            if source_idx != len(sides) - 1:
                continue
            bet_side = hand_for_color(sides, road_key, target)
            if bet_side is None:
                continue                      # el gráfico no puede mostrar ese color con una sola mano
            # Historial: biblioteca compartida (todas las mesas) o, si no se da, los zapatos de esta mesa.
            prior = []
            hist_cases = []          # (5 rondas del caso histórico, resultado)
            if library is not None:
                for rec in library.get((road_key, pattern), {}).values():
                    if rec.get("outcome") is None:
                        continue         # aparición sin resolver (incluye la actual)
                    prior.append(rec["outcome"])
                    hist_cases.append((rec["rounds"], rec["outcome"]))
            else:
                for sid, gh in groups.items():
                    v = [(i, h) for i, h in enumerate(gh) if h.get("r") in ("P", "B")]
                    ss = [h["r"] for _, h in v]
                    if len(ss) < PATTERN_LEN:
                        continue
                    sq = _road_sequence(ss, road_key)
                    for pos, src in _pattern_hits(sq, pattern):
                        if sid == current_sid and src >= source_idx:
                            continue
                        if road_key == "big":
                            outcome = ss[src + 1] if src + 1 < len(ss) else None
                        else:
                            outcome = sq[pos + 1]["symbol"] if pos + 1 < len(sq) else None
                        if outcome is None:
                            continue
                        prior.append(outcome)
                        hist_cases.append(([v[sq[k]["source_index"]][1] for k in range(pos - PATTERN_LEN + 1, pos + 1)], outcome))
            n = len(prior)
            wins = sum(x == target for x in prior)
            losses = n - wins
            rate = (wins + 1.0) / (n + 2.0)  # Laplace: evita 100% con 1 caso
            # Rondas que FORMARON el patrón en este gráfico (una por casilla del patrón).
            cur_src = [seq[k]["source_index"] for k in range(cur_pos - PATTERN_LEN + 1, cur_pos + 1)]
            cur_hands = [valid[i][1] for i in cur_src]
            pattern_rounds = []
            for i, h in zip(cur_src, cur_hands):
                pc, bc = _round_cards(h)
                pattern_rounds.append({"r": h.get("r"), "player_cards": list(pc), "banker_cards": list(bc),
                                       "player_total": (h.get("cards") or {}).get("player_total"),
                                       "banker_total": (h.get("cards") or {}).get("banker_total")})
            exact, near = [], []                    # resultados de casos históricos con las mismas cartas / mínima variación
            for hist_hands, outcome in hist_cases:
                kind = _pattern_card_match(cur_hands, hist_hands)
                if kind == "exact":
                    exact.append(outcome)
                elif kind == "near":
                    near.append(outcome)
            similar = [(1.0, o) for o in exact] + [(0.5, o) for o in near]
            sn = len(similar)
            sw = sum(x[1] == target for x in similar)
            ex_n, ex_w = len(exact), sum(o == target for o in exact)
            sim_rate = (sw + 1.0) / (sn + 2.0) if sn else 0.5
            total_weight = min(n, 50) + min(sn, 30) * 1.5
            if total_weight:
                probability = (rate * min(n, 50) + sim_rate * min(sn, 30) * 1.5) / total_weight
            else:
                probability = 0.5
            card_match = (ex_n / sn) if sn else 0.0
            if similar:
                probability = probability * (0.85 + 0.15 * card_match)
            out.append({
                "road": road_key, "road_name": rd["name"], "symbol": rd["symbol"],
                "pattern": pattern, "target": target, "bet_side": bet_side,
                "occurrences": n, "wins": wins, "losses": losses,
                "historical_rate": rate, "similar_cases": sn, "similar_wins": sw,
                "similar_rate": sim_rate, "card_similarity": card_match,
                "exact_cases": ex_n, "exact_wins": ex_w, "near_cases": len(near), "pattern_rounds": pattern_rounds,
                "probability": probability, "current_source_index": source_idx,
                "derived_color_pattern": "".join(x["symbol"] for x in seq[cur_pos - PATTERN_LEN + 1:cur_pos + 1]),
                "line_focus": (rd["symbol"] == "/"),
            })
    return out


CARD_REQUIRE_BACKING = os.environ.get("CARD_REQUIRE_BACKING", "0") == "1"   # 1 = exigir siempre respaldo por cartas


def choose_best_candidate(candidates):
    """Basta UN gráfico donde el patrón se cumple ahora.
    · CON respaldo: en ese mismo gráfico el patrón ya salió antes con cartas iguales o de mínima variación
      (>= CARD_MIN_CASES casos) y en la mayoría el color esperado continuó (aciertos > fallos) -> señal.
    · Con evidencia EN CONTRA (hay casos parecidos y fallaron más de lo que acertaron) -> sin señal.
    · SIN evidencia (aún no hay casos parecidos en el historial, p. ej. al arrancar) -> señal, salvo que se
      active CARD_REQUIRE_BACKING=1 (entonces exige respaldo siempre).
    Si varios gráficos califican se usa uno solo: primero con respaldo, luego más casos exactos y mejor probabilidad."""
    backed = [c for c in candidates
              if c["similar_cases"] >= CARD_MIN_CASES and c["similar_wins"] > c["similar_cases"] - c["similar_wins"]]
    pool = backed
    if not pool and not CARD_REQUIRE_BACKING:
        pool = [c for c in candidates if c["similar_cases"] == 0]     # sin casos parecidos: ni a favor ni en contra
    if not pool:
        return None
    def score(c):
        confidence = min(1.0, math.log1p(max(c["similar_cases"], c["occurrences"])) / math.log1p(20))
        prob = 0.5 + (c["probability"] - 0.5) * (0.55 + 0.45 * confidence)
        return (c in backed, c["exact_cases"], prob)
    return max(pool, key=score)


def analysis_text(a):
    if not a:
        return "sin historial"
    return (f"{a['road_name']} {a['pattern']} → {('Player' if a.get('bet_side', a['target'])=='P' else 'Banker')} · "
            f"{a['probability']*100:.1f}% · n={a['occurrences']} · "
            f"mismas cartas={a.get('exact_cases', 0)} · mínima variación={a.get('near_cases', 0)}")


# Patrones aceptados: únicamente PBPBP (🔵🔴🔵🔴🔵 → confirma 🔴🔴) y BPBPB (🔴🔵🔴🔵🔴 → confirma 🔵🔵).
PATTERNS = [
    {"key": "PBPBP", "short": "PBPBP", "name": "PBPBP", "target": "B", "confirm": 2},
    {"key": "BPBPB", "short": "BPBPB", "name": "BPBPB", "target": "P", "confirm": 2},
]


# ══════════════════════════════════════════════
#  TELEGRAM (cola secuencial con confirmación)
# ══════════════════════════════════════════════
bot = AsyncTeleBot(BOT_TOKEN, parse_mode="HTML") if (TELEBOT_OK and BOT_TOKEN) else None
if bot is None:
    log.warning("Telegram deshabilitado (falta BOT_TOKEN o la librería 'telebot').")

_tg_queue: Optional[asyncio.Queue] = None

async def send_msg(text: str) -> Optional[int]:
    """Envía directamente al CANAL de Telegram. No usa grupo ni temas."""
    if bot is None:
        return None
    try:
        msg = await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            disable_web_page_preview=True
        )
        return msg.message_id
    except Exception as e:
        log.warning(f"[Telegram] Error enviando mensaje al canal: {e}")
        return None

async def delete_msg(thread_id: int, msg_id: int) -> bool:
    if bot is None or msg_id is None:
        return False
    try:
        await bot.delete_message(chat_id=CHAT_ID, message_id=msg_id)
        return True
    except Exception as e:
        log.warning(f"[Telegram] No se pudo borrar el mensaje {msg_id} "
                    f"(¿el bot es admin con permiso de borrar?): {e}")
        return False

async def send_reliable(*args) -> Optional[int]:
    """Envía al canal. Acepta send_reliable(text) y, por compatibilidad, send_reliable(_, text)."""
    text = args[-1] if args else ""
    for attempt in range(1, TG_SEND_RETRIES + 1):
        mid = await send_msg(text)
        if mid is not None:
            return mid
        await asyncio.sleep(min(2 * attempt, 20))
    log.error(f"[Telegram] Mensaje descartado tras {TG_SEND_RETRIES} intentos: {text[:60]!r}")
    return None

def enqueue_job(job):
    """Encola un trabajo (función async sin argumentos). Se ejecutan estrictamente en orden."""
    if bot is None:
        return
    if _tg_queue is None:
        log.warning("[Telegram] Cola no inicializada; mensaje descartado")
        return
    _tg_queue.put_nowait(job)

async def telegram_sender_loop():
    while True:
        job = await _tg_queue.get()
        try:
            await job()
        except Exception as e:
            log.error(f"[Telegram] Error en trabajo de cola: {e}")
        finally:
            _tg_queue.task_done()
        await asyncio.sleep(TG_MIN_GAP)


# ══════════════════════════════════════════════
#  FORMATO DE MENSAJES
# ══════════════════════════════════════════════
def keycap(n: int) -> str:
    """1 -> 1️⃣, 2 -> 2️⃣ ..."""
    return "".join(d + "\ufe0f\u20e3" for d in str(n))

def build_possible_message(label: str, k: int = 1, total: int = 2) -> str:
    """POSIBLE ENTRADA 1️⃣ de 2️⃣ / 2️⃣ de 2️⃣ (confirmación k de total)."""
    return f"🚸 POSIBLE ENTRADA {keycap(k)} de {keycap(total)}!!! 🚸\n- Mesa: {label} en {PROVIDER_NAME}"

def side_label(side: str) -> str:
    return "Banker 🔴" if side == "B" else "Player 🔵"

def build_attempt_message(label: str, sig: "Signal", att: dict) -> str:
    """Mensaje de cada intento (mismo diseño en los 3): lado elegido por el análisis y monto de Labouchere."""
    return ("✅✅ ENTRADA CONFIRMADA ✅✅\n\n"
            f"🕹️ Mesa: {label}\n"
            f"🏁 Entrada: ${att['stake_amount']} a {side_label(att['bet_side'])}\n"
            f"🍀 Opcional: ${TIE_AMOUNT} a Tie 🟢\n\n"
            f"🔔 Intento {att['level']} de {MAX_ENTRIES} - Labouchere")

def _cards_txt(labels, values) -> str:
    items = list(labels or []) or [str(v) for v in (values or [])]
    return " ".join(items) or "—"

def build_hands_line(hand: Optional[dict]) -> str:
    """~ Player [4♦ 2♣] = 6 🔵\n    ~ Banker [4♥ 5♦] = 9 🔴"""
    h = hand or {}
    c = h.get("cards") or {}
    pt = c.get("player_total") if c.get("player_total") is not None else h.get("p")
    bt = c.get("banker_total") if c.get("banker_total") is not None else h.get("b")
    return (f"~ Player [{_cards_txt(c.get('player_labels'), c.get('player_cards'))}] = {'—' if pt is None else pt} 🔵\n"
            f"~ Banker [{_cards_txt(c.get('banker_labels'), c.get('banker_cards'))}] = {'—' if bt is None else bt} 🔴")

def build_result_message(kind: str, level: int, hand: Optional[dict], side: Optional[str]) -> str:
    """UN solo mensaje: título + manos + resultado con el Win/Tie/Loss.
    💹 ¡GANAMOS ENTRADA! 💹
    ~ Player [4♦ 2♣] = 6 🔵
    ~ Banker [4♥ 5♦] = 9 🔴
    ~ Resultado Banker 🔴 ~ Win 1 Exp."""
    head = {"win": "💹 ¡GANAMOS ENTRADA! 💹", "tie": "💹 ¡EMPATAMOS ENTRADA! 💹"}.get(kind, "💮 ¡PERDIMOS ENTRADA! 💮")
    detail = {"win": f"Win {level} Exp.", "tie": f"Tie {level} Exp."}.get(kind, f"Loss {MAX_ENTRIES} Exp.")
    res = {"P": "Player 🔵", "B": "Banker 🔴", "T": "Tie 🟢"}.get(side or (hand or {}).get("r"), "—")
    return f"{head}\n{build_hands_line(hand)}\n~ Resultado {res} ~ {detail}"

def build_result_tag(kind: str, side: Optional[str]) -> str:
    """Texto que reemplaza al antiguo sticker de resultado."""
    if kind == "win":
        return "🏆 GANADA con Banker 🔴" if side == "B" else "🏆 GANADA con Player 🔵"
    if kind == "tie":
        return "🏆 GANADA con Tie 🟢"
    return "❌ SEÑAL PERDIDA"

def build_stat_line(kind: str, level: int) -> str:
    if kind == "win":
        return f"💹 Win {level} Exp."
    if kind == "tie":
        return f"🍀 Tie {level} Exp."
    return f"💮 Loss {MAX_ENTRIES} Exp."


# ══════════════════════════════════════════════
#  ESTADÍSTICAS GLOBALES (persistentes, sin reseteo diario)
# ══════════════════════════════════════════════
class GlobalStats:
    def __init__(self):
        self.wins = [0] * MAX_ENTRIES   # Win 1, Win 2, Win 3
        self.ties = 0
        self.losses = 0

    @property
    def total(self) -> int:
        return sum(self.wins) + self.ties + self.losses

    def record(self, kind: str, level: int):
        if kind == "win":
            self.wins[level - 1] += 1
        elif kind == "tie":
            self.ties += 1
        else:
            self.losses += 1

    def _pct(self, n: int) -> str:
        t = self.total
        return f"{(n / t * 100) if t else 0.0:.2f}%"

    def marker_message(self) -> str:
        lines = ["🏁 MARCADOR GLOBAL"]
        for i, n in enumerate(self.wins, start=1):
            lines.append(f"✅ Win {i} ({n}) = {self._pct(n)}")
        lines.append(f"🍀 Tie ({self.ties}) = {self._pct(self.ties)}")
        lines.append(f"❌ Loss ({self.losses}) = {self._pct(self.losses)}")
        lines.append("")
        lines.append(f"🎯 Total de Señales = ({self.total})")
        lines.append(f"📈 Efectividad Global = {self._pct(sum(self.wins) + self.ties)}")
        return "\n".join(lines)

    def to_persist(self):
        return {"wins": self.wins, "ties": self.ties, "losses": self.losses}

    def load_persist(self, data):
        if not isinstance(data, dict):
            return
        wins = data.get("wins")
        if isinstance(wins, list) and len(wins) == MAX_ENTRIES:
            self.wins = [int(x) for x in wins]
        self.ties = int(data.get("ties", 0))
        self.losses = int(data.get("losses", 0))

STATS = GlobalStats()

def save_stats():
    tmp = STATS_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(STATS.to_persist(), f)
        os.replace(tmp, STATS_FILE)
    except Exception as e:
        log.warning(f"Error guardando estadísticas: {e}")

def load_stats():
    if not os.path.exists(STATS_FILE):
        return
    try:
        with open(STATS_FILE, "r") as f:
            STATS.load_persist(json.load(f))
        log.info(f"Estadísticas cargadas: {STATS.to_persist()}")
    except Exception as e:
        log.warning(f"Error cargando estadísticas: {e}")


# ══════════════════════════════════════════════
#  GESTIÓN LABOUCHERE (global, persistente)
# ══════════════════════════════════════════════
class Labouchere:
    """Secuencia 1,1,1 · apuesta = primero + último (extremos) · ganar: se eliminan los extremos ·
    perder: se agrega lo apostado al final · secuencia vacía = ciclo completo → vuelve a 1,1,1."""
    def __init__(self):
        self.seq = list(LAB_START)

    def stake_units(self) -> int:
        return self.seq[0] + self.seq[-1] if len(self.seq) > 1 else self.seq[0]

    def on_win(self):
        self.seq = self.seq[1:-1] if len(self.seq) > 1 else []
        if not self.seq:
            self.seq = list(LAB_START)
        self.save()

    def on_loss(self, stake: int):
        self.seq.append(stake)
        self.save()

    def label(self) -> str:
        return "-".join(str(x) for x in self.seq)

    def save(self):
        tmp = LAB_FILE + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"seq": self.seq}, f)
            os.replace(tmp, LAB_FILE)
        except Exception as e:
            log.warning(f"Error guardando Labouchere: {e}")

    def load(self):
        if not os.path.exists(LAB_FILE):
            return
        try:
            with open(LAB_FILE, "r") as f:
                seq = json.load(f).get("seq")
            if isinstance(seq, list) and seq and all(isinstance(x, int) and x > 0 for x in seq):
                self.seq = seq
                log.info(f"Labouchere cargado: {self.label()}")
        except Exception as e:
            log.warning(f"Error cargando Labouchere: {e}")

LAB = Labouchere()


# ══════════════════════════════════════════════
#  REGISTRO DE SEÑALES (JSONL) — para análisis y mejora futura
# ══════════════════════════════════════════════
def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()

def build_signal_record(table: "BaccaratTable", sig: "Signal", result: str, reason: Optional[str] = None) -> dict:
    win_att = next((a for a in sig.attempts if a.get("outcome") == "win"), None)
    return {
        "id": f"{int(sig.started_at * 1000)}-{table.key}",
        "ts_start": _iso(sig.started_at), "ts_end": _iso(time.time()),
        "mesa": table.label, "key": table.key, "live": sig.live,
        "pattern": sig.pattern_info, "pattern_text": sig.pattern,
        "base_target": sig.target, "orig_target": sig.orig_target or sig.target, "flipped": sig.flipped, "shoe_changed": sig.shoe_changed,
        "confirm_total": sig.confirm_total,
        "shoe_pos": sig.shoe_pos, "context20": sig.context20,
        "pattern_hands": sig.pattern_hands,   # manos que formaron el patrón
        "hands": sig.hands_after,             # manos desde el patrón hasta la resolución (fase, nivel, puntajes, raw)
        "confirmations": sig.confirmations,   # análisis y resultado de cada confirmación (dirección esperada)
        "attempts": sig.attempts,             # decisión ML, lado, monto, features, mano y resultado de cada intento
        "result": result, "reason": reason,
        "win_level": win_att["level"] if win_att else None,
        "win_hand": win_att["hand"] if win_att else None,
        "labouchere_final": list(LAB.seq),
    }

def append_signal_log(record: dict):
    try:
        with open(SIGNALS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        log.warning(f"Error guardando señal en {SIGNALS_FILE}: {e}")


# ══════════════════════════════════════════════
#  MODELOS ESTADÍSTICOS (puro Python, sin dependencias)
#  Pregunta que responden en cada intento: ¿MANTENER la dirección o CAMBIAR?
#  Etiqueta de cada fila: keep_won = 1 si salió el lado "mantener" (descartando empates)
# ══════════════════════════════════════════════
NB_CARD = {"pattern": 6, "level": 3, "keep_side": 2, "last_vs_keep": 2, "last2_vs_keep": 2,
           "run_len": 4, "bias20": 3, "pos": 6, "alt10": 3, "phase": 2}
N_NUM = 11

def _sigmoid(z: float) -> float:
    z = max(-30.0, min(30.0, z))
    return 1.0 / (1.0 + math.exp(-z))

def build_features(table: "BaccaratTable", sig: "Signal", level: int, keep_side: str, phase: int = 1) -> dict:
    """phase=0: confirmación (level = nº de confirmación) · phase=1: entrada (level = nº de intento)."""
    sides = table.shoe_sides
    last = sides[-1] if sides else None
    last2 = sides[-2] if len(sides) >= 2 else None
    run = 0
    for sd in reversed(sides):
        if sd == last:
            run += 1
        else:
            break
    w20 = sides[-20:]
    b_ratio = (w20.count("B") / len(w20)) if w20 else 0.5
    w11 = sides[-11:]
    changes = sum(1 for i in range(1, len(w11)) if w11[i] != w11[i - 1])
    alt = (changes / (len(w11) - 1)) if len(w11) > 1 else 0.5
    return {
        "pattern": sig.key, "level": level, "keep_side": keep_side, "phase": phase,
        "last_vs_keep": int(last == keep_side), "last2_vs_keep": int(last2 == keep_side),
        "run_len": min(run, 4),
        "bias20": 0 if b_ratio < 0.4 else (1 if b_ratio <= 0.6 else 2),
        "pos": min(len(table.shoe_hands) // 15, 5),
        "alt10": 0 if alt < 0.4 else (1 if alt <= 0.6 else 2),
    }

class MLEngine:
    SIM_TIERS = [("A", ["pattern", "phase", "level", "keep_side", "last_vs_keep", "last2_vs_keep", "run_len"]),
                 ("B", ["pattern", "phase", "level", "last_vs_keep"]),
                 ("C", ["pattern", "phase", "level"]),
                 ("D", ["phase", "level"])]
    WEIGHTS = {"ctx": 1.0, "nb": 1.0, "lr": 1.0, "sim": 1.5}
    LR, L2 = 0.05, 0.001

    def __init__(self):
        self.rows: list = []              # (features, keep_won)
        self.ctx: dict = {}               # (pattern, level) -> [keep_wins, total]
        self.nb_class = [0, 0]
        self.nb_counts: dict = {}         # (feature, value, label) -> n
        self.w = [0.0] * N_NUM
        self.hits: deque = deque(maxlen=RELIABLE_WINDOW)   # acierto de cada predicción ANTES de aprender (1/0)
        self.signals_seen = 0                              # señales resueltas (reales + simuladas)
        self.warmup_signals = 0                             # señales resueltas mientras el ML estaba en precalentamiento
        self.warmup_wins = 0
        self.warmup_ties = 0
        self.warmup_losses = 0
        self.warmup_recent: deque = deque(maxlen=5)          # últimas señales de precalentamiento

    # ---- confiabilidad ----
    def accuracy(self) -> Optional[float]:
        return (sum(self.hits) / len(self.hits)) if self.hits else None

    def warmup_effectiveness(self) -> Optional[float]:
        total = self.warmup_wins + self.warmup_ties + self.warmup_losses
        return ((self.warmup_wins + self.warmup_ties) / total) if total else None

    def register_signal_result(self, record: dict, was_reliable: bool):
        """Registra una señal en precalentamiento y prepara el reporte periódico."""
        if was_reliable:
            return False
        result = record.get("result")
        if result not in ("win", "loss", "tie"):
            return False
        self.warmup_signals += 1
        if result == "win":
            self.warmup_wins += 1
        elif result == "tie":
            self.warmup_ties += 1
        else:
            self.warmup_losses += 1

        attempts = record.get("attempts") or []
        final_att = attempts[-1] if attempts else {}
        bet_side = final_att.get("bet_side")
        self.warmup_recent.append({
            "n": self.warmup_signals,
            "mesa": record.get("mesa"),
            "pattern": record.get("pattern_text") or record.get("pattern", {}).get("pattern"),
            "target": record.get("base_target"),
            "bet": bet_side,
            "result": result,
            "level": record.get("win_level") or final_att.get("level"),
            "p": final_att.get("p_keep"),
        })
        return True

    def warmup_report_due(self) -> bool:
        return self.warmup_signals > 0 and self.warmup_signals % max(1, ML_REPORT_EVERY) == 0

    def training_report_text(self) -> str:
        total = self.warmup_wins + self.warmup_ties + self.warmup_losses
        eff = self.warmup_effectiveness()
        eff_txt = "—" if eff is None else f"{eff * 100:.2f}%"
        acc = self.accuracy()
        acc_txt = "—" if acc is None else f"{acc * 100:.2f}%"
        state = "CONFIABLE" if self.reliable() else "PRECALENTAMIENTO"
        lines = [
            "🧠📊 ENTRENAMIENTO ML",
            f"Estado: {'✅' if state == 'CONFIABLE' else '🔥'} {state}",
            f"Señales de precalentamiento: {self.warmup_signals}",
            f"✅ Wins: {self.warmup_wins}   🍀 Ties: {self.warmup_ties}   ❌ Losses: {self.warmup_losses}",
            f"📈 Efectividad precalentamiento: {eff_txt}",
            f"🎯 Predicciones ML evaluadas: {len(self.hits)}",
            f"🤖 Efectividad del modelo: {acc_txt}",
            f"📚 Filas de entrenamiento: {len(self.rows)}",
            f"🃏 Manos aprendidas: {LEARNED['hands']} (con cartas: {LEARNED['with_cards']} · recuperadas al iniciar: {LEARNED['restored']})",
            f"🧩 Patrones guardados: {lib_counts()[0]} (con resultado: {lib_counts()[1]})",
            f"🔢 Señales resueltas totales: {self.signals_seen}",
            f"⏳ Umbral para confiable: {MIN_SIGNALS_TRAIN} señales / {RELIABLE_MIN_PRED} predicciones / {RELIABLE_MIN_ACC*100:.0f}%",
        ]
        if self.warmup_recent:
            lines += ["", "🧪 ÚLTIMAS SEÑALES DE PRECALENTAMIENTO"]
            for x in list(self.warmup_recent)[-ML_REPORT_EVERY:]:
                result_icon = {"win":"✅", "tie":"🍀", "loss":"❌"}.get(x["result"], "•")
                target = side_label(x["target"]) if x.get("target") in ("P", "B") else "—"
                bet = side_label(x["bet"]) if x.get("bet") in ("P", "B") else "—"
                ptxt = "—" if x.get("p") is None else f"{x['p']*100:.1f}%"
                lines.append(f"{x['n']}. {result_icon} {x.get('mesa','—')} | {x.get('pattern','—')} | base {target} | apuesta {bet} | {ptxt}")
        return "\n".join(lines)

    def reliable(self) -> bool:
        acc = self.accuracy()
        return (self.signals_seen >= MIN_SIGNALS_TRAIN and len(self.hits) >= RELIABLE_MIN_PRED
                and acc is not None and acc >= RELIABLE_MIN_ACC)

    def readiness_text(self) -> str:
        acc = self.accuracy()
        acc_txt = "—" if acc is None else f"{acc * 100:.1f}%"
        state = "✅ CONFIABLE (enviando señales)" if self.reliable() else "🧪 ENTRENAMIENTO (sin enviar señales)"
        return (f"{state}\n"
                f"   Señales: {self.signals_seen}/{MIN_SIGNALS_TRAIN} · "
                f"Predicciones: {len(self.hits)}/{RELIABLE_MIN_PRED} · "
                f"Acierto: {acc_txt} (mín. {RELIABLE_MIN_ACC * 100:.0f}%)")

    # ---- aprendizaje ----
    @staticmethod
    def vec(f: dict) -> list:
        return [1.0, float(f["last_vs_keep"]), float(f["last2_vs_keep"]), f["run_len"] / 4.0,
                float(f["bias20"] - 1), 1.0 if f["keep_side"] == "B" else 0.0,
                1.0 if f["level"] == 2 else 0.0, 1.0 if f["level"] == 3 else 0.0,
                f["pos"] / 5.0, float(f["alt10"] - 1), float(f["phase"])]

    def _sgd(self, f: dict, y: int):
        x = self.vec(f)
        p = _sigmoid(sum(wi * xi for wi, xi in zip(self.w, x)))
        for i in range(N_NUM):
            self.w[i] += self.LR * ((y - p) * x[i] - self.L2 * self.w[i])

    def learn(self, f: dict, y: int, fit: bool = True, p_keep: Optional[float] = None):
        if p_keep is not None and p_keep != 0.5:
            self.hits.append(int((p_keep > 0.5) == bool(y)))
        f.setdefault("phase", 1)          # registros viejos (solo intentos) no traían la fase
        f.setdefault("level", 1)
        f.setdefault("pattern", "?")
        self.rows.append((f, y))
        c = self.ctx.setdefault((f["pattern"], f["phase"], f["level"]), [0, 0])
        c[0] += y
        c[1] += 1
        self.nb_class[y] += 1
        for name in NB_CARD:
            if name not in f:             # las features de la estrategia de 4 roads no traen todas las claves
                continue
            k = (name, f[name], y)
            self.nb_counts[k] = self.nb_counts.get(k, 0) + 1
        if fit and all(k in f for k in ("last_vs_keep", "last2_vs_keep", "run_len", "bias20",
                                        "keep_side", "pos", "alt10")):
            self._sgd(f, y)

    def refit(self, epochs: int = 10):
        self.w = [0.0] * N_NUM
        for _ in range(epochs):
            for f, y in self.rows:
                if all(k in f for k in ("last_vs_keep", "last2_vs_keep", "run_len", "bias20",
                                        "keep_side", "pos", "alt10")):
                    self._sgd(f, y)

    # ---- predicción ----
    def _nb(self, f: dict) -> float:
        n = len(self.rows)
        logp = []
        for y in (0, 1):
            lp = math.log((self.nb_class[y] + 1) / (n + 2))
            for name, card in NB_CARD.items():
                lp += math.log((self.nb_counts.get((name, f[name], y), 0) + 1) / (self.nb_class[y] + card))
            logp.append(lp)
        m = max(logp)
        e0, e1 = math.exp(logp[0] - m), math.exp(logp[1] - m)
        return e1 / (e0 + e1)

    def similar(self, f: dict) -> Optional[dict]:
        for tier, keys in self.SIM_TIERS:
            ys = [y for ff, y in self.rows if all(ff.get(k) == f.get(k) for k in keys)]
            if len(ys) >= SIM_MIN_CASES:
                return {"tier": tier, "n": len(ys), "keep_wins": sum(ys), "rate": sum(ys) / len(ys)}
        return None

    def decide(self, f: dict, default_change: bool) -> dict:
        n = len(self.rows)
        sim = self.similar(f)
        probs: dict = {}
        c = self.ctx.get((f["pattern"], f["phase"], f["level"]))
        if c and c[1] >= SIM_MIN_CASES:
            probs["ctx"] = (c[0] + 1) / (c[1] + 2)
        if n >= ML_MIN_ROWS:
            probs["nb"] = self._nb(f)
            probs["lr"] = _sigmoid(sum(wi * xi for wi, xi in zip(self.w, self.vec(f))))
        if sim:
            probs["sim"] = sim["rate"]
        mode, source, p_keep = ("change" if default_change else "keep"), "base", None
        if n >= ML_MIN_ROWS and probs:
            p_keep = sum(self.WEIGHTS[k] * v for k, v in probs.items()) / sum(self.WEIGHTS[k] for k in probs)
            if abs(p_keep - 0.5) >= ML_MARGIN:
                mode, source = ("keep" if p_keep > 0.5 else "change"), "ml"
        return {"mode": mode, "source": source, "p_keep": p_keep, "models": probs,
                "similar": sim, "n_rows": n, "features": f}

ML = MLEngine()

def load_ml_from_log():
    """Reconstruye los modelos desde el registro de señales (se entrena con todo el historial)."""
    if not os.path.exists(SIGNALS_FILE):
        return
    n_sig = 0
    try:
        with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                n_sig += 1
                if rec.get("result") in ("win", "loss", "tie"):
                    ML.signals_seen += 1
                    # El historial no contiene necesariamente el estado exacto del ML al momento
                    # de la señal; para el reporte inicial consideramos precalentamiento hasta el umbral.
                    was_warmup = ML.signals_seen <= MIN_SIGNALS_TRAIN
                    ML.register_signal_result(rec, was_reliable=not was_warmup)
                for att in list(rec.get("confirmations", [])) + list(rec.get("attempts", [])):
                    if att.get("keep_won") in (0, 1) and isinstance(att.get("features"), dict):
                        try:
                            ML.learn(att["features"], int(att["keep_won"]), fit=False, p_keep=att.get("p_keep"))
                        except Exception:
                            pass
        ML.refit(10)
        log.info(f"ML entrenado desde {n_sig} señales registradas ({len(ML.rows)} filas)")
        log.info("Estado: " + ML.readiness_text().replace("\n", " "))
    except Exception as e:
        log.warning(f"Error cargando historial de señales: {e}")


# ══════════════════════════════════════════════
#  NOTIFICACIONES (señales → Telegram)
# ══════════════════════════════════════════════
def enqueue_tracked_send(thread_id: int, text: str) -> dict:
    """Encola un envío y deja el message_id en el dict devuelto (para poder borrarlo después)."""
    holder: dict = {}

    async def job():
        holder["id"] = await send_reliable(thread_id, text)

    enqueue_job(job)
    return holder

def enqueue_delete(holder: Optional[dict], thread_id: int = THREAD_SIGNALS):
    """Borra un mensaje enviado con enqueue_tracked_send (se encola después de su envío, así que el id ya existe)."""
    if holder is None or holder.get("queued_delete"):
        return
    holder["queued_delete"] = True

    async def job():
        mid = holder.get("id")
        if mid is None:
            log.warning("[Telegram] Borrado omitido: el mensaje nunca se confirmó")
            return
        for attempt in range(1, 4):
            if await delete_msg(thread_id, mid):
                return
            await asyncio.sleep(2 * attempt)
        log.error(f"[Telegram] NO se pudo borrar el mensaje {mid}")

    enqueue_job(job)

def notify_ml_training_report():
    """Envía al tema de estadísticas el resumen automático del ML cada N señales de precalentamiento."""
    if bot is None or not ML.warmup_report_due():
        return
    text = ML.training_report_text()
    async def job():
        await send_reliable(THREAD_STATS, text)
    enqueue_job(job)

def notify_possible(table: "BaccaratTable", sig: "Signal"):
    """POSIBLE ENTRADA: mensaje de texto (se borra cuando la señal se confirma o se descarta)."""
    holder: dict = {}
    text = build_possible_message(table.label, sig.attempt, sig.confirm_total)   # confirmación actual

    async def job():
        holder["id"] = await send_reliable(THREAD_SIGNALS, text)

    enqueue_job(job)
    sig.possible_holder = holder

def delete_possible(sig: "Signal"):
    enqueue_delete(sig.possible_holder)

def notify_discarded(table: "BaccaratTable", sig: "Signal"):
    """Señal descartada (confirmación fallida, bloqueo o cambio de zapato): se borra la posible entrada."""
    delete_possible(sig)

def send_attempt(table: "BaccaratTable", sig: "Signal", att: dict) -> dict:
    """Envía el mensaje del intento. El intento anterior lo borra quien llama (enqueue_delete) antes de enviar."""
    return enqueue_tracked_send(THREAD_SIGNALS, build_attempt_message(table.label, sig, att))

def notify_result(table: "BaccaratTable", kind: str, level: int, side: Optional[str] = None, hand: Optional[dict] = None):
    """Resolución: UN mensaje en señales (manos + resultado + Win/Loss) → línea de resultado + marcador en estadísticas."""
    STATS.record(kind, level)
    save_stats()
    if not ((hand or {}).get("cards") or {}).get("player_cards"):
        log.warning(f"[{table.label}] Resultado sin cartas legibles: el mensaje saldrá con '—' en las manos")
    text = build_result_message(kind, level, hand, side)
    stat_line = build_stat_line(kind, level)
    marker = STATS.marker_message()    # calculado ahora, para que el orden de la cola sea consistente

    async def job():
        await send_reliable(THREAD_SIGNALS, text)
        await send_reliable(THREAD_STATS, stat_line)
        await send_reliable(THREAD_STATS, marker)

    enqueue_job(job)


# ══════════════════════════════════════════════
#  MESA DE BACCARAT (lógica de processCoup/handleMsg del HTML)
# ══════════════════════════════════════════════
@dataclass
class Signal:
    key: str
    pattern: str          # "La Caja (2-2)"
    target: str           # lado base del patrón ('P' | 'B'); las confirmaciones se miden contra este lado
    last: str
    confirm_total: int
    total_attempts: int   # confirmaciones + MAX_ENTRIES
    attempt: int = 1      # intento actual (1..total_attempts)
    possible_holder: Optional[dict] = None   # message_id de POSIBLE ENTRADA (para borrarlo)
    alt: bool = False     # regla base cuando no hay datos: cambiar de dirección en cada reintento
    bet_side: Optional[str] = None           # lado apostado en el intento actual
    attempt_msg: Optional[dict] = None       # message_id del mensaje del intento actual
    pattern_info: dict = field(default_factory=dict)
    pattern_hands: list = field(default_factory=list)
    hands_after: list = field(default_factory=list)
    attempts: list = field(default_factory=list)
    live: bool = True                        # False = señal simulada (modo entrenamiento): no envía nada a Telegram
    conf_side: Optional[str] = None          # lado esperado en la confirmación actual (lo decide el análisis)
    confirmations: list = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    shoe_pos: int = 0
    context20: str = ""
    analysis: dict = field(default_factory=dict)
    shoe_changed: bool = False               # True = hubo cambio de zapato con la entrada ya confirmada (la señal sigue)
    carry_keep: Optional[str] = None         # mano a apostar heredada del zapato anterior (los gráficos derivados se reinician)
    flipped: bool = False                    # True = la 1.ª confirmación falló y la señal pasó a la dirección contraria (2 de 2)
    orig_target: Optional[str] = None        # dirección original del patrón (antes de invertir)

    @property
    def in_confirmation(self) -> bool:
        return self.attempt <= self.confirm_total

    @property
    def entry_level(self) -> int:
        return self.attempt - self.confirm_total

    @property
    def expected_side(self) -> str:
        return (self.conf_side or self.target) if self.in_confirmation else (self.bet_side or self.target)



# ══════════════════════════════════════════════
#  MANOS APRENDIDAS (registro + persistencia)
# ══════════════════════════════════════════════
LEARNED = {"hands": 0, "with_cards": 0, "restored": 0}   # contadores globales (todas las mesas)

def save_hand(table_key, hand: dict):
    """Guarda cada mano aprendida (una línea JSON) para no perder el historial al reiniciar."""
    if hand.get("gid") is None:
        return
    c = hand.get("cards") or {}
    rec = {"t": table_key, "sid": hand.get("shoe_id", 0), "gid": str(hand.get("gid")), "r": hand.get("r"),
           "p": hand.get("p"), "b": hand.get("b"), "ts": int(time.time()),
           "cards": {k: c.get(k) for k in ("player_cards", "banker_cards", "player_labels", "banker_labels", "player_total", "banker_total")}}
    try:
        with open(HANDS_FILE, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, default=str) + "\n")
    except Exception as e:
        log.warning(f"Error guardando mano aprendida: {e}")

def load_hands() -> dict:
    """Lee las manos guardadas: {mesa: [manos]} (las últimas HANDS_KEEP por mesa)."""
    out: dict = {}
    if not os.path.exists(HANDS_FILE):
        return out
    try:
        with open(HANDS_FILE, "r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                c = rec.get("cards") or {}
                pc, bc = c.get("player_cards") or [], c.get("banker_cards") or []
                hand = {"gid": rec.get("gid"), "r": rec.get("r"), "p": rec.get("p"), "b": rec.get("b"),
                        "shoe_id": rec.get("sid", 0), "ts": rec.get("ts", 0), "raw": {},
                        "cards": {"player_cards": pc, "banker_cards": bc,
                                  "player_labels": c.get("player_labels") or [], "banker_labels": c.get("banker_labels") or [],
                                  "player_ncards": len(pc), "banker_ncards": len(bc),
                                  "player_total": c.get("player_total"), "banker_total": c.get("banker_total")}}
                out.setdefault(rec.get("t"), []).append(hand)
    except Exception as e:
        log.warning(f"Error cargando manos aprendidas: {e}")
    return {k: v[-HANDS_KEEP:] for k, v in out.items()}

def _pct(p) -> str:
    return "sin datos" if p is None else f"{p * 100:.1f}%"


class BaccaratTable:
    def __init__(self, key: int, label: str):
        self.key = key
        self.label = label
        self.shoe_hands: list = []        # manos del zapato actual
        self.history_hands: list = []      # historial persistente en memoria para comparar zapatos anteriores
        self.shoe_id = 0
        self.shoe_sides: list = []        # lados P/B del zapato actual (sin empates)
        self.last_shoe_total: Optional[int] = None
        self.seed_pending = True          # primer lote de historial: no genera señales
        self.seen: set = set()
        self.signal: Optional[Signal] = None
        self.connected = False
        self.hands = 0
        self.peers: list = []             # todas las mesas (lo asigna ServerState) para el bloqueo entre mesas
        # ── Motor Hong Kong 2x5 ──
        self.hk_row1: list = []
        self.hk_row2: list = []
        self.hk_pending: Optional[dict] = None
        self.hk_net_units: int = 0
        self.hk_signals: int = 0
        self.hk_target_reached: bool = False

    def restore_hands(self, hands: list):
        """Recupera las manos aprendidas en ejecuciones anteriores. Si la última es reciente (<10 min) se asume
        que el zapato sigue siendo el mismo y se continúa su camino; si no, se empieza un zapato nuevo."""
        if not hands:
            return
        self.history_hands = list(hands)
        for h in hands:
            if h.get("gid") is not None:
                self.seen.add(h["gid"])
        lib_scan_hands(self.key, hands)   # completa la biblioteca con las manos recuperadas
        LEARNED["hands"] += len(hands)
        LEARNED["restored"] += len(hands)
        LEARNED["with_cards"] += sum(1 for h in hands if (h.get("cards") or {}).get("player_cards")
                                     and (h.get("cards") or {}).get("banker_cards"))
        last = hands[-1]
        sid = last.get("shoe_id", 0)
        # Hong Kong comienza siempre el motor 2x5 con un bloque de 5 rondas
        # del zapato que llega en vivo. El historial se conserva para aprendizaje,
        # pero no se reutiliza para rellenar la Fila 1.
        self.shoe_id = sid
        self.shoe_hands = []
        self.shoe_sides = []
        self.hk_row1 = []
        self.hk_row2 = []
        self.hk_pending = None
        self.hk_net_units = 0
        self.hk_signals = 0
        self.hk_target_reached = False
        self.seed_pending = True
        log.info(f"[{self.label}] 📚 {len(hands)} manos aprendidas recuperadas; "
                 "motor Hong Kong listo para cargar 5 rondas del zapato actual")

    # ---- bloqueo entre mesas: una señal confirmada bloquea las demás hasta resolverse ----
    def _other_confirmed(self) -> Optional["BaccaratTable"]:
        for t in self.peers:
            if t is not self and t.signal is not None and t.signal.live and not t.signal.in_confirmation:
                return t
        return None

    def _discard_other_pending(self):
        for t in self.peers:
            if t is not self and t.signal is not None and t.signal.live and t.signal.in_confirmation:
                log.info(f"[{t.label}] Señal descartada (bloqueada por la entrada confirmada en {self.label})")
                t._abort(f"bloqueada por entrada confirmada en {self.label}")

    # ---- zapato ----
    def _reset_shoe(self, detected=True):
        # Si ya hay una sesión activa en esta mesa y el zapato termina antes de +10u,
        # no cambiamos de mesa a mitad de sesión: se cierra esa sesión y se espera
        # a que otra mesa (o esta misma más adelante) vuelva a iniciar un zapato.
        was_active = bool(self.coordinator and self.coordinator.active_key == self.key)
        if detected and was_active and not self.hk_target_reached and self.hk_signals > 0:
            log.info(f"[{self.label}] 🛑 El zapato terminó antes de +{HK_SHOE_TARGET}u; sesión cerrada sin cambiar a otra mesa ahora")
            self.coordinator.finish_session(self, reason="fin de zapato antes de alcanzar el objetivo")

        self.shoe_id += 1
        self.shoe_hands = []
        self.shoe_sides = []
        self.hk_pending = None
        self.hk_row1 = []
        self.hk_row2 = []
        self.hk_net_units = 0
        self.hk_signals = 0
        self.hk_target_reached = False
        self.seed_pending = True
        self.signal = None

        # Solo la mesa que acaba de detectar el nuevo zapato puede convertirse
        # en la mesa activa si no existe otra sesión en curso.
        if detected and self.coordinator:
            self.coordinator.try_activate(self)

        log.info(f"[{self.label}] 🇭🇰 Nuevo zapato detectado: {'MESA ACTIVA' if self.coordinator and self.coordinator.active_key == self.key else 'monitorizada sin registrar'}")

    def _check_shoe(self, data: dict):
        st = data.get("baccaratShoeSummary")
        total = to_num(st.get("totalGames")) if isinstance(st, dict) else None
        changed = False
        if data.get("shuffle") is True:
            changed = True
        elif total is not None and self.last_shoe_total is not None and total < self.last_shoe_total:
            changed = True

        if changed:
            self._reset_shoe(detected=True)

        if total is not None:
            self.last_shoe_total = total

    # ---- nueva estrategia de 4 roads ----
    def detect_pattern(self):
        """Busca PBPBP/BPBPB en los 4 gráficos: señal si el patrón se cumple en este momento (con respaldo histórico en ese mismo gráfico)."""
        if len(self.shoe_sides) < PATTERN_LEN:
            return None
        # history_hands ya contiene las manos del zapato actual (process_coup las agrega a ambas listas):
        # sumar shoe_hands duplicaba el zapato actual y creaba casos históricos y patrones falsos.
        data = analyze_road_candidates(self.history_hands, library=PATTERN_LIB)
        if not data:
            return None
        for _c in data:   # qué rondas formaron el patrón y qué respaldo encontró (para revisar en el log)
            rondas = " | ".join(f"{i}:{x['r']} P{x['player_cards']} B{x['banker_cards']}"
                                for i, x in enumerate(_c.get("pattern_rounds", []), 1))
            log.info(f"[{getattr(self, 'label', '?')}] Patrón {_c['pattern']} en {_c['road_name']} → rondas: {rondas} · "
                     f"previas={_c['occurrences']} · mismas cartas={_c['exact_cases']} · "
                     f"mínima variación={_c['near_cases']} · aciertos={_c['similar_wins']}/{_c['similar_cases']}")
        best = choose_best_candidate(data)
        if not best:
            return None
        best = dict(best)   # copia: si no, all_candidates se contiene a sí mismo (referencia circular → json falla)
        best["all_candidates"] = sorted(data, key=lambda x: x["probability"], reverse=True)
        return best

    def start_signal(self, det, live: bool):
        target = det["target"]
        ph = list(self.shoe_hands[-min(12, len(self.shoe_hands)):])
        self.signal = Signal(
            key=det["pattern"],
            pattern=f"{det['road_name']} · {det['pattern']}",
            target=target, last=self.shoe_sides[-1],
            confirm_total=2, total_attempts=2 + MAX_ENTRIES,
            alt=False, live=live,
            pattern_info={
                "strategy": "4_ROADS_CARD_HISTORY",
                "road": det["road"], "road_name": det["road_name"],
                "symbol": det["symbol"], "pattern": det["pattern"],
                "target": target, "occurrences": det["occurrences"],
                "historical_rate": det["historical_rate"],
                "similar_cases": det["similar_cases"], "similar_rate": det["similar_rate"],
                "exact_cases": det.get("exact_cases", 0), "near_cases": det.get("near_cases", 0),
                "pattern_rounds": det.get("pattern_rounds", []),
                "card_similarity": det["card_similarity"],
                "probability": det["probability"], "line_focus": det["line_focus"],
                "current_hand_cards": (self.shoe_hands[-1].get("cards") if self.shoe_hands else {}),
                "all_candidates": det.get("all_candidates", []),
            },
            pattern_hands=ph, shoe_pos=len(self.shoe_hands),
            context20="".join(self.shoe_sides[-20:]), analysis=det)
        log.info(f"[{self.label}] {'🚸 Posible entrada' if live else '🧪 Señal simulada'}: "
                 f"{det['road_name']} {det['pattern']} → {target} "
                 f"({det['probability']*100:.1f}%, n={det['occurrences']})")
        if live:
            notify_possible(self, self.signal)
        self._plan_confirmation(self.signal)

    def _decision_for_signal(self, sig, level, phase):
        """Decisión fijada al detectar el patrón (road, patrón y probabilidad de la señal).
        `sig.target` es el símbolo esperado EN EL GRÁFICO (lado en la Big Road, color en los demás);
        aquí se traduce a la mano a apostar con el estado actual del zapato. None = sin mano definida."""
        det = sig.analysis or {}
        target = sig.target
        road = det.get("road", "big")
        # tras un cambio de zapato los gráficos derivados quedan vacíos: se conserva la mano que ya se venía apostando
        keep = sig.carry_keep if (sig.shoe_changed and sig.carry_keep) else hand_for_color(self.shoe_sides, road, target)
        if keep is None:
            return None
        if det:
            # POCOS DATOS -> manda el patrón: se apuesta la mano que hace aparecer el color esperado en ese gráfico
            # y cada resultado entrena al modelo en tiempo real. Cuando el modelo ya es CONFIABLE
            # (señales/predicciones/acierto mínimos) y tiene datos de este contexto, puede decidir mantener o cambiar.
            ctx_key = f"{road}|{sig.key}" + ("|inv" if sig.flipped else "")
            c = ML.ctx.get((ctx_key, phase, level))
            p_ml = (c[0] + 1.0) / (c[1] + 2.0) if c and c[1] >= ML_CTX_MIN else None
            use_ml = ML.reliable() and p_ml is not None and abs(p_ml - 0.5) >= ML_MARGIN
            expected = keep if not use_ml else (keep if p_ml > 0.5 else other_side(keep))
            source = "modelo ML" if use_ml else ("patrón (entrenando)" if not ML.reliable() else "patrón")
            last = (self.shoe_hands[-1].get("cards") or {}) if self.shoe_hands else {}
            return {
                "mode": "ml" if use_ml else "pattern_default", "source": source, "p_keep": p_ml,
                "keep_side": keep, "expected_side": expected, "candidate": det,
                "models": {x["road"]: round(x["probability"], 4) for x in det.get("all_candidates", [])},
                "n_rows": (c[1] if c else 0), "features": {
                    "road": det.get("road"), "pattern": ctx_key, "phase": phase,
                    "level": level, "target": target,
                    "player_cards": last.get("player_cards", []),
                    "banker_cards": last.get("banker_cards", []),
                }
            }
        return {"mode": "base", "source": "sin_historial", "p_keep": 0.5,
                "keep_side": keep, "expected_side": keep, "candidate": None, "models": {}, "n_rows": 0,
                "features": {"pattern": sig.key, "phase": phase, "level": level}}

    def _plan_confirmation(self, sig: Signal) -> bool:
        k = sig.attempt
        dec = self._decision_for_signal(sig, k, 0)
        if dec is None:
            log.info(f"[{self.label}] Señal descartada: el gráfico no tiene una mano que dé el color esperado")
            self._abort("sin mano definida para el color esperado")
            return False
        sig.conf_side = dec["expected_side"]
        sig.confirmations.append({
            "k": k, "keep_side": dec["keep_side"], "expected_side": sig.conf_side,
            "mode": dec["mode"], "source": dec["source"], "p_keep": dec["p_keep"],
            "models": dec["models"], "similar": (dec.get("candidate") or {}).get("similar_cases", 0),
            "n_rows": dec["n_rows"], "features": dec["features"],
            "candidate": dec.get("candidate"),
        })
        log.info(f"[{self.label}] 🔎 Confirmación {k}/{sig.confirm_total}: "
                 f"{side_label(sig.conf_side)} · {dec['source']} · {_pct(dec['p_keep'])}")
        return True

    # ---- intentos (entrada real) ----
    def _begin_entry(self, sig: Signal):
        if sig.live:
            self._discard_other_pending()    # las posibles entradas de otras mesas se cancelan
            delete_possible(sig)
        self._start_attempt(sig)

    def _start_attempt(self, sig: Signal) -> bool:
        level = sig.entry_level
        dec = self._decision_for_signal(sig, level, 1)
        if dec is None:
            log.info(f"[{self.label}] Entrada cancelada: el gráfico no tiene una mano que dé el color esperado")
            self._abort("sin mano definida para el color esperado")
            return False
        bet_side = dec["expected_side"]
        units = LAB.stake_units()
        att = {
            "level": level, "keep_side": dec["keep_side"], "bet_side": bet_side,
            "mode": dec["mode"], "source": dec["source"], "p_keep": dec["p_keep"],
            "models": dec["models"], "similar": dec.get("candidate", {}).get("similar_cases", 0) if dec.get("candidate") else 0,
            "n_rows": dec["n_rows"], "features": dec["features"],
            "candidate": dec.get("candidate"),
            "stake_units": units, "stake_amount": units * LAB_UNIT, "lab_before": list(LAB.seq),
        }
        sig.bet_side = bet_side
        sig.attempts.append(att)
        log.info(f"[{self.label}] 🎯 Intento {level}: {bet_side} ({dec['source']}, {_pct(dec['p_keep'])}) "
                 f"{units}u · Labouchere {LAB.label()}")
        if sig.live:
            sig.attempt_msg = send_attempt(self, sig, att)
        return True

    def _resolve_entry(self, sig: Signal, r: str, hand: dict) -> bool:
        """Evalúa la mano contra el intento actual. Devuelve True si la señal continúa (siguiente intento)."""
        att = sig.attempts[-1]
        level = att["level"]
        # la mano que resolvió el intento, CON sus cartas (el mensaje de resultado las muestra)
        att["hand"] = {"gid": hand.get("gid"), "r": r, "p": hand.get("p"), "b": hand.get("b"), "cards": hand.get("cards")}
        if r == "T":
            outcome = "tie"
        else:
            outcome = "win" if r == att["bet_side"] else "loss"
        att["outcome"] = outcome
        att["keep_won"] = None if r == "T" else int(r == att["keep_side"])
        if att["keep_won"] is not None:
            ML.learn(att["features"], att["keep_won"], p_keep=att["p_keep"])
        if sig.live:                             # la gestión Labouchere solo se mueve con señales reales
            if outcome == "win":
                LAB.on_win()
            elif outcome == "loss":
                LAB.on_loss(att["stake_units"])
        att["lab_after"] = list(LAB.seq)

        if outcome == "loss" and level < MAX_ENTRIES:
            sig.attempt += 1
            enqueue_delete(sig.attempt_msg)      # el intento anterior se borra y se envía el nuevo
            self._start_attempt(sig)
            return True
        self._finish(sig, outcome, level)         # ganada / empatada / perdida en el último intento: el mensaje queda
        return False

    def _finish(self, sig: Signal, kind: str, level: int):
        self.signal = None
        log.info(f"[{self.label}] Resultado {kind.upper()} (nivel {level}) — {sig.pattern}"
                 f"{'' if sig.live else ' [simulada]'}")
        record = build_signal_record(self, sig, kind)
        append_signal_log(record)
        was_reliable = ML.reliable()
        ML.signals_seen += 1
        counted = ML.register_signal_result(record, was_reliable=was_reliable)
        if counted and ML.warmup_report_due():
            notify_ml_training_report()
        if ML.reliable() != was_reliable:
            log.info("🔔 Cambio de estado del bot → " + ML.readiness_text().replace("\n", " "))
        if sig.live:
            hand = (sig.attempts[-1].get("hand") or {}) if sig.attempts else {}
            notify_result(self, kind, level, hand.get("r"), hand)

    def _abort(self, reason: str):
        """Señal que termina sin resultado (confirmación fallida, bloqueo, cambio de zapato)."""
        sig = self.signal
        if sig is None:
            return
        in_conf = sig.in_confirmation
        if in_conf and sig.live:
            notify_discarded(self, sig)
        append_signal_log(build_signal_record(self, sig, "discarded" if in_conf else "aborted", reason))
        self.signal = None

    # ---- una mano ----
    # ---- una mano: estrategia Hong Kong 2 x 5 ----
    def _hk_reset(self):
        """Reinicia únicamente el motor Hong Kong para un nuevo zapato."""
        self.hk_row1 = []
        self.hk_row2 = []
        self.hk_pending = None
        self.hk_net_units = 0
        self.hk_signals = 0
        self.hk_target_reached = False

    def _hk_direction(self) -> Optional[str]:
        """Dirección de la señal Hong Kong para la próxima ronda.

        Fila 1 = patrón base de 5 rondas.
        Fila 2 = rondas operativas.
        Columna 1 de Fila 2 no genera señal.
        Desde Columna 2:
          CHECK en la columna anterior -> misma dirección de Fila 1.
          X en la columna anterior     -> dirección contraria de Fila 1.
        """
        if len(self.hk_row1) < 5 or len(self.hk_row2) < 1:
            return None
        col = len(self.hk_row2)  # próxima columna 1..5
        if col >= 5:
            return None
        base = self.hk_row1[col]
        previous = self.hk_row2[col - 1]
        return base if previous["status"] == "CHECK" else other_side(base)

    def _graph_confirms_hk(self, hk_direction: str):
        """Confirma una señal Hong Kong con el análisis ORIGINAL de los 4 gráficos.

        Hong Kong propone la dirección para la próxima ronda. Solo se autoriza el envío
        si, en esta misma ronda, el motor original de gráficos detecta su condición y
        la mano que ese gráfico representa coincide con la dirección Hong Kong.
        """
        det = self.detect_pattern()
        if not det:
            log.info(f"[{self.label}] 🇭🇰 HK propone {side_label(hk_direction)}, pero los 4 gráficos no confirman → NO señal")
            return None

        road = det.get("road", "big")
        graph_side = hand_for_color(self.shoe_sides, road, det.get("target"))
        if graph_side is None:
            log.info(f"[{self.label}] 🇭🇰 HK propone {side_label(hk_direction)}, pero el gráfico no define mano → NO señal")
            return None

        if graph_side != hk_direction:
            log.info(
                f"[{self.label}] 🇭🇰 HK={side_label(hk_direction)} | "
                f"4 gráficos={side_label(graph_side)} ({det.get('road_name')} {det.get('pattern')}) → NO señal"
            )
            return None

        log.info(
            f"[{self.label}] ✅ CONFIRMACIÓN DOBLE: Hong Kong={side_label(hk_direction)} + "
            f"4 gráficos={side_label(graph_side)} ({det.get('road_name')} {det.get('pattern')}, "
            f"{det.get('probability', 0)*100:.1f}%) → ENVIAR señal"
        )
        return det

    def _hk_send_signal(self, direction: str, graph_det: Optional[dict] = None):
        """Envía una señal real al canal de Telegram."""
        self.hk_signals += 1
        self.hk_pending = {
            "direction": direction,
            "cycle": self.hk_signals,
            "row1": list(self.hk_row1),
            "row2_before": list(self.hk_row2),
            "stake_units": 1,
            "sent_at": time.time(),
        }

        text = (
            "🎯 <b>SEÑAL HONG KONG</b>\n\n"
            f"🕹️ Mesa: <b>{self.label}</b>\n"
            f"🎲 Entrada: <b>{'BANKER 🔴' if direction == 'B' else 'PLAYER 🔵'}</b>\n"
            "💰 1 unidad\n"
            f"📊 Señal #{self.hk_signals}\n"
            f"💹 Neto del zapato: <b>{self.hk_net_units:+d}u</b>"
        )
        async def job():
            await send_reliable(text)
        enqueue_job(job)

        log.info(
            f"[{self.label}] 🇭🇰 HONG KONG → "
            f"{'BANKER' if direction == 'B' else 'PLAYER'} | "
            f"señal #{self.hk_signals} | neto {self.hk_net_units:+d}u"
        )

    def _hk_resolve_signal(self, r: str, hand: dict):
        """Resuelve la señal pendiente con la ronda recién llegada."""
        sig = self.hk_pending
        if sig is None:
            return

        direction = sig["direction"]
        if r == "T":
            # Igual que el motor visual: el empate no forma P/B; se considera 0u.
            result = "TIE"
            delta = 0
        elif r == direction:
            result = "WIN"
            delta = 1
        else:
            result = "LOSS"
            delta = -1

        self.hk_net_units += delta

        cards = hand.get("cards") or {}
        p_cards = " ".join(cards.get("player_labels") or [str(x) for x in cards.get("player_cards", [])]) or "—"
        b_cards = " ".join(cards.get("banker_labels") or [str(x) for x in cards.get("banker_cards", [])]) or "—"
        p_total = cards.get("player_total", "—")
        b_total = cards.get("banker_total", "—")

        emoji = "✅" if result == "WIN" else ("➖" if result == "TIE" else "❌")
        text = (
            f"{emoji} <b>HONG KONG — {result}</b>\n\n"
            f"🕹️ Mesa: <b>{self.label}</b>\n"
            f"🎯 Señal: <b>{'BANKER 🔴' if direction == 'B' else 'PLAYER 🔵'}</b>\n"
            f"🏁 Resultado: <b>{'BANKER 🔴' if r == 'B' else 'PLAYER 🔵' if r == 'P' else 'TIE 🟢'}</b>\n"
            f"~ Player [{p_cards}] = {p_total} 🔵\n"
            f"~ Banker [{b_cards}] = {b_total} 🔴\n\n"
            f"💰 Neto del zapato: <b>{self.hk_net_units:+d}u</b> / +{HK_SHOE_TARGET}u"
        )
        async def job():
            await send_reliable(text)
        enqueue_job(job)

        # La meta es por zapato: al alcanzar +10u dejamos de emitir nuevas señales.
        if self.hk_net_units >= HK_SHOE_TARGET:
            self.hk_target_reached = True
            done = (
                f"🏆 <b>OBJETIVO DEL ZAPATO ALCANZADO</b>\n\n"
                f"🕹️ Mesa: <b>{self.label}</b>\n"
                f"💰 Neto: <b>+{self.hk_net_units}u</b>\n"
                f"🎯 Objetivo: <b>+{HK_SHOE_TARGET}u</b>\n"
                "⏸️ No se enviarán más señales hasta el próximo zapato."
            )
            async def done_job():
                await send_reliable(done)
            enqueue_job(done_job)
            if self.coordinator:
                self.coordinator.finish_session(self, reason=f"objetivo +{HK_SHOE_TARGET}u alcanzado")

        self.hk_pending = None

    def _hk_add_result(self, r: str, hand: dict):
        """Añade una ronda P/B a la matriz 2x5 y prepara la siguiente señal."""
        if r not in ("P", "B"):
            return

        if len(self.hk_row1) < 5:
            self.hk_row1.append(r)
            log.info(
                f"[{self.label}] 🇭🇰 Fila 1 {len(self.hk_row1)}/5: {r}"
            )
            return

        # Fila 2: primero se compara con la casilla correspondiente de Fila 1.
        col = len(self.hk_row2)
        base = self.hk_row1[col]
        status = "CHECK" if r == base else "X"
        self.hk_row2.append({"val": r, "status": status})

        log.info(
            f"[{self.label}] 🇭🇰 Fila 2 col {col + 1}: {r} vs "
            f"Fila 1 {base} → {status}"
        )

        # Al completar 5 columnas, Fila 2 pasa a ser la nueva Fila 1.
        if len(self.hk_row2) == 5:
            self.hk_row1 = [x["val"] for x in self.hk_row2]
            self.hk_row2 = []
            log.info(
                f"[{self.label}] 🇭🇰 Ciclo 2x5 completado. "
                "Fila 2 pasa a ser Fila 1."
            )
            return

        # Columna 1 no manda señal. La señal se genera para la próxima
        # ronda a partir de la columna 2.
        direction = self._hk_direction()
        if direction is not None and not self.hk_target_reached:
            # Hong Kong es el disparador. El envío real exige además que el
            # análisis ORIGINAL de los 4 gráficos confirme la misma mano en esta ronda.
            graph_det = self._graph_confirms_hk(direction)
            if graph_det is not None:
                self._hk_send_signal(direction, graph_det)

    def process_coup(self, r: str, seed: bool = False, hand: Optional[dict] = None):
        if hand is None:
            hand = {"gid": None, "r": r, "p": None, "b": None, "raw": {}}

        # Todas las mesas siguen siendo observadas para detectar cambios de zapato,
        # pero solo la mesa seleccionada registra rondas y ejecuta Hong Kong.
        if self.coordinator and self.coordinator.active_key != self.key:
            return

        self.hands += 1

        if len(self.shoe_hands) >= SHOE_MAX_HANDS:
            log.warning(
                f"[{self.label}] {len(self.shoe_hands)} manos sin aviso de barajado: "
                "se asume zapato nuevo"
            )
            self._reset_shoe()

        hand = dict(hand)
        hand["shoe_id"] = self.shoe_id

        self.shoe_hands.append(hand)
        self.history_hands.append(hand)
        if len(self.history_hands) > HANDS_KEEP:
            self.history_hands.pop(0)

        LEARNED["hands"] += 1
        if (hand.get("cards") or {}).get("player_cards") and (hand.get("cards") or {}).get("banker_cards"):
            LEARNED["with_cards"] += 1

        save_hand(self.key, hand)
        lib_scan_shoe(self.key, self.shoe_id, self.shoe_hands)

        if len(self.shoe_hands) > 300:
            self.shoe_hands.pop(0)

        if r != "T":
            self.shoe_sides.append(r)
            if len(self.shoe_sides) > 200:
                self.shoe_sides.pop(0)

        # Las primeras 5 rondas cargadas al iniciar el zapato son solamente
        # la Fila 1. Nunca generan señal.
        if seed:
            if r in ("P", "B") and len(self.hk_row1) < 5:
                self.hk_row1.append(r)
            return

        # Si había señal, la ronda actual es su resultado.
        if self.hk_pending is not None:
            self._hk_resolve_signal(r, hand)

        # Un empate no ocupa una casilla de la matriz HK, igual que el HTML,
        # que trabaja exclusivamente con Player/Banker.
        self._hk_add_result(r, hand)

    # ---- mensaje del WebSocket ----
    @staticmethod
    def _chronological(arr):
        items = [r for r in arr if isinstance(r, dict)]
        ids = [pick(r, ["gameId", "id", "gameNumber", "tableGameId"]) for r in items]
        try:
            nums = [int(str(i)) for i in ids]
            return [r for _, r in sorted(zip(nums, items), key=lambda x: x[0])]
        except (TypeError, ValueError):
            return items

    def _feed(self, raw: dict, gid, seed: bool):
        parsed = normalize_baccarat(raw)
        if gid and parsed and gid not in self.seen:
            self.seen.add(gid)
            if len(self.seen) > 4000:
                self.seen.clear()
            self.process_coup(parsed, seed, make_hand(raw, gid, parsed))

    def handle_msg(self, data: dict):
        self._check_shoe(data)

        # El WS sigue vivo en todas las mesas, pero solo la mesa activa
        # procesa/guarda manos. Las demás se usan únicamente como detector
        # de un nuevo zapato.
        if self.coordinator and self.coordinator.active_key != self.key:
            return

        # 1) histórico / stream de manos
        arr = None
        for k in ("gameResult", "last20Results", "lastResults", "results", "history"):
            v = data.get(k)
            if v is not None and v is not False and v != 0 and v != "":
                arr = v
                break
        if isinstance(arr, list):
            chronological = self._chronological(arr)
            if self.seed_pending and chronological:
                # Al iniciar el zapato solo necesitamos las primeras 5 rondas
                # de la matriz HK. Tomamos las 5 más recientes disponibles.
                seed_rows = [x for x in chronological if normalize_baccarat(x) in ("P", "B")]
                seed_rows = seed_rows[-5:]
                for r0 in seed_rows:
                    self._feed(
                        r0,
                        pick(r0, ["gameId", "id", "gameNumber", "tableGameId"]),
                        True
                    )
                if len(self.hk_row1) >= 5:
                    self.seed_pending = False
            else:
                for r0 in chronological:
                    self._feed(
                        r0,
                        pick(r0, ["gameId", "id", "gameNumber", "tableGameId"]),
                        False
                    )

        # 2) resultado en vivo
        live_gid = pick(data, ["gameId", "id", "gameNumber", "tableGameId"])
        if live_gid and any(data.get(k) is not None for k in ("result", "winner", "playerScore", "bankerScore")):
            self._feed(data, live_gid, False)

    def get_state(self) -> dict:
        sig = self.signal
        return {
            "mesa": self.label, "key": self.key, "connected": self.connected,
            "simulada": (None if sig is None else (not sig.live)),
            "manos_zapato": len(self.shoe_sides),
            "senal": None if sig is None else {
                "patron": sig.pattern, "lado": "Banker" if sig.expected_side == "B" else "Player",
                "intento": sig.attempt, "total": sig.total_attempts,
                "fase": "confirmación" if sig.in_confirmation else f"entrada {sig.entry_level}/{MAX_ENTRIES}",
            },
        }


class ServerState:
    """Coordinador: observa todas las mesas, pero solo una puede tener la sesión activa."""
    def __init__(self):
        self.tables = {k: BaccaratTable(k, name) for k, name in BACCARAT_TABLES.items()}
        self.active_key = None
        self.session_number = 0
        self.wait_other_key = None
        for t in self.tables.values():
            t.peers = list(self.tables.values())
            t.coordinator = self
        saved = load_hands()
        for t in self.tables.values():
            t.restore_hands(saved.get(t.key, []))
            # El historial aprendido NO activa una sesión.
            t._hk_reset()
            t.seed_pending = True

    @property
    def active_table(self):
        return self.tables.get(self.active_key) if self.active_key is not None else None

    def try_activate(self, table):
        # Nunca cambiamos de mesa mientras exista una sesión activa.
        if self.active_key is not None:
            return False
        # Después de terminar una sesión, exigimos que sea OTRA mesa la que
        # inicie el próximo zapato antes de volver a seleccionar una mesa.
        if self.wait_other_key is not None:
            if table.key == self.wait_other_key:
                return False
            self.wait_other_key = None
        self.active_key = table.key
        self.session_number += 1
        table.hk_target_reached = False
        table.seed_pending = True
        log.info(f"🎯 MESA SELECCIONADA: {table.label} (zapato nuevo #{table.shoe_id}) — sesión #{self.session_number}, objetivo +{HK_SHOE_TARGET}u")

        text = (
            "🎯 <b>NUEVA SESIÓN HONG KONG</b>\n\n"
            f"🕹️ Mesa seleccionada: <b>{table.label}</b>\n"
            "🆕 Motivo: acaba de comenzar un zapato nuevo\n"
            "📋 Se cargarán 5 rondas para formar Fila 1\n"
            f"💰 Objetivo de sesión: <b>+{HK_SHOE_TARGET}u</b>\n"
            "⏸️ Las demás mesas quedan en espera."
        )
        async def job():
            await send_reliable(text)
        enqueue_job(job)
        return True

    def finish_session(self, table, reason="objetivo alcanzado"):
        if self.active_key != table.key:
            return
        self.active_key = None
        table.hk_pending = None
        table.signal = None
        table.hk_target_reached = True
        self.wait_other_key = table.key
        log.info(f"🏁 SESIÓN TERMINADA: {table.label} — {reason}. Esperando nuevo zapato en cualquier mesa.")

        text = (
            "⏸️ <b>SESIÓN TERMINADA</b>\n\n"
            f"🕹️ Mesa: <b>{table.label}</b>\n"
            f"💰 Neto: <b>{table.hk_net_units:+d}u</b>\n"
            f"📌 Motivo: {reason}\n"
            "🔎 Ahora se monitorean todas las mesas y se esperará a que una de ellas cambie a un zapato nuevo."
        )
        async def job():
            await send_reliable(text)
        enqueue_job(job)


# ══════════════════════════════════════════════
#  WEBSOCKET PRAGMATIC (uno por mesa)
# ══════════════════════════════════════════════
class PragmaticWebSocketHandler:
    def __init__(self, table: BaccaratTable):
        self.table = table

    async def run(self):
        sub = {"type": "subscribe", "casinoId": CASINO_ID, "currency": CURRENCY_ID, "key": [self.table.key]}
        delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60, close_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    self.table.connected = True
                    log.info(f"✅ WS Pragmatic conectado ({self.table.label}, key={self.table.key})")
                    delay = 5
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue
                        try:
                            self.table.handle_msg(data)
                        except Exception as e:
                            log.exception(f"[{self.table.label}] Error procesando mensaje: {e}")
            except Exception as e:
                log.warning(f"🔌 WS {self.table.label}: {e}. Reconectando en {delay}s…")
            self.table.connected = False
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


# ══════════════════════════════════════════════
#  COMANDO /status
# ══════════════════════════════════════════════
_server_state: Optional[ServerState] = None

def build_status_message(server_state: ServerState) -> str:
    lines = [STATS.marker_message(), "", ML.training_report_text(), "",
             f"💰 Labouchere: {LAB.label()} → {LAB.stake_units()} u. (${LAB.stake_units() * LAB_UNIT})",
             "🇭🇰 Estrategia: Hong Kong 2x5 · objetivo +10u por zapato", "", "🎲 MESAS"]
    for t in server_state.tables.values():
        st = t.get_state()
        con = "🟢" if st["connected"] else "🔴"
        if st["senal"]:
            s = st["senal"]
            icon = "🧪" if st.get("simulada") else "🔔"
            lines.append(f"{con} {t.label}: {icon} {s['patron']} → {s['lado']} ({s['fase']}) · 🃏 {len(t.history_hands)} manos")
        else:
            lines.append(f"{con} {t.label}: sin señal · 🃏 {len(t.history_hands)} manos aprendidas")
    return "\n".join(lines)

if bot is not None:
    @bot.message_handler(commands=["status"])
    async def handle_status_command(message):
        if _server_state is None:
            await bot.reply_to(message, "⏳ El servidor todavía se está iniciando, intenta de nuevo en unos segundos.")
            return
        try:
            await bot.reply_to(message, build_status_message(_server_state))
        except Exception as e:
            log.warning(f"[Telegram] Error respondiendo /status: {e}")


# ══════════════════════════════════════════════
#  HTTP (aiohttp)
# ══════════════════════════════════════════════
async def http_home(request: web.Request):
    return web.json_response({"status": "ok", "service": "Baccarat Pattern Bot",
                              "mesas": list(BACCARAT_TABLES.keys())})

async def http_ping(request: web.Request):
    return web.json_response({"status": "pong", "ts": time.time()})

async def http_health(request: web.Request):
    if _server_state is None:
        return web.json_response({"status": "not_ready"}, status=503)
    return web.json_response({
        "status": "ok",
        "mesas_conectadas": [k for k, t in _server_state.tables.items() if t.connected],
        "total_senales": STATS.total,
    })

async def http_api_all(request: web.Request):
    if _server_state is None:
        return web.json_response({"error": "server not ready"}, status=503)
    return web.json_response({"stats": STATS.to_persist(),
                              "mesas": {str(k): t.get_state() for k, t in _server_state.tables.items()}})


# ══════════════════════════════════════════════
#  SELF-PING
# ══════════════════════════════════════════════
async def self_ping_loop():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url or "localhost" in render_url:
        log.info("Self-ping desactivado (no URL)")
        return
    await asyncio.sleep(30)
    log.info(f"Self-ping activo → {render_url}/ping cada {PING_INTERVAL}s")
    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        while True:
            try:
                async with session.get(f"{render_url}/ping") as resp:
                    await resp.read()
            except Exception:
                pass
            await asyncio.sleep(PING_INTERVAL)


# ══════════════════════════════════════════════
#  TELEGRAM POLLING (resiliente ante 409/errores)
# ══════════════════════════════════════════════
async def bot_polling_loop():
    """Polling para el comando /status. Un error 409 significa que hay OTRA instancia
    con el mismo BOT_TOKEN haciendo polling: hay que dejar una sola instancia corriendo."""
    if bot is None:
        return
    delay = 5
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            log.warning(f"[Telegram] No se pudo eliminar webhook antes de iniciar polling: {e}")

        started = time.time()
        try:
            await bot.infinity_polling(skip_pending=True, timeout=20, request_timeout=30)
        except Exception as e:
            log.warning(f"[Telegram] Polling interrumpido: {e}")

        ran_for = time.time() - started
        delay = 5 if ran_for > 60 else min(delay * 2, 60)
        log.warning(f"[Telegram] Reintentando polling en {delay}s…")
        await asyncio.sleep(delay)


# ══════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════
def build_http_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", http_home)
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/health", http_health)
    app.router.add_get("/api/all", http_api_all)
    return app

async def main():
    global _server_state, _tg_queue
    log.info("═" * 60)
    log.info("BOT DE PATRONES BACCARAT — PRAGMATIC PLAY")
    log.info(f"Mesas: {', '.join(f'{n} ({k})' for k, n in BACCARAT_TABLES.items())}")
    log.info("═" * 60)

    load_stats()
    log.info(f"🧩 Biblioteca de patrones cargada: {lib_load()} apariciones")
    LAB.load()
    load_ml_from_log()
    _server_state = ServerState()

    async def save_loop():
        while True:
            await asyncio.sleep(SAVE_INTERVAL)
            save_stats()

    _tg_queue = asyncio.Queue()
    tasks = [asyncio.create_task(telegram_sender_loop())]
    for table in _server_state.tables.values():
        tasks.append(asyncio.create_task(PragmaticWebSocketHandler(table).run()))
    tasks.append(asyncio.create_task(save_loop()))
    tasks.append(asyncio.create_task(self_ping_loop()))
    if bot is not None:
        tasks.append(asyncio.create_task(bot_polling_loop()))

    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(build_http_app())
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"Servidor HTTP escuchando en puerto {port}")

    try:
        await asyncio.Event().wait()
    finally:
        for t in tasks:
            t.cancel()
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Servidor detenido")
