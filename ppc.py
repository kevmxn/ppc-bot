"""
╔══════════════════════════════════════════════════════════════╗
║   RUSSIAN ROULETTE (key 221) — BOT DE SEÑALES DE COLOR        ║
║   Motor por SECUENCIA (ROJO/NEGRO) · ancla en primer NEGRO    ║
║   Sin fallos · 6 giros de espera entre señales · 3 intentos   ║
║   1 mensaje por intento · al resolver borra los anteriores    ║
║   Marcador diario (ganadas / perdidas) · SIN Labouchère       ║
║   Render-ready (Flask + self-ping)                            ║
╚══════════════════════════════════════════════════════════════
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from telebot.async_telebot import AsyncTeleBot  # pip install pyTelegramBotAPI
import websockets
from flask import Flask, jsonify

# ──────────────────────────────────────────────
#  CONFIGURACIÓN
# ──────────────────────────────────────────────
BOT_TOKEN = "8714149875:AAFJugWY0E5A4C0lrxn2bMcKsQEieqo_t5M"
CHAT_ID   = -1003254755914

WS_URL        = "wss://dga.pragmaticplaylive.net/ws"
CASINO_ID     = "ppcjd00000007254"
KEY_WS        = 221            # Russian Roulette
PING_INTERVAL = 240

AR_TZ = timezone(timedelta(hours=-3))

# ── Secuencia de COLOR a procesar (ROJO/NEGRO) ──
SEQUENCE =  ["NEGRO", "ROJO", "NEGRO", "NEGRO", "ROJO", "ROJO", "ROJO", "NEGRO", "ROJO", "ROJO",
"NEGRO", "ROJO", "NEGRO", "NEGRO"]

SYNC_MATCH   = 5         # giros consecutivos que deben coincidir con la secuencia para SINCRONIZAR
WAIT_SPINS   = 4         # giros de espera entre señales antes de emitir la siguiente
BET_ATTEMPTS = 3         # intentos reales de apuesta

MESA_NAME    = "RUSSIAN ROULETTE"
WIN_STICKER  = "CAACAgEAAyEFAATX2gFEAAInyWproeTou4bcmFvCyVDgdIAVFRrVAAI8AgACMfeZR8QcG-o23geZPQQ"
LOSS_STICKER = "CAACAgEAAyEFAATX2gFEAAInzmpronpt02LZ1RdhfUBs7ri8ddQZAAIXAgACeDWRR0B2J3UhK58rPQQ"


# ──────────────────────────────────────────────
#  LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(message)s",
                    datefmt="%H:%M:%S", handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger(__name__)
for _ln in ("werkzeug", "flask.app", "flask"):
    logging.getLogger(_ln).setLevel(logging.ERROR)


# ══════════════════════════════════════════════
#  RULETA — color real por número
# ══════════════════════════════════════════════
REAL_COLOR = {0: "VERDE", 1: "ROJO", 2: "NEGRO", 3: "ROJO", 4: "NEGRO", 5: "ROJO", 6: "NEGRO",
              7: "ROJO", 8: "NEGRO", 9: "ROJO", 10: "NEGRO", 11: "NEGRO", 12: "ROJO", 13: "NEGRO",
              14: "ROJO", 15: "NEGRO", 16: "ROJO", 17: "NEGRO", 18: "ROJO", 19: "ROJO", 20: "NEGRO",
              21: "ROJO", 22: "NEGRO", 23: "ROJO", 24: "NEGRO", 25: "ROJO", 26: "NEGRO", 27: "ROJO",
              28: "NEGRO", 29: "NEGRO", 30: "ROJO", 31: "NEGRO", 32: "ROJO", 33: "NEGRO", 34: "ROJO",
              35: "NEGRO", 36: "ROJO"}


def color_of(n):
    return REAL_COLOR.get(n, "VERDE")


# ══════════════════════════════════════════════
#  MOTOR DE SEÑALES — por secuencia (lógica del HTML de color)
#  Sincroniza fase → espera WAIT_SPINS giros → 3 intentos de apuesta.
#  Verde (0): con señal activa cuenta como GANADA; siempre rompe secuencia y re-sincroniza.
#  process(number) → lista de EVENTOS para la capa de Telegram.
# ══════════════════════════════════════════════
class SequenceColorEngine:
    def __init__(self):
        self.synced = False
        self.recent = []          # últimos colores para sincronizar la fase (ROJO/NEGRO/VERDE)
        self.armed = False
        self.seq_idx = 0
        self.attempt = 0          # intento de apuesta en curso (1..BET_ATTEMPTS)
        self.wait_count = 0       # giros de espera acumulados entre señales
        self.spins = 0
        self.last_number = None
        # marcador diario
        self.day = datetime.now(AR_TZ).date()
        self.won = 0
        self.lost = 0

    def _find_phase(self):
        """Busca una fase p donde los últimos SYNC_MATCH giros coincidan con
        SYNC_MATCH valores consecutivos de la secuencia. Devuelve p o None."""
        L = len(SEQUENCE)
        for p in range(L):
            if all(SEQUENCE[(p + k) % L] == self.recent[k] for k in range(SYNC_MATCH)):
                return p
        return None

    def check_daily_rollover(self):
        """Si cambió el día (AR), devuelve el resumen del día anterior y reinicia. Solo en vivo."""
        today = datetime.now(AR_TZ).date()
        if today != self.day:
            prev = {"date": self.day, "won": self.won, "lost": self.lost}
            self.day = today
            self.won = 0
            self.lost = 0
            return prev
        return None

    def process(self, number):
        self.spins += 1
        self.last_number = number
        real = color_of(number)
        ev = []

        # ── CERO / VERDE (0): SIEMPRE rompe la secuencia ──
        #    · Con señal activa (armed): se cuenta como GANADA en el intento en curso.
        #    · Con o sin señal activa: des-sincroniza y fuerza la RE-SINCRONIZACIÓN,
        #      porque el 0 se considera ruptura de secuencia.
        if real == "VERDE":
            if self.synced and self.armed:
                self.won += 1
                bet = SEQUENCE[self.seq_idx]
                ev.append(self._ev("win", number, bet, self.attempt))
                log.info(f"🟢 CERO con señal activa → GANADA (intento {self.attempt}) · "
                         f"HOY ✅{self.won} ❌{self.lost}")
            else:
                log.info("🟢 CERO sin señal activa → secuencia rota")
            # el 0 rompe la secuencia → re-sincronizar en TODOS los casos
            self.synced = False
            self.recent = []
            self.armed = False
            self.attempt = 0
            self.wait_count = 0
            self.seq_idx = 0
            log.info("🔄 Secuencia rota por CERO · esperando re-sincronización…")
            return ev

        # ── SINCRONIZACIÓN de fase: cuando los últimos SYNC_MATCH giros coincidan con
        #    SYNC_MATCH valores consecutivos de la secuencia, se fija la fase. Luego
        #    espera WAIT_SPINS giros y emite señal (sin fallos). ──
        if not self.synced:
            self.recent.append(real)
            if len(self.recent) > SYNC_MATCH:
                self.recent.pop(0)
            if len(self.recent) == SYNC_MATCH:
                p = self._find_phase()
                if p is not None:
                    self.synced = True
                    self.seq_idx = (p + SYNC_MATCH) % len(SEQUENCE)   # próximo giro = posición siguiente
                    self.armed = False
                    self.wait_count = 0
                    log.info(f"🔗 SINCRONIZADA en fase {p} · próximo índice {self.seq_idx} "
                             f"(coincidieron {SYNC_MATCH} giros)")
            return ev

        expected = SEQUENCE[self.seq_idx]
        match = (real == expected)
        next_idx = (self.seq_idx + 1) % len(SEQUENCE)

        if self.armed:
            if match:
                # GANA en cualquier intento → señal resuelta · vuelve a esperar WAIT_SPINS
                self.won += 1
                ev.append(self._ev("win", number, expected, self.attempt))
                self.armed = False
                self.wait_count = 0
            elif self.attempt >= BET_ATTEMPTS:
                # PIERDE el 3er intento → espera SINCRONIZACIÓN (des-sincroniza)
                self.lost += 1
                ev.append(self._ev("loss", number, expected, self.attempt))
                self.armed = False
                self.synced = False
                self.recent = []
                self.wait_count = 0
                log.info("🔄 Señal perdida · esperando re-sincronización…")
            else:
                # fallo intermedio del intento → siguiente intento (predicción para el próximo giro)
                self.attempt += 1
                ev.append(self._ev("attempt", number, SEQUENCE[next_idx], self.attempt))
        else:
            # ── ESPERA entre señales: WAIT_SPINS giros y luego emite (sin contar fallos) ──
            self.wait_count += 1
            if self.wait_count >= WAIT_SPINS:
                self.armed = True
                self.attempt = 1
                self.wait_count = 0
                ev.append(self._ev("attempt", number, SEQUENCE[next_idx], 1))

        self.seq_idx = next_idx
        return ev

    def _ev(self, action, number, bet_color, attempt):
        return {"action": action, "number": number, "bet_color": bet_color,
                "attempt": attempt, "max_att": BET_ATTEMPTS,
                "won": self.won, "lost": self.lost}


# ══════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════
class TelegramClient:
    def __init__(self):
        self._bot = AsyncTeleBot(BOT_TOKEN)

    async def send(self, chat_id, text) -> Optional[int]:
        try:
            msg = await self._bot.send_message(chat_id, text, parse_mode="HTML")
            return msg.message_id
        except Exception as e:
            log.error(f"❌ send_message: {e}")
        return None

    async def edit_text(self, chat_id, message_id, text) -> bool:
        try:
            await self._bot.edit_message_text(text=text, chat_id=chat_id,
                                              message_id=message_id, parse_mode="HTML")
            return True
        except Exception as e:
            if "not modified" in str(e):
                return True
            log.error(f"❌ edit_text mid={message_id}: {e}")
        return False

    async def delete(self, chat_id, message_id) -> bool:
        try:
            await self._bot.delete_message(chat_id, message_id)
            return True
        except Exception as e:
            log.warning(f"⚠ delete mid={message_id}: {e}")
        return False

    async def send_sticker(self, chat_id, sticker_id) -> Optional[int]:
        try:
            msg = await self._bot.send_sticker(chat_id, sticker_id)
            return msg.message_id
        except Exception as e:
            log.error(f"❌ send_sticker: {e}")
        return None


# ══════════════════════════════════════════════
#  MENSAJERÍA
# ══════════════════════════════════════════════
def _color_emoji(sig):
    return "ROJO 🔴" if sig == "ROJO" else "NEGRO ⚫" if sig == "NEGRO" else "—"


def _num_emoji(n):
    c = color_of(n)
    return "🟢" if c == "VERDE" else "🔴" if c == "ROJO" else "⚫"


def build_signal_text(ev):
    return ("<b>✅ SEÑAL CONFIRMADA ✅</b>\n\n"
            f"<b>🎰 MESA: {MESA_NAME}</b>\n"
            f"<b>➡️ ULTIMO RESULTADO: {ev['number']}</b>\n"
            f"<b>🔥 APOSTAR EN: {_color_emoji(ev['bet_color'])}</b>\n\n"
            f"<b>🔁 INTENTO {ev['attempt']} DE {ev['max_att']}</b>")


def build_marcador(won, lost):
    total = won + lost
    ef = (won / total * 100) if total else 0.0
    return ("<b>📅 MARCADOR DIARIO</b>\n"
            f"<b>✅ VICTORIAS: {won}</b>\n"
            f"<b>❌ PERDIDAS: {lost}</b>\n"
            f"<b>📈 ACIERTO: {ef:.2f}%</b>")


# ══════════════════════════════════════════════
#  BOT — conecta el motor con Telegram
#  1 mensaje por intento; al resolver borra los intentos anteriores y
#  deja sólo el último con el banner de resultado.
# ══════════════════════════════════════════════
class SignalBot:
    def __init__(self, tg: TelegramClient):
        self.tg = tg
        self.engine = SequenceColorEngine()
        self.msgs = []          # message_ids de los intentos de la señal actual (en orden)
        self.last_text = None   # texto del último intento mostrado

    async def on_number(self, number):
        roll = self.engine.check_daily_rollover()
        if roll:
            await self.tg.send(CHAT_ID, build_marcador(roll["won"], roll["lost"]))
        for ev in self.engine.process(number):
            await self._dispatch(ev)

    async def _dispatch(self, ev):
        action = ev["action"]
        if action == "attempt":
            text = build_signal_text(ev)
            mid = await self.tg.send(CHAT_ID, text)
            if mid:
                self.msgs.append(mid)
                self.last_text = text
            log.info(f"🎨 intento {ev['attempt']}/{ev['max_att']} · apostar {ev['bet_color']} · Nº{ev['number']}")

        elif action in ("win", "loss"):
            # borra los mensajes de los intentos anteriores; deja el último como la señal
            for mid in self.msgs[:-1]:
                await self.tg.delete(CHAT_ID, mid)
            # sticker de resultado
            sticker = WIN_STICKER if action == "win" else LOSS_STICKER
            await self.tg.send_sticker(CHAT_ID, sticker)
            # marcador diario (acumulado del día)
            await self.tg.send(CHAT_ID, build_marcador(ev["won"], ev["lost"]))
            self.msgs = []
            self.last_text = None
            log.info(f"🎨 {action.upper()} · Intento {ev['attempt']}/{ev['max_att']} · "
                     f"Nº{ev['number']} · HOY ✅{ev['won']} ❌{ev['lost']}")


# ══════════════════════════════════════════════
#  WEBSOCKET
# ══════════════════════════════════════════════
class WebSocketHandler:
    def __init__(self, bot: SignalBot):
        self.bot = bot
        self.seen = set()

    async def run(self):
        sub = {"type": "subscribe", "casinoId": CASINO_ID, "currency": "USD", "key": [KEY_WS]}
        delay = 5
        while True:
            try:
                async with websockets.connect(WS_URL, ping_interval=30, ping_timeout=60,
                                              close_timeout=10) as ws:
                    await ws.send(json.dumps(sub))
                    log.info(f"✅ WS conectado (Russian Roulette key={KEY_WS})")
                    delay = 5
                    batch_done = False
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(data, dict):
                            continue

                        results = data.get("last20Results")
                        if isinstance(results, list):
                            for r in reversed(results):
                                await self._feed(r.get("gameId"), r.get("result"), emit=batch_done)
                            batch_done = True

                        if data.get("gameId") is not None and data.get("result") is not None:
                            await self._feed(data.get("gameId"), data.get("result"), emit=True)

            except Exception as e:
                log.warning(f"🔌 WS: {e}. Reconectando en {delay}s…")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)

    async def _feed(self, gid, result, emit):
        if gid is None:
            return
        try:
            num = int(result)
        except (TypeError, ValueError):
            return
        if not (0 <= num <= 36) or gid in self.seen:
            return
        self.seen.add(gid)
        if len(self.seen) > 3000:
            self.seen.clear()
        if emit:
            await self.bot.on_number(num)         # señales EN VIVO
        else:
            self.bot.engine.process(num)          # batch inicial: solo alimenta el estado


# ══════════════════════════════════════════════
#  FLASK
# ══════════════════════════════════════════════
flask_app = Flask(__name__)
_bot: "Optional[SignalBot]" = None


@flask_app.route("/")
def home():
    return jsonify({"status": "ok", "bot": "Russian Roulette - Color (secuencia)", "key": KEY_WS})


@flask_app.route("/ping")
def ping():
    return jsonify({"status": "pong", "ts": time.time()})


@flask_app.route("/health")
def health():
    if _bot is None:
        return jsonify({"status": "not_ready"}), 503
    e = _bot.engine
    ar_now = datetime.now(AR_TZ).strftime("%Y-%m-%d %H:%M ART")
    return jsonify({
        "status": "ok", "ar_time": ar_now, "spins": e.spins, "last_number": e.last_number,
        "synced": e.synced, "armed": e.armed, "attempt": e.attempt,
        "seq_idx": e.seq_idx, "wait_count": e.wait_count,
        "daily": {"date": str(e.day), "won": e.won, "lost": e.lost},
    })


def run_flask():
    port = int(os.environ.get("PORT", 10000))
    flask_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


async def self_ping_loop():
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url or "localhost" in render_url:
        log.info("Self-ping desactivado (no URL)")
        return
    await asyncio.sleep(30)
    log.info(f"Self-ping activo → {render_url}/ping cada {PING_INTERVAL}s")
    while True:
        try:
            import urllib.request as _ur
            _ur.urlopen(f"{render_url}/ping", timeout=15)
        except Exception:
            pass
        await asyncio.sleep(PING_INTERVAL)


async def main():
    global _bot
    log.info("═" * 60)
    log.info("  RUSSIAN ROULETTE — BOT DE SEÑALES DE COLOR (por secuencia)")
    log.info(f"  Mesa (key): {KEY_WS} · Secuencia: {len(SEQUENCE)} pasos · "
             f"sincroniza con {SYNC_MATCH} · espera {WAIT_SPINS} giros → {BET_ATTEMPTS} intentos · Canal: {CHAT_ID}")
    log.info("═" * 60)

    tg = TelegramClient()
    _bot = SignalBot(tg)
    ar_now = datetime.now(AR_TZ).strftime("%d/%m/%Y %H:%M:%S")
    await tg.send(CHAT_ID, "<b>🎡 Russian Roulette · Señales de COLOR iniciado</b>\n"
                           f"🕐 {ar_now} (AR)\n"
                           f"🎯 Secuencia de {len(SEQUENCE)} pasos · sincroniza con {SYNC_MATCH} coincidencias · espera {WAIT_SPINS} giros → {BET_ATTEMPTS} intentos")

    tasks = [asyncio.create_task(WebSocketHandler(_bot).run()),
             asyncio.create_task(self_ping_loop())]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    time.sleep(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot detenido")
