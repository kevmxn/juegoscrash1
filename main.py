#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor unificado SLIDE + SPACEMAN con servidor HTTP para Render
- Slide: polling HTTP robusto (20 user‑agents, backoff, circuit breaker)
- Spaceman: WebSocket persistente con reconexión automática
- Servidor HTTP en el puerto de Render con endpoint /health
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import os
from typing import Set

# ============================================
# CONFIGURACIÓN SLIDE
# ============================================
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.76',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0',
    'Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300   # segundos

slide_ids: Set[str] = set()
slide_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}

# ============================================
# CONFIGURACIÓN SPACEMAN
# ============================================
SPACEMAN_WS = 'wss://dga.pragmaticplaylive.net/ws'
SPACEMAN_CASINO_ID = 'ppcdk00000005349'
SPACEMAN_CURRENCY = 'BRL'
SPACEMAN_GAME_ID = 1301

BASE_RECONNECT_DELAY = 1.0
MAX_RECONNECT_DELAY = 60.0

spaceman_last_multiplier: float = None
spaceman_events_seen: Set[str] = set()

# ============================================
# FUNCIONES SLIDE
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_slide(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()

    if now < slide_status['blocked_until']:
        wait = slide_status['blocked_until'] - now
        print(f"[SLIDE] 🚫 Bloqueado por {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    if now < slide_status['next_allowed_time']:
        wait = slide_status['next_allowed_time'] - now
        print(f"[SLIDE] ⏳ Backoff {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    # Headers más completos para simular navegador
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0',
    }

    try:
        async with session.get(API_SLIDE, headers=headers, timeout=10) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                print(f"[SLIDE] ⚠️ Esperar {retry_after}s (Retry-After)")
                return None

            if resp.status == 200:
                slide_status['consecutive_errors'] = 0
                return await resp.json()

            if resp.status == 403:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                print(f"[SLIDE] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s por exceso de errores")
                return None

            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                print(f"[SLIDE] ⚠️ Rate limit, esperar {retry_after}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s por errores")
                return None

            if 500 <= resp.status < 600:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                print(f"[SLIDE] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None

            # Otros códigos
            print(f"[SLIDE] ⚠️ Código inesperado: {resp.status}")
            slide_status['consecutive_errors'] += 1
            backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
            slide_status['next_allowed_time'] = time.time() + backoff
            if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                slide_status['blocked_until'] = time.time() + BLOCK_TIME
            return None

    except asyncio.TimeoutError:
        slide_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
        slide_status['next_allowed_time'] = time.time() + backoff
        print(f"[SLIDE] ⏰ Timeout, backoff {backoff:.1f}s")
        if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            slide_status['blocked_until'] = time.time() + BLOCK_TIME
        return None
    except Exception as e:
        slide_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
        slide_status['next_allowed_time'] = time.time() + backoff
        print(f"[SLIDE] 💥 {e}, backoff {backoff:.1f}s")
        if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            slide_status['blocked_until'] = time.time() + BLOCK_TIME
        return None

async def procesar_slide(data: dict):
    event_id = data.get('id')
    if not event_id or event_id in slide_ids:
        return

    slide_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    started_at = data_inner.get('startedAt')

    if max_mult is not None and max_mult > 0:
        print(f"[SLIDE] ✅ NUEVO: ID={event_id} | {max_mult}x | Inicio={started_at}")
    else:
        print(f"[SLIDE] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_slide():
    print("[SLIDE] 🚀 Iniciando")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_slide(session)
            if data:
                await procesar_slide(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# FUNCIONES SPACEMAN (igual que antes)
# ============================================
async def monitor_spaceman():
    global spaceman_last_multiplier
    reconnect_delay = BASE_RECONNECT_DELAY
    print("[SPACEMAN] 🚀 Iniciando")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(SPACEMAN_WS) as ws:
                    print("[SPACEMAN] ✅ Conectado")
                    subscribe_msg = {
                        "type": "subscribe",
                        "casinoId": SPACEMAN_CASINO_ID,
                        "currency": SPACEMAN_CURRENCY,
                        "key": [SPACEMAN_GAME_ID]
                    }
                    await ws.send_json(subscribe_msg)
                    print("[SPACEMAN] 📡 Suscripción enviada")
                    reconnect_delay = BASE_RECONNECT_DELAY

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = msg.json()
                                if "gameResult" in data and data["gameResult"]:
                                    result_str = data["gameResult"][0].get("result")
                                    if result_str:
                                        multiplier = float(result_str)
                                        if multiplier >= 1.00 and multiplier != spaceman_last_multiplier:
                                            spaceman_last_multiplier = multiplier
                                            game_id = data.get("gameId", "unknown")
                                            if game_id not in spaceman_events_seen:
                                                spaceman_events_seen.add(game_id)
                                                print(f"[SPACEMAN] 🚀 NUEVO: GameID={game_id} | {multiplier:.2f}x")
                                            else:
                                                print(f"[SPACEMAN] ⚠️ Duplicado: {game_id} | {multiplier:.2f}x")
                            except (json.JSONDecodeError, KeyError, ValueError, IndexError):
                                pass
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            print("[SPACEMAN] 🔌 Conexión cerrada")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"[SPACEMAN] ❌ Error: {ws.exception()}")
                            break
        except Exception as e:
            print(f"[SPACEMAN] 💥 {e}, reconexión en {reconnect_delay:.1f}s")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(MAX_RECONNECT_DELAY, reconnect_delay * 2)

# ============================================
# SERVIDOR HTTP PARA RENDER
# ============================================
async def health_handler(request):
    return web.Response(text="OK", status=200)

async def start_http_server():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', health_handler)
    port = int(os.environ.get('PORT', 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"✅ Servidor HTTP escuchando en puerto {port} (endpoint /health)")
    await asyncio.Future()

# ============================================
# MAIN
# ============================================
async def main():
    print("=" * 60)
    print("🚀 Monitor unificado SLIDE + SPACEMAN con servidor HTTP")
    print("=" * 60)

    tasks = [
        asyncio.create_task(start_http_server(), name="HTTP"),
        asyncio.create_task(monitor_slide(), name="Slide"),
        asyncio.create_task(monitor_spaceman(), name="Spaceman"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\n⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("✅ Detenido")

if __name__ == "__main__":
    asyncio.run(main())
