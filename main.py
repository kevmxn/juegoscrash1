#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SLIDE (Stake) con servidor HTTP para Render
- Slide: polling HTTP robusto (20 user‑agents, backoff, circuit breaker)
- Servidor HTTP en el puerto de Render con endpoint /health y /ws
- Logs mejorados con timestamps y niveles
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import os
import logging
from typing import Set

# ============================================
# CONFIGURACIÓN DE LOGGING
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

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
# FUNCIONES SLIDE
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_slide(session: aiohttp.ClientSession) -> dict | None:
    now = time.time()

    if now < slide_status['blocked_until']:
        wait = slide_status['blocked_until'] - now
        logger.info(f"[SLIDE] 🚫 Bloqueado por {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    if now < slide_status['next_allowed_time']:
        wait = slide_status['next_allowed_time'] - now
        logger.info(f"[SLIDE] ⏳ Backoff {wait:.1f}s")
        await asyncio.sleep(wait)
        return None

    # Headers sin br (brotli) para evitar error de decodificación
    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',  # IMPORTANTE: eliminamos 'br'
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
                logger.warning(f"[SLIDE] ⚠️ Esperar {retry_after}s (Retry-After)")
                return None

            if resp.status == 200:
                slide_status['consecutive_errors'] = 0
                return await resp.json()

            if resp.status == 403:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[SLIDE] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s por exceso de errores")
                return None

            if resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                logger.warning(f"[SLIDE] ⚠️ Rate limit, esperar {retry_after}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s por errores")
                return None

            if 500 <= resp.status < 600:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[SLIDE] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None

            logger.warning(f"[SLIDE] ⚠️ Código inesperado: {resp.status}")
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
        logger.error(f"[SLIDE] ⏰ Timeout, backoff {backoff:.1f}s")
        if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            slide_status['blocked_until'] = time.time() + BLOCK_TIME
        return None
    except Exception as e:
        slide_status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
        slide_status['next_allowed_time'] = time.time() + backoff
        logger.error(f"[SLIDE] 💥 Excepción: {e}")
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
        logger.info(f"[SLIDE] ✅ NUEVO: ID={event_id} | {max_mult}x | Inicio={started_at}")
        return max_mult
    else:
        logger.warning(f"[SLIDE] ⚠️ ID {event_id} mult inválido: {max_mult}")
        return None

async def monitor_slide():
    logger.info("[SLIDE] 🚀 Iniciando monitor")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_slide(session)
            if data:
                await procesar_slide(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# SERVIDOR HTTP + WEBSOCKET (aiohttp)
# ============================================
connected_clients: Set[web.WebSocketResponse] = set()

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        logger.info("Cliente WebSocket conectado a Slide")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
        logger.info("Cliente WebSocket desconectado de Slide")
    return ws

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def root_handler(request):
    return web.Response(text="Servidor Slide activo. Use /ws para WebSocket o /health para health check.", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', root_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"✅ Servidor HTTP/WebSocket escuchando en puerto {port}")
    await asyncio.Future()

# ============================================
# BROADCAST PARA CLIENTES (opcional)
# ============================================
async def broadcast_slide(event_data: dict):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

# ============================================
# MAIN
# ============================================
async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor exclusivo de SLIDE con servidor HTTP")
    logger.info("=" * 60)

    # Crear directorio para posibles archivos
    os.makedirs("data", exist_ok=True)

    tasks = [
        asyncio.create_task(start_web_server(), name="HTTP"),
        asyncio.create_task(monitor_slide(), name="Slide"),
    ]

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo monitor...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("✅ Monitor detenido.")

if __name__ == "__main__":
    asyncio.run(main())
