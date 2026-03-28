#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SLIDE con servidor HTTP y WebSocket completo
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import time
import random
import os
import logging
from datetime import datetime
from typing import Set, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

USER_AGENTS = [  # (igual que antes)
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    # ... (todos los 20 user agents) ...
]
BASE_SLEEP = 1.0
MAX_SLEEP = 60.0
MAX_CONSECUTIVE_ERRORS = 10
BLOCK_TIME = 300

slide_ids: Set[str] = set()
slide_status = {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}
slide_history: list = []
MAX_HISTORY = 15000

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_slide(session: aiohttp.ClientSession) -> dict | None:
    # (misma lógica que crash, con API_SLIDE)
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

    headers = {
        'User-Agent': get_random_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
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
            elif resp.status == 403:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.warning(f"[SLIDE] 🚫 403 Forbidden - backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                    logger.error(f"[SLIDE] 🔒 Bloqueado {BLOCK_TIME}s")
                return None
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + retry_after
                slide_status['consecutive_errors'] += 1
                logger.warning(f"[SLIDE] ⚠️ Rate limit, esperar {retry_after}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            elif 500 <= resp.status < 600:
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
                logger.error(f"[SLIDE] ❌ Error {resp.status}, backoff {backoff:.1f}s")
                if slide_status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    slide_status['blocked_until'] = time.time() + BLOCK_TIME
                return None
            else:
                logger.warning(f"[SLIDE] ⚠️ Código inesperado: {resp.status}")
                slide_status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** slide_status['consecutive_errors']))
                slide_status['next_allowed_time'] = time.time() + backoff
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
    global slide_history
    event_id = data.get('id')
    if not event_id or event_id in slide_ids:
        return
    slide_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    started_at = data_inner.get('startedAt')
    if max_mult is not None and max_mult > 0:
        evento = {
            'event_id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': datetime.now().isoformat()
        }
        slide_history.insert(0, evento)
        if len(slide_history) > MAX_HISTORY:
            slide_history.pop()
        logger.info(f"[SLIDE] ✅ NUEVO: ID={event_id} | {max_mult}x | Inicio={started_at}")
        await broadcast({
            'tipo': 'slide',
            'id': event_id,
            'maxMultiplier': max_mult,
            'startedAt': started_at,
            'timestamp_recepcion': evento['timestamp_recepcion']
        })
    else:
        logger.warning(f"[SLIDE] ⚠️ ID {event_id} mult inválido: {max_mult}")

async def monitor_slide():
    logger.info("[SLIDE] 🚀 Iniciando monitor")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_slide(session)
            if data:
                await procesar_slide(data)
            await asyncio.sleep(random.uniform(0.5, 1.5))

# Servidor WebSocket
connected_clients: Set[web.WebSocketResponse] = set()

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    connected_clients.add(ws)
    try:
        if slide_history:
            await ws.send_json({
                'tipo': 'historial',
                'api': 'slide',
                'eventos': slide_history
            })
        logger.info("Cliente Slide conectado, historial enviado")
        async for msg in ws:
            if msg.type == web.WSMsgType.CLOSE:
                break
    finally:
        connected_clients.remove(ws)
    return ws

async def broadcast(event_data: Dict[str, Any]):
    if not connected_clients:
        return
    message = json.dumps(event_data, default=str)
    await asyncio.gather(
        *[client.send_str(message) for client in connected_clients],
        return_exceptions=True
    )

async def health_handler(request):
    return web.Response(text="OK", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/ws', websocket_handler)
    app.router.add_get('/health', health_handler)
    app.router.add_get('/', lambda r: web.Response(text="Servidor Slide activo."))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"✅ Servidor Slide escuchando en puerto {port}")
    await asyncio.Future()

async def main():
    logger.info("=" * 60)
    logger.info("🚀 Monitor exclusivo de SLIDE con WebSocket")
    logger.info("=" * 60)
    tasks = [
        asyncio.create_task(start_web_server()),
        asyncio.create_task(monitor_slide()),
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹ Deteniendo...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
