#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monitor exclusivo para SLIDE (Stake)
Obtiene eventos de https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest
Con mecanismos anti-bloqueo: user-agents rotativos, backoff exponencial, circuit breaker.
"""

import asyncio
import aiohttp
import json
import time
import random
from typing import Set, Dict, Any

# ============================================
# CONFIGURACIÓN
# ============================================
API_SLIDE = 'https://api-cs.casino.org/svc-evolution-game-events/api/stakeslide/latest'

# 20 User-Agents realistas
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

# Estado
slide_ids: Set[str] = set()
api_status = {
    'slide': {'consecutive_errors': 0, 'next_allowed_time': 0, 'blocked_until': 0}
}

# ============================================
# FUNCIONES DE BACKOFF Y USER-AGENT
# ============================================
def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def consultar_slide(session: aiohttp.ClientSession) -> dict | None:
    status = api_status['slide']
    now = time.time()

    # Circuit breaker
    if now < status['blocked_until']:
        wait = status['blocked_until'] - now
        print(f"🚫 SLIDE bloqueado por {wait:.1f}s (demasiados errores)")
        await asyncio.sleep(wait)
        return None

    # Backoff activo
    if now < status['next_allowed_time']:
        wait = status['next_allowed_time'] - now
        print(f"⏳ SLIDE en espera por {wait:.1f}s (backoff)")
        await asyncio.sleep(wait)
        return None

    headers = {'User-Agent': get_random_user_agent()}
    try:
        async with session.get(API_SLIDE, headers=headers, timeout=5) as resp:
            if 'Retry-After' in resp.headers:
                retry_after = int(resp.headers['Retry-After'])
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ SLIDE pide esperar {retry_after}s")
                return None

            if resp.status == 200:
                status['consecutive_errors'] = 0
                return await resp.json()
            elif resp.status == 429:
                retry_after = int(resp.headers.get('Retry-After', 2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + retry_after
                status['consecutive_errors'] += 1
                print(f"⚠️ SLIDE rate limit. Esperando {retry_after}s")
                if status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"🔒 SLIDE bloqueado por {BLOCK_TIME}s por exceso de errores")
                return None
            elif 500 <= resp.status < 600:
                status['consecutive_errors'] += 1
                backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
                status['next_allowed_time'] = time.time() + backoff
                print(f"❌ SLIDE error {resp.status}. Backoff {backoff:.1f}s")
                if status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
                    status['blocked_until'] = time.time() + BLOCK_TIME
                    print(f"🔒 SLIDE bloqueado por {BLOCK_TIME}s por exceso de errores")
                return None
            else:
                print(f"⚠️ SLIDE código no esperado: {resp.status}")
                return None
    except asyncio.TimeoutError:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"⏰ SLIDE timeout. Backoff {backoff:.1f}s")
        if status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            status['blocked_until'] = time.time() + BLOCK_TIME
            print(f"🔒 SLIDE bloqueado por {BLOCK_TIME}s por exceso de errores")
        return None
    except Exception as e:
        status['consecutive_errors'] += 1
        backoff = min(MAX_SLEEP, BASE_SLEEP * (2 ** status['consecutive_errors']))
        status['next_allowed_time'] = time.time() + backoff
        print(f"💥 SLIDE error: {e}. Backoff {backoff:.1f}s")
        if status['consecutive_errors'] >= MAX_CONSECUTIVE_ERRORS:
            status['blocked_until'] = time.time() + BLOCK_TIME
            print(f"🔒 SLIDE bloqueado por {BLOCK_TIME}s por exceso de errores")
        return None

# ============================================
# PROCESAMIENTO DE EVENTOS
# ============================================
async def procesar_evento(data: dict):
    event_id = data.get('id')
    if not event_id or event_id in slide_ids:
        return False

    slide_ids.add(event_id)
    data_inner = data.get('data', {})
    result = data_inner.get('result', {})
    max_mult = result.get('maxMultiplier')
    started_at = data_inner.get('startedAt')

    if max_mult is not None and max_mult > 0:
        print(f"✅ SLIDE nuevo: ID={event_id} | Multiplicador={max_mult}x | Inicio={started_at}")
        return True
    else:
        print(f"⚠️ SLIDE ID {event_id} con multiplicador inválido: {max_mult}")
        return False

# ============================================
# MONITOR PRINCIPAL
# ============================================
async def monitor_slide():
    print("🚀 Iniciando monitor exclusivo de SLIDE (Stake)")
    async with aiohttp.ClientSession() as session:
        while True:
            data = await consultar_slide(session)
            if data:
                await procesar_evento(data)
            # Espera aleatoria entre 0.5 y 1.5 segundos para evitar patrones
            await asyncio.sleep(random.uniform(0.5, 1.5))

# ============================================
# EJECUCIÓN
# ============================================
if __name__ == "__main__":
    try:
        asyncio.run(monitor_slide())
    except KeyboardInterrupt:
        print("\n⏹ Monitor SLIDE detenido por el usuario.")
