import os
import re
import json
import uuid
import time
import hashlib
import logging
import asyncio
import random
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import contextvars

import redis.asyncio as redis
import httpx
import uvicorn
import chromadb
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from fastapi import FastAPI, Request, HTTPException, Header, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings
from sentence_transformers import SentenceTransformer
from openai import AsyncOpenAI

# ============================================================
# SETTINGS & TOPOLOGY
# ============================================================

class Settings(BaseSettings):
    # تم إضافة المفاتيح هنا كقيم افتراضية
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw")
    deepseek_api_key: str = os.getenv("DEEPSEEK_API_KEY", "sk-e60a2a3169954be082f4ed96190610e1")
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "93850ca6e4mshc965f580ee18a04p16301djsn87885afe8ab2")
    booking_aff_id: str = os.getenv("BOOKING_AFF_ID", "")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "CHANGE_ME_SECURELY")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Topology
    run_api_tier: bool = os.getenv("RUN_API_TIER", "True").lower() == "true"
    run_worker_tier: bool = os.getenv("RUN_WORKER_TIER", "True").lower() == "true"
    
    # Advanced Queue & Load Limits
    queue_shards: int = 5 
    safe_queue_depth: int = 5000
    max_queue_depth: int = 10000
    worker_concurrency: int = 10
    workflow_timeout: float = 28.0
    visibility_timeout: float = 30.0 # Time before reaper assumes worker crash

settings = Settings()

# ============================================================
# OBSERVABILITY & TRACING CONTEXT
# ============================================================

trace_id_var = contextvars.ContextVar("trace_id", default="system")
span_id_var = contextvars.ContextVar("span_id", default="root")
deadline_var = contextvars.ContextVar("deadline", default=0.0)

def get_remaining_timeout() -> float:
    return max(0.1, deadline_var.get() - time.monotonic())

class JSONLogFormatter(logging.Formatter):
    def format(self, record):
        msg = re.sub(r"(sk-[a-zA-Z0-9]{20,}|rapidapi-key:[a-zA-Z0-9]+)", "[REDACTED]", record.getMessage())
        return json.dumps({
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "trace_id": trace_id_var.get(),
            "span_id": span_id_var.get(),
            "message": msg
        })

logger = logging.getLogger("platform")
handler = logging.StreamHandler()
handler.setFormatter(JSONLogFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

REQ_COUNT = Counter("api_requests_total", "Requests", ["endpoint", "status"])

# ============================================================
# EXCEPTIONS
# ============================================================

class RetryableError(Exception): pass
class NonRetryableError(Exception): pass

# ============================================================
# GLOBAL STATE
# ============================================================

app_state = {
    "redis": None,
    "http_clients": {},
    "ai_client": None,
    "thread_pool": None,
    "worker_tasks": [],
    "reaper_task": None,
    "shutdown_event": asyncio.Event()
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    app_state["thread_pool"] = ThreadPoolExecutor(max_workers=2)
    
    try:
        app_state["redis"] = redis.from_url(settings.redis_url, decode_responses=True)
        await app_state["redis"].ping()
    except Exception as e:
        logger.critical(f"Redis initialization failed: {e}. Platform degraded.")
    
    # Mock for Qdrant/Weaviate (Chroma is blocking in Multi-Process)
    # chromadb.PersistentClient() -> Replace with async VectorDB client in real cluster

    app_state["http_clients"]["telegram"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
    app_state["http_clients"]["booking"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=20))
    app_state["ai_client"] = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url="https://api.deepseek.com")

    if settings.run_worker_tier:
        logger.info(f"Booting Worker Tier ({settings.worker_concurrency} Consumers over {settings.queue_shards} Shards)")
        for i in range(settings.worker_concurrency):
            # Workers distribute themselves across shards for fairness
            shard_id = i % settings.queue_shards
            task = asyncio.create_task(reliable_queue_consumer(f"worker_{i}", shard_id))
            app_state["worker_tasks"].append(task)
        app_state["reaper_task"] = asyncio.create_task(queue_reaper())

    yield

    app_state["shutdown_event"].set()
    if settings.run_worker_tier:
        await asyncio.gather(*app_state["worker_tasks"], return_exceptions=True)
        if app_state["reaper_task"]: app_state["reaper_task"].cancel()

    for c in app_state["http_clients"].values(): await c.aclose()
    if app_state["redis"]: await app_state["redis"].close()
    app_state["thread_pool"].shutdown(wait=True)

app = FastAPI(title="Distributed Control Plane", lifespan=lifespan)

# ============================================================
# RATIO-BASED CIRCUIT BREAKER (SRE Grade)
# ============================================================

class RatioCircuitBreaker:
    def __init__(self, name: str, window: int, min_volume: int, max_failure_ratio: float):
        self.prefix = f"cb:ratio:{name}"
        self.window = window
        self.min_volume = min_volume
        self.max_ratio = max_failure_ratio

    async def _clean_and_count(self, key: str, now: float) -> int:
        r = app_state["redis"]
        await r.zremrangebyscore(key, "-inf", now - self.window)
        return await r.zcard(key)

    async def can_execute(self) -> bool:
        r = app_state["redis"]
        if not r: return True
        now = time.time()
        
        successes = await self._clean_and_count(f"{self.prefix}:ok", now)
        failures = await self._clean_and_count(f"{self.prefix}:fail", now)
        total = successes + failures
        
        if total >= self.min_volume:
            ratio = failures / total
            if ratio >= self.max_ratio:
                logger.warning(f"Circuit {self.prefix} OPEN (Ratio: {ratio:.2f})")
                return False
        return True

    async def record(self, success: bool):
        r = app_state["redis"]
        if not r: return
        now = time.time()
        key = f"{self.prefix}:ok" if success else f"{self.prefix}:fail"
        await r.zadd(key, {f"{now}:{random.random()}": now})
        await r.expire(key, self.window * 2)

cb_ai = RatioCircuitBreaker("ai", window=60, min_volume=20, max_failure_ratio=0.5)

# ============================================================
# EFFECTIVELY-ONCE ZSET QUEUE & HEARTBEATS
# ============================================================

class QueueMessage(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    payload: Dict[str, Any]
    retries: int = 0

async def renew_visibility(task_id: str, shard_id: int):
    """Heartbeat task extending visibility timeout while processing."""
    r = app_state["redis"]
    try:
        while True:
            await asyncio.sleep(settings.visibility_timeout / 2)
            new_deadline = time.time() + settings.visibility_timeout
            await r.zadd(f"queue:shard:{shard_id}:processing", {task_id: new_deadline})
    except asyncio.CancelledError:
        pass

async def reliable_queue_consumer(worker_id: str, shard_id: int):
    r = app_state["redis"]
    q_pending = f"queue:shard:{shard_id}:pending"
    q_processing = f"queue:shard:{shard_id}:processing"
    h_payloads = f"queue:shard:{shard_id}:payloads"
    q_dlq = "queue:dlq" # Global DLQ

    while not app_state["shutdown_event"].is_set():
        heartbeat_task = None
        task_id = None
        try:
            # 1. Pop from pending list
            raw_msg = await r.bpop(q_pending, timeout=2) # Returns tuple (list_name, element) or None
            if not raw_msg: continue
            
            msg = QueueMessage.model_validate_json(raw_msg[1])
            task_id = msg.task_id
            
            # 2. Add to ZSET processing & Hash (Atomically if possible, pipelined here)
            async with r.pipeline() as pipe:
                deadline = time.time() + settings.visibility_timeout
                pipe.zadd(q_processing, {task_id: deadline})
                pipe.hset(h_payloads, task_id, raw_msg[1])
                await pipe.execute()

            # 3. Start Heartbeat & Execute
            heartbeat_task = asyncio.create_task(renew_visibility(task_id, shard_id))
            
            success = False
            try:
                await process_workflow(msg.payload, task_id)
                success = True
            except NonRetryableError as e:
                logger.error(f"[{task_id}] NonRetryable: {e}")
            except Exception as e:
                logger.error(f"[{task_id}] Retryable: {e}")

            # 4. Acknowledgment (DLQ or Success)
            heartbeat_task.cancel()
            
            async with r.pipeline() as pipe:
                pipe.zrem(q_processing, task_id)
                pipe.hdel(h_payloads, task_id)
                
                if not success:
                    msg.retries += 1
                    if msg.retries >= 3:
                        logger.warning(f"Task {task_id} poisoned. Moving to DLQ.")
                        pipe.lpush(q_dlq, msg.model_dump_json())
                    else:
                        logger.info(f"Task {task_id} Re-queued (Attempt {msg.retries}).")
                        pipe.rpush(q_pending, msg.model_dump_json())
                await pipe.execute()

        except asyncio.CancelledError:
            # Graceful Worker Shutdown handling
            logger.info(f"Worker {worker_id} cancelling. Safely re-queuing {task_id}.")
            if heartbeat_task: heartbeat_task.cancel()
            if task_id and raw_msg:
                async with r.pipeline() as pipe:
                    pipe.zrem(q_processing, task_id)
                    pipe.hdel(h_payloads, task_id)
                    pipe.lpush(q_pending, raw_msg[1]) # Put back at FRONT
                    await pipe.execute()
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} Crashed: {e}")
            await asyncio.sleep(1)

async def queue_reaper():
    """O(log N) Reaper scanning only expired items using ZRANGEBYSCORE"""
    r = app_state["redis"]
    while not app_state["shutdown_event"].is_set():
        try:
            await asyncio.sleep(10)
            now = time.time()
            
            for shard_id in range(settings.queue_shards):
                q_processing = f"queue:shard:{shard_id}:processing"
                h_payloads = f"queue:shard:{shard_id}:payloads"
                q_pending = f"queue:shard:{shard_id}:pending"
                
                # O(log N) fetch of only expired tasks
                expired_tasks = await r.zrangebyscore(q_processing, "-inf", now)
                
                for task_id in expired_tasks:
                    logger.warning(f"Reaper: Recovering dead task {task_id} on shard {shard_id}")
                    payload = await r.hget(h_payloads, task_id)
                    
                    if payload:
                        async with r.pipeline() as pipe:
                            pipe.zrem(q_processing, task_id)
                            pipe.hdel(h_payloads, task_id)
                            pipe.lpush(q_pending, payload)
                            await pipe.execute()
                    else:
                        # Orphaned score without payload
                        await r.zrem(q_processing, task_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Reaper Error: {e}")

# ============================================================
# WORKFLOW ORCHESTRATION & IDEMPOTENCY
# ============================================================

@asynccontextmanager
async def distributed_concurrency_lease(name: str, max_concurrent: int, timeout: int = 30):
    """Effectively limits global concurrency without Token leak (using simple sets/counters safely)"""
    r = app_state["redis"]
    client_id = str(uuid.uuid4())
    lease_key = f"lease:{name}"
    
    # Clean expired leases safely (simple approach)
    await r.zremrangebyscore(lease_key, "-inf", time.time())
    
    active = await r.zcard(lease_key)
    if active >= max_concurrent:
        raise RetryableError("Concurrency Limit Reached")
        
    await r.zadd(lease_key, {client_id: time.time() + timeout})
    try:
        yield
    finally:
        await r.zrem(lease_key, client_id)

async def process_workflow(payload: dict, task_id: str):
    trace_id_var.set(payload.get("trace_id", str(uuid.uuid4())))
    span_id_var.set(task_id)
    deadline_var.set(time.monotonic() + settings.workflow_timeout)

    r = app_state["redis"]
    
    # 1. Effectively-Once Idempotency Check (Check before Side-Effects)
    idem_key = f"task:done:{task_id}"
    if r and await r.get(idem_key):
        logger.info("Task already completed. Skipping side-effects.")
        return

    chat_id = payload["chat_id"]
    text = payload["text"]
    retries_left = max(1, 3 - payload.get("retries", 0))

    try:
        if not await cb_ai.can_execute():
            raise RetryableError("AI Circuit Open")

        async with distributed_concurrency_lease("ai_calls", max_concurrent=20):
            # 2. Smart Retry Deadline Budgeting
            allocated_timeout = get_remaining_timeout() / retries_left
            
            resp = await app_state["ai_client"].chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": text[:500]}],
                timeout=allocated_timeout
            )
            await cb_ai.record(success=True)

        # 3. Side Effects (Telegram API simulated)
        await app_state["http_clients"]["telegram"].post(
            f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
            json={"chat_id": chat_id, "text": "تمت المعالجة."},
            timeout=get_remaining_timeout()
        )

        # 4. Commit Idempotency Mark Atomically Post-Success
        if r: await r.set(idem_key, "1", ex=86400)

    except Exception as e:
        await cb_ai.record(success=False)
        raise RetryableError(f"Workflow Interrupted: {e}")

# ============================================================
# API TIER & ADAPTIVE LOAD SHEDDING
# ============================================================

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(default="")):
    if not settings.run_api_tier: raise HTTPException(404, "API Disabled")
    if x_telegram_bot_api_secret_token != settings.webhook_secret: raise HTTPException(403, "Unauthorized")

    try:
        payload = await req.json()
        chat_id = payload["message"]["chat"]["id"]
        
        r = app_state["redis"]
        if r:
            # 1. Tenant Sharding (Fairness)
            shard_id = hash(str(chat_id)) % settings.queue_shards
            q_pending = f"queue:shard:{shard_id}:pending"
            
            # 2. Adaptive Probabilistic Load Shedding
            q_len = await r.llen(q_pending)
            if q_len > settings.safe_queue_depth:
                drop_prob = min(0.95, (q_len - settings.safe_queue_depth) / (settings.max_queue_depth - settings.safe_queue_depth))
                if random.random() < drop_prob:
                    logger.warning(f"Load Shedding active. Dropped webhook for shard {shard_id} (Prob: {drop_prob:.2f})")
                    raise HTTPException(503, "Queue Saturated - Adaptive Drop")

            # 3. Enqueue
            task = QueueMessage(payload={"chat_id": chat_id, "text": payload["message"]["text"]})
            await r.lpush(q_pending, task.model_dump_json())

        return JSONResponse({"status": "ok"})
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Ingress Failure: {e}")
        # Graceful degradation: If Redis is down, return 500 to trigger upstream webhook retries
        return JSONResponse({"status": "error"}, status_code=500)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1 if settings.run_worker_tier else 4)
