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
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from fastapi import FastAPI, Request, HTTPException, Header, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings
from openai import AsyncOpenAI

# ============================================================
# SETTINGS & TOPOLOGY
# ============================================================

class Settings(BaseSettings):
    # مفاتيح العمليات والربط الفعلي
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw")
    deepseek_api_key: str = os.getenv("DEEPSEEK_API_KEY", "sk-e60a2a3169954be082f4ed96190610e1")
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "93850ca6e4mshc965f580ee18a04p16301djsn87885afe8ab2")
    booking_aff_id: str = os.getenv("BOOKING_AFF_ID", "")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "CHANGE_ME_SECURELY")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    # Process Topology (فصل الطبقات)
    run_api_tier: bool = os.getenv("RUN_API_TIER", "True").lower() == "true"
    run_worker_tier: bool = os.getenv("RUN_WORKER_TIER", "True").lower() == "true"
    
    # حدود الطوابير والتحكم بالضغط
    queue_shards: int = 5 
    safe_queue_depth: int = 5000
    max_queue_depth: int = 10000
    worker_concurrency: int = 10
    workflow_timeout: float = 28.0
    visibility_timeout: float = 30.0 # الوقت المتاح للـ Worker قبل افتراض انهياره

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
# DATA TRANSFER OBJECTS (DTOs)
# ============================================================

class HotelSearchRequest(BaseModel):
    city: str = Field(..., max_length=50)
    check_in: str
    check_out: str
    guests: int = Field(ge=1, le=10)

class HotelDTO(BaseModel):
    name: str
    price: float
    currency: str = "SAR"
    rating: float = 0.0
    url: str

    @classmethod  
    def from_booking_raw(cls, raw: dict) -> Optional['HotelDTO']:  
        try:  
            price = raw.get("price_breakdown", {}).get("gross_price", {}).get("value", 0)
            return cls(  
                name=raw.get("hotel_name", "Unknown"), price=float(price), 
                currency=raw.get("price_breakdown", {}).get("gross_price", {}).get("currency", "SAR"),  
                rating=float(raw.get("review_score", {}).get("score", 0)), url=raw.get("url", "#")  
            )  
        except Exception: return None

class QueueMessage(BaseModel):
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    payload: Dict[str, Any]
    retries: int = 0

# ============================================================
# GLOBAL STATE & LIFESPAN
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

    app_state["http_clients"]["telegram"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
    app_state["http_clients"]["booking"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=20))
    app_state["ai_client"] = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url="https://api.deepseek.com")

    if settings.run_worker_tier:
        logger.info(f"Booting Worker Tier ({settings.worker_concurrency} Consumers over {settings.queue_shards} Shards)")
        for i in range(settings.worker_concurrency):
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
cb_booking = RatioCircuitBreaker("booking", window=60, min_volume=10, max_failure_ratio=0.5)

# ============================================================
# EFFECTIVELY-ONCE ZSET QUEUE & HEARTBEATS
# ============================================================

async def renew_visibility(task_id: str, shard_id: int):
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
    q_dlq = "queue:dlq" 

    while not app_state["shutdown_event"].is_set():
        heartbeat_task = None
        task_id = None
        try:
            raw_msg = await r.bpop(q_pending, timeout=2) 
            if not raw_msg: continue
            
            msg = QueueMessage.model_validate_json(raw_msg[1])
            task_id = msg.task_id
            
            async with r.pipeline() as pipe:
                deadline = time.time() + settings.visibility_timeout
                pipe.zadd(q_processing, {task_id: deadline})
                pipe.hset(h_payloads, task_id, raw_msg[1])
                await pipe.execute()

            heartbeat_task = asyncio.create_task(renew_visibility(task_id, shard_id))
            
            success = False
            try:
                await process_workflow(msg.payload, task_id)
                success = True
            except NonRetryableError as e:
                logger.error(f"[{task_id}] NonRetryable: {e}")
            except Exception as e:
                logger.error(f"[{task_id}] Retryable: {e}")

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
            logger.info(f"Worker {worker_id} cancelling. Safely re-queuing {task_id}.")
            if heartbeat_task: heartbeat_task.cancel()
            if task_id and raw_msg:
                async with r.pipeline() as pipe:
                    pipe.zrem(q_processing, task_id)
                    pipe.hdel(h_payloads, task_id)
                    pipe.lpush(q_pending, raw_msg[1]) 
                    await pipe.execute()
            break
        except Exception as e:
            logger.error(f"Worker {worker_id} Crashed: {e}")
            await asyncio.sleep(1)

async def queue_reaper():
    r = app_state["redis"]
    while not app_state["shutdown_event"].is_set():
        try:
            await asyncio.sleep(10)
            now = time.time()
            
            for shard_id in range(settings.queue_shards):
                q_processing = f"queue:shard:{shard_id}:processing"
                h_payloads = f"queue:shard:{shard_id}:payloads"
                q_pending = f"queue:shard:{shard_id}:pending"
                
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
                        await r.zrem(q_processing, task_id)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Reaper Error: {e}")

# ============================================================
# EXTERNAL SERVICES LOGIC (Booking & Leases)
# ============================================================

@asynccontextmanager
async def distributed_concurrency_lease(name: str, max_concurrent: int, timeout: int = 30):
    r = app_state["redis"]
    client_id = str(uuid.uuid4())
    lease_key = f"lease:{name}"
    
    await r.zremrangebyscore(lease_key, "-inf", time.time())
    
    active = await r.zcard(lease_key)
    if active >= max_concurrent:
        raise RetryableError("Concurrency Limit Reached")
        
    await r.zadd(lease_key, {client_id: time.time() + timeout})
    try:
        yield
    finally:
        await r.zrem(lease_key, client_id)

async def search_hotels_logic(data: HotelSearchRequest) -> List[HotelDTO]:
    key = hashlib.md5(f"{data.city}:{data.check_in}:{data.check_out}:{data.guests}".encode()).hexdigest()
    
    r = app_state["redis"]
    if r:
        cached = await r.get(f"booking:{key}")
        if cached: return [HotelDTO(**h) for h in json.loads(cached) if HotelDTO.from_booking_raw(h)]

    if not await cb_booking.can_execute(): 
        raise RetryableError("Booking Service unavailable.")  

    headers = {"X-RapidAPI-Key": settings.rapidapi_key, "X-RapidAPI-Host": "booking-com15.p.rapidapi.com"}  
    client = app_state["http_clients"]["booking"]

    try:  
        dest_resp = await client.get("https://booking-com15.p.rapidapi.com/api/v1/hotels/searchDestination", headers=headers, params={"query": data.city})  
        dest_resp.raise_for_status()  
        dest_data = dest_resp.json().get("data", [])
        if not dest_data: raise NonRetryableError("City not found")  
        dest_id = dest_data[0].get("dest_id")

        hotels_resp = await client.get(
            "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels",  
            headers=headers,  
            params={"dest_id": dest_id, "search_type": "CITY", "arrival_date": data.check_in, "departure_date": data.check_out, "adults": data.guests, "page_number": "1", "currency_code": "SAR"}  
        )  
        hotels_resp.raise_for_status()  
        
        raw_hotels = hotels_resp.json().get("data", {}).get("hotels", [])  
        normalized = [dto for h in raw_hotels if (dto := HotelDTO.from_booking_raw(h))]
        
        if r: await r.setex(f"booking:{key}", 1800, json.dumps(raw_hotels[:10])) 
        await cb_booking.record(success=True)
        return normalized  
        
    except httpx.HTTPStatusError as e:
        await cb_booking.record(success=False)
        if e.response.status_code in (400, 401, 403): raise NonRetryableError("Auth/Bad Request")
        raise RetryableError("Upstream API Error")
    except Exception:  
        await cb_booking.record(success=False)
        raise RetryableError("Booking API Error")

# ============================================================
# WORKFLOW ORCHESTRATION & IDEMPOTENCY
# ============================================================

async def process_workflow(payload: dict, task_id: str):
    trace_id_var.set(payload.get("trace_id", str(uuid.uuid4())))
    span_id_var.set(task_id)
    deadline_var.set(time.monotonic() + settings.workflow_timeout)

    r = app_state["redis"]
    idem_key = f"task:done:{task_id}"
    if r and await r.get(idem_key):
        logger.info("Task already completed. Skipping side-effects.")
        return

    chat_id = payload["chat_id"]
    text = payload["text"]
    retries_left = max(1, 3 - payload.get("retries", 0))
    telegram_client = app_state["http_clients"]["telegram"]

    # --- 1. استخراج النوايا بواسطة الذكاء الاصطناعي ---
    try:
        if not await cb_ai.can_execute(): raise RetryableError("AI Circuit Open")

        async with distributed_concurrency_lease("ai_calls", max_concurrent=20):
            prompt = """أنت مساعد سفر ذكي ومحترف. استخرج معلومات الحجز ورجع JSON فقط يحتوي على الحقول التالية:
            intent (يجب أن يكون 'search' إذا ذكر مدينة أو طلب فندق، و 'unknown' لغير ذلك), 
            city (اسم المدينة بالإنجليزية دائماً مثل Al Ahsa أو Dubai), 
            check_in (تاريخ اليوم بالصيغة YYYY-MM-DD), 
            check_out (تاريخ الغد بالصيغة YYYY-MM-DD), 
            guests (عدد الضيوف كعدد صحيح، الافتراضي 1), 
            error (أي رسالة خطأ إذا كانت البيانات ناقصة تماماً)
            
            ملاحظة هامة: إذا أرسل المستخدم اسم مدينة فقط (مثل الأحساء)، اعتبر الـ intent هو 'search' واجعل التاريخ يبدأ من اليوم ولمدة ليلة واحدة تلقائياً."""
            
            resp = await app_state["ai_client"].chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "system", "content": prompt}, {"role": "user", "content": text[:500]}],
                response_format={"type": "json_object"},
                timeout=get_remaining_timeout() / retries_left
            )
            await cb_ai.record(success=True)
            
            raw_intent = json.loads(resp.choices[0].message.content)
            
            if raw_intent.get("error"):
                raise NonRetryableError(raw_intent["error"])
            if raw_intent.get("intent") != "search" or not raw_intent.get("city"):
                raise NonRetryableError("مرحباً بك! يرجى تزويدي بالمدينة وتواريخ الرحلة للبحث عن أفضل الفنادق المتاحة.")
                
            # تعبئة تلقائية للتواريخ إذا لم يتم توفيرها لضمان عدم الانهيار
            today_str = datetime.now().strftime("%Y-%m-%d")
            tomorrow_str = (datetime.now().replace(day=datetime.now().day+1)).strftime("%Y-%m-%d") if datetime.now().day < 28 else "2026-06-01" # Safe fallbacks
            
            req = HotelSearchRequest(
                city=raw_intent["city"],
                check_in=raw_intent.get("check_in") or today_str,
                check_out=raw_intent.get("check_out") or tomorrow_str,
                guests=raw_intent.get("guests") or 1
            )

    except NonRetryableError as e:
        await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
            json={"chat_id": chat_id, "text": str(e)})
        if r: await r.set(idem_key, "1", ex=86400)
        return
    except Exception as e:
        await cb_ai.record(success=False)
        raise RetryableError(f"AI Failure: {e}")

    # --- 2. جلب الفنادق الفعلية وبناء الأزرار التفاعلية الحقيقية ---
    try:
        await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
            json={"chat_id": chat_id, "text": "🔍 جاري البحث في قواعد البيانات وجلب الأسعار الفورية..."})
            
        hotels = await search_hotels_logic(req)
        
        if not hotels:
            res_text = "😕 لم أجد فنادق متاحة لتلك المدينة أو التواريخ حالياً."
            await telegram_client.post(
                f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
                json={"chat_id": chat_id, "text": res_text}
            )
        else:
            res_text = f"✨ <b>إليك أفضل الخيارات المتاحة في {req.city}:</b>\n\n"
            inline_keyboard = [] # مصفوفة الأزرار الشفافة
            
            for i, h in enumerate(hotels[:3], 1):
                # دمج الـ Affiliate ID للتسويق بالعمولة في الرابط
                aff_url = f"{h.url}?aid={settings.booking_aff_id}" if settings.booking_aff_id else h.url
                
                # بناء النص بصيغة HTML آمنة
                res_text += f"<b>{i}. {h.name}</b>\n💰 السعر: {h.price:,.2f} {h.currency}\n⭐ تقييم الفندق: {h.rating}\n\n"
                
                # إضافة الأزرار أسفل الرسالة بشكل تفاعلي حقيقي
                inline_keyboard.append([
                    {"text": f"🏨 احجز الخيار رقم {i}", "url": aff_url}
                ])

            # إرسال الرسالة النهائية مع لوحة الأزرار (Inline Keyboard)
            await telegram_client.post(
                f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
                json={
                    "chat_id": chat_id, 
                    "text": res_text, 
                    "parse_mode": "HTML",
                    "reply_markup": {
                        "inline_keyboard": inline_keyboard
                    }
                },
                timeout=get_remaining_timeout()
            )
        
        if r: await r.set(idem_key, "1", ex=86400)

    except NonRetryableError as e:
        await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
            json={"chat_id": chat_id, "text": f"⚠️ {str(e)}"})
        if r: await r.set(idem_key, "1", ex=86400)
    except Exception as e:
        raise RetryableError(f"Booking Failure: {e}")

# ============================================================
# API TIER & ADAPTIVE LOAD SHEDDING
# ============================================================

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(default="")):
    if not settings.run_api_tier: raise HTTPException(404, "API Disabled")
    if x_telegram_bot_api_secret_token != settings.webhook_secret: raise HTTPException(403, "Unauthorized")

    try:
        payload = await req.json()
        
        # دعم مزدوج للنصوص العادية ونقرات أزرار الكيبورد المدمجة (Callback Queries)
        if "message" in payload and "text" in payload["message"]:
            chat_id = payload["message"]["chat"]["id"]
            text = payload["message"]["text"]
        elif "callback_query" in payload:
            chat_id = payload["callback_query"]["message"]["chat"]["id"]
            text = payload["callback_query"]["data"]
        else:
            return JSONResponse({"status": "ignored"})
        
        r = app_state["redis"]
        if r:
            # شحن وتوزيع المهام بطريقة عادلة بناءً على الـ Chat ID
            shard_id = hash(str(chat_id)) % settings.queue_shards
            q_pending = f"queue:shard:{shard_id}:pending"
            
            # حماية ذكية من هجمات الإغراق (Load Shedding)
            q_len = await r.llen(q_pending)
            if q_len > settings.safe_queue_depth:
                drop_prob = min(0.95, (q_len - settings.safe_queue_depth) / (settings.max_queue_depth - settings.safe_queue_depth))
                if random.random() < drop_prob:
                    logger.warning(f"Load Shedding active. Dropped webhook for shard {shard_id} (Prob: {drop_prob:.2f})")
                    raise HTTPException(503, "Queue Saturated - Adaptive Drop")

            # حزم المهمة ودفعها إلى طابور المعالجة الآمن
            task = QueueMessage(payload={"chat_id": chat_id, "text": text})
            await r.lpush(q_pending, task.model_dump_json())

        return JSONResponse({"status": "ok"})
    except HTTPException: raise
    except Exception as e:
        logger.error(f"Ingress Failure: {e}")
        return JSONResponse({"status": "error"}, status_code=500)

@app.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    # تشغيل طبقة المعالجة والأي بي آي بكفاءة إنتاجية عالية
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=1 if settings.run_worker_tier else 4)
