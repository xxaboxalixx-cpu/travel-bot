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
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

from fastapi import FastAPI, Request, HTTPException, Header, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings
from openai import AsyncOpenAI

# ============================================================
# SETTINGS & TOPOLOGY
# ============================================================

class Settings(BaseSettings):
    telegram_token: str = os.getenv("TELEGRAM_TOKEN", "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw")
    deepseek_api_key: str = os.getenv("DEEPSEEK_API_KEY", "sk-e60a2a3169954be082f4ed96190610e1")
    rapidapi_key: str = os.getenv("RAPIDAPI_KEY", "306c7368b1msh8820d2aceb8457bp1ba20cjsn980c79328197")
    booking_aff_id: str = os.getenv("BOOKING_AFF_ID", "")
    webhook_secret: str = os.getenv("WEBHOOK_SECRET", "CHANGE_ME_SECURELY")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    run_api_tier: bool = True
    run_worker_tier: bool = True
    
    queue_shards: int = 1  # تقليل الشظايا لتوحيد الطابور وتسريع المعالجة في الباقة المجانية
    safe_queue_depth: int = 5000
    max_queue_depth: int = 10000
    worker_concurrency: int = 2
    workflow_timeout: float = 28.0
    visibility_timeout: float = 30.0

settings = Settings()

# ============================================================
# OBSERVABILITY
# ============================================================

trace_id_var = contextvars.ContextVar("trace_id", default="system")
span_id_var = contextvars.ContextVar("span_id", default="root")
deadline_var = contextvars.ContextVar("deadline", default=0.0)

def get_remaining_timeout() -> float:
    return max(0.1, deadline_var.get() - time.monotonic())

logger = logging.getLogger("platform")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ============================================================
# DATA TRANSFER OBJECTS (DTOs)
# ============================================================

class HotelSearchRequest(BaseModel):
    city: str
    check_in: str
    check_out: str
    guests: int

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
    "shutdown_event": asyncio.Event()
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    app_state["thread_pool"] = ThreadPoolExecutor(max_workers=2)
    
    # محاولة الاتصال بالـ Redis بمرونة كاملة
    r_url = settings.redis_url
    if not r_url or "localhost" in r_url:
        # إذا لم يجد السيرفر المتغير، يحاول القراءة من المتغيرات الشائعة لـ Render Key Value تلقائياً
        r_url = os.getenv("REDIS_URL", os.getenv("REDIS_INTERNAL_URL", "redis://localhost:6379/0"))
        
    logger.info(f"Connecting to Redis URL: {r_url}")
    try:
        app_state["redis"] = redis.from_url(r_url, decode_responses=True)
        await app_state["redis"].ping()
        logger.info("Successfully connected to Redis Stack!")
    except Exception as e:
        logger.critical(f"Redis initialization failed: {e}. Running in Fallback Mode.")

    app_state["http_clients"]["telegram"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
    app_state["http_clients"]["booking"] = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=20))
    app_state["ai_client"] = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url="https://api.deepseek.com")

    if settings.run_worker_tier and app_state["redis"]:
        logger.info("Booting Fallback Worker Engine...")
        for i in range(settings.worker_concurrency):
            task = asyncio.create_task(reliable_queue_consumer(f"worker_{i}", 0))
            app_state["worker_tasks"].append(task)

    yield

    app_state["shutdown_event"].set()
    if app_state["worker_tasks"]:
        await asyncio.gather(*app_state["worker_tasks"], return_exceptions=True)

    for c in app_state["http_clients"].values(): await c.aclose()
    if app_state["redis"]: await app_state["redis"].close()
    app_state["thread_pool"].shutdown(wait=True)

app = FastAPI(title="Travel Bot Control Plane", lifespan=lifespan)

# ============================================================
# QUEUE CONSUMER
# ============================================================

async def reliable_queue_consumer(worker_id: str, shard_id: int):
    r = app_state["redis"]
    q_pending = "queue:fallback:pending"
    logger.info(f"Worker {worker_id} is online and listening to queue...")

    while not app_state["shutdown_event"].is_set():
        try:
            raw_msg = await r.brpop(q_pending, timeout=2) 
            if not raw_msg: continue
            
            logger.info(f"Worker {worker_id} picked up a new message!")
            msg = QueueMessage.model_validate_json(raw_msg[1])
            
            # تشغيل المعالجة الفورية
            await process_workflow(msg.payload, msg.task_id)

        except Exception as e:
            logger.error(f"Worker Exception: {e}")
            await asyncio.sleep(1)

# ============================================================
# CORE LOGIC (Booking & AI)
# ============================================================

async def search_hotels_logic(data: HotelSearchRequest) -> List[HotelDTO]:
    headers = {"X-RapidAPI-Key": settings.rapidapi_key, "X-RapidAPI-Host": "booking-com15.p.rapidapi.com"}  
    client = app_state["http_clients"]["booking"]

    try:  
        await asyncio.sleep(1.0)
        dest_resp = await client.get("https://booking-com15.p.rapidapi.com/api/v1/hotels/searchDestination", headers=headers, params={"query": data.city})  
        dest_data = dest_resp.json().get("data", [])
        if not dest_data: return []
        dest_id = dest_data[0].get("dest_id")

        await asyncio.sleep(1.0)
        hotels_resp = await client.get(
            "https://booking-com15.p.rapidapi.com/api/v1/hotels/searchHotels",  
            headers=headers,  
            params={"dest_id": dest_id, "search_type": "CITY", "arrival_date": data.check_in, "departure_date": data.check_out, "adults": data.guests, "page_number": "1", "currency_code": "SAR"}  
        )  
        
        raw_hotels = hotels_resp.json().get("data", {}).get("hotels", [])  
        return [dto for h in raw_hotels if (dto := HotelDTO.from_booking_raw(h))]
    except Exception as e:
        logger.error(f"Error in booking request: {e}")
        return []

async def process_workflow(payload: dict, task_id: str):
    chat_id = payload["chat_id"]
    text = payload["text"]
    telegram_client = app_state["http_clients"]["telegram"]

    logger.info(f"Processing chat_id {chat_id} with text: {text}")

    # 1. AI Parsing
    try:
        prompt = "أنت مساعد سفر محترف. استخرجه كـ JSON فقط: intent (اجعله 'search'), city (اسم المدينة بالإنجليزية), check_in (تاريخ اليوم YYYY-MM-DD), check_out (تاريخ الغد YYYY-MM-DD), guests (1)."
        resp = await app_state["ai_client"].chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "system", "content": prompt}, {"role": "user", "content": text}],
            response_format={"type": "json_object"}
        )
        raw_intent = json.loads(resp.choices[0].message.content)
        
        req = HotelSearchRequest(
            city=raw_intent.get("city", "Riyadh"),
            check_in=raw_intent.get("check_in", datetime.now().strftime("%Y-%m-%d")),
            check_out=raw_intent.get("check_out", "2026-06-01"),
            guests=1
        )
    except Exception as e:
        logger.error(f"AI Error: {e}")
        await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage", json={"chat_id": chat_id, "text": "🤖 مرحباً بك! يرجى كتابة اسم المدينة التي تود البحث عن فنادق فيها."})
        return

    # 2. Search & Send
    try:
        await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage", json={"chat_id": chat_id, "text": f"🔍 جاري البحث عن أفضل الفنادق في {req.city}..."})
        hotels = await search_hotels_logic(req)
        
        if not hotels:
            await telegram_client.post(f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage", json={"chat_id": chat_id, "text": "😕 عذراً، لم أجد خيارات متاحة حالياً لهذه المدينة."})
            return

        res_text = f"✨ <b>إليك أفضل الخيارات المتاحة في {req.city}:</b>\n\n"
        inline_keyboard = []
        for i, h in enumerate(hotels[:3], 1):
            res_text += f"<b>{i}. {h.name}</b>\n💰 السعر: {h.price:,.2f} SAR\n⭐ التقييم: {h.rating}\n\n"
            inline_keyboard.append([{"text": f"🔗 احجز الخيار رقم {i}", "url": h.url}])

        await telegram_client.post(
            f"https://api.telegram.org/bot{settings.telegram_token}/sendMessage",
            json={"chat_id": chat_id, "text": res_text, "parse_mode": "HTML", "reply_markup": {"inline_keyboard": inline_keyboard}}
        )
    except Exception as e:
        logger.error(f"Workflow Endpoint Error: {e}")

# ============================================================
# API ENDPOINT
# ============================================================

@app.post("/webhook")
async def webhook(req: Request, x_telegram_bot_api_secret_token: str = Header(default="")):
    try:
        payload = await req.json()
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
            task = QueueMessage(payload={"chat_id": chat_id, "text": text})
            await r.lpush("queue:fallback:pending", task.model_dump_json())
            logger.info("Message successfully pushed to the Redis queue!")
        else:
            # تشغيل احتياطي مباشر في حال عدم وجود سيرفر Redis تماماً لتفادي التعليق
            asyncio.create_task(process_workflow({"chat_id": chat_id, "text": text}, "direct_task"))

        return JSONResponse({"status": "ok"})
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return JSONResponse({"status": "error"}, status_code=500)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000)
