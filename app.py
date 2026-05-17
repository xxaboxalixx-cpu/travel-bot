#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✈️  مساعد السفر الذكي — EnterNow MVP v4.0 (Business Logic)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import json
import time
import logging
import requests
from datetime import datetime, date, timedelta
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler

# ─────────────────────────────────────────
#  الإعدادات
# ─────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw")
DEEPSEEK_KEY   = os.environ.get("DEEPSEEK_KEY", "sk-e60a2a3169954be082f4ed96190610e1")
RAPIDAPI_KEY   = os.environ.get("RAPIDAPI_KEY", "93850ca6e4mshc965f580ee18a04p16301djsn87885afe8ab2") 
BOOKING_AFF_ID = os.environ.get("BOOKING_AFF_ID", "") 
CURRENCY       = "SAR"

logging.basicConfig(format="%(asctime)s │ %(levelname)-7s │ %(message)s", level=logging.INFO)
log = logging.getLogger("EnterNow")

TG = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
TODAY = date.today().strftime("%Y-%m-%d")

app = Flask(__name__)

# تهيئة مجدول المهام للمتابعة (Follow-ups)
scheduler = BackgroundScheduler()
scheduler.start()

# ─────────────────────────────────────────
#  الجلسات وقاعدة بيانات "رحلاتي" المصغرة
# ─────────────────────────────────────────
_sessions: dict = {}

def get_session(uid: int) -> dict:
    if uid not in _sessions:
        _sessions[uid] = {
            "history": [],
            "ctx": {"city": None, "check_in": None, "check_out": None, "guests": None},
            "results": [],
            "my_trips": [], # لحفظ الحجوزات
            "step": "idle",
        }
    return _sessions[uid]

def clear_session(uid: int):
    # نمسح سياق البحث فقط، ونحتفظ بـ "رحلاتي"
    s = get_session(uid)
    s["ctx"] = {"city": None, "check_in": None, "check_out": None, "guests": None}
    s["history"] = []

# ─────────────────────────────────────────
#  Telegram
# ─────────────────────────────────────────
def _tg(method: str, payload: dict) -> dict:
    try:
        r = requests.post(f"{TG}/{method}", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"[TG] {e}")
        return {}

def tg_send(chat_id: int, text: str, keyboard: dict = None, preview: bool = False):
    p = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "Markdown", "disable_web_page_preview": not preview}
    if keyboard: p["reply_markup"] = json.dumps(keyboard)
    return _tg("sendMessage", p)

def tg_edit(chat_id: int, msg_id: int, text: str, keyboard: dict = None, preview: bool = False):
    p = {"chat_id": chat_id, "message_id": msg_id, "text": text[:4096], "parse_mode": "Markdown", "disable_web_page_preview": not preview}
    if keyboard: p["reply_markup"] = json.dumps(keyboard)
    return _tg("editMessageText", p)

def tg_delete(chat_id: int, msg_id: int):
    _tg("deleteMessage", {"chat_id": chat_id, "message_id": msg_id})

def tg_typing(chat_id: int):
    _tg("sendChatAction", {"chat_id": chat_id, "action": "typing"})

# ─────────────────────────────────────────
#  المهام المجدولة (Retargeting Logic)
# ─────────────────────────────────────────
def follow_up_24h(chat_id: int, hotel_name: str):
    log.info(f"[Retargeting] 24h follow up for {chat_id}")
    msg = f"أهلاً بك! 👋\nبخصوص اختيارك لفندق *{hotel_name}* بالأمس، هل أتممت الحجز؟"
    kb = {"inline_keyboard": [
        [{"text": "نعم، حجزت ✅", "callback_data": "track_yes"}, 
         {"text": "لا، غيرت رأيي ❌", "callback_data": "track_no"}]
    ]}
    tg_send(chat_id, msg, keyboard=kb)

def follow_up_7d(chat_id: int, hotel_name: str, original_price: float):
    log.info(f"[Retargeting] 7d follow up for {chat_id}")
    new_price = original_price - 50 # تخفيض وهمي لأغراض الـ MVP
    msg = (f"عندي لك خبر سعيد! 🎉\n\n"
           f"لقيت لك سعر أقل بـ *50 ر.س* لنفس الفندق اللي عجبك (*{hotel_name}*).\n"
           f"السعر الجديد صار: *{new_price} ر.س*\n\n"
           f"الفرصة ما تتعوض، تبي تحجز الآن؟ 👇")
    kb = {"inline_keyboard": [[{"text": "ورني الرابط أحجز! 🔗", "callback_data": "resend_link"}]]}
    tg_send(chat_id, msg, keyboard=kb)

# ─────────────────────────────────────────
#  RapidAPI و DeepSeek (نفس المنطق السابق)
# ─────────────────────────────────────────
_BHOST = "booking-com15.p.rapidapi.com"
_BBASE = f"https://{_BHOST}/api/v1/hotels"
_BH    = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": _BHOST}

def _booking_dest_id(city: str) -> str | None:
    try:
        r = requests.get(f"{_BBASE}/searchDestination", headers=_BH, params={"query": city}, timeout=15)
        r.raise_for_status()
        items = r.json().get("data", [])
        if not items: return None
        for it in items:
            if it.get("search_type") in ("city", "region"): return it.get("dest_id")
        return items[0].get("dest_id")
    except Exception as e: return None

def _extract_price(h: dict) -> float:
    pb = h.get("priceBreakdown") or {}
    gp = pb.get("grossPrice") or {}
    v  = gp.get("value") or gp.get("amount_rounded") or gp.get("amount") or h.get("min_total_price")
    return float(v) if v else 0.0

def search_hotels(city: str, check_in: str, check_out: str, guests: int) -> list[dict]:
    dest_id = _booking_dest_id(city)
    if not dest_id: return []
    params = {"dest_id": dest_id, "search_type": "CITY", "arrival_date": check_in, "departure_date": check_out, "adults": str(guests), "room_qty": "1", "page_number": "1", "languagecode": "ar", "currency_code": CURRENCY, "units": "metric", "sort_by": "popularity"}
    try:
        r = requests.get(f"{_BBASE}/searchHotels", headers=_BH, params=params, timeout=20)
        r.raise_for_status()
        raw = r.json().get("data", {}).get("hotels", []) or []
    except Exception as e: return []

    n_nights = max(1, (datetime.strptime(check_out, "%Y-%m-%d") - datetime.strptime(check_in, "%Y-%m-%d")).days)
    parsed = []
    for h in raw:
        prop = h.get("property") or {}
        name = (prop.get("name") or h.get("hotel_name") or "").strip()
        price = _extract_price(h)
        if not name or price == 0: continue
        price_per_night = round(price / n_nights) if (price > 2000 and n_nights > 1) else round(price)
        parsed.append({
            "id": str(prop.get("id") or h.get("hotel_id") or name[:10]), "name": name, 
            "rating": round(float(prop.get("reviewScore") or 0), 1),
            "stars": int(prop.get("propertyClass") or 0),
            "price_night": price_per_night, "price_total": price_per_night * n_nights,
        })

    by_price = sorted(parsed, key=lambda x: x["price_night"])
    by_score = sorted(parsed, key=lambda x: x["rating"]*2.5 - x["price_night"]/500, reverse=True)
    
    top3, seen = [], set()
    for h in by_price + by_score:
        if h["id"] not in seen:
            top3.append(h)
            seen.add(h["id"])
        if len(top3) == 3: break
    return top3

def make_booking_link(h: dict, ci: str, co: str, guests: int) -> str:
    name_q = requests.utils.quote(h["name"])
    base = f"https://www.booking.com/search.html?ss={name_q}&checkin={ci}&checkout={co}&group_adults={guests}&no_rooms=1&selected_currency={CURRENCY}"
    return base + f"&aid={BOOKING_AFF_ID}" if BOOKING_AFF_ID else base

_SYS = f"""أنت مساعد سفر ذكي. مهمتك: أعد JSON فقط:
{{"intent": "search"|"select"|"reset"|"other", "city": "اسم بالإنجليزي أو null", "check_in": "YYYY-MM-DD أو null", "check_out": "YYYY-MM-DD أو null", "guests": عدد أو null, "selection": 1|2|3|null, "reply": "رد مختصر سعودي"}}
• اليوم: {TODAY}"""

def gpt_parse(text: str, history: list, ctx: dict) -> dict:
    msg = text + f"\n[سياق: {json.dumps({k:v for k,v in ctx.items() if v}, ensure_ascii=False)}]"
    messages = [{"role": "system", "content": _SYS}] + history[-5:] + [{"role": "user", "content": msg}]
    try:
        r = requests.post("https://api.deepseek.com/chat/completions", headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"}, json={"model": "deepseek-chat", "messages": messages, "response_format": {"type": "json_object"}}, timeout=15)
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])
    except Exception: return {"intent": "other", "reply": "صار خطأ، حاول مرة ثانية 🙏"}

# ─────────────────────────────────────────
#  الرسائل والتفاعل
# ─────────────────────────────────────────
def results_message(hotels: list, city: str, ci: str, co: str, g: int) -> str:
    cards = "\n\n".join(f"{idx+1}️⃣ *{h['name']}*\n⭐ {h['rating']} | 💰 {h['price_total']} ر.س إجمالي" for idx, h in enumerate(hotels))
    return f"✅ لقيت 3 خيارات في {city}\n📅 {ci} → {co} | 👥 {g} أشخاص\n\n{cards}\n\nأي واحد يعجبك؟ 👇"

def handle_message(chat_id: int, uid: int, text: str, first_name: str):
    s = get_session(uid)
    text = text.strip()

    if text.startswith("/start"):
        clear_session(uid)
        tg_send(chat_id, f"هلا {first_name}! 👋\nوين تبي تروح؟\n_(تقدر تكتب: ابغى فندق بدبي بكرة لشخصين)_")
        return
    
    if text.startswith("/mytrips"):
        if not s["my_trips"]:
            tg_send(chat_id, "سجلك فارغ 🧳\nابدأ بحث جديد واضغط على حجز لتُحفظ هنا.")
            return
        trips_text = "🧳 *رحلاتك المحفوظة:*\n\n" + "\n".join(f"🏨 {t['hotel']} ({t['date']})" for t in s["my_trips"])
        tg_send(chat_id, trips_text)
        return

    s["history"].append({"role": "user", "content": text})
    tg_typing(chat_id)
    p = gpt_parse(text, s["history"], s["ctx"])
    
    for f in ("city", "check_in", "check_out", "guests"):
        if p.get(f): s["ctx"][f] = p.get(f)
    
    if p.get("intent") == "search" or any(s["ctx"].values()):
        missing = [f for f in ("city", "check_in", "check_out", "guests") if not s["ctx"].get(f)]
        if missing:
            q = {"city": "وين تبي تروح؟ 🌍", "check_in": "متى الدخول؟ 📅", "check_out": "ومتى الخروج؟ 📅", "guests": "كم شخص؟ 👥"}.get(missing[0])
            tg_send(chat_id, q)
            s["history"].append({"role": "assistant", "content": q})
            return
        
        # ── Loading Animation ──
        tmp = tg_send(chat_id, "🔍 ثواني.. جاري البحث في قواعد البيانات 🌐")
        tmp_id = tmp.get("result", {}).get("message_id")
        time.sleep(1)
        if tmp_id: tg_edit(chat_id, tmp_id, "⚙️ جاري فلترة أفضل الأسعار 💰...")
        tg_typing(chat_id)
        
        hotels = search_hotels(s["ctx"]["city"], s["ctx"]["check_in"], s["ctx"]["check_out"], s["ctx"]["guests"])
        if tmp_id: tg_delete(chat_id, tmp_id)

        if not hotels:
            tg_send(chat_id, "ما لقيت فنادق متاحة. جرب تواريخ أخرى.")
            return

        s["results"] = hotels
        kb = {"inline_keyboard": [[{"text": f"{i+1}️⃣ احجز الخيار {i+1}", "callback_data": f"book_{i}"}] for i in range(len(hotels))]}
        tg_send(chat_id, results_message(hotels, s["ctx"]["city"], s["ctx"]["check_in"], s["ctx"]["check_out"], s["ctx"]["guests"]), keyboard=kb)
        return

    reply = p.get("reply", "وين تبي تروح؟")
    tg_send(chat_id, reply)
    s["history"].append({"role": "assistant", "content": reply})

def handle_callback(chat_id: int, uid: int, msg_id: int, data: str):
    s = get_session(uid)
    
    if data.startswith("book_"):
        idx = int(data.split("_")[1])
        h = s["results"][idx]
        link = make_booking_link(h, s["ctx"]["check_in"], s["ctx"]["check_out"], s["ctx"]["guests"])
        
        # 1. حفظ في رحلاتي
        s["my_trips"].append({"hotel": h['name'], "date": TODAY})
        
        # 2. جدولة الاستهداف (24 ساعة و 7 أيام)
        scheduler.add_job(follow_up_24h, 'date', run_date=datetime.now() + timedelta(days=1), args=[chat_id, h['name']])
        scheduler.add_job(follow_up_7d, 'date', run_date=datetime.now() + timedelta(days=7), args=[chat_id, h['name'], h['price_total']])
        
        msg = f"تم حفظ الفندق في /mytrips 🧳\n\n🏨 *{h['name']}*\n💰 *{h['price_total']} ر.س*\n🔗 [اضغط هنا لإكمال الحجز]({link})"
        tg_edit(chat_id, msg_id, msg, preview=True)
        clear_session(uid) # تصفير بعد نجاح الاستخراج

    elif data == "track_yes":
        tg_edit(chat_id, msg_id, "ممتاز! رحلة سعيدة ✈️ شاركنا تقييمك بعد العودة.")
    elif data == "track_no":
        tg_edit(chat_id, msg_id, "مو مشكلة، إذا غيرت رأيك أنا موجود دايم 🫡 اكتب /start لبحث جديد.")
    elif data == "resend_link":
        tg_edit(chat_id, msg_id, "رائع! اكتب /start وسأبحث لك عن السعر المحدث فوراً.")

# ─────────────────────────────────────────
#  Flask Webhooks
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def index(): return "🚀 EnterNow API is Live!", 200
@app.route('/webhook', methods=['POST'])
def webhook():
    upd = request.json
    try:
        if "message" in upd:
            m = upd["message"]
            if m.get("text"): handle_message(m["chat"]["id"], m["from"]["id"], m["text"], m["from"].get("first_name", ""))
        elif "callback_query" in upd:
            cq = upd["callback_query"]
            _tg("answerCallbackQuery", {"callback_query_id": cq["id"]})
            handle_callback(cq["message"]["chat"]["id"], cq["from"]["id"], cq["message"]["message_id"], cq["data"])
    except Exception as e: log.error(f"[Webhook Error] {e}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
