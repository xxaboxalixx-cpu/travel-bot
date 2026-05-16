#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✈️  مساعد السفر الذكي — EnterNow MVP v3.0 (Bulletproof)
    Telegram Webhooks + DeepSeek + Booking RapidAPI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import json
import logging
import requests
from datetime import datetime, date
from flask import Flask, request, jsonify

# ─────────────────────────────────────────
#  الإعدادات 
# ─────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw")
DEEPSEEK_KEY   = os.environ.get("DEEPSEEK_KEY", "sk-e60a2a3169954be082f4ed96190610e1")
# تأكد من وضع مفتاحك الجديد هنا قبل الحفظ
RAPIDAPI_KEY   = os.environ.get("RAPIDAPI_KEY", "93850ca6e4mshc965f580ee18a04p16301djsn87885afe8ab2'")
BOOKING_AFF_ID = os.environ.get("BOOKING_AFF_ID", "") 
CURRENCY       = "SAR"

# ─────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
log = logging.getLogger("EnterNow")

TG = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
TODAY = date.today().strftime("%Y-%m-%d")

app = Flask(__name__)

# ─────────────────────────────────────────
#  الجلسات
# ─────────────────────────────────────────
_sessions: dict = {}

def get_session(uid: int) -> dict:
    if uid not in _sessions:
        _sessions[uid] = {
            "history": [],
            "ctx": {"city": None, "check_in": None, "check_out": None, "guests": None},
            "results": [],
            "step": "idle",
        }
    return _sessions[uid]

def clear_session(uid: int):
    _sessions.pop(uid, None)

# ─────────────────────────────────────────
#  Telegram
# ─────────────────────────────────────────
def _tg(method: str, payload: dict) -> dict:
    try:
        r = requests.post(f"{TG}/{method}", json=payload, timeout=10)
        return r.json()
    except Exception as e:
        log.error(f"[TG/{method}] {e}")
        return {}

def tg_send(chat_id: int, text: str, keyboard: dict = None, preview: bool = False):
    p = {"chat_id": chat_id, "text": text[:4096], "parse_mode": "Markdown", "disable_web_page_preview": not preview}
    if keyboard: p["reply_markup"] = json.dumps(keyboard)
    return _tg("sendMessage", p)

def tg_edit(chat_id: int, msg_id: int, text: str, keyboard: dict = None, preview: bool = False):
    p = {"chat_id": chat_id, "message_id": msg_id, "text": text[:4096], "parse_mode": "Markdown", "disable_web_page_preview": not preview}
    if keyboard: p["reply_markup"] = json.dumps(keyboard)
    _tg("editMessageText", p)

def tg_delete(chat_id: int, msg_id: int):
    _tg("deleteMessage", {"chat_id": chat_id, "message_id": msg_id})

def tg_typing(chat_id: int):
    _tg("sendChatAction", {"chat_id": chat_id, "action": "typing"})

def tg_answer_cb(cb_id: str, text: str = ""):
    _tg("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})

# ─────────────────────────────────────────
#  Booking.com
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
    except Exception as e:
        log.error(f"[RapidAPI DestID Error] {e}")
    return None

def _extract_price(h: dict) -> float:
    pb = h.get("priceBreakdown") or {}
    gp = pb.get("grossPrice") or {}
    v  = gp.get("value") or gp.get("amount_rounded") or gp.get("amount")
    if v: return float(v)
    v = h.get("min_total_price")
    if v: return float(v)
    prop = h.get("property") or {}
    v = prop.get("priceBreakdown", {}).get("grossPrice", {}).get("value")
    return float(v) if v else 0.0

def search_hotels(city: str, check_in: str, check_out: str, guests: int) -> list[dict]:
    log.info(f"[Action] Searching dest_id for {city}...")
    dest_id = _booking_dest_id(city)
    if not dest_id: 
        log.warning(f"[Hotels] Could not find dest_id for {city}")
        return []

    log.info(f"[Action] Requesting hotels for dest_id: {dest_id}...")
    params = {
        "dest_id": dest_id, "search_type": "CITY", "arrival_date": check_in,
        "departure_date": check_out, "adults": str(guests), "room_qty": "1",
        "page_number": "1", "languagecode": "ar", "currency_code": CURRENCY,
        "units": "metric", "sort_by": "popularity",
    }
    try:
        r = requests.get(f"{_BBASE}/searchHotels", headers=_BH, params=params, timeout=20)
        r.raise_for_status()
        raw = r.json().get("data", {}).get("hotels", []) or []
        log.info(f"[RapidAPI] Successfully found {len(raw)} raw hotel entries.")
    except Exception as e:
        log.error(f"[RapidAPI Search Error] {e}")
        return []

    try:
        n_nights = max(1, (datetime.strptime(check_out, "%Y-%m-%d") - datetime.strptime(check_in, "%Y-%m-%d")).days)
    except Exception as e:
        log.error(f"[Date Parsing Error] {e}")
        n_nights = 1

    parsed = []
    for h in raw:
        prop = h.get("property") or {}
        name = (prop.get("name") or h.get("hotel_name") or "").strip()
        if not name: continue
        rating = float(prop.get("reviewScore") or prop.get("review_score") or 0)
        stars  = int(prop.get("propertyClass") or prop.get("hotel_class") or 0)
        price  = _extract_price(h)

        if price > 0:
            price_per_night = round(price / n_nights) if (price > 2000 and n_nights > 1) else round(price)
        else: continue

        hotel_id = str(prop.get("id") or h.get("hotel_id") or name[:30])
        parsed.append({
            "id": hotel_id, "name": name, "rating": round(rating, 1),
            "stars": stars, "price_night": price_per_night,
            "price_total": price_per_night * n_nights,
            "photo": (prop.get("photoUrls") or [None])[0] or "",
        })

    if not parsed: return []
    by_price  = sorted(parsed, key=lambda x: x["price_night"])
    by_rating = sorted(parsed, key=lambda x: -x["rating"])
    by_score  = sorted(parsed, key=lambda x: x["rating"]*2.5 + x["stars"]*1.5 - x["price_night"]/500, reverse=True)

    top3, seen = [], set()
    for h in [by_price[0], by_rating[0]] + by_score:
        if h["id"] not in seen:
            top3.append(h)
            seen.add(h["id"])
        if len(top3) == 3: break
    return top3

def make_booking_link(h: dict, ci: str, co: str, guests: int) -> str:
    name_q = requests.utils.quote(h["name"])
    base = f"https://www.booking.com/search.html?ss={name_q}&checkin={ci}&checkout={co}&group_adults={guests}&no_rooms=1&selected_currency={CURRENCY}"
    if BOOKING_AFF_ID: base += f"&aid={BOOKING_AFF_ID}"
    return base

# ─────────────────────────────────────────
#  DeepSeek 
# ─────────────────────────────────────────
_SYS = f"""أنت مساعد سفر ذكي على تيليجرام مهمتك جمع بيانات الحجز.
أسلوبك: اللهجة السعودية، مختصر جداً.
مهمتك: أعد JSON فقط بهذا الشكل:
{{
  "intent":    "search" | "select" | "reset" | "help" | "other",
  "city":      "اسم المدينة بالإنجليزي أو null",
  "check_in":  "YYYY-MM-DD أو null",
  "check_out": "YYYY-MM-DD أو null",
  "guests":    عدد_صحيح أو null,
  "selection": 1 | 2 | 3 | null,
  "reply":     "رد مختصر إذا كانت الرسالة لا علاقة لها بالحجز"
}}
• اليوم: {TODAY}
• التواريخ يجب أن تكون YYYY-MM-DD بدقة تامة.
• "شخصين" تعني 2.
"""

def gpt_parse(text: str, history: list, ctx: dict) -> dict:
    known = {k: v for k, v in ctx.items() if v is not None}
    msg = text + (f"\n[السياق المعروف: {json.dumps(known, ensure_ascii=False)}]" if known else "")
    messages = [{"role": "system", "content": _SYS}] + history[-8:] + [{"role": "user", "content": msg}]

    try:
        r = requests.post(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {DEEPSEEK_KEY}", "Content-Type": "application/json"},
            json={"model": "deepseek-chat", "messages": messages, "temperature": 0.05, "max_tokens": 350, "response_format": {"type": "json_object"}},
            timeout=20,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        result  = json.loads(content)
        # الرادار: سحبنا كامل الإجابة لنفهم ماذا يفعل النموذج بالضبط
        log.info(f"[DeepSeek Output] FULL JSON: {json.dumps(result, ensure_ascii=False)}")
        return result
    except Exception as e:
        log.error(f"[DeepSeek Error] {e}")
        return {"intent": "other", "reply": "صار خطأ بالذكاء الاصطناعي، حاول مرة ثانية 🙏"}

# ─────────────────────────────────────────
#  تنسيق الرسائل
# ─────────────────────────────────────────
_LABELS = {0: ("1️⃣", "الأرخص"), 1: ("2️⃣", "الأفضل تقييماً"), 2: ("3️⃣", "الأميز")}

def _nights(ci: str, co: str) -> int:
    try: return max(1, (datetime.strptime(co, "%Y-%m-%d") - datetime.strptime(ci, "%Y-%m-%d")).days)
    except: return 1

def _stars(n: int) -> str:
    n = max(0, min(n, 5))
    return "★" * n + "☆" * (5 - n) if n > 0 else ""

def hotel_card(h: dict, idx: int, ci: str, co: str, g: int) -> str:
    emoji, label = _LABELS.get(idx, ("🔘", "خيار"))
    n = _nights(ci, co)
    return (f"{emoji} *{label}*\n🏨 {h['name']}\n⭐ {h['rating']}  {_stars(h['stars'])}\n"
            f"💰 {h['price_night']:,} ر.س/ليلة  ·  المجموع: *{h['price_total']:,} ر.س* ({n} ليالٍ)")

def results_message(hotels: list, city: str, ci: str, co: str, g: int) -> str:
    n = _nights(ci, co)
    header = f"✅ وجدت *{len(hotels)} خيارات* في {city}\n📅 {ci}  →  {co}  ·  {n} ليالٍ\n👥 {g} أشخاص\n{'━'*26}\n\n"
    cards = "\n\n".join(hotel_card(h, i, ci, co, g) for i, h in enumerate(hotels))
    return header + cards + f"\n\n{'━'*26}\nأي واحد يعجبك؟ 👇"

def booking_message(h: dict, ci: str, co: str, g: int, link: str) -> str:
    return (f"ممتاز! 🎉\n\n🏨 *{h['name']}*\n⭐ {h['rating']}  {_stars(h['stars'])}\n"
            f"💰 *{h['price_total']:,} ر.س* إجمالي\n\n🔗 [اضغط هنا لإكمال الحجز]({link})\n\n"
            f"✅ السعر مضمون\n✅ الإلغاء حسب سياسة الفندق\n━━━━━━━━━━━━━━━━━━━━━━━━\nتحتاج بحث ثاني؟  /start")

def pick_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "1️⃣  احجز الأرخص", "callback_data": "book_0"}],
        [{"text": "2️⃣  احجز الأفضل تقييماً", "callback_data": "book_1"}],
        [{"text": "3️⃣  احجز الأميز", "callback_data": "book_2"}],
        [{"text": "🔄  بحث جديد", "callback_data": "reset"}],
    ]}

def _q_for(field: str) -> str:
    return {"city": "وين تبي تروح؟ 🌍", "check_in": "من أي تاريخ تبغى تدخل؟ 📅 (مثال: 2026-05-20)", 
            "check_out": "وإلى أي تاريخ؟ 📅", "guests": "كم شخص؟ 👥"}.get(field, "وين تبي تروح؟")

# ─────────────────────────────────────────
#  معالجة الأحداث
# ─────────────────────────────────────────
def handle_message(chat_id: int, uid: int, text: str, first_name: str):
    s = get_session(uid)
    text = text.strip()

    if text.startswith("/start"):
        clear_session(uid)
        tg_send(chat_id, f"هلا {first_name}! 👋\nأنا مساعد سفرك الذكي ✈️\n\nوين تبي تروح وامتى؟\n_مثال: أبغى فندق في دبي من 20 إلى 23 يونيو لشخصين_")
        return
    if text.startswith("/reset"):
        clear_session(uid)
        tg_send(chat_id, "تم المسح 🔄\nوين تبي تروح؟")
        return

    s["history"].append({"role": "user", "content": text})
    tg_typing(chat_id)

    p = gpt_parse(text, s["history"], s["ctx"])
    intent, sel = p.get("intent", "other"), p.get("selection")

    for f in ("city", "check_in", "check_out", "guests"):
        if p.get(f) is not None: s["ctx"][f] = p.get(f)
    ctx = s["ctx"]

    if intent == "reset":
        clear_session(uid)
        tg_send(chat_id, "تم المسح 🔄\nوين تبي تروح؟")
        return

    if (intent == "select" or sel is not None) and s["results"]:
        idx = max(0, min(int(sel or 1) - 1, len(s["results"]) - 1))
        h = s["results"][idx]
        msg = booking_message(h, ctx.get("check_in"), ctx.get("check_out"), ctx.get("guests"), make_booking_link(h, ctx.get("check_in"), ctx.get("check_out"), ctx.get("guests")))
        tg_send(chat_id, msg, preview=True)
        s["history"].append({"role": "assistant", "content": msg})
        return

    # شبكة الأمان: نتحقق بأنفسنا مما ينقص، ولا نعتمد فقط على الذكاء الاصطناعي
    if intent == "search" or any(ctx.get(f) for f in ("city", "check_in", "check_out", "guests")):
        actual_missing = [f for f in ("city", "check_in", "check_out", "guests") if not ctx.get(f)]
        
        if actual_missing:
            q = _q_for(actual_missing[0])
            tg_send(chat_id, q)
            s["history"].append({"role": "assistant", "content": q})
            return
        else:
            # كل المتطلبات مكتملة، ننطلق للبحث
            tmp = tg_send(chat_id, f"🔍 أدور لك على فنادق في *{ctx['city']}*...\n_قد يأخذ بضع ثوانٍ_ ⏳")
            tg_typing(chat_id)
            hotels = search_hotels(ctx["city"], ctx["check_in"], ctx["check_out"], ctx["guests"])
            if tmp.get("result"): tg_delete(chat_id, tmp["result"]["message_id"])

            if not hotels:
                err = "ما لقيت فنادق متاحة بهالمواصفات 😔\nجرّب تواريخ أو مدينة أخرى\n/reset — ابدأ بحثاً جديداً"
                tg_send(chat_id, err)
                s["history"].append({"role": "assistant", "content": err})
                return

            s["results"], s["step"] = hotels, "results"
            msg = results_message(hotels, ctx["city"], ctx["check_in"], ctx["check_out"], ctx["guests"])
            tg_send(chat_id, msg, keyboard=pick_keyboard())
            s["history"].append({"role": "assistant", "content": msg})
            return

    reply = p.get("reply") or "تفضل، أخبرني وين تبي تروح؟ 😊"
    tg_send(chat_id, reply)
    s["history"].append({"role": "assistant", "content": reply})

def handle_callback(chat_id: int, uid: int, msg_id: int, data: str, cb_id: str):
    tg_answer_cb(cb_id)
    s = get_session(uid)
    if data == "reset":
        clear_session(uid)
        tg_edit(chat_id, msg_id, "تم المسح 🔄\nابدأ بحثاً جديداً: /start")
        return
    if data.startswith("book_"):
        idx = int(data.split("_")[1])
        if not s["results"]:
            tg_edit(chat_id, msg_id, "انتهت صلاحية هذه النتائج ⏰\nابدأ بحثاً جديداً: /start")
            return
        h = s["results"][max(0, min(idx, len(s["results"]) - 1))]
        msg = booking_message(h, s["ctx"].get("check_in"), s["ctx"].get("check_out"), s["ctx"].get("guests"), make_booking_link(h, s["ctx"].get("check_in"), s["ctx"].get("check_out"), s["ctx"].get("guests")))
        tg_edit(chat_id, msg_id, msg, preview=True)

# ─────────────────────────────────────────
#  Flask Webhooks
# ─────────────────────────────────────────
@app.route('/', methods=['GET'])
def index(): return "🚀 EnterNow Bot is running!", 200

@app.route('/webhook', methods=['POST'])
def webhook():
    upd = request.json
    if not upd: return "No payload", 400
    try:
        if "message" in upd:
            m = upd["message"]
            text = m.get("text", "").strip()
            if text:
                u, c = m.get("from", {}), m.get("chat", {})
                handle_message(c["id"], u["id"], text, u.get("first_name", ""))
        elif "callback_query" in upd:
            cq = upd["callback_query"]
            u, m, c = cq.get("from", {}), cq.get("message", {}), cq.get("message", {}).get("chat", {})
            handle_callback(c.get("id"), u.get("id"), m.get("message_id"), cq.get("data", ""), cq.get("id"))
    except Exception as e:
        log.error(f"[Webhook Error] {e}")
    return "OK", 200

@app.route('/set_webhook', methods=['GET'])
def set_webhook():
    host_url = request.host_url.rstrip('/')
    webhook_url = f"{host_url}/webhook"
    r = requests.post(f"{TG}/setWebhook", json={"url": webhook_url})
    return jsonify({"status": "success" if r.status_code == 200 else "failed", "response": r.json()}), r.status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
