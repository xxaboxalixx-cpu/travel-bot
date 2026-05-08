#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✈️  مساعد السفر الذكي — MVP v1.0
    Telegram + GPT-4o + Booking.com RapidAPI
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
المتطلبات: pip install requests
التشغيل  : python bot.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import time
import logging
import requests
from datetime import datetime, date

# ─────────────────────────────────────────
#  الإعدادات
# ─────────────────────────────────────────
TELEGRAM_TOKEN = "8758650754:AAGmMh3KYV_2O7jndipDNTZfiNJw6JYW5Xw"
OPENAI_KEY     = "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"
RAPIDAPI_KEY   = "a23a17d86cmsh842318377a900e3p141dcajsn5c6bd1cd77ab"
BOOKING_AFF_ID = ""      # أضفه لاحقاً من partner.booking.com
CURRENCY       = "SAR"

# ─────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s │ %(levelname)-7s │ %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
)
log = logging.getLogger("TravelBot")

TG = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
TODAY = date.today().strftime("%Y-%m-%d")

# ─────────────────────────────────────────
#  الجلسات  (In-Memory)
# ─────────────────────────────────────────
_sessions: dict = {}

def get_session(uid: int) -> dict:
    if uid not in _sessions:
        _sessions[uid] = {
            "history": [],
            "ctx": {
                "city":      None,
                "check_in":  None,
                "check_out": None,
                "guests":    None,
            },
            "results": [],
            "step":    "idle",   # idle | collecting | searching | results
        }
    return _sessions[uid]

def clear_session(uid: int):
    _sessions.pop(uid, None)

# ─────────────────────────────────────────
#  Telegram  –  wrappers
# ─────────────────────────────────────────

def _tg(method: str, payload: dict) -> dict:
    try:
        r = requests.post(f"{TG}/{method}", json=payload, timeout=10)
        data = r.json()
        if not data.get("ok"):
            log.warning(f"[TG/{method}] {data.get('description','')}")
        return data
    except requests.exceptions.ConnectionError as e:
        log.error(f"[TG/{method}] Connection error: {e}")
        return {}
    except Exception as e:
        log.error(f"[TG/{method}] {e}")
        return {}


def tg_send(chat_id: int, text: str, keyboard: dict = None,
            preview: bool = False) -> dict:
    p = {
        "chat_id":                  chat_id,
        "text":                     text[:4096],
        "parse_mode":               "Markdown",
        "disable_web_page_preview": not preview,
    }
    if keyboard:
        p["reply_markup"] = json.dumps(keyboard)
    return _tg("sendMessage", p)


def tg_edit(chat_id: int, msg_id: int, text: str,
            keyboard: dict = None, preview: bool = False):
    p = {
        "chat_id":                  chat_id,
        "message_id":               msg_id,
        "text":                     text[:4096],
        "parse_mode":               "Markdown",
        "disable_web_page_preview": not preview,
    }
    if keyboard:
        p["reply_markup"] = json.dumps(keyboard)
    _tg("editMessageText", p)


def tg_delete(chat_id: int, msg_id: int):
    _tg("deleteMessage", {"chat_id": chat_id, "message_id": msg_id})


def tg_typing(chat_id: int):
    _tg("sendChatAction", {"chat_id": chat_id, "action": "typing"})


def tg_answer_cb(cb_id: str, text: str = ""):
    _tg("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})


def tg_get_updates(offset: int) -> list:
    try:
        r = requests.get(
            f"{TG}/getUpdates",
            params={"offset": offset, "timeout": 30, "limit": 100},
            timeout=35,
        )
        data = r.json()
        return data.get("result", []) if data.get("ok") else []
    except requests.exceptions.ConnectionError as e:
        log.error(f"[getUpdates] Connection: {e}")
        time.sleep(5)
        return []
    except Exception as e:
        log.error(f"[getUpdates] {e}")
        return []

# ─────────────────────────────────────────
#  Booking.com  via  RapidAPI
# ─────────────────────────────────────────

_BHOST = "booking-com15.p.rapidapi.com"
_BBASE = f"https://{_BHOST}/api/v1/hotels"
_BH    = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": _BHOST}


def _booking_dest_id(city: str) -> str | None:
    """يحوّل اسم المدينة إلى dest_id لـ Booking.com"""
    try:
        r = requests.get(
            f"{_BBASE}/searchDestination",
            headers=_BH,
            params={"query": city},
            timeout=10,
        )
        r.raise_for_status()
        items = r.json().get("data", [])
        if not items:
            return None
        # فضّل نوع city أو region
        for it in items:
            if it.get("search_type") in ("city", "region"):
                return it.get("dest_id")
        return items[0].get("dest_id")
    except requests.exceptions.HTTPError as e:
        log.error(f"[dest_id] HTTP {e.response.status_code}: {e.response.text[:120]}")
    except Exception as e:
        log.error(f"[dest_id] {e}")
    return None


def _extract_price(h: dict) -> float:
    """يستخرج السعر من أي مكان في استجابة Booking"""
    # المسار الأول: priceBreakdown.grossPrice.value
    pb = h.get("priceBreakdown") or {}
    gp = pb.get("grossPrice") or {}
    v  = gp.get("value") or gp.get("amount_rounded") or gp.get("amount")
    if v:
        return float(v)

    # المسار الثاني: min_total_price
    v = h.get("min_total_price")
    if v:
        return float(v)

    # المسار الثالث: داخل property
    prop = h.get("property") or {}
    v = prop.get("priceBreakdown", {}).get("grossPrice", {}).get("value")
    if v:
        return float(v)

    return 0.0


def search_hotels(city: str, check_in: str,
                  check_out: str, guests: int) -> list[dict]:
    """
    يبحث في Booking.com ويرجع قائمة بأفضل 3 فنادق.
    كل فندق: id, name, rating, stars, price, nights_total
    """
    # 1) dest_id
    dest_id = _booking_dest_id(city)
    if not dest_id:
        log.warning(f"[Hotels] No dest_id for '{city}'")
        return []

    # 2) بحث الفنادق
    params = {
        "dest_id":        dest_id,
        "search_type":    "CITY",
        "arrival_date":   check_in,
        "departure_date": check_out,
        "adults":         str(guests),
        "room_qty":       "1",
        "page_number":    "1",
        "languagecode":   "ar",
        "currency_code":  CURRENCY,
        "units":          "metric",
        "sort_by":        "popularity",
    }
    try:
        r = requests.get(f"{_BBASE}/searchHotels",
                         headers=_BH, params=params, timeout=15)
        r.raise_for_status()
        raw = r.json().get("data", {}).get("hotels", []) or []
    except requests.exceptions.HTTPError as e:
        log.error(f"[Hotels] HTTP {e.response.status_code}: {e.response.text[:200]}")
        return []
    except Exception as e:
        log.error(f"[Hotels] {e}")
        return []

    # 3) توحيد البيانات
    n_nights = _nights(check_in, check_out)
    parsed   = []
    for h in raw:
        prop   = h.get("property") or {}
        name   = (prop.get("name") or h.get("hotel_name") or "").strip()
        if not name:
            continue

        rating = float(prop.get("reviewScore") or
                       prop.get("review_score") or 0)
        stars  = int(prop.get("propertyClass") or
                     prop.get("hotel_class") or 0)
        price  = _extract_price(h)

        # السعر قد يكون إجمالي الرحلة — نقسم للحصول على سعر الليلة
        if price > 0 and n_nights > 1:
            # إذا السعر أكبر بكثير من المعتاد، على الأرجح إجمالي
            if price > 2000 and n_nights > 1:
                price_per_night = round(price / n_nights)
            else:
                price_per_night = round(price)
        elif price > 0:
            price_per_night = round(price)
        else:
            continue   # بدون سعر نتجاهله

        hotel_id = str(prop.get("id") or h.get("hotel_id") or name[:30])
        country  = prop.get("countryCode") or ""
        photo    = (prop.get("photoUrls") or [None])[0] or ""

        parsed.append({
            "id":             hotel_id,
            "name":           name,
            "rating":         round(rating, 1),
            "stars":          stars,
            "price_night":    price_per_night,
            "price_total":    price_per_night * n_nights,
            "country":        country,
            "photo":          photo,
        })

    if not parsed:
        log.warning(f"[Hotels] No priced results for {city}")
        return []

    # 4) اختيار أفضل 3 مختلفون
    by_price  = sorted(parsed, key=lambda x: x["price_night"])
    by_rating = sorted(parsed, key=lambda x: -x["rating"])

    def composite(x):
        return x["rating"] * 2.5 + x["stars"] * 1.5 - x["price_night"] / 500

    by_score = sorted(parsed, key=composite, reverse=True)

    top3: list[dict] = []
    seen: set        = set()
    for h in [by_price[0], by_rating[0]] + by_score:
        if h["id"] not in seen:
            top3.append(h)
            seen.add(h["id"])
        if len(top3) == 3:
            break

    log.info(f"[Hotels] {city}: found {len(parsed)} → top {len(top3)}")
    return top3


def make_booking_link(h: dict, ci: str, co: str, guests: int) -> str:
    name_q = requests.utils.quote(h["name"])
    base   = (
        f"https://www.booking.com/search.html"
        f"?ss={name_q}"
        f"&checkin={ci}"
        f"&checkout={co}"
        f"&group_adults={guests}"
        f"&no_rooms=1"
        f"&selected_currency={CURRENCY}"
    )
    if BOOKING_AFF_ID:
        base += f"&aid={BOOKING_AFF_ID}"
    return base

# ─────────────────────────────────────────
#  GPT-4o  –  تحليل الرسائل
# ─────────────────────────────────────────

_SYS = f"""أنت مساعد سفر ذكي على تيليجرام. مهمتك الوحيدة: مساعدة المستخدم للبحث عن فنادق وحجزها.

أسلوبك: اللهجة السعودية (وين، تبي، زين، هلا، ودّ، أبغى، بعدين).
إذا كتب المستخدم بالإنجليزية → رد بالإنجليزية.
الردود مختصرة جداً. لا سؤالين بنفس الرسالة أبداً.

مهمتك: أعد JSON فقط — لا أي نص إضافي — بهذا الشكل الدقيق:
{{
  "intent":    "search" | "select" | "reset" | "help" | "other",
  "city":      "اسم المدينة بالإنجليزي أو null",
  "check_in":  "YYYY-MM-DD أو null",
  "check_out": "YYYY-MM-DD أو null",
  "guests":    عدد_صحيح أو null,
  "missing":   ["city"|"check_in"|"check_out"|"guests"],
  "question":  "سؤال واحد مختصر إذا ناقصة معلومة، وإلا null",
  "selection": 1 | 2 | 3 | null,
  "reply":     "رد مختصر إذا intent=other|help، وإلا null"
}}

قواعد الاستخراج:
• اليوم: {TODAY}
• حوّل الأسماء العربية: دبي→Dubai، الرياض→Riyadh، مكة→Mecca، جدة→Jeddah،
  أبوظبي→Abu Dhabi، لندن→London، باريس→Paris، إسطنبول→Istanbul،
  القاهرة→Cairo، بيروت→Beirut، عمّان→Amman، المدينة→Medina
• "الأسبوع الجاي/القادم" → check_in = يوم الاثنين القادم
• "نهاية الشهر" → آخر 3 أيام الشهر الحالي
• "شخصين / اثنين" → guests: 2
• "عيلة / ٤ أشخاص" → guests: 4
• إذا قال فقط رقم 1،2،3 أو ١،٢،٣ أو "الأول/الثاني/الثالث" → selection: الرقم
• missing يحتوي فقط الحقول الفعلاً الناقصة
• إذا ذكر كل شيء في جملة واحدة → missing: []"""


def gpt_parse(text: str, history: list, ctx: dict) -> dict:
    """يحلل رسالة المستخدم ويرجع dict منظم"""
    # أضف السياق المعروف للرسالة
    known = {k: v for k, v in ctx.items() if v is not None}
    msg   = text
    if known:
        msg += f"\n[السياق المعروف: {json.dumps(known, ensure_ascii=False)}]"

    messages = [
        {"role": "system", "content": _SYS},
        *history[-8:],              # آخر 8 رسائل فقط → توفير tokens
        {"role": "user", "content": msg},
    ]

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":           "gpt-4o",
                "messages":        messages,
                "temperature":     0.05,
                "max_tokens":      350,
                "response_format": {"type": "json_object"},
            },
            timeout=18,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"]
        result  = json.loads(content)
        log.info(f"[GPT] intent={result.get('intent')} "
                 f"city={result.get('city')} "
                 f"ci={result.get('check_in')} "
                 f"co={result.get('check_out')} "
                 f"g={result.get('guests')} "
                 f"missing={result.get('missing')} "
                 f"sel={result.get('selection')}")
        return result

    except requests.exceptions.HTTPError as e:
        body = e.response.text[:200] if e.response else ""
        log.error(f"[GPT] HTTP {e.response.status_code if e.response else '?'}: {body}")
        if e.response and e.response.status_code == 401:
            return {"intent": "other", "missing": [], "reply":
                    "⚠️ مفتاح OpenAI منتهي أو خاطئ. تواصل مع المطوّر."}
        if e.response and e.response.status_code == 429:
            return {"intent": "other", "missing": [], "reply":
                    "⚠️ تجاوزنا حد الطلبات. انتظر دقيقة وحاول مرة ثانية."}
        return {"intent": "other", "missing": [], "reply":
                "صار خطأ مؤقت، حاول مرة ثانية 🙏"}

    except json.JSONDecodeError as e:
        log.error(f"[GPT] JSON decode: {e}")
        return {"intent": "other", "missing": [], "reply":
                "ما فهمت طلبك. قولي وين تبي تروح؟"}

    except Exception as e:
        log.error(f"[GPT] {type(e).__name__}: {e}")
        return {"intent": "other", "missing": [],
                "reply": "صار خطأ مؤقت، حاول مرة ثانية 🙏"}

# ─────────────────────────────────────────
#  تنسيق الرسائل
# ─────────────────────────────────────────

_LABELS = {
    0: ("1️⃣", "الأرخص"),
    1: ("2️⃣", "الأفضل تقييماً"),
    2: ("3️⃣", "الأميز"),
}

def _nights(ci: str, co: str) -> int:
    try:
        return max(1, (datetime.strptime(co, "%Y-%m-%d") -
                       datetime.strptime(ci, "%Y-%m-%d")).days)
    except Exception:
        return 1

def _stars(n: int) -> str:
    n = max(0, min(n, 5))
    return "★" * n + "☆" * (5 - n) if n > 0 else ""

def _fmt_price(p: int) -> str:
    return f"{p:,}"

def hotel_card(h: dict, idx: int, ci: str, co: str, g: int) -> str:
    emoji, label = _LABELS[idx]
    n = _nights(ci, co)
    return (
        f"{emoji} *{label}*\n"
        f"🏨 {h['name']}\n"
        f"⭐ {h['rating']}  {_stars(h['stars'])}\n"
        f"💰 {_fmt_price(h['price_night'])} ر.س/ليلة"
        f"  ·  المجموع: *{_fmt_price(h['price_total'])} ر.س* ({n} ليالٍ)"
    )

def results_message(hotels: list, city: str, ci: str, co: str, g: int) -> str:
    n = _nights(ci, co)
    header = (
        f"✅ وجدت *{len(hotels)} خيارات* في {city}\n"
        f"📅 {ci}  →  {co}  ·  {n} {'ليلة' if n == 1 else 'ليالٍ'}\n"
        f"👥 {g} {'شخص' if g == 1 else 'أشخاص'}\n"
        f"{'━' * 26}\n\n"
    )
    cards  = "\n\n".join(hotel_card(h, i, ci, co, g) for i, h in enumerate(hotels))
    footer = f"\n\n{'━' * 26}\nأي واحد يعجبك؟ 👇"
    return header + cards + footer

def booking_message(h: dict, ci: str, co: str, g: int, link: str) -> str:
    n = _nights(ci, co)
    return (
        f"ممتاز! 🎉\n\n"
        f"🏨 *{h['name']}*\n"
        f"⭐ {h['rating']}  {_stars(h['stars'])}\n"
        f"📅 {ci}  →  {co}  ·  {n} ليالٍ\n"
        f"👥 {g} أشخاص\n"
        f"💰 *{_fmt_price(h['price_total'])} ر.س* إجمالي\n\n"
        f"🔗 [اضغط هنا لإكمال الحجز على Booking\\.com]({link})\n\n"
        f"✅ السعر مضمون\n"
        f"✅ الإلغاء حسب سياسة الفندق\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"تحتاج بحث ثاني؟  /start"
    )

def pick_keyboard() -> dict:
    return {"inline_keyboard": [
        [{"text": "1️⃣  احجز الأرخص",          "callback_data": "book_0"}],
        [{"text": "2️⃣  احجز الأفضل تقييماً",   "callback_data": "book_1"}],
        [{"text": "3️⃣  احجز الأميز",            "callback_data": "book_2"}],
        [{"text": "🔄  بحث جديد",               "callback_data": "reset"}],
    ]}

def _q_for(field: str) -> str:
    return {
        "city":      "وين تبي تروح؟ 🌍",
        "check_in":  "من أي تاريخ تبغى تدخل؟ 📅\n_(مثال: 20 يونيو)_",
        "check_out": "وإلى أي تاريخ؟ 📅\n_(مثال: 23 يونيو)_",
        "guests":    "كم شخص؟ 👥",
    }.get(field, "وين تبي تروح؟")

# ─────────────────────────────────────────
#  معالجة الأحداث
# ─────────────────────────────────────────

def handle_message(chat_id: int, uid: int, text: str, first_name: str):
    s    = get_session(uid)
    text = text.strip()

    # ── أوامر بسيطة ──────────────────────
    if text.startswith("/start"):
        clear_session(uid)
        tg_send(chat_id,
            f"هلا {first_name}! 👋\n"
            "أنا مساعد سفرك الذكي ✈️\n\n"
            "وين تبي تروح وامتى؟\n\n"
            "_مثال: أبغى فندق في دبي من 20 إلى 23 يونيو لشخصين_"
        )
        return

    if text.startswith("/reset"):
        clear_session(uid)
        tg_send(chat_id, "تم المسح 🔄\nوين تبي تروح؟")
        return

    if text.startswith("/help"):
        tg_send(chat_id,
            "🤖 *كيف أستخدمك؟*\n\n"
            "قولي مثلاً:\n"
            "👉 _أبغى فندق في الرياض الأسبوع الجاي لأسبوع_\n"
            "👉 _فندق في لندن من 1 يوليو لـ 5 أيام لشخصين_\n\n"
            "أو خطوة بخطوة وأنا أسألك 😊\n\n"
            "/start  —  بداية جديدة\n"
            "/reset  —  مسح البحث الحالي\n"
            "/help   —  هذه الرسالة"
        )
        return

    # ── أضف للتاريخ ───────────────────────
    s["history"].append({"role": "user", "content": text})
    tg_typing(chat_id)

    # ── تحليل GPT ─────────────────────────
    p       = gpt_parse(text, s["history"], s["ctx"])
    intent  = p.get("intent", "other")
    missing = p.get("missing") or []
    sel     = p.get("selection")

    # تحديث السياق بما استخرجه GPT
    for f in ("city", "check_in", "check_out", "guests"):
        v = p.get(f)
        if v is not None:
            s["ctx"][f] = v

    ctx = s["ctx"]

    # ── reset من GPT ──────────────────────
    if intent == "reset":
        clear_session(uid)
        tg_send(chat_id, "تم المسح 🔄\nوين تبي تروح؟")
        return

    # ── اختيار فندق ─────────────────────
    if (intent == "select" or sel is not None) and s["results"]:
        idx = int(sel or 1) - 1
        idx = max(0, min(idx, len(s["results"]) - 1))
        _deliver_link(chat_id, s, idx)
        return

    # ── ناقصة معلومة ─────────────────────
    if missing:
        q = p.get("question") or _q_for(missing[0])
        tg_send(chat_id, q)
        s["history"].append({"role": "assistant", "content": q})
        return

    # ── كل المعلومات موجودة → ابحث ────────
    if all(ctx.get(f) for f in ("city", "check_in", "check_out", "guests")):
        _run_search(chat_id, s)
        return

    # ── رد عام ────────────────────────────
    reply = p.get("reply") or "تفضل، أخبرني وين تبي تروح؟ 😊"
    tg_send(chat_id, reply)
    s["history"].append({"role": "assistant", "content": reply})


def _run_search(chat_id: int, s: dict):
    """تنفيذ بحث الفنادق وإرسال النتائج"""
    ctx  = s["ctx"]
    city = ctx["city"]
    ci   = ctx["check_in"]
    co   = ctx["check_out"]
    g    = ctx["guests"]

    # رسالة بحث مؤقتة
    tmp = tg_send(chat_id,
        f"🔍 أدور لك على فنادق في *{city}*...\n"
        f"📅 {ci}  →  {co}  |  👥 {g} أشخاص\n"
        f"_قد يأخذ بضع ثوانٍ_ ⏳"
    )
    tmp_id = (tmp.get("result") or {}).get("message_id")
    tg_typing(chat_id)

    hotels = search_hotels(city, ci, co, g)

    # احذف رسالة الانتظار
    if tmp_id:
        tg_delete(chat_id, tmp_id)

    if not hotels:
        err_msg = (
            "ما لقيت فنادق متاحة بهالمواصفات 😔\n\n"
            "جرّب:\n"
            "• تواريخ مختلفة\n"
            "• مدينة أخرى\n"
            "• تقليل عدد الأشخاص\n\n"
            "/reset  —  ابدأ بحثاً جديداً"
        )
        tg_send(chat_id, err_msg)
        s["history"].append({"role": "assistant", "content": err_msg})
        return

    s["results"] = hotels
    s["step"]    = "results"

    msg = results_message(hotels, city, ci, co, g)
    tg_send(chat_id, msg, keyboard=pick_keyboard())
    s["history"].append({"role": "assistant", "content": msg})


def _deliver_link(chat_id: int, s: dict, idx: int):
    """إرسال رابط الحجز للفندق المختار"""
    h   = s["results"][idx]
    ctx = s["ctx"]
    ci  = ctx.get("check_in",  "")
    co  = ctx.get("check_out", "")
    g   = ctx.get("guests",    2)

    link = make_booking_link(h, ci, co, g)
    msg  = booking_message(h, ci, co, g, link)
    tg_send(chat_id, msg, preview=True)
    s["history"].append({"role": "assistant", "content": msg})


def handle_callback(chat_id: int, uid: int, msg_id: int,
                    data: str, cb_id: str):
    tg_answer_cb(cb_id)
    s = get_session(uid)

    # زر Reset
    if data == "reset":
        clear_session(uid)
        tg_edit(chat_id, msg_id, "تم المسح 🔄\nابدأ بحثاً جديداً: /start")
        return

    # زر حجز
    if data.startswith("book_"):
        idx = int(data.split("_")[1])
        if not s["results"]:
            tg_edit(chat_id, msg_id,
                    "انتهت صلاحية هذه النتائج ⏰\n"
                    "ابدأ بحثاً جديداً:  /start")
            return

        idx  = max(0, min(idx, len(s["results"]) - 1))
        h    = s["results"][idx]
        ctx  = s["ctx"]
        ci   = ctx.get("check_in",  "")
        co   = ctx.get("check_out", "")
        g    = ctx.get("guests",    2)
        link = make_booking_link(h, ci, co, g)
        msg  = booking_message(h, ci, co, g, link)

        tg_edit(chat_id, msg_id, msg, preview=True)

# ─────────────────────────────────────────
#  الحلقة الرئيسية
# ─────────────────────────────────────────

def main():
    print()
    print("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  ✈️   مساعد السفر الذكي — MVP v1.0")
    print("  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print()

    # تحقق من الاتصال بتيليجرام
    try:
        r    = requests.get(f"{TG}/getMe", timeout=12)
        r.raise_for_status()
        info = r.json().get("result", {})
        print(f"  ✅  @{info.get('username')}  —  {info.get('first_name')}")
        print(f"  🤖  Bot ID: {info.get('id')}")
    except requests.exceptions.ConnectionError as e:
        print(f"\n  ❌  لا يمكن الاتصال بتيليجرام: {e}")
        print("  تأكد من اتصالك بالإنترنت وصحة TELEGRAM_TOKEN\n")
        return
    except requests.exceptions.HTTPError as e:
        print(f"\n  ❌  خطأ HTTP {e.response.status_code}: التوكن خاطئ أو منتهي\n")
        return
    except Exception as e:
        print(f"\n  ❌  خطأ غير متوقع: {e}\n")
        return

    print()
    print("  🟢  البوت يستمع للرسائل...")
    print("  ⌨️   اضغط Ctrl+C للإيقاف")
    print()

    offset     = 0
    error_wait = 2   # انتظار عند الأخطاء المتكررة

    while True:
        try:
            updates = tg_get_updates(offset)

            for upd in updates:
                offset = upd["update_id"] + 1

                # ── رسالة نصية ──────────────────
                if "message" in upd:
                    m    = upd["message"]
                    text = m.get("text", "").strip()
                    if not text:
                        continue
                    u = m.get("from") or {}
                    c = m.get("chat") or {}
                    log.info(f"MSG  {u.get('first_name','?')} ({u.get('id')}) → {text[:60]}")
                    try:
                        handle_message(c["id"], u["id"], text,
                                       u.get("first_name", ""))
                    except Exception as e:
                        log.error(f"[handle_message] {e}")

                # ── ضغطة زر ─────────────────────
                elif "callback_query" in upd:
                    cq   = upd["callback_query"]
                    u    = cq.get("from") or {}
                    m    = cq.get("message") or {}
                    c    = m.get("chat") or {}
                    data = cq.get("data", "")
                    log.info(f"BTN  {u.get('first_name','?')} ({u.get('id')}) → {data}")
                    try:
                        handle_callback(c["id"], u["id"],
                                        m.get("message_id", 0),
                                        data, cq["id"])
                    except Exception as e:
                        log.error(f"[handle_callback] {e}")

            error_wait = 2   # reset عند النجاح

        except KeyboardInterrupt:
            print("\n\n  ⛔  تم إيقاف البوت. مع السلامة!\n")
            break
        except Exception as e:
            log.error(f"[main_loop] {type(e).__name__}: {e}")
            time.sleep(error_wait)
            error_wait = min(error_wait * 2, 30)   # exponential backoff


if __name__ == "__main__":
    main()
