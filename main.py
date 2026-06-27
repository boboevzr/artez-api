import os
import re
import secrets
import logging
import asyncio
import random
import string
from datetime import datetime, timedelta, timezone

import json as _json
import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Body, Request, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, field_validator
from passlib.context import CryptContext
from jose import jwt, JWTError

import database as db

logging.basicConfig(level=logging.INFO)

# ── Конфигурация ──
JWT_SECRET     = os.getenv("JWT_SECRET", "dev-secret-change-me")
JWT_ALGORITHM  = "HS256"
JWT_EXPIRE_DAYS = 30
SMS_CODE_TTL_MIN = 5
ADMIN_PASS     = os.getenv("ADMIN_PASS", "")

async def get_admin_pass() -> str:
    """Читает пароль из БД (если изменён через UI), иначе — env var."""
    return await db.get_config("admin_pass") or ADMIN_PASS
VAPID_PRIVATE  = os.getenv("VAPID_PRIVATE", "")
VAPID_PUBLIC   = os.getenv("VAPID_PUBLIC", "")

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
GROUP_ID              = os.getenv("GROUP_ID", "")
GROUP_ID_ZARAFSHAN    = os.getenv("GROUP_ID_ZARAFSHAN", "")
LEADS_GROUP_ID        = os.getenv("LEADS_GROUP_ID", "-1004486597965")
GROUP_NEW_CLIENTS_ID  = os.getenv("GROUP_NEW_CLIENTS_ID", "-1003768571929")
GROUP_DELIVERY_ID            = os.getenv("GROUP_DELIVERY_ID", "-5434866533")
GROUP_DELIVERY_ZARAFSHAN_ID      = os.getenv("GROUP_DELIVERY_ZARAFSHAN_ID", "-1004327266702")
GROUP_DELIVERY_NAVOI_ID          = os.getenv("GROUP_DELIVERY_NAVOI_ID", "-1004327266702")
GROUP_DELIVERY_ZARAFSHAN_CHANNEL = os.getenv("GROUP_DELIVERY_ZARAFSHAN_CHANNEL", "-1004483444044")
GROUP_DELIVERY_NAVOI_CHANNEL     = os.getenv("GROUP_DELIVERY_NAVOI_CHANNEL", "-1004483444044")
GROUP_ID_NAVOI     = os.getenv("GROUP_ID_NAVOI", "")
MEDIA_CHANNEL_ID   = os.getenv("MEDIA_CHANNEL_ID", "-1004453880659")
APP_URL            = os.getenv("APP_URL", "")  # https://your-app.railway.app

async def _get_media_channel() -> str:
    ch = await db.get_media_channel_id()
    return ch or MEDIA_CHANNEL_ID
SHEETS_URL = os.getenv("SHEETS_URL", "https://script.google.com/macros/s/AKfycbyU5a3pMuTFme3dBNEgu46qzA1sN1Ekw-Q7p39F1Pg872lnnXZEFhJPjuc4TzZNHlpObQ/exec")

# ── Eskiz SMS ──
ESKIZ_EMAIL    = os.getenv("ESKIZ_EMAIL", "")
ESKIZ_PASSWORD = os.getenv("ESKIZ_PASSWORD", "")
ESKIZ_FROM     = os.getenv("ESKIZ_FROM", "4546")   # имя отправителя — 4546 для тестов
_eskiz_token   = ""  # кэш токена в памяти

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI(title="ARTEZ API")

def bi(ru: str, uz: str) -> str:
    """Двуязычное сообщение для ошибок, видимых пользователю."""
    return f"{ru} / {uz}"

# CORS — разрешаем запросы с сайта (уточните домен в проде)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logging.error(f"422 on {request.method} {request.url.path}: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
        headers={"Access-Control-Allow-Origin": "*"},
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logging.error(f"Unhandled exception on {request.method} {request.url.path}: {type(exc).__name__}: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"{type(exc).__name__}: {exc}"},
        headers={"Access-Control-Allow-Origin": "*"},
    )


@app.on_event("startup")
async def startup():
    await db.init_db()
    await db.ensure_plans_table()
    await db.ensure_chat_tables()
    await db.ensure_chat_templates()
    asyncio.create_task(_tg_reminder_worker())
    asyncio.create_task(_chat_timeout_worker())
    asyncio.create_task(_measure_review_worker())
    # Webhook не нужен — бот работает в режиме polling (ARTEZ-BOT сервис на Railway)
    # if BOT_TOKEN and APP_URL:
    #     asyncio.create_task(_set_tg_webhook())

async def send_web_push(staff_id: int, title: str, body: str, lead_id: int = None, phone: str = None,
                        order_id: int = None, item_id: int = None, push_type: str = None):
    if not VAPID_PRIVATE or not VAPID_PUBLIC:
        return
    try:
        from pywebpush import webpush, WebPushException
        subs = await db.get_push_subscriptions(staff_id)
        for sub in subs:
            try:
                payload = _json.dumps({"title": title, "body": body, "lead_id": lead_id, "phone": phone,
                                       "order_id": order_id, "item_id": item_id, "type": push_type})
                webpush(
                    subscription_info={"endpoint": sub["endpoint"],
                                       "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]}},
                    data=payload,
                    vapid_private_key=VAPID_PRIVATE,
                    vapid_claims={"sub": "mailto:admin@artez.uz"},
                )
            except Exception as ex:
                resp = getattr(ex, 'response', None)
                if resp and resp.status_code in (404, 410):
                    await db.delete_push_subscription(sub["endpoint"])
                else:
                    logging.warning(f"web_push error for sub {sub['id']}: {ex}")
    except ImportError:
        logging.warning("pywebpush not installed, skipping web push")
    except Exception as e:
        logging.warning(f"send_web_push error: {e}")


async def _tg_reminder_worker():
    """Каждую минуту проверяет напоминания и шлёт в Telegram + Web Push."""
    await asyncio.sleep(10)
    while True:
        try:
            if BOT_TOKEN:
                rows = await db.get_pending_tg_reminders()
                for r in rows:
                    lead_code = r["lead_code"] or f"#{r['lead_id']}"
                    client    = r["client_name"] or r["client_phone"]
                    msg       = r["message"] or "Запланированный звонок"
                    tg_id     = r["staff_tg_id"]
                    staff_name = " ".join(filter(None, [r.get("staff_last_name"), r.get("staff_first_name")])) or "Сотрудник"

                    if tg_id:
                        text = (f"⏰ Напоминание о звонке\n\n"
                                f"Лид {lead_code} — {client}\n"
                                f"📞 {r['client_phone']}\n"
                                f"💬 {msg}")
                        await send_tg(tg_id, text)
                    else:
                        text = (f"⏰ Напоминание ({staff_name})\n\n"
                                f"Лид {lead_code} — {client}\n"
                                f"📞 {r['client_phone']}\n"
                                f"💬 {msg}")
                        await send_tg(LEADS_GROUP_ID, text)
                    # Web Push + уведомление — только тому, кто взял лид (target_staff_id)
                    target_id = r.get("target_staff_id") or r.get("staff_id")
                    if target_id:
                        push_body = f"📞 {r['client_phone']}" + (f"\n{msg}" if msg != "Запланированный звонок" else "")
                        asyncio.create_task(send_web_push(
                            target_id,
                            f"🔔 Перезвонить: {client}",
                            push_body,
                            r["lead_id"],
                            r["client_phone"]
                        ))
                        try:
                            await db.create_agent_notification(
                                target_id, r["lead_id"],
                                "callback",
                                f"Пора перезвонить: {client} — {r['client_phone']}"
                                + (f". {msg}" if msg != "Запланированный звонок" else "")
                            )
                        except Exception:
                            pass
                    await db.mark_reminder_sent(r["id"], "tg")
        except Exception as e:
            logging.warning(f"TG reminder worker error: {e}")
        await asyncio.sleep(60)


async def _measure_review_worker():
    """Каждые 5 минут: один сводный push на каждого проверяющего."""
    await asyncio.sleep(30)
    while True:
        try:
            from datetime import datetime, timezone
            reviews   = await db.get_pending_measure_reviews()
            approvers = await db.get_all_approvers()
            if not reviews:
                await asyncio.sleep(300)
                continue
            now = datetime.now(timezone.utc)

            # Замеры которые принял кто-то и прошло > 5 мин — напомнить именно ему
            reminded_claimer = set()
            for rev in reviews:
                claimed_by = rev.get("review_claimed_by")
                claimed_at = rev.get("review_claimed_at")
                if claimed_by and claimed_at and claimed_by not in reminded_claimer:
                    elapsed = (now - claimed_at.replace(tzinfo=timezone.utc)).total_seconds()
                    if elapsed > 300:
                        order_num = rev.get("order_num") or f"#{rev['order_id']}"
                        asyncio.create_task(send_web_push(
                            claimed_by, "⏰ Не забудь проверить замеры",
                            f"Принятые замеры ждут утверждения (заказ {order_num})",
                            order_id=rev["order_id"], item_id=rev["item_id"], push_type="measure"
                        ))
                        reminded_claimer.add(claimed_by)

            # Незаклеймленные — один сводный пуш на каждого проверяющего
            unclaimed = [r for r in reviews if not r.get("review_claimed_by")]
            if unclaimed:
                cnt = len(unclaimed)
                if cnt == 1:
                    r = unclaimed[0]
                    title = f"📐 Замер на проверку — {r.get('order_num') or '#'+str(r['order_id'])}"
                    body  = f"«{r.get('service') or 'позиция'}» ожидает утверждения"
                    first_order_id = r["order_id"]
                    first_item_id  = r["item_id"]
                else:
                    orders = list({r["order_id"] for r in unclaimed})
                    title = f"📐 {cnt} замеров ожидают проверки"
                    body  = f"Заказов: {len(orders)} · Нажмите для просмотра"
                    first_order_id = unclaimed[0]["order_id"]
                    first_item_id  = unclaimed[0]["item_id"]
                for approver in approvers:
                    asyncio.create_task(send_web_push(
                        approver["id"], title, body,
                        order_id=first_order_id, item_id=first_item_id, push_type="measure"
                    ))
        except Exception as e:
            logging.warning(f"measure_review_worker error: {e}")
        await asyncio.sleep(300)


async def send_tg(chat_id, text: str):
    if not BOT_TOKEN or not chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            await s.post(url, json={"chat_id": str(chat_id), "text": text},
                         timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"send_tg error: {e}")


async def _send_tg_with_kb(chat_id, text: str, keyboard: dict, parse_mode: str | None = "HTML") -> int | None:
    """Отправить сообщение с inline-клавиатурой, вернуть message_id."""
    if not BOT_TOKEN or not chat_id:
        return None
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as s:
            payload = {"chat_id": str(chat_id), "text": text,
                       "reply_markup": keyboard, "disable_web_page_preview": True}
            if parse_mode: payload["parse_mode"] = parse_mode
            r = await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
            d = await r.json()
            if not d.get("ok"):
                # Группа мигрировала в супергруппу — повторяем с новым ID
                new_id = (d.get("parameters") or {}).get("migrate_to_chat_id")
                if new_id:
                    logging.info(f"_send_tg_with_kb: group migrated → {new_id}, retrying")
                    payload["chat_id"] = str(new_id)
                    r2 = await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
                    d2 = await r2.json()
                    if d2.get("ok"):
                        return d2.get("result", {}).get("message_id")
                    logging.warning(f"_send_tg_with_kb retry error: {d2.get('description')}")
                    return None
                logging.warning(f"_send_tg_with_kb TG error: {d.get('description')}")
                return None
            return d.get("result", {}).get("message_id")
    except Exception as e:
        logging.warning(f"_send_tg_with_kb error: {e}")
        return None

async def _edit_tg_with_kb(chat_id, message_id: int, text: str, keyboard: dict):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": str(chat_id), "message_id": message_id,
                      "text": text, "parse_mode": "HTML",
                      "reply_markup": keyboard, "disable_web_page_preview": True},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"_edit_tg_with_kb error: {e}")

async def _tg_answer_callback(callback_query_id: str, text: str, alert: bool = False):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                json={"callback_query_id": callback_query_id, "text": text, "show_alert": alert},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"answerCallbackQuery error: {e}")


async def _tg_edit_message(chat_id, message_id: int, text: str):
    if not BOT_TOKEN: return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageText",
                json={"chat_id": str(chat_id), "message_id": message_id,
                      "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception as e:
        logging.warning(f"editMessageText error: {e}")


_STATUS_LABELS_RU = {
    "new":       "🆕 Новый",
    "contacted": "📞 Связались с клиентом",
    "no_answer": "📵 Не дозвонились",
    "callback":  "🔔 Перезвонить",
    "converted": "🏆 Стал заказом!",
    "lost":      "❌ Закрыт как потерянный",
}
_STATUS_LABELS_UZ = {
    "new":       "🆕 Yangi",
    "contacted": "📞 Mijoz bilan bog'landi",
    "no_answer": "📵 Qo'ng'iroq qilmadi",
    "callback":  "🔔 Qayta qo'ng'iroq",
    "converted": "🏆 Buyurtmaga aylandi!",
    "lost":      "❌ Yo'qotilgan deb yopildi",
}

async def _notify_agent_status(lead_id: int, status: str, note: str):
    lead = await db.get_lead_by_id(lead_id)
    if not lead or not lead["volunteer_id"]:
        return
    agent = await db.get_staff_by_id(lead["volunteer_id"])
    if not agent:
        return

    code   = lead.get("lead_code") or f"#{lead_id}"
    client = lead.get("client_name") or lead.get("client_phone") or "—"
    phone  = lead.get("client_phone") or "—"
    label_ru = _STATUS_LABELS_RU.get(status, status)
    label_uz = _STATUS_LABELS_UZ.get(status, status)

    msg_ru = (f"🎯 Обновление по вашему лиду {code}\n\n"
              f"👤 {client}\n📞 {phone}\n\n"
              f"Статус: {label_ru}\n"
              + (f"💬 {note}" if note and note not in _STATUS_LABELS_RU.values() else ""))
    msg_uz = (f"🎯 Sizning lidingiz bo'yicha yangilik {code}\n\n"
              f"👤 {client}\n📞 {phone}\n\n"
              f"Holat: {label_uz}\n"
              + (f"💬 {note}" if note and note not in _STATUS_LABELS_RU.values() else ""))

    # В личный кабинет (таблица)
    await db.create_agent_notification(agent["id"], lead_id, f"status_{status}", msg_ru)

    tg_id = agent.get("tg_id")
    if tg_id:
        await send_tg(tg_id, msg_ru + "\n\n" + msg_uz)


async def _notify_new_lead(lead: dict, staff: dict):
    # Web push к callcenter/manager/admin — всегда, независимо от TG настроек
    if db.pool and lead.get("id"):
        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT DISTINCT s.id FROM staff s "
                    "JOIN push_subscriptions ps ON ps.staff_id = s.id "
                    "WHERE s.active=TRUE AND s.role IN ('callcenter','manager','admin')"
                )
            lead_code   = lead.get("lead_code") or f"#{lead.get('id')}"
            push_title  = f"🎯 Новый лид — {lead_code}"
            push_body   = f"{lead.get('client_name') or '—'} · {lead.get('client_phone') or '—'}"
            client_phone = lead.get("client_phone", "")
            for row in rows:
                asyncio.create_task(send_web_push(
                    row["id"], push_title, push_body,
                    lead_id=lead.get("id"), phone=client_phone, push_type="new_lead"
                ))
        except Exception as _ex:
            logging.warning(f"_notify_new_lead push error: {_ex}")

    # Telegram группа — только если включено в настройках
    enabled = await _get_cfg("leads_group_enabled")
    if enabled not in ("1", "true"):
        return

    # Роутинг по филиалу: своя группа или общая fallback
    branch = (lead.get("branch", "") or "").lower().replace("📍", "").strip()
    if branch in ("zarafshan", "зарафшан", "zarafshon"):
        group_id = await _get_cfg("leads_group_zarafshan") or await _get_cfg("leads_group_id")
    elif branch in ("navoi", "навои", "navoiy"):
        group_id = await _get_cfg("leads_group_navoi") or await _get_cfg("leads_group_id")
    else:
        group_id = await _get_cfg("leads_group_id")

    if not group_id:
        return

    template = await _get_cfg("lead_notify_ru")

    role    = staff.get("role", "")
    if role == "agent":   source = "🤝 Агент"
    elif role == "site":  source = "🌐 Сайт"
    elif role == "bot":   source = "✈️ Telegram"
    else:                 source = "👤 Сотрудник"
    creator = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "—")

    # source_full: для агентов/сотрудников добавляем имя, для сайта/бота — только иконка
    if role == "agent":
        source_full = f"🤝 {creator}" if creator and creator != "—" else "🤝 Агент"
    elif role == "site":
        source_full = "🌐 Сайт"
    elif role == "bot":
        source_full = "✈️ Telegram"
    else:
        source_full = f"👤 {creator}" if creator and creator != "—" else "👤 Сотрудник"

    loc = (lead.get("location") or "").strip()
    if loc:
        parts = loc.split(",")
        try:
            lat, lon = parts[0].strip(), parts[1].strip()
            map_url = f"https://yandex.uz/maps/?pt={lon},{lat}&z=16"
            location_link = f'<a href="{map_url}">📍 Локация</a>'
        except Exception:
            location_link = ""
    else:
        location_link = ""

    note_full = lead.get("note") or ""
    # note_short: первый сегмент заметки (до " · "), убираем префикс "Тип: "
    note_first = note_full.split(" · ")[0] if note_full else ""
    if note_first.startswith("Тип: "):
        note_first = note_first[5:]
    note_inline = f" · {note_first}" if note_first else ""

    vars_ = {
        "lead_code":     lead.get("lead_code") or f"#{lead.get('id')}",
        "client_name":   lead.get("client_name") or "—",
        "client_phone":  lead.get("client_phone") or "—",
        "branch":        branch_ru(branch) if branch else "—",
        "note":          note_full or "—",
        "note_short":    note_first,
        "note_inline":   note_inline,
        "source":        source,
        "source_full":   source_full,
        "creator":       creator,
        "location_link": location_link,
    }

    text = template
    if not text:
        return

    try:
        msg_text = text.format_map(vars_)
    except Exception:
        msg_text = text

    # Кнопка "Взять лид" — только если лид не занят
    lead_id  = lead.get("id")
    keyboard = None
    if lead_id and not lead.get("assigned_to"):
        keyboard = {"inline_keyboard": [[
            {"text": "✋ Взять лид", "callback_data": f"take_lead_{lead_id}"}
        ]]}

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": str(group_id),
            "text": msg_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if keyboard:
            payload["reply_markup"] = keyboard
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logging.warning(f"_notify_new_lead error: {e}")


# ══════════════════════════════════════
#  МОДЕЛИ
# ══════════════════════════════════════
PHONE_RE = re.compile(r"^\+998\d{9}$")

def normalize_phone(phone: str) -> str:
    phone = phone.strip().replace(" ", "").replace("-", "")
    if not phone.startswith("+"):
        phone = "+" + phone
    return phone

class RegisterRequest(BaseModel):
    phone: str
    password: str
    first_name: str
    via_tg: bool = False
    lang: str = "ru"

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        if len(v) < 6:
            raise ValueError("Пароль должен быть не короче 6 символов")
        return v

class VerifyRequest(BaseModel):
    phone: str
    code: str

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)

class LoginRequest(BaseModel):
    phone: str
    password: str

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)

class ResendCodeRequest(BaseModel):
    phone: str
    purpose: str = "register"
    via_tg: bool = False

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)


class AgentApplyRequest(BaseModel):
    branch: str = ""

class OrderRequest(BaseModel):
    first_name: str
    last_name: str = ""
    phone: str
    branch: str = ""
    city: str = ""
    address: str
    location: str = ""
    location_address: str = ""
    service: str = ""
    service_type: str = ""
    pickup_date: str = ""
    pickup_time: str = ""
    is_quick: bool = False
    total_price: int | None = None
    source: str = "site"  # "site" or "bot"
    client_tg_id: int | None = None

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

    @field_validator("first_name")
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Укажите имя")
        return v.strip()

    @field_validator("address")
    @classmethod
    def validate_address(cls, v):
        return v.strip()  # allow empty for quick/bot orders


class StaffOrderRequest(BaseModel):
    first_name: str
    phone: str
    service: str = ""
    service_type: str = "standard"
    pickup_type: str = "courier"
    delivery_type: str = "courier"
    branch: str = ""
    address: str = ""
    short_address: str = ""
    location: str = ""
    location_address: str = ""
    note: str = ""


# ══════════════════════════════════════
#  SMS — Eskiz.uz
# ══════════════════════════════════════
async def _eskiz_get_token() -> str:
    """Получает/обновляет токен Eskiz. Читает email/пароль из БД (приоритет) или env."""
    global _eskiz_token
    email    = await _get_cfg("eskiz_email")
    password = await _get_cfg("eskiz_password")
    if not email or not password:
        return ""

    if not _eskiz_token:
        _eskiz_token = await db.get_config("eskiz_token") or ""

    async with aiohttp.ClientSession() as session:
        if _eskiz_token:
            resp = await session.patch(
                "https://notify.eskiz.uz/api/auth/refresh",
                headers={"Authorization": f"Bearer {_eskiz_token}"},
            )
            if resp.status == 200:
                data = await resp.json()
                new_token = data.get("data", {}).get("token", _eskiz_token)
                if new_token != _eskiz_token:
                    _eskiz_token = new_token
                    await db.set_config("eskiz_token", _eskiz_token)
                return _eskiz_token

        resp = await session.post(
            "https://notify.eskiz.uz/api/auth/login",
            data={"email": email, "password": password},
        )
        if resp.status == 200:
            data = await resp.json()
            _eskiz_token = data.get("data", {}).get("token", "")
            if _eskiz_token:
                await db.set_config("eskiz_token", _eskiz_token)
            logging.info("✅ Eskiz: токен получен")
        else:
            body = await resp.text()
            logging.error(f"❌ Eskiz login failed: {resp.status} {body}")
    return _eskiz_token


async def send_sms(phone: str, message: str):
    """Отправляет SMS через Eskiz.uz. Если ключи не заданы — пишет в лог."""
    logging.info(f"📲 [SMS->{phone}] {message}")

    email    = await _get_cfg("eskiz_email")
    password = await _get_cfg("eskiz_password")
    if not email or not password:
        logging.warning("⚠️ eskiz_email/eskiz_password не заданы — SMS не отправлен")
        return

    token = await _eskiz_get_token()
    if not token:
        logging.error("❌ Eskiz: не удалось получить токен")
        return

    mobile = phone.lstrip("+")  # Eskiz принимает без «+»

    async with aiohttp.ClientSession() as session:
        resp = await session.post(
            "https://notify.eskiz.uz/api/message/sms/send",
            headers={"Authorization": f"Bearer {token}"},
            data={"mobile_phone": mobile, "message": message, "from": await _get_cfg("eskiz_from")},
        )
        if resp.status == 200:
            data = await resp.json()
            logging.info(f"✅ Eskiz SMS отправлен: {data}")
        else:
            body = await resp.text()
            logging.error(f"❌ Eskiz SMS error: {resp.status} {body}")


def generate_code() -> str:
    return f"{secrets.randbelow(1000000):06d}"


async def sms_text(code: str, purpose: str = "register") -> str:
    """Формирует текст SMS, читая шаблон из config (если задан)."""
    defaults = {
        "reset":    "Kod vosstanovleniya parolya dlya vhoda na sayt ARTEZ.uz: {code}",
        "login":    "Kod podtverzhdeniya dlya vhoda na sayt ARTEZ.uz: {code}",
        "register": "Kod podtverzhdeniya dlya registracii na sayte ARTEZ.uz: {code}",
    }
    key = f"sms_text_{purpose}"
    tpl = await db.get_config(key) or defaults.get(purpose, defaults["register"])
    return tpl.replace("{code}", code)


# ══════════════════════════════════════
#  JWT
# ══════════════════════════════════════
def create_token(user_id: int, phone: str) -> str:
    payload = {
        "sub": str(user_id),
        "phone": phone,
        "type": "client",
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def create_staff_token(staff_id: int, login: str, role: str) -> str:
    payload = {
        "sub": str(staff_id),
        "login": login,
        "role": role,
        "type": "staff",
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


# Разрешения по ролям
ROLE_PERMISSIONS: dict[str, list[str]] = {
    "admin":      ["leads", "orders", "clients", "status", "staff", "reports", "settings"],
    "manager":    ["leads", "orders", "clients", "status", "reports"],
    "callcenter": ["leads", "orders", "clients"],
    "driver":     ["leads", "orders", "status_delivery"],
    "logistics":  ["leads", "orders", "status"],
    "washer":     ["leads", "orders", "status_wash"],
    "agent":      ["leads_own"],  # агент видит только свои лиды
}

# Допустимые переходы статусов для мойщиков
WASHER_STATUS_FLOW = {
    "received": "washing",
    "washing":  "drying",
    "drying":   "packing",
}
ALL_ORDER_STATUSES = [
    "new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"
]

async def get_current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("type") == "staff":
        raise HTTPException(status_code=401, detail="Используйте клиентский токен")
    user = await db.get_user_by_id(int(payload["sub"]))
    if not user:
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    return user


async def get_optional_user(authorization: str = Header(None)):
    """Как get_current_user, но возвращает None вместо 401 для незалогиненных."""
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ").strip()
    if token in ("null", "undefined", ""):
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        return None
    if payload.get("type") == "staff":
        return None
    try:
        return await db.get_user_by_id(int(payload["sub"]))
    except Exception:
        return None


async def get_current_staff(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    # Admin panel token — treat as super-admin staff
    if payload.get("sub") == "admin":
        return {"id": 0, "login": "admin", "role": "admin", "active": True,
                "first_name": "Admin", "last_name": None, "phone": None,
                "branch": None, "tg_username": None, "position": None}
    if payload.get("type") != "staff":
        raise HTTPException(status_code=401, detail="Требуется токен сотрудника")
    staff = await db.get_staff_by_id(int(payload["sub"]))
    if not staff or not staff["active"]:
        raise HTTPException(status_code=401, detail="Сотрудник не найден или деактивирован")
    return dict(staff)


def require_perm(permission: str):
    async def dep(staff=Depends(get_current_staff)):
        if staff["role"] == "admin":  # admin has all permissions
            return staff
        perms = ROLE_PERMISSIONS.get(staff["role"], [])
        if permission not in perms:
            raise HTTPException(status_code=403, detail="Нет доступа")
        return staff
    return dep


# ══════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════
@app.get("/api/health")
async def health():
    return {"ok": True, "version": "2026-06-20-v1"}


# ══════════════════════════════════════
#  СОТРУДНИКИ — авторизация и профиль
# ══════════════════════════════════════
class StaffLoginRequest(BaseModel):
    login: str
    password: str

class StaffCreateRequest(BaseModel):
    first_name: str
    last_name: str | None = None
    middle_name: str | None = None
    phone: str | None = None
    login: str
    password: str
    role: str = "callcenter"
    position: str | None = None
    branch: str | None = None
    tg_id: int | None = None
    tg_username: str | None = None
    salary_type: str | None = None
    salary_rate: float | None = None
    hire_date: str | None = None
    note: str | None = None
    gender: str = "M"
    birth_date: str | None = None

def _staff_public(s: dict) -> dict:
    return {
        "id":         s["id"],
        "first_name": s["first_name"],
        "last_name":  s.get("last_name"),
        "login":      s["login"],
        "role":       s["role"],
        "position":   s.get("position"),
        "branch":     s.get("branch"),
        "phone":      s.get("phone"),
        "tg_id":      s.get("tg_id"),
        "tg_username":s.get("tg_username"),
        "active":         s["active"],
        "permissions":    ROLE_PERMISSIONS.get(s["role"], []),
        "can_edit_items":      s.get("can_edit_items", True),
        "can_measure":         s.get("can_measure", False),
        "can_approve_measure": s.get("can_approve_measure", False),
        "can_create_order":    s.get("can_create_order", True),
        "can_confirm_order":   s.get("can_confirm_order", True),
        "can_edit_confirmed":  s.get("can_edit_confirmed", False),
        "can_send_pickup":     s.get("can_send_pickup", False),
        "can_edit_delivery":   s.get("can_edit_delivery", False),
        "can_accept_payment":  s.get("can_accept_payment", False),
        "can_manage_cash":     s.get("can_manage_cash", False),
        "order_stages":        s.get("order_stages") or None,
        "gender":              s.get("gender", "M"),
        "birth_date":          str(s["birth_date"]) if s.get("birth_date") else None,
        "plain_password": s.get("plain_password"),
    }

@app.post("/api/staff/login")
async def staff_login(req: StaffLoginRequest):
    staff = await db.get_staff_by_login(req.login)
    if not staff:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    pw = req.password[:72]
    valid = pwd_context.verify(pw, staff["password_hash"])

    # Проверяем временный пароль если основной не подошёл
    if not valid and staff.get("temp_password_hash") and staff.get("temp_password_expires"):
        from datetime import datetime, timezone
        if datetime.now(timezone.utc) < staff["temp_password_expires"]:
            valid = pwd_context.verify(pw, staff["temp_password_hash"])

    if not valid:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    token = create_staff_token(staff["id"], staff["login"], staff["role"])
    pub = _staff_public(dict(staff))
    pub["must_change_password"] = bool(staff.get("must_change_password"))
    return {"ok": True, "token": token, "staff": pub}

@app.get("/api/staff/me")
async def staff_me(staff=Depends(get_current_staff)):
    return {"ok": True, "staff": _staff_public(staff)}

@app.get("/api/staff/list")
async def staff_list(role: str = None, _=Depends(get_current_staff)):
    rows = await db.get_all_staff()
    staff = [_staff_public(dict(r)) for r in rows]
    if role:
        staff = [s for s in staff if s.get("role") == role]
    return {"ok": True, "staff": staff}

@app.post("/api/staff/create")
async def staff_create(req: StaffCreateRequest, _=Depends(require_perm("staff"))):
    from datetime import date as date_type
    import traceback
    hashed = pwd_context.hash(req.password[:72])
    hire = None
    if req.hire_date:
        try: hire = date_type.fromisoformat(req.hire_date)
        except ValueError: pass
    try:
        sid = await db.create_staff({
            "first_name": req.first_name, "last_name": req.last_name,
            "middle_name": req.middle_name, "phone": req.phone,
            "login": req.login, "password_hash": hashed, "plain_password": req.password,
            "role": req.role, "position": req.position, "branch": req.branch,
            "tg_id": req.tg_id, "tg_username": req.tg_username,
            "salary_type": req.salary_type, "salary_rate": req.salary_rate,
            "hire_date": hire, "note": req.note,
            "gender": req.gender,
            "birth_date": date_type.fromisoformat(req.birth_date) if req.birth_date else None,
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"DB error: {type(e).__name__}: {e}")
    return {"ok": True, "id": sid}

@app.patch("/api/staff/{staff_id}")
async def staff_update(staff_id: int, body: dict, me=Depends(get_current_staff)):
    is_admin = me.get("role") == "admin"
    is_self  = me.get("id") == staff_id
    if not is_admin and not is_self:
        raise HTTPException(status_code=403, detail="Нет доступа")
    if is_admin:
        allowed = {"first_name","last_name","middle_name","phone","login","role","branch","position","active","is_active","note","hire_date","salary_type","salary_rate","tg_id","tg_username","gender","birth_date"}
    else:
        allowed = {"gender","birth_date","branch"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    if "tg_id" in updates and updates["tg_id"] is not None:
        try:
            updates["tg_id"] = int(updates["tg_id"])
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="tg_id должен быть числом")
    if "birth_date" in updates and updates["birth_date"]:
        from datetime import date as _date
        try: updates["birth_date"] = _date.fromisoformat(str(updates["birth_date"]))
        except: updates["birth_date"] = None
    if "hire_date" in updates and updates["hire_date"]:
        from datetime import date as _date
        try: updates["hire_date"] = _date.fromisoformat(str(updates["hire_date"]))
        except: updates["hire_date"] = None
    try:
        await db.update_staff(staff_id, **updates)
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower():
            raise HTTPException(status_code=409, detail="Логин или tg_id уже занят другим сотрудником")
        logging.error(f"update_staff error: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка БД: {err}")
    row = await db.get_staff_by_id(staff_id)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, "staff": _staff_public(dict(row))}

@app.get("/api/admin/staff/{staff_id}/personal")
async def get_staff_personal_ep(staff_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    data = await db.get_staff_personal(staff_id)
    if data and data.get("spouse_birth_date"):
        data["spouse_birth_date"] = str(data["spouse_birth_date"])
    return {"ok": True, "personal": data or {}}

@app.put("/api/admin/staff/{staff_id}/personal")
async def save_staff_personal_ep(staff_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    from datetime import date as _date
    if body.get("spouse_birth_date"):
        try: body["spouse_birth_date"] = _date.fromisoformat(body["spouse_birth_date"])
        except: body["spouse_birth_date"] = None
    else:
        body["spouse_birth_date"] = None
    if body.get("children_count") is not None:
        try: body["children_count"] = int(body["children_count"])
        except: body["children_count"] = 0
    await db.upsert_staff_personal(staff_id, body)
    return {"ok": True}

# ══════════════════════════════════════
#  МАРШРУТЫ (routes)
# ══════════════════════════════════════

@app.get("/api/admin/routes")
async def list_routes(date: str | None = None, driver_id: int | None = None,
                      branch: str | None = None, status: str | None = None,
                      me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    rows = await db.get_routes(date=date, driver_id=driver_id, branch=branch, status=status)
    for r in rows:
        if r.get("date"): r["date"] = str(r["date"])
        if r.get("created_at"): r["created_at"] = r["created_at"].isoformat()
        if r.get("updated_at"): r["updated_at"] = r["updated_at"].isoformat()
    return {"ok": True, "routes": rows}

@app.post("/api/admin/routes")
async def create_route(body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    row = await db.create_route(body)
    if row.get("date"): row["date"] = str(row["date"])
    return {"ok": True, "route": row}

@app.get("/api/admin/routes/{route_id}")
async def get_route(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager","driver"):
        raise HTTPException(status_code=403)
    route = await db.get_route(route_id)
    if not route: raise HTTPException(status_code=404)
    if route.get("date"): route["date"] = str(route["date"])
    for s in route.get("stops", []):
        if s.get("created_at"): s["created_at"] = s["created_at"].isoformat()
    return {"ok": True, "route": route}

@app.patch("/api/admin/routes/{route_id}")
async def update_route(route_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    row = await db.update_route(route_id, body)
    if row.get("date"): row["date"] = str(row["date"])
    return {"ok": True, "route": row}

@app.delete("/api/admin/routes/{route_id}")
async def delete_route(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics"):
        raise HTTPException(status_code=403)
    await db.delete_route(route_id)
    return {"ok": True}

@app.post("/api/admin/routes/{route_id}/orders")
async def add_route_orders(route_id: int, body: dict, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    order_ids = body.get("order_ids", [])
    count = await db.add_orders_to_route(route_id, order_ids)
    return {"ok": True, "added": count}

@app.delete("/api/admin/routes/{route_id}/orders/{order_id}")
async def remove_route_order(route_id: int, order_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin","logistics","manager"):
        raise HTTPException(status_code=403)
    await db.remove_order_from_route(route_id, order_id)
    return {"ok": True}

@app.patch("/api/admin/routes/{route_id}/orders/{order_id}")
async def update_route_stop(route_id: int, order_id: int, body: dict, me=Depends(get_current_staff)):
    await db.update_route_stop(route_id, order_id, body)
    return {"ok": True}

@app.post("/api/admin/routes/{route_id}/send-to-driver")
async def send_route_to_driver(route_id: int, me=Depends(get_current_staff)):
    if me.get("role") not in ("admin", "logistics", "manager"):
        raise HTTPException(status_code=403)
    route = await db.get_route(route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Маршрут не найден")
    if not route.get("driver_id"):
        raise HTTPException(status_code=400, detail="Водитель не назначен")

    # Получить tg_id водителя
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        driver = await conn.fetchrow(
            "SELECT first_name, last_name, tg_id FROM staff WHERE id=$1",
            route["driver_id"]
        )
    if not driver or not driver["tg_id"]:
        raise HTTPException(status_code=400, detail="У водителя не указан Telegram ID")

    stops = route.get("stops", [])
    branch_label = "Зарафшан" if route.get("branch") == "zarafshan" else "Навои" if route.get("branch") == "navoi" else ""

    type_map = {"pickup": "Забор", "delivery": "Доставка", "mixed": "Смешанный"}
    type_label = type_map.get(route.get("type", ""), "")

    lines = [
        f"🚗 Маршрут: {route['name']}",
        f"📅 {route.get('date', '')}  {branch_label}  {type_label}".strip(),
        "",
    ]

    import json as _json
    for i, s in enumerate(stops, 1):
        client = f"{s.get('client_first_name', '')} {s.get('client_last_name', '')}".strip()
        addr = s.get("address") or s.get("location_address") or "—"
        line = f"{i}. {s.get('order_num', '')} — {client}\n   📍 {addr}"
        # Google Maps ссылка если есть геометка
        if s.get("location"):
            try:
                loc = _json.loads(s["location"])
                if loc.get("lat") and loc.get("lon"):
                    line += f"\n   🗺 https://maps.google.com/?q={loc['lat']},{loc['lon']}"
            except Exception:
                pass
        if s.get("client_phone"):
            line += f"\n   📞 {s['client_phone']}"
        lines.append(line)

    lines += ["", f"Всего точек: {len(stops)}"]
    if route.get("note"):
        lines += ["", f"📝 {route['note']}"]

    text = "\n".join(lines)
    await send_tg(driver["tg_id"], text)
    return {"ok": True, "sent_to": driver["tg_id"]}


_ORDER_STATUS_RU = {
    "new": "Новый", "confirmed": "Подтверждён", "pickup": "Вывоз",
    "received": "В мастерской", "washing": "Мойка", "drying": "Сушка",
    "packing": "Упаковка", "ready": "Готов", "delivery": "Доставка",
    "delivered": "Доставлен", "cancelled": "Отменён",
}

def _route_pickup_kb(order_id: int, status: str) -> dict:
    """Inline-клавиатура для сообщения в канале водителей."""
    h = {"text": "📋 История", "callback_data": f"rp:{order_id}:history"}
    if status == "confirmed":
        return {"inline_keyboard": [
            [{"text": "✅ Забрал", "callback_data": f"rp:{order_id}:take"}],
            [h],
        ]}
    elif status == "pickup":
        return {"inline_keyboard": [
            [{"text": "🏭 Сдал в мастерскую", "callback_data": f"rp:{order_id}:deliver"}],
            [{"text": "↩️ Не забирал", "callback_data": f"rp:{order_id}:undo"}],
            [h],
        ]}
    else:
        return {"inline_keyboard": [[h]]}

def _parse_loc_str(val: str | None):
    if not val: return None
    try:
        import json as _j
        j = _j.loads(val)
        if j.get("lat") and j.get("lon"): return float(j["lat"]), float(j["lon"])
    except Exception: pass
    parts = str(val).split(",")
    if len(parts) == 2:
        try: return float(parts[0]), float(parts[1])
        except Exception: pass
    return None

def _build_stop_text(route: dict, stop: dict, num: int, template: str) -> str:
    branch_label = {"zarafshan": "Зарафшан", "navoi": "Навои"}.get(route.get("branch", ""), "")
    type_label   = {"pickup": "📥 Забор", "delivery": "📤 Доставка", "mixed": "🔄 Смешанный"}.get(route.get("type", ""), "")
    client = f"{stop.get('client_first_name', '')} {stop.get('client_last_name', '')}".strip() or "—"
    addr   = stop.get("short_address") or stop.get("address") or stop.get("location_address") or "—"
    phone  = f"📞 {stop['client_phone']}\n" if stop.get("client_phone") else ""
    loc    = _parse_loc_str(stop.get("location"))
    map_link = f"🗺 https://maps.google.com/?q={loc[0]},{loc[1]}\n" if loc else ""
    status = _ORDER_STATUS_RU.get(stop.get("order_status", ""), "—")
    return template.format(
        route_name=route.get("name", ""),
        route_type=type_label,
        branch=branch_label,
        date=str(route.get("date", "")),
        num=num,
        order_num=stop.get("order_num", ""),
        client=client,
        address=addr,
        phone=phone,
        map_link=map_link,
        status=status,
    )

def _build_stop_text_short(stop: dict, num: int) -> str:
    """Компактный HTML-формат сообщения для канала водителей."""
    import html as _html
    def h(s): return _html.escape(str(s)) if s else ""

    order_num = (stop.get("order_num", "") or "").replace("ARTEZ-", "")
    addr  = stop.get("short_address") or stop.get("address") or stop.get("location_address") or "—"
    first = (stop.get("client_first_name") or "").strip()
    last  = (stop.get("client_last_name")  or "").strip()
    client = f"{first} {last}".strip() or "—"
    phone  = stop.get("client_phone", "") or ""
    loc    = _parse_loc_str(stop.get("location"))

    if loc:
        yandex = f"https://yandex.com/maps/?rtext=~{loc[0]},{loc[1]}&rtt=auto"
        addr_part = f'📍<a href="{yandex}">{h(addr)}</a>'
    else:
        addr_part = f"📍{h(addr)}"

    contact = f"👤 {h(client)}"
    if phone: contact += f" 📞{h(phone)}"

    return f"📦 #{num}·{h(order_num)} {addr_part}\n{contact}"

@app.post("/api/admin/routes/{route_id}/send-to-delivery-group")
async def send_route_to_delivery_group(route_id: int, me=Depends(get_current_staff)):
    route = await db.get_route(route_id)
    if not route:
        raise HTTPException(404, "Маршрут не найден")

    branch = route.get("branch", "")
    if branch == "navoi":
        group_id_str   = await _get_cfg("delivery_group_navoi_id")   or await _get_cfg("delivery_group_id")
        channel_id_str = await _get_cfg("delivery_channel_navoi_id")
    else:
        group_id_str   = await _get_cfg("delivery_group_zarafshan_id") or await _get_cfg("delivery_group_id")
        channel_id_str = await _get_cfg("delivery_channel_zarafshan_id")
    group_id   = int(group_id_str)   if group_id_str   else 0
    channel_id = int(channel_id_str) if channel_id_str else 0
    if not channel_id and not group_id:
        raise HTTPException(400, "Канал/группа водителей не настроены (Настройки → Telegram → Водители)")

    stops = route.get("stops", [])
    if not stops:
        raise HTTPException(400, "В маршруте нет заказов")

    from datetime import datetime
    from zoneinfo import ZoneInfo
    import json as _jmod
    now_uz     = datetime.now(ZoneInfo("Asia/Tashkent"))
    time_str   = now_uz.strftime("%H:%M:%S")
    date_short = now_uz.strftime("%d.%m")
    type_label = {"pickup": "Забор", "delivery": "Доставка", "mixed": "Смешанный"}.get(route.get("type", ""), "")
    type_emoji = {"pickup": "📥", "delivery": "📤", "mixed": "🔄"}.get(route.get("type", ""), "🚗")
    route_date = str(route.get("date", ""))
    route_name = route.get("name", "")

    # Удаляем предыдущие сообщения (канал и группа)
    _raw = route.get("tg_delivery_msg_ids")
    if isinstance(_raw, str):
        try: _raw = _jmod.loads(_raw)
        except Exception: _raw = {}
    old_msg_ids: dict = _raw or {}
    if old_msg_ids:
        async with aiohttp.ClientSession() as sess:
            for key, msg_id_str in old_msg_ids.items():
                # __group__ → удалять из группы, остальное → из канала
                target = group_id if key == "__group__" else (channel_id or group_id)
                if not target:
                    continue
                try:
                    await sess.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
                        json={"chat_id": str(target), "message_id": int(msg_id_str)},
                        timeout=aiohttp.ClientTimeout(total=4))
                except Exception:
                    pass

    # ── Канал: заголовок + остановки + подвал ──
    dest = channel_id or group_id
    new_msg_ids: dict = {}
    tg_error = None

    header_text = (
        f"━━━━━━━━━━\n"
        f"🚗 {route_name}-{len(stops)} — {type_emoji} {type_label}\n"
        f"📅 {route_date}   {time_str}\n"
        f"━━━━━━━━━━"
    )
    hdr_id = await _send_tg_with_kb(dest, header_text, {"inline_keyboard": []})
    if hdr_id:
        new_msg_ids["__header__"] = hdr_id
    elif tg_error is None:
        tg_error = "Не удалось отправить в канал"

    sent = 0
    for i, s in enumerate(stops, 1):
        text     = _build_stop_text_short(s, i)
        order_id = s.get("order_id") or s.get("id")
        status   = s.get("order_status", "confirmed")
        kb       = _route_pickup_kb(order_id, status)
        msg_id   = await _send_tg_with_kb(dest, text, kb)
        if msg_id:
            sent += 1
            new_msg_ids[str(order_id)] = msg_id
        elif tg_error is None:
            tg_error = "Ошибка отправки остановки"

    footer_text = f"━━━━━━━━━━\n✅ Конец списка · {sent} из {len(stops)}\n━━━━━━━━━━"
    ftr_id = await _send_tg_with_kb(dest, footer_text, {"inline_keyboard": []})
    if ftr_id:
        new_msg_ids["__footer__"] = ftr_id

    if sent == 0 and tg_error:
        logging.error(f"send-to-delivery-group failed: {tg_error}")
        raise HTTPException(400, f"Telegram: {tg_error}")

    # ── Группа: короткое уведомление (только если есть и канал, и группа) ──
    if group_id and channel_id:
        tpl = await _get_cfg("delivery_group_template") or "🚗 {route_name}-{count} — {route_type} · {date} {time}"
        try:
            notify = tpl.format(
                route_name=route_name, count=len(stops),
                route_type=f"{type_emoji} {type_label}",
                date=date_short, time=time_str,
            )
        except Exception:
            notify = f"🚗 {route_name}-{len(stops)} — {type_emoji} {type_label} · {date_short} {time_str}"
        ch_link_key = "delivery_channel_navoi_link" if branch == "navoi" else "delivery_channel_zarafshan_link"
        ch_link = await _get_cfg(ch_link_key)
        notify_kb = {"inline_keyboard": [[{"text": "↗️ Открыть канал", "url": ch_link}]]} if ch_link else {"inline_keyboard": []}
        grp_msg_id = await _send_tg_with_kb(group_id, notify, notify_kb, parse_mode=None)
        if grp_msg_id:
            new_msg_ids["__group__"] = grp_msg_id

    if new_msg_ids and db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE routes SET tg_delivery_msg_ids=$1 WHERE id=$2",
                _jmod.dumps(new_msg_ids), route_id)

    return {"ok": True, "sent": sent}


@app.delete("/api/admin/staff/{staff_id}")
async def delete_staff(staff_id: int, me=Depends(get_current_staff)):
    if me.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, first_name, last_name FROM staff WHERE id=$1", staff_id)
        if not row:
            raise HTTPException(status_code=404, detail="Сотрудник не найден")
        # NULL out FK references that don't have ON DELETE SET NULL
        await conn.execute("UPDATE leads        SET volunteer_id=NULL WHERE volunteer_id=$1", staff_id)
        await conn.execute("UPDATE leads        SET assigned_to=NULL  WHERE assigned_to=$1",  staff_id)
        await conn.execute("UPDATE leads        SET created_by=NULL   WHERE created_by=$1",   staff_id)
        await conn.execute("UPDATE leads        SET converted_by=NULL WHERE converted_by=$1", staff_id)
        await conn.execute("UPDATE lead_calls   SET operator_id=NULL  WHERE operator_id=$1",  staff_id)
        await conn.execute("UPDATE lead_reminders SET staff_id=NULL   WHERE staff_id=$1",     staff_id)
        try:
            await conn.execute("DELETE FROM staff WHERE id=$1", staff_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Ошибка БД: {str(e)}")
    return {"ok": True}

@app.put("/api/staff/{staff_id}/password")
async def staff_change_password(staff_id: int, body: dict, me=Depends(get_current_staff)):
    if me["role"] != "admin" and me["id"] != staff_id:
        raise HTTPException(status_code=403, detail="Нет доступа")
    new_pw = body.get("password", "")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail=bi("Минимум 6 символов","Kamida 6 ta belgi"))
    await db.update_staff_password(staff_id, pwd_context.hash(new_pw[:72]), plain=new_pw)
    return {"ok": True}


# ══════════════════════════════════════
#  ЛИДЫ
# ══════════════════════════════════════
class LeadCreateRequest(BaseModel):
    client_name: str | None = None
    client_phone: str
    service: str | None = None
    branch: str | None = None
    city: str | None = None
    address: str | None = None
    short_address: str | None = None
    note: str | None = None
    assigned_to: int | None = None
    volunteer_id: int | None = None
    location: str | None = None
    location_address: str | None = None
    notify_group: bool = True

@app.get("/api/staff/search")
async def staff_search(q: str = "", limit: int = 8, _=Depends(get_current_staff)):
    """Поиск клиентов из CRM + справочника. Доступен всем авторизованным сотрудникам."""
    q = q.strip()
    if not q or len(q) < 2:
        return {"ok": True, "results": []}
    crm      = await db.get_crm_clients_list(search=q, limit=limit)
    contacts = await db.search_contacts(q, limit=limit)
    seen = set()
    results = []
    for c in crm:
        p = c.get("phone") or ""
        seen.add(p)
        results.append({"phone": p, "phone2": c.get("phone2") or "",
                        "first_name": c.get("first_name") or "", "last_name": c.get("last_name") or "",
                        "middle_name": "", "address": c.get("address") or "",
                        "short_address": c.get("short_address") or "", "_src": "crm"})
    for c in contacts:
        p = c.get("phone") or ""
        if p not in seen:
            results.append({"phone": p, "phone2": c.get("phone2") or "",
                            "first_name": c.get("first_name") or "", "last_name": c.get("last_name") or "",
                            "middle_name": c.get("middle_name") or "", "address": c.get("address") or "",
                            "short_address": c.get("short_address") or "", "_src": "contacts"})
    return {"ok": True, "results": results[:limit]}


@app.post("/api/staff/leads")
async def create_lead(req: LeadCreateRequest, staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    if "leads" not in perms and "leads_own" not in perms and staff.get("sub") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    creator_id = None if staff.get("sub") == "admin" else staff.get("id")
    # агент автоматически становится agent_id лида
    agent_id = req.volunteer_id
    if role == "agent" and not agent_id:
        agent_id = creator_id
    lead_source = "agent" if role == "agent" else "staff"
    lead = await db.create_lead({
        "client_name": req.client_name,
        "client_phone": req.client_phone, "service": req.service,
        "branch": req.branch, "city": req.city, "address": req.address,
        "short_address": req.short_address, "note": req.note,
        "assigned_to": req.assigned_to, "created_by": creator_id,
        "volunteer_id": agent_id,
        "location": req.location, "location_address": req.location_address,
        "source": lead_source,
    })
    if lead:
        await db.add_lead_call(lead["id"], creator_id, action="created",
                               note=f"Лид создан ({lead.get('lead_code','')})")
        if req.notify_group:
            asyncio.create_task(_notify_new_lead(lead, staff))
        elif creator_id:
            # Взять себе: назначаем на создателя, не отправляем в ТГ
            async with db.pool.acquire() as _conn:
                await _conn.execute(
                    "UPDATE leads SET assigned_to=$1 WHERE id=$2", creator_id, lead["id"])
            await db.add_lead_call(lead["id"], creator_id, action="note",
                                   note="Лид взят создателем")
    return {"ok": True, "lead": lead}

@app.get("/api/staff/leads")
async def get_leads(status: str = None, branch: str = None,
                    staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    # агент: только свои лиды (где он создатель или агент)
    if "leads_own" in perms and "leads" not in perms:
        rows = await db.get_leads_by_agent(staff["id"], status=status)
    elif "leads" in perms or staff.get("sub") == "admin":
        rows = await db.get_leads(status=status, branch=branch)
    else:
        raise HTTPException(status_code=403, detail="Нет доступа")
    return {"ok": True, "leads": [dict(r) for r in rows]}

@app.patch("/api/staff/leads/{lead_id}")
async def update_lead(lead_id: int, body: dict, staff=Depends(require_perm("leads"))):
    allowed = {"client_name","client_phone","branch","address","short_address","note","volunteer_id","location","location_address","pickup_type","delivery_type"}
    fields = {k: v for k, v in body.items() if k in allowed}
    lead = await db.update_lead(lead_id, **fields)
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    await db.add_lead_call(lead_id, operator_id, action="edited", note="Лид отредактирован")
    return {"ok": True, "lead": lead}

@app.patch("/api/staff/leads/{lead_id}/assign")
async def assign_lead(lead_id: int, body: dict = Body({}),
                      staff=Depends(require_perm("leads"))):
    """Взять или освободить лид. assign=true — взять, assign=false — освободить."""
    take = body.get("assign", True)
    staff_id = staff.get("id")
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    role = staff.get("role", "")
    is_admin = role in ("admin", "manager")
    async with db.pool.acquire() as conn:
        if take:
            row = await conn.fetchrow("SELECT assigned_to FROM leads WHERE id=$1", lead_id)
            if not row:
                raise HTTPException(status_code=404, detail="Лид не найден")
            # Обычный сотрудник не может взять лид занятый другим; admin/manager могут
            if row["assigned_to"] and row["assigned_to"] != staff_id and not is_admin:
                raise HTTPException(status_code=409, detail="Лид уже взят другим сотрудником")
            await conn.execute("UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)
            note = f"Лид взят: {staff.get('first_name','')} {staff.get('last_name','')}".strip()
        else:
            row = await conn.fetchrow("SELECT assigned_to FROM leads WHERE id=$1", lead_id)
            if not row:
                raise HTTPException(status_code=404, detail="Лид не найден")
            # Освободить можно свой лид, или admin/manager любой
            if row["assigned_to"] != staff_id and not is_admin:
                raise HTTPException(status_code=403, detail="Можно освободить только свой лид")
            await conn.execute("UPDATE leads SET assigned_to=NULL WHERE id=$1", lead_id)
            note = f"Лид освобождён: {staff.get('first_name','')} {staff.get('last_name','')}".strip()
        await db.add_lead_call(lead_id, staff_id, action="note", note=note)
    lead = await db.get_lead_by_id(lead_id)
    return {"ok": True, "lead": dict(lead) if lead else {}}

@app.patch("/api/staff/leads/{lead_id}/status")
async def update_lead_status(lead_id: int, body: dict,
                             staff=Depends(require_perm("leads"))):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost","no_answer"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    order_num = body.get("order_num")
    if status == "converted" and order_num:
        await db.convert_lead_to_order(lead_id, order_num, operator_id or 0)
    else:
        scheduled_at_pre = body.get("scheduled_at")
        from datetime import datetime as _dt
        sched_pre = _dt.fromisoformat(scheduled_at_pre) if scheduled_at_pre and status == "callback" else None
        await db.update_lead_status(lead_id, status, scheduled_at=sched_pre)
    # лог
    action_labels = {
        "new": "Сменил статус на «Новый»",
        "contacted": "Связался с клиентом",
        "callback": "Клиент попросил перезвонить",
        "no_answer": "Не дозвонился",
        "converted": "Конвертировал в заказ",
        "lost": "Закрыл как потерянный",
    }
    note = body.get("note") or action_labels.get(status, status)
    scheduled_at = body.get("scheduled_at")  # ISO string or None
    from datetime import datetime
    sched = datetime.fromisoformat(scheduled_at) if scheduled_at else None
    await db.add_lead_call(lead_id, operator_id, action=f"status_{status}", note=note, scheduled_at=sched)
    if sched and operator_id:
        await db.add_lead_reminder(lead_id, operator_id, remind_at=sched,
                                   message=f"Перезвонить клиенту — лид {lead_id}")
    # Уведомить агента если лид агентский
    asyncio.create_task(_notify_agent_status(lead_id, status, note))
    return {"ok": True}

@app.get("/api/staff/my-notifications")
async def get_my_notifications(staff=Depends(get_current_staff)):
    rows = await db.get_agent_notifications(staff["id"])
    return {"ok": True, "notifications": [dict(r) for r in rows]}

@app.get("/api/staff/my-notifications/unread-count")
async def get_unread_count(staff=Depends(get_current_staff)):
    count = await db.count_unread_agent_notifications(staff["id"])
    return {"ok": True, "count": count}

@app.post("/api/staff/my-notifications/read")
async def mark_notifications_read(staff=Depends(get_current_staff)):
    await db.mark_agent_notifications_read(staff["id"])
    return {"ok": True}

@app.patch("/api/staff/my-notifications/{notif_id}/read")
async def mark_one_notification_read(notif_id: int, staff=Depends(get_current_staff)):
    await db.mark_agent_notification_read_by_id(notif_id, staff["id"])
    return {"ok": True}

@app.get("/api/staff/leads/{lead_id}/calls")
async def get_lead_calls(lead_id: int, _=Depends(require_perm("leads"))):
    rows = await db.get_lead_calls(lead_id)
    return {"ok": True, "calls": [dict(r) for r in rows]}

@app.post("/api/staff/leads/{lead_id}/calls")
async def add_lead_call(lead_id: int, body: dict, staff=Depends(require_perm("leads"))):
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    action = body.get("action", "note")
    note = body.get("note", "")
    scheduled_at = body.get("scheduled_at")
    from datetime import datetime
    sched = datetime.fromisoformat(scheduled_at) if scheduled_at else None
    row = await db.add_lead_call(lead_id, operator_id, action=action, note=note, scheduled_at=sched)
    if sched and operator_id:
        await db.add_lead_reminder(lead_id, operator_id, remind_at=sched,
                                   message=note or "Запланированный звонок")
    return {"ok": True, "call": row}

@app.get("/api/staff/reminders/due")
async def get_due_reminders(staff=Depends(require_perm("leads"))):
    if staff.get("sub") == "admin":
        return {"ok": True, "reminders": []}
    rows = await db.get_due_reminders(staff["id"])
    result = [dict(r) for r in rows]
    return {"ok": True, "reminders": result}

@app.post("/api/staff/reminders/{reminder_id}/ack")
async def ack_reminder(reminder_id: int, staff=Depends(require_perm("leads"))):
    await db.mark_reminder_sent(reminder_id, "browser")
    return {"ok": True}

async def _tg_send_reply_keyboard(chat_id, text: str):
    """Отправляет сообщение с кнопкой 'Поделиться номером'."""
    if not BOT_TOKEN: return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {
                    "keyboard": [[{"text": "📱 Поделиться номером", "request_contact": True}]],
                    "resize_keyboard": True,
                    "one_time_keyboard": True,
                }
            }
        )

async def _tg_remove_keyboard(chat_id, text: str):
    """Отправляет сообщение и убирает клавиатуру."""
    if not BOT_TOKEN: return
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "reply_markup": {"remove_keyboard": True}
            }
        )


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request):
    """Обрабатывает сообщения и callback_query от Telegram."""
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}

    # ── Обычные сообщения (текст и контакт) ──────────────────────────
    msg = data.get("message") or data.get("edited_message")
    if msg:
        chat_id  = msg.get("chat", {}).get("id")
        tg_user_id = msg.get("from", {}).get("id")
        text     = (msg.get("text") or "").strip()
        contact  = msg.get("contact")

        if text == "/start" and chat_id:
            await _tg_send_reply_keyboard(
                chat_id,
                "👋 <b>Добро пожаловать в ARTEZ!</b>\n\n"
                "Нажмите кнопку ниже, чтобы привязать ваш номер телефона.\n"
                "После этого при регистрации на сайте вы сможете получить код подтверждения через Telegram."
            )
            return {"ok": True}

        if contact and tg_user_id and chat_id:
            phone_raw = contact.get("phone_number", "")
            # Нормализуем: +998901234567 → +998901234567
            phone = phone_raw if phone_raw.startswith("+") else "+" + phone_raw
            owner_tg = contact.get("user_id")
            # Принимаем только собственный контакт
            if owner_tg and int(owner_tg) != int(tg_user_id):
                await _tg_remove_keyboard(chat_id, "❌ Пожалуйста, поделитесь <b>своим</b> номером.")
                return {"ok": True}
            await db.save_tg_phone_link(phone, int(tg_user_id))
            await _tg_remove_keyboard(
                chat_id,
                f"✅ <b>Номер привязан!</b>\n\n"
                f"📱 <code>{phone}</code>\n\n"
                f"Теперь при регистрации на сайте ARTEZ вы можете выбрать "
                f"«Получить код через Telegram»."
            )
            return {"ok": True}

    # ── Callback query (кнопка 'Взять лид') ──────────────────────────
    cq = data.get("callback_query")
    if not cq:
        return {"ok": True}

    cq_id      = cq["id"]
    cq_data    = cq.get("data", "")
    tg_user_id = cq["from"]["id"]
    message    = cq.get("message", {})
    chat_id    = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    orig_text  = message.get("text", "")

    if not cq_data.startswith("take_lead_"):
        return {"ok": True}

    try:
        lead_id = int(cq_data.split("_")[2])
    except (IndexError, ValueError):
        await _tg_answer_callback(cq_id, "❌ Ошибка: неверный формат данных")
        return {"ok": True}

    # Проверяем — сотрудник ли нажавший (не агент)
    staff = await db.get_staff_by_tg_id(tg_user_id)
    if not staff:
        await _tg_answer_callback(cq_id,
            "❌ Ваш Telegram не привязан к аккаунту сотрудника ARTEZ.\n"
            "Обратитесь к администратору.", alert=True)
        return {"ok": True}
    if staff.get("role") == "agent":
        await _tg_answer_callback(cq_id,
            "❌ Агенты не могут брать лиды через Telegram.\n"
            "Лиды берут только сотрудники.", alert=True)
        return {"ok": True}

    staff_id   = staff["id"]
    staff_name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get("login","")
    took_verb  = "Взяла" if staff.get("gender") == "F" else "Взял"

    if not db.pool:
        await _tg_answer_callback(cq_id, "❌ Ошибка базы данных", alert=True)
        return {"ok": True}

    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT assigned_to, lead_code FROM leads WHERE id=$1", lead_id)
        if not row:
            await _tg_answer_callback(cq_id, "❌ Лид не найден", alert=True)
            return {"ok": True}

        if row["assigned_to"] and row["assigned_to"] != staff_id:
            taker = await db.get_staff_by_id(row["assigned_to"])
            taker_name = ""
            taker_verb = "Взяла" if taker and taker.get("gender") == "F" else "Взял"
            if taker:
                taker_name = f"{taker.get('first_name','')} {taker.get('last_name','')}".strip()
            await _tg_answer_callback(cq_id,
                f"❌ Лид уже взят: {taker_name or 'другой сотрудник'}", alert=True)
            # Убираем кнопку из сообщения — лид уже не свободен
            new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {taker_verb}: {taker_name or 'другой сотрудник'}"
            await _tg_edit_message(chat_id, message_id, new_text)
            return {"ok": True}

        if row["assigned_to"] == staff_id:
            await _tg_answer_callback(cq_id, "✅ Этот лид уже ваш!")
            return {"ok": True}

        await conn.execute(
            "UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)

    await db.add_lead_call(lead_id, staff_id, action="note",
                           note=f"Лид взят через Telegram: {staff_name}")

    await _tg_answer_callback(cq_id, f"✅ Лид взят! Откройте приложение.")

    # Редактируем сообщение — убираем кнопку, добавляем кто взял
    new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {took_verb}: {staff_name}"
    await _tg_edit_message(chat_id, message_id, new_text)

    return {"ok": True}


@app.get("/api/push/vapid-key")
async def get_vapid_key():
    return {"public_key": VAPID_PUBLIC}

@app.post("/api/staff/push-subscription")
async def save_push_subscription(body: dict, staff=Depends(get_current_staff)):
    endpoint = body.get("endpoint")
    keys     = body.get("keys") or {}
    p256dh   = keys.get("p256dh")
    auth     = keys.get("auth")
    if not endpoint or not p256dh or not auth:
        raise HTTPException(400, "Неверные данные подписки")
    await db.upsert_push_subscription(staff["id"], endpoint, p256dh, auth)
    return {"ok": True}

@app.delete("/api/staff/push-subscription")
async def remove_push_subscription(body: dict, staff=Depends(get_current_staff)):
    endpoint = body.get("endpoint")
    if endpoint:
        await db.delete_push_subscription(endpoint)
    return {"ok": True}

@app.delete("/api/staff/leads/{lead_id}")
async def delete_lead_staff(lead_id: int, body: dict, _=Depends(require_perm("leads"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/staff/leads/bulk-delete")
async def bulk_delete_leads(body: dict, _=Depends(require_perm("leads"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID лидов")
    deleted = 0
    for lead_id in ids:
        ok = await db.delete_lead(int(lead_id))
        if ok:
            deleted += 1
    return {"ok": True, "deleted": deleted}

@app.post("/api/staff/leads/bulk-status")
async def bulk_status_leads(body: dict, staff=Depends(require_perm("leads"))):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost","no_answer"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID лидов")
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    for lead_id in ids:
        await db.update_lead_status(int(lead_id), status)
        await db.add_lead_call(int(lead_id), operator_id, action=f"status_{status}",
                               note=f"Массовая смена статуса")
    return {"ok": True, "updated": len(ids)}

@app.post("/api/staff/orders/bulk-status")
async def bulk_status_orders(body: dict, staff=Depends(require_perm("orders"))):
    status = body.get("status")
    valid = {"new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"}
    if status not in valid:
        raise HTTPException(status_code=400, detail="Неверный статус")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID заказов")
    for order_id in ids:
        await db.update_order_status(int(order_id), status, note="Массовая смена статуса")
    return {"ok": True, "updated": len(ids)}

@app.post("/api/staff/orders/bulk-delete")
async def bulk_delete_orders(body: dict, _=Depends(require_perm("orders"))):
    if not body.get("admin_password") or body["admin_password"] != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID заказов")
    deleted = 0
    skipped = []
    for order_id in ids:
        try:
            ok = await db.delete_order(int(order_id))
            if ok: deleted += 1
        except ValueError as e:
            if "has_payments" in str(e):
                skipped.append(int(order_id))
    result = {"ok": True, "deleted": deleted}
    if skipped:
        result["skipped"] = skipped
        result["skipped_reason"] = "Заказы с платежами не удалены — сначала удалите платежи в карточке заказа"
    return result


# ══════════════════════════════════════
#  ЗАЯВКИ — для сотрудников
# ══════════════════════════════════════

# Какие статусы видит сотрудник в зависимости от этапа
_STAGE_STATUSES = {
    "pickup":  {"new", "confirmed", "pickup", "cancelled"},
    "wash":    {"received", "washing", "cancelled"},
    "dry":     {"washing", "drying", "cancelled"},
    "pack":    {"drying", "packing", "ready", "cancelled"},
    "deliver": {"ready", "delivery", "delivered", "cancelled"},
}

@app.get("/api/staff/orders")
async def staff_orders(status: str = None, branch: str = None,
                       staff=Depends(require_perm("orders"))):
    rows = await db.get_admin_orders(status=status, limit=200)
    result = [dict(r) for r in rows]
    # Фильтр по этапам: если order_stages заданы — показывать только нужные статусы
    stages_raw = staff.get("order_stages") or ""
    stages = [s.strip() for s in stages_raw.split(",") if s.strip()]
    if stages:
        visible = set()
        for stage in stages:
            visible |= _STAGE_STATUSES.get(stage, set())
        result = [o for o in result if o.get("status") in visible]
    if branch:
        result = [o for o in result if o.get("branch") == branch]
    return {"ok": True, "orders": result}

@app.get("/api/staff/orders/own")
async def staff_own_orders(staff=Depends(get_current_staff)):
    rows = await db.get_admin_orders(limit=200)
    result = [dict(r) for r in rows
              if dict(r).get("branch") == staff.get("branch")]
    return {"ok": True, "orders": result}

@app.post("/api/staff/orders/create")
async def staff_create_order(req: StaffOrderRequest, staff=Depends(require_perm("orders"))):
    if not staff.get("can_create_order", True):
        raise HTTPException(status_code=403, detail="Нет права создавать заказы")
    try:
        order_num = await db.get_next_order_num()
        first_name = staff.get("first_name") or ""
        last_name  = staff.get("last_name") or ""
        login      = staff.get("login") or ""
        staff_label = " ".join(filter(None, [first_name, last_name])) or login or "сотрудник"
        if login and login != staff_label:
            staff_label = f"{staff_label} (@{login})"
        branch = req.branch or staff.get("branch") or ""
        location = req.location or ""
        location_address = req.location_address or ""
        note_full = f"📱 Заявка от сотрудника: {staff_label}" + (f"\n{req.note}" if req.note else "")
        await db.save_site_order({
            "order_num":   order_num,
            "first_name":  req.first_name,
            "last_name":   "",
            "phone":       req.phone,
            "branch":      branch,
            "city":        "",
            "address":       req.address or "",
            "short_address": req.short_address or "",
            "location":      location,
            "service":      req.service,
            "service_type": req.service_type or "standard",
            "pickup_type":  req.pickup_type or "courier",
            "delivery_type": req.delivery_type or "courier",
            "pickup_date": "",
            "pickup_time": "",
            "note":        note_full,
            "total_price": None,
        }, source="staff")
        # Уведомление в Telegram — строим текст вручную, без Pydantic
        if BOT_TOKEN:
            staff_chat_id = await _group_id_for_branch(branch)
            if staff_chat_id:
                full_name = req.first_name
                staff_name = staff_label
                if location:
                    try:
                        lat, lon = location.split(",", 1)
                        yandex_url = f"https://yandex.uz/maps/?pt={lon.strip()},{lat.strip()}&z=16"
                        link_text = location_address if location_address else f"{lat.strip()}, {lon.strip()}"
                        loc_line = f"\n🗺 <a href=\"{yandex_url}\">{link_text}</a>"
                    except Exception:
                        loc_line = f"\n🗺 {location_address or location}"
                else:
                    loc_line = ""
                SERVICE_RU = {
                    "carpet":      "Ковры",
                    "carpet_home": "Ковры на дому",
                    "sofa":        "Диваны",
                    "mattress":    "Матрасы",
                    "curtains":    "Шторы",
                }
                service_ru = SERVICE_RU.get(req.service, req.service or "—")
                text = (
                    f"📱 Заявка от сотрудника {order_num}\n"
                    f"━━━━━━━━━━\n"
                    f"👤 {full_name}\n"
                    f"📞 {req.phone}\n"
                    f"🏢 {branch_ru(branch)}\n"
                    f"🧺 {service_ru}\n"
                    f"🏠 {req.short_address or req.address or '—'}{(' | ' + req.address) if req.short_address and req.address and req.short_address != req.address else ''}{loc_line}\n"
                    f"👷 {staff_name}\n"
                    f"━━━━━━━━━━"
                )
                keyboard = {"inline_keyboard": [[
                    {"text": "✅ Принять", "callback_data": f"accept_{order_num}_0"},
                    {"text": "❌ Отклонить", "callback_data": f"reject_{order_num}_0"},
                ]]}
                async with aiohttp.ClientSession() as session:
                    await session.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": staff_chat_id, "text": text, "reply_markup": keyboard,
                              "parse_mode": "HTML", "disable_web_page_preview": True},
                        timeout=aiohttp.ClientTimeout(total=8),
                    )
        # Авто-регистрация клиента в CRM
        await db.upsert_crm_client(
            phone=req.phone,
            first_name=req.first_name,
            source="staff",
        )
        await db.refresh_crm_client_stats(req.phone)
        return {"ok": True, "order_num": order_num}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Ошибка: {type(e).__name__}: {e}")


# ══════════════════════════════════════
#  CRM КЛИЕНТЫ
# ══════════════════════════════════════
class ClientCreateRequest(BaseModel):
    phone: str
    phone2: str = ""
    first_name: str = ""
    last_name: str = ""
    source: str = "staff"
    status: str = "new"
    note: str = ""
    address: str = ""
    short_address: str = ""

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера")
        return v

# ── Admin auth helpers (defined early so they can be used anywhere below) ──────

async def _get_admin(authorization: str = Header(None)):
    """Проверяет admin JWT (sub='admin')."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") != "admin":
            raise HTTPException(status_code=403, detail="Нет доступа")
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    return True

async def _get_admin_or_staff_clients(authorization: str = Header(None)):
    """Принимает admin JWT или staff JWT с пермиссией clients."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    if payload.get("sub") == "admin":
        return True
    if payload.get("type") != "staff":
        raise HTTPException(status_code=403, detail="Нет доступа")
    staff = await db.get_staff_by_id(int(payload["sub"]))
    if not staff or not staff["active"]:
        raise HTTPException(status_code=403, detail="Нет доступа")
    role = staff.get("role") or ""
    perms = ROLE_PERMISSIONS.get(role, [])
    if "clients" in perms or role == "admin":
        return True
    raise HTTPException(status_code=403, detail="Нет доступа")

# ──────────────────────────────────────────────────────────────────────────────

class ClientUpdateRequest(BaseModel):
    phone2: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    status: str | None = None
    note: str | None = None
    address: str | None = None
    short_address: str | None = None


@app.get("/api/clients")
async def clients_list(search: str = "", limit: int = 50, offset: int = 0,
                       _=Depends(_get_admin_or_staff_clients)):
    rows = await db.get_crm_clients_list(search=search, limit=limit, offset=offset)
    counts = await db.get_crm_clients_count()
    return {"ok": True, "clients": rows, "counts": counts}


@app.get("/api/clients/by-phone/{phone}")
async def client_by_phone(phone: str, _=Depends(_get_admin_or_staff_clients)):
    phone = normalize_phone(phone)
    row = await db.get_crm_client_by_phone(phone)
    return {"ok": True, "client": row}


@app.get("/api/clients/{client_id}")
async def client_detail(client_id: int, _=Depends(_get_admin_or_staff_clients)):
    row = await db.get_crm_client_by_id(client_id)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    orders = await db.get_crm_client_orders(row["phone"])
    return {"ok": True, "client": row, "orders": orders}


@app.post("/api/clients")
async def client_create(req: ClientCreateRequest, _=Depends(_get_admin_or_staff_clients)):
    existing = await db.get_crm_client_by_phone(req.phone)
    if existing:
        raise HTTPException(status_code=409, detail={
            "msg": "Клиент с таким номером уже существует",
            "client": existing
        })
    row = await db.upsert_crm_client(
        phone=req.phone, first_name=req.first_name, last_name=req.last_name,
        source=req.source, address=req.address, short_address=req.short_address,
    )
    if req.phone2 or req.note or req.status != "new":
        row = await db.update_crm_client(
            row["id"], phone2=req.phone2 or None,
            note=req.note or None, status=req.status
        ) or row
    return {"ok": True, "client": row}


@app.put("/api/clients/{client_id}")
async def client_update(client_id: int, req: ClientUpdateRequest,
                        _=Depends(_get_admin_or_staff_clients)):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    row = await db.update_crm_client(client_id, **updates)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True, "client": row}


class ClientDeleteRequest(BaseModel):
    password: str

@app.post("/api/clients/{client_id}/delete")
async def client_delete(client_id: int, req: ClientDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_crm_client(client_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True}

@app.get("/api/clients/{client_id}/orders")
async def client_orders(client_id: int, _=Depends(_get_admin_or_staff_clients)):
    row = await db.get_crm_client_by_id(client_id)
    if not row:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    orders = await db.get_crm_client_orders(row["phone"])
    return {"ok": True, "orders": orders}


# ══════════════════════════════════════════════════════════════════════════════
# CONTACTS — справочник контактов
# ══════════════════════════════════════════════════════════════════════════════


class ContactCreateRequest(BaseModel):
    phone:         str
    first_name:    str = ""
    last_name:     str = ""
    middle_name:   str = ""
    phone2:        str = ""
    address:       str = ""
    short_address: str = ""
    source:        str = "ARTEZ"

class ContactUpdateRequest(BaseModel):
    phone:         str | None = None
    first_name:    str | None = None
    last_name:     str | None = None
    middle_name:   str | None = None
    phone2:        str | None = None
    address:       str | None = None
    short_address: str | None = None
    source:        str | None = None

class ContactsBulkRequest(BaseModel):
    rows: list[dict]

@app.get("/api/contacts/search")
async def contacts_search(q: str = "", limit: int = 10, _=Depends(get_current_staff)):
    results = await db.search_contacts(q.strip(), limit=min(limit, 20))
    return {"ok": True, "contacts": results}

@app.get("/api/contacts")
async def contacts_list(search: str = "", limit: int = 50, offset: int = 0,
                        _=Depends(_get_admin)):
    contacts = await db.get_contacts_list(search, limit=min(limit, 200), offset=offset)
    total    = await db.get_contacts_total(search)
    counts   = await db.get_contacts_source_counts()
    return {"ok": True, "contacts": contacts, "total": total, "counts": counts}

@app.post("/api/contacts")
async def contact_create(req: ContactCreateRequest, _=Depends(_get_admin)):
    contact = await db.upsert_contact(
        phone=req.phone, first_name=req.first_name, last_name=req.last_name,
        middle_name=req.middle_name, phone2=req.phone2,
        address=req.address, short_address=req.short_address, source=req.source)
    return {"ok": True, "contact": contact}

@app.post("/api/contacts/bulk")
async def contacts_bulk(req: ContactsBulkRequest, _=Depends(_get_admin)):
    result = await db.bulk_insert_contacts(req.rows)
    return {"ok": True, **result}

@app.get("/api/contacts/{contact_id}")
async def contact_get(contact_id: int, _=Depends(_get_admin)):
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM contacts WHERE id=$1", contact_id)
    if not row:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True, "contact": dict(row)}

@app.put("/api/contacts/{contact_id}")
async def contact_update(contact_id: int, req: ContactUpdateRequest,
                         _=Depends(_get_admin)):
    data = {k: v for k, v in req.dict().items() if v is not None}
    contact = await db.update_contact(contact_id, **data)
    if not contact:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True, "contact": contact}

class ContactDeleteRequest(BaseModel):
    password: str

@app.post("/api/contacts/{contact_id}/delete")
async def contact_delete(contact_id: int, req: ContactDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_contact(contact_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Контакт не найден")
    return {"ok": True}


class ContactsPurgeRequest(BaseModel):
    password: str

@app.post("/api/contacts/purge")
async def contacts_purge(req: ContactsPurgeRequest):
    """Удалить все контакты — только по паролю администратора."""
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    deleted = await db.delete_all_contacts()
    return {"ok": True, "deleted": deleted}


@app.get("/api/prices")
async def get_prices():
    """Возвращает актуальные цены из БД для калькулятора и прайс-листа на сайте"""
    prices = await db.get_all_prices()
    if not prices:
        # Дефолты на случай пустой таблицы
        prices = {
            "carpet":      {"standard": {"price": 13000, "unit_key": "m2", "min_order": 10}, "express": {"price": 18000, "unit_key": "m2", "min_order": 10}},
            "carpet_home": {"standard": {"price": 15000, "unit_key": "m2", "min_order": 10}, "express": {"price": 20000, "unit_key": "m2", "min_order": 10}},
            "sofa":        {"standard": {"price": 100000, "unit_key": "m2", "min_order": None}, "express": {"price": 150000, "unit_key": "m2", "min_order": None}},
            "mattress":    {"standard": {"price": 30000, "unit_key": "m2", "min_order": None}, "express": {"price": 40000, "unit_key": "m2", "min_order": None}},
            "curtains":    {"standard": {"price": 5000,  "unit_key": "m2", "min_order": None}, "express": {"price": 8000,  "unit_key": "m2", "min_order": None}},
        }
    units = await db.get_all_units()
    units_dict = {u["key"]: dict(u) for u in units}
    return {"ok": True, "prices": prices, "units": units_dict}


@app.get("/api/check-tg-link")
async def check_tg_link(phone: str):
    """Проверяет, привязан ли телефон к Telegram боту."""
    normalized = normalize_phone(phone)
    tg_id = await db.get_tg_id_by_phone(normalized)
    return {"has_tg": tg_id is not None}


@app.post("/api/tg-phone-link")
async def tg_phone_link(body: dict):
    """Бот вызывает этот endpoint когда клиент делится номером для привязки к сайту."""
    phone  = str(body.get("phone", "")).strip()
    tg_id  = body.get("tg_id")
    if not phone or not tg_id:
        raise HTTPException(400, "phone and tg_id required")
    phone = normalize_phone(phone)
    await db.save_tg_phone_link(phone, int(tg_id))
    user = await db.get_user_by_phone(phone)
    return {"ok": True, "registered": user is not None and user.get("is_verified", False)}


@app.post("/api/register")
async def register(req: RegisterRequest):
    uz = req.lang == "uz"
    existing = await db.get_user_by_phone(req.phone)
    if existing and existing["is_verified"]:
        raise HTTPException(status_code=400, detail=(
            "Bu raqam allaqachon ro'yxatdan o'tgan" if uz
            else "Этот номер уже зарегистрирован"))

    ok, err = await db.check_sms_rate_limit(req.phone, "register")
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    password_hash = pwd_context.hash(req.password[:72])
    await db.create_user(req.phone, password_hash, req.first_name)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, "register", expires_at)

    if req.via_tg:
        tg_id = await db.get_tg_id_by_phone(req.phone)
        if not tg_id:
            raise HTTPException(status_code=400, detail=(
                "Telegram не привязан. Сначала напишите боту /start и поделитесь номером."
                if not uz else
                "Telegram bog'lanmagan. Botga /start yozing va raqamingizni ulashing."))
        code_text = (
            f"🔐 <b>ARTEZ</b> — код подтверждения регистрации:\n\n<code>{code}</code>\n\n⏱ Действителен 5 минут."
            if not uz else
            f"🔐 <b>ARTEZ</b> — ro'yxatdan o'tish tasdiqlash kodi:\n\n<code>{code}</code>\n\n⏱ 5 daqiqa davomida amal qiladi."
        )
        await send_tg(tg_id, code_text)
        return {"ok": True, "via_tg": True, "message": "Код отправлен в Telegram", "phone": req.phone}

    await send_sms(req.phone, await sms_text(code, "register"))
    return {"ok": True, "via_tg": False, "message": "Код подтверждения отправлен", "phone": req.phone}


@app.post("/api/verify")
async def verify(req: VerifyRequest):
    ok = await db.check_sms_code(req.phone, req.code, "register")
    if not ok:
        raise HTTPException(status_code=400, detail=bi("Неверный или просроченный код","Noto'g'ri yoki muddati o'tgan kod"))

    await db.verify_user(req.phone)
    user = await db.get_user_by_phone(req.phone)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    asyncio.create_task(_notify_new_site_user(user.get("first_name") or "", user["phone"], "sms"))
    token = create_token(user["id"], user["phone"])

    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user["address"],
            "car_plate": user["car_plate"],
            "osago_expiry": user["osago_expiry"].isoformat() if user.get("osago_expiry") else None,
        }
    }


@app.post("/api/resend-code")
async def resend_code(req: ResendCodeRequest):
    user = await db.get_user_by_phone(req.phone)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    ok, err = await db.check_sms_rate_limit(req.phone, req.purpose)
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, req.purpose, expires_at)

    if req.via_tg:
        tg_id = await db.get_tg_id_by_phone(req.phone)
        if tg_id:
            await send_tg(tg_id,
                f"🔐 <b>ARTEZ</b> — код подтверждения:\n\n<code>{code}</code>\n\n⏱ Действителен 5 минут.")
            return {"ok": True, "message": "Код отправлен в Telegram"}
    await send_sms(req.phone, await sms_text(code, req.purpose))
    return {"ok": True, "message": "Код отправлен повторно"}


@app.post("/api/login")
async def login(req: LoginRequest):
    user = await db.get_user_by_phone(req.phone)
    if not user or not pwd_context.verify(req.password[:72], user["password_hash"]):
        raise HTTPException(status_code=401, detail=bi("Неверный номер или пароль","Noto'g'ri telefon yoki parol"))

    if not user["is_verified"]:
        raise HTTPException(status_code=403, detail=bi("Номер не подтверждён. Запросите код заново","Raqam tasdiqlanmagan. Kodni qayta so'rang"))

    asyncio.create_task(db.update_user_last_login(user["id"]))
    token = create_token(user["id"], user["phone"])
    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user["address"],
            "car_plate": user["car_plate"],
            "osago_expiry": user["osago_expiry"].isoformat() if user.get("osago_expiry") else None,
        }
    }


@app.get("/api/me")
async def me(user = Depends(get_current_user)):
    expiry = user.get("osago_expiry")
    return {
        "id": user["id"],
        "phone": user["phone"],
        "first_name": user["first_name"],
        "is_verified": user["is_verified"],
        "address": user["address"],
        "car_plate": user["car_plate"],
        "osago_expiry": expiry.isoformat() if expiry else None,
        "tg_id": user.get("tg_id"),
    }


class UpdateProfileRequest(BaseModel):
    first_name: str
    address: str | None = None
    car_plate: str | None = None
    osago_expiry: str | None = None  # ISO date YYYY-MM-DD или null

    @field_validator("first_name")
    @classmethod
    def validate_name(cls, v):
        if not v.strip():
            raise ValueError("Имя не может быть пустым")
        return v.strip()


class UpdatePasswordRequest(BaseModel):
    old_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_new_password(cls, v):
        if len(v) < 6:
            raise ValueError("Пароль должен быть не короче 6 символов")
        return v


@app.patch("/api/me")
async def update_profile(req: UpdateProfileRequest, user = Depends(get_current_user)):
    from datetime import date as date_type
    expiry = None
    if req.osago_expiry:
        try:
            expiry = date_type.fromisoformat(req.osago_expiry)
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты (ожидается YYYY-MM-DD)")
    await db.update_user_profile(user["id"], req.first_name, req.address, req.car_plate, expiry)
    return {"ok": True, "first_name": req.first_name}


@app.patch("/api/me/password")
async def update_password(req: UpdatePasswordRequest, user = Depends(get_current_user)):
    if not pwd_context.verify(req.old_password[:72], user["password_hash"]):
        raise HTTPException(status_code=400, detail="Неверный текущий пароль")
    new_hash = pwd_context.hash(req.new_password[:72])
    await db.update_user_password(user["id"], new_hash)
    return {"ok": True}


class LinkTgRequest(BaseModel):
    user_id: int
    tg_id: int
    tg_username: str | None = None

@app.post("/api/user/link-tg")
async def link_tg(req: LinkTgRequest):
    """Бот вызывает этот endpoint чтобы привязать tg_id к аккаунту сайта."""
    user = await db.get_user_by_id(req.user_id)
    if not user:
        raise HTTPException(404, "Пользователь не найден")
    await db.link_user_tg_id(user["phone"], req.tg_id)
    return {"ok": True, "phone": user["phone"], "name": user.get("first_name") or ""}

@app.get("/api/orders")
async def my_orders(user = Depends(get_current_user)):
    orders = await db.get_orders_by_phone(user["phone"])
    return {"orders": [dict(o) for o in orders]}


@app.post("/api/orders/{order_num}/cancel")
async def cancel_order(order_num: str, user = Depends(get_current_user)):
    order = await db.cancel_order_by_phone(order_num, user["phone"])
    if not order:
        raise HTTPException(status_code=400, detail="Заказ не найден или уже нельзя отменить")
    asyncio.create_task(notify_group_client_cancel(order))
    return {"ok": True}


async def notify_group_client_cancel(order: dict):
    if not BOT_TOKEN or not GROUP_ID:
        return
    text = (
        f"🚫 Заявка {order['order_num']} отменена клиентом\n"
        f"━━━━━━━━━━\n"
        f"👤 {order.get('client_name') or '—'}\n"
        f"📞 {order.get('client_phone') or '—'}\n"
        f"🧺 {order.get('service') or '—'}\n"
        f"🏢 {branch_ru(order.get('branch') or '')}\n"
        f"━━━━━━━━━━"
    )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={"chat_id": GROUP_ID, "text": text})
    except Exception as e:
        logging.warning(f"Cancel notify error: {e}")


# ══════════════════════════════════════
#  УВЕДОМЛЕНИЕ TELEGRAM-ГРУППЫ О НОВОЙ ЗАЯВКЕ С САЙТА
# ══════════════════════════════════════
def md_escape(text):
    if not text:
        return ""
    text = str(text)
    for ch in ['_', '*', '[', ']', '`']:
        text = text.replace(ch, f"\\{ch}")
    return text


BRANCH_RU = {
    "zarafshan": "Зарафшан", "зарафшан": "Зарафшан", "zarafshon": "Зарафшан",
    "navoi":     "Навои",    "навои":    "Навои",    "navoiy":    "Навои",
}

def branch_ru(branch: str) -> str:
    if not branch: return "—"
    key = branch.lower().replace("📍", "").strip()
    return BRANCH_RU.get(key, branch.strip("📍 ").strip())

async def _group_id_for_branch(branch: str) -> str:
    """Возвращает chat_id группы для указанного филиала (из БД или env)."""
    if branch in ("zarafshan", "Зарафшан"):
        gid = await _get_cfg("tg_group_zarafshan")
        return gid or GROUP_ID
    if branch in ("navoi", "Навои"):
        gid = await _get_cfg("tg_group_navoi")
        return gid or GROUP_ID
    return GROUP_ID

async def notify_group_new_order(order_num: str, data: "OrderRequest"):
    if not BOT_TOKEN:
        logging.warning("BOT_TOKEN not set — skipping group notification")
        return
    chat_id = await _group_id_for_branch(getattr(data, "branch", "") or "")
    if not chat_id:
        logging.warning("No GROUP_ID configured — skipping group notification")
        return

    full_name = f"{data.first_name} {data.last_name}".strip()

    # Строим ссылку на Яндекс Карты, если есть координаты
    location_url = None
    loc_display = "—"
    if data.location:
        try:
            lat_s, lon_s = data.location.split(",", 1)
            location_url = f"https://yandex.uz/maps/?pt={lon_s.strip()},{lat_s.strip()}&z=16"
        except Exception:
            pass
        loc_display = data.location_address if data.location_address else data.location

    def he(s):
        return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") if s else "—"

    loc_line = f'🗺 <a href="{location_url}">{he(loc_display)}</a>' if location_url else f"🗺 {he(loc_display)}"

    if data.is_quick:
        text = (
            f"⚡ Быстрая заявка {order_num} (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"━━━━━━━━━━"
        )
    else:
        text = (
            f"🌐 Новая заявка {order_num} (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"🏢 {he(branch_ru(data.branch))}\n"
            f"📍 {he(data.city)}\n"
            f"🏠 {he(data.address)}\n"
            f"{loc_line}\n"
            f"🧺 {he(data.service)}\n"
            f"⚙️ {he(data.service_type)}\n"
            f"📅 {he(data.pickup_date)}\n"
            f"🕐 {he(data.pickup_time)}\n"
            f"━━━━━━━━━━"
        )

    kb_rows = []
    kb_rows.extend([
        [{"text": "✅ Принять заказ", "callback_data": f"accept_{order_num}_0"}],
        [
            {"text": "🚗 Назначить водителя", "callback_data": f"driver_{order_num}_0"},
            {"text": "❌ Отклонить", "callback_data": f"reject_{order_num}_0"},
        ],
    ])
    keyboard = {"inline_keyboard": kb_rows}

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "reply_markup": keyboard, "parse_mode": "HTML", "disable_web_page_preview": True}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.warning(f"Telegram notify failed: {resp.status} {body}")
    except Exception as e:
        logging.warning(f"Telegram notify error: {e}")


# ══════════════════════════════════════
#  GOOGLE-ТАБЛИЦА — ТА ЖЕ, КУДА ПИШЕТ БОТ
# ══════════════════════════════════════
async def send_to_sheets(data: dict):
    url = await _get_cfg("sheets_url")
    if not url:
        logging.warning("sheets_url not set — skipping sheets export")
        return
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=10))
    except Exception as e:
        logging.warning(f"Sheets error: {e}")


async def notify_sheets_new_order(order_num: str, data: "OrderRequest"):
    """Формирует строку для Google-таблицы в том же формате, что использует бот"""
    full_name = f"{data.first_name} {data.last_name}".strip()
    await send_to_sheets({
        "name":         full_name,
        "tg_id":        "",
        "tg_username":  "",
        "tg_name":      "",
        "phone":        data.phone,
        "branch":       data.branch,
        "city":         data.city,
        "address":      data.address,
        "location":     data.location or "",
        "service":      data.service,
        "service_type": data.service_type,
        "date":         data.pickup_date,
        "time":         data.pickup_time,
        "note":         f"Сайт ARTEZ {order_num}",
        "status":       "Новый",
    })


# ══════════════════════════════════════
#  ADMIN
# ══════════════════════════════════════
class AdminLoginRequest(BaseModel):
    password: str

class SetPriceRequest(BaseModel):
    service_key: str
    type_key: str
    price: int
    unit_key: str = None
    min_order: float = None

class UnitRequest(BaseModel):
    key: str
    name_ru: str
    name_uz: str
    symbol_ru: str
    symbol_uz: str

ADMIN_TOKEN_PREFIX = "admin:"

def create_admin_token() -> str:
    payload = {
        "sub": "admin",
        "exp": datetime.now(timezone.utc) + timedelta(days=30),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

async def get_admin(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("sub") != "admin":
            raise HTTPException(status_code=403, detail="Нет доступа")
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    return True

@app.post("/api/admin/login")
async def admin_login(req: AdminLoginRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=401, detail="Неверный пароль")
    return {"ok": True, "token": create_admin_token()}

@app.post("/api/admin/change-master-password")
async def change_master_password(body: dict, _=Depends(_get_admin)):
    current = body.get("current_password", "")
    new_pass = body.get("new_password", "")
    if not current or current != await get_admin_pass():
        raise HTTPException(status_code=403, detail="Неверный текущий пароль")
    if not new_pass or len(new_pass) < 4:
        raise HTTPException(status_code=400, detail="Новый пароль минимум 4 символа")
    await db.set_config("admin_pass", new_pass)
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN LEADS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/leads")
async def admin_get_leads(status: str = None, branch: str = None,
                          search: str = "", _=Depends(_get_admin)):
    rows = await db.get_leads(status=status, branch=branch, limit=500)
    leads = [dict(r) for r in rows]
    if search:
        q = search.lower()
        leads = [l for l in leads if
                 q in (l.get("client_name") or "").lower() or
                 q in (l.get("client_phone") or "").lower() or
                 q in (l.get("address") or "").lower() or
                 q in (l.get("short_address") or "").lower()]
    return {"ok": True, "leads": leads}

class LeadUpdateRequest(BaseModel):
    client_name:  str | None = None
    client_phone: str | None = None
    service:      str | None = None
    branch:       str | None = None
    city:         str | None = None
    address:      str | None = None
    short_address: str | None = None
    note:         str | None = None
    status:       str | None = None

@app.put("/api/admin/leads/{lead_id}")
async def admin_update_lead(lead_id: int, req: LeadUpdateRequest, _=Depends(_get_admin)):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    row = await db.update_lead(lead_id, **updates)
    if not row:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True, "lead": row}

@app.patch("/api/admin/leads/{lead_id}/status")
async def admin_update_lead_status(lead_id: int, body: dict, _=Depends(_get_admin)):
    status = body.get("status")
    if status not in ("new","contacted","callback","converted","lost"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    await db.update_lead_status(lead_id, status)
    return {"ok": True}

class LeadDeleteRequest(BaseModel):
    password: str

@app.post("/api/admin/leads/{lead_id}/delete")
async def admin_delete_lead(lead_id: int, req: LeadDeleteRequest):
    if not (apass := await get_admin_pass()) or req.password != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/admin/leads")
async def admin_create_lead(req: LeadCreateRequest, _=Depends(_get_admin)):
    lead = await db.create_lead({
        "client_name": req.client_name,
        "client_phone": req.client_phone, "service": req.service,
        "branch": req.branch, "city": req.city, "address": req.address,
        "short_address": req.short_address, "note": req.note,
        "assigned_to": req.assigned_to, "created_by": None,
    })
    return {"ok": True, "lead": lead}

# ══════════════════════════════════════════════════════════════════════════════
# АГЕНТЫ — регистрация и сброс пароля
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/agent/status")
async def agent_status(user=Depends(get_current_user)):
    """Возвращает статус агента для текущего пользователя сайта."""
    # 1. По site_user_id
    staff = await db.get_staff_by_site_user(user["id"])
    if staff:
        return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    # 2. По tg_id
    if user.get("tg_id"):
        staff = await db.get_staff_by_tg_id(user["tg_id"])
        if staff and staff["role"] == "agent":
            return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    # 3. По номеру телефона (логину)
    staff = await db.get_staff_by_login(user["phone"])
    if staff and staff["role"] == "agent":
        # Заодно прописываем site_user_id чтобы следующий раз найти быстрее
        await db.link_staff_to_site_user(staff["id"], user["id"])
        return {"ok": True, "is_agent": True, "must_change_password": bool(staff.get("must_change_password"))}
    return {"ok": True, "is_agent": False}

@app.post("/api/agent/apply")
async def agent_apply(req: AgentApplyRequest, user=Depends(get_current_user)):
    """Пользователь сайта регистрируется как агент.
    Ищет клиента бота по clients.tg_phone = users.phone.
    Если не найден — возвращает needs_bot=True (нужно написать боту).
    """
    if not user.get("is_verified"):
        raise HTTPException(400, "Сначала подтвердите номер телефона")

    # Уже агент?
    existing = await db.get_staff_by_site_user(user["id"])
    if existing:
        return {"ok": True, "already": True, "message": "Вы уже зарегистрированы как агент"}
    existing2 = await db.get_staff_by_login(user["phone"])
    if existing2 and existing2["role"] == "agent":
        await db.link_staff_to_site_user(existing2["id"], user["id"])
        return {"ok": True, "already": True, "message": "Вы уже зарегистрированы как агент"}

    # Ищем клиента бота по tg_phone = phone сайта
    client = await db.get_client_by_tg_phone(user["phone"])
    if not client:
        # Telegram-контакт не верифицирован — нужно зайти в бот и поделиться номером
        return {"ok": False, "needs_bot": True}

    # Привязываем tg_id к аккаунту сайта (если ещё не привязан)
    tg_id = client.get("tg_id")
    if tg_id and not user.get("tg_id"):
        await db.link_user_tg_id(user["phone"], int(tg_id))

    site_user = await db.get_user_by_id(user["id"])
    password_hash = site_user["password_hash"] if site_user else None
    if not password_hash:
        raise HTTPException(400, "Пароль не установлен.")

    # Передаём актуальный tg_id в create_agent_from_user
    user_data = dict(user)
    if tg_id:
        user_data["tg_id"] = int(tg_id)

    staff_id = await db.create_agent_from_user(user_data, password_hash, req.branch)
    if not staff_id:
        return {"ok": True, "already": True, "message": "Аккаунт агента уже существует"}

    return {"ok": True, "already": False, "message": "Вы зарегистрированы как агент! Войдите через artez.uz/staff.html"}

class ApplyByTgRequest(BaseModel):
    tg_id: int
    phone: str | None = None  # телефон из базы бота как запасной вариант

async def _find_site_user_for_bot(tg_id: int, phone: str | None):
    """Ищет пользователя сайта: сначала по tg_id, потом по телефону из бота."""
    try:
        user = await db.get_user_by_tg_id(tg_id)
        if user:
            return user
    except Exception:
        pass
    if phone:
        try:
            norm = normalize_phone(phone)
            user = await db.get_user_by_phone(norm)
            if not user and norm.startswith("+"):
                user = await db.get_user_by_phone(norm[1:])
        except Exception:
            user = None
        if user:
            try:
                await db.link_user_tg_id(user["phone"], tg_id)
            except Exception:
                pass
            return user
    return None

@app.get("/api/agent/status-by-tg/{tg_id}")
async def agent_status_by_tg_endpoint(tg_id: int, phone: str | None = None):
    """Для бота: проверить статус агента по tg_id без авторизации."""
    staff = await db.get_staff_by_tg_id(tg_id)
    if staff and staff["role"] == "agent":
        return {"ok": True, "is_agent": True, "has_site_account": True}
    site_user = await _find_site_user_for_bot(tg_id, phone)
    return {"ok": True, "is_agent": False, "has_site_account": bool(site_user)}

@app.post("/api/agent/apply-by-tg")
async def agent_apply_by_tg(req: ApplyByTgRequest):
    """Бот регистрирует агента по tg_id — ищет аккаунт сайта по tg_id или телефону."""
    site_user = await _find_site_user_for_bot(req.tg_id, req.phone)
    if not site_user:
        return {"ok": False, "reason": "no_site_account"}
    if not site_user.get("is_verified"):
        return {"ok": False, "reason": "not_verified"}
    existing = await db.get_staff_by_login(site_user["phone"])
    if existing and existing["role"] == "agent":
        return {"ok": True, "already": True, "phone": site_user["phone"]}
    password_hash = site_user.get("password_hash")
    if not password_hash:
        return {"ok": False, "reason": "no_password"}
    staff_id = await db.create_agent_from_user(dict(site_user), password_hash)
    if not staff_id:
        return {"ok": True, "already": True, "phone": site_user["phone"]}
    return {"ok": True, "already": False, "phone": site_user["phone"], "name": site_user.get("first_name") or ""}

@app.post("/api/agent/reset-password")
async def agent_reset_password(body: dict):
    """Сброс пароля агента — отправляет временный пароль через Telegram."""
    phone = normalize_phone(body.get("phone", ""))
    staff = await db.get_staff_by_login(phone)
    if not staff or staff["role"] != "agent":
        # Не раскрываем что аккаунта нет
        return {"ok": True, "message": "Если аккаунт агента найден — пароль отправлен в Telegram"}

    if not staff.get("tg_id"):
        raise HTTPException(400, "Telegram не привязан. Обратитесь к администратору.")

    import random, string
    from datetime import datetime, timezone, timedelta
    temp_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    expires = datetime.now(timezone.utc) + timedelta(minutes=10)
    hashed  = pwd_context.hash(temp_pw)
    await db.set_staff_temp_password(staff["id"], hashed, expires)

    text = (f"🔑 Временный пароль для входа в систему ARTEZ:\n\n"
            f"<b>{temp_pw}</b>\n\n"
            f"⏰ Действует 10 минут.\n"
            f"После входа сразу смените пароль.")
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(tg_url, json={"chat_id": staff["tg_id"], "text": text, "parse_mode": "HTML"},
                     timeout=aiohttp.ClientTimeout(total=8))

    return {"ok": True, "message": "Временный пароль отправлен в Telegram"}

@app.post("/api/agent/change-password")
async def agent_change_password(body: dict, staff=Depends(get_current_staff)):
    """Смена пароля после входа по временному."""
    if staff.get("role") != "agent":
        raise HTTPException(403, "Только для агентов")
    new_pw = (body.get("password") or "").strip()
    if len(new_pw) < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    hashed = pwd_context.hash(new_pw[:72])
    await db.update_staff_password(staff["id"], hashed, plain=new_pw)
    await db.clear_staff_temp_password(staff["id"])
    return {"ok": True}


class ResetByTgRequest(BaseModel):
    tg_id: int

@app.post("/api/agent/reset-password-by-tg")
async def agent_reset_password_by_tg(req: ResetByTgRequest):
    """Для бота: сброс пароля агента по tg_id."""
    staff = await db.get_staff_by_tg_id(str(req.tg_id))
    if not staff or staff["role"] != "agent":
        return {"ok": True}
    import random, string
    from datetime import datetime, timezone, timedelta
    temp_pw = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    expires = datetime.now(timezone.utc) + timedelta(minutes=10)
    hashed  = pwd_context.hash(temp_pw)
    await db.set_staff_temp_password(staff["id"], hashed, expires)
    text = (f"🔑 Временный пароль для входа в систему ARTEZ:\n\n"
            f"<b>{temp_pw}</b>\n\n"
            f"⏰ Действует 10 минут.\n"
            f"Войдите на artez.uz/staff.html и сразу смените пароль.")
    tg_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(tg_url, json={"chat_id": req.tg_id, "text": text, "parse_mode": "HTML"},
                     timeout=aiohttp.ClientTimeout(total=8))
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════
# ADMIN: ПОЛЬЗОВАТЕЛИ САЙТА
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/site-users")
async def admin_get_site_users(search: str = "", _=Depends(_get_admin)):
    rows = await db.get_all_site_users(search=search.strip())
    def _row(r):
        d = dict(r)
        for k in ("osago_expiry",):
            if d.get(k) and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        for k in ("created_at", "updated_at", "last_login"):
            if d.get(k) and hasattr(d[k], "isoformat"):
                d[k] = d[k].isoformat()
        return d
    return {"ok": True, "users": [_row(r) for r in rows]}

@app.patch("/api/admin/site-users/{user_id}")
async def admin_update_site_user(user_id: int, body: dict, _=Depends(_get_admin)):
    first_name   = (body.get("first_name")   or "").strip() or None
    address      = (body.get("address")      or "").strip() or None
    car_plate    = (body.get("car_plate")    or "").strip().upper() or None
    osago_str    = (body.get("osago_expiry") or "").strip() or None
    osago_expiry = None
    if osago_str:
        try:
            from datetime import date as _d
            osago_expiry = _d.fromisoformat(osago_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты ОСАГО (YYYY-MM-DD)")
    await db.update_user_profile(user_id, first_name, address, car_plate, osago_expiry)
    return {"ok": True}

@app.post("/api/admin/site-users/{user_id}/reset-password")
async def admin_reset_site_user_password(user_id: int, body: dict, _=Depends(_get_admin)):
    new_password = (body.get("new_password") or "").strip()
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="Пароль минимум 4 символа")
    send_tg = bool(body.get("send_tg", False))
    hashed = pwd_context.hash(new_password[:72])
    await db.update_user_password(user_id, hashed)
    if send_tg and BOT_TOKEN:
        user = await db.get_user_by_id(user_id)
        tg_id = user.get("tg_id") if user else None
        if tg_id:
            text = (
                f"🔑 <b>ARTEZ</b> — ваш пароль изменён администратором.\n\n"
                f"📱 Логин: <code>{user['phone']}</code>\n"
                f"🔑 Новый пароль: <code>{new_password}</code>\n\n"
                f"Не передавайте пароль третьим лицам."
            )
            try:
                async with aiohttp.ClientSession() as _s:
                    await _s.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                        json={"chat_id": str(tg_id), "text": text, "parse_mode": "HTML"},
                        timeout=aiohttp.ClientTimeout(total=5)
                    )
            except Exception:
                pass
    return {"ok": True, "tg_sent": send_tg and bool(body.get("send_tg"))}


@app.post("/api/register-via-tg")
async def register_via_tg(body: dict):
    phone      = (body.get("phone") or "").strip()
    first_name = (body.get("first_name") or "").strip()
    password   = (body.get("password") or "").strip()
    uz = (body.get("lang") or "ru") == "uz"

    if not phone or not first_name or not password:
        raise HTTPException(400, "Yetishmayotgan maydonlar" if uz else "Заполните все поля")
    if len(password) < 6:
        raise HTTPException(400, "Parol kamida 6 ta belgi" if uz else "Пароль минимум 6 символов")

    tg_id = await db.get_tg_id_by_phone(phone)
    if not tg_id:
        raise HTTPException(400,
            "Bu raqam botda topilmadi. Avval bot bilan telefon raqamingizni ulashing."
            if uz else
            "Телефон не найден в боте. Сначала поделитесь номером через бота.")

    existing = await db.get_user_by_phone(phone)
    if existing and existing["is_verified"]:
        raise HTTPException(400,
            "Bu raqam allaqachon ro'yxatdan o'tgan" if uz
            else "Этот номер уже зарегистрирован")

    password_hash = pwd_context.hash(password[:72])
    await db.create_user(phone, password_hash, first_name)
    await db.verify_user(phone)
    await db.set_user_tg_id(phone, tg_id)

    user = await db.get_user_by_phone(phone)
    asyncio.create_task(db.update_user_last_login(user["id"]))
    token = create_token(user["id"], user["phone"])

    # Отправляем данные аккаунта в Telegram
    if BOT_TOKEN:
        text = (
            f"🎉 <b>ARTEZ</b> — регистрация завершена!\n\n"
            f"👤 Имя: <b>{first_name}</b>\n"
            f"📱 Номер / Логин: <code>{phone}</code>\n"
            f"🔑 Пароль: <code>{password}</code>\n\n"
            f"Используйте эти данные для входа на сайте artez.uz"
        ) if not uz else (
            f"🎉 <b>ARTEZ</b> — ro'yxatdan o'tdingiz!\n\n"
            f"👤 Ism: <b>{first_name}</b>\n"
            f"📱 Raqam / Login: <code>{phone}</code>\n"
            f"🔑 Parol: <code>{password}</code>\n\n"
            f"artez.uz saytiga kirish uchun ushbu ma'lumotlardan foydalaning."
        )
        asyncio.create_task(_send_tg_safe(tg_id, text))

    asyncio.create_task(_notify_new_site_user(first_name, phone, "tg"))

    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
            "address": user.get("address"),
            "car_plate": user.get("car_plate"),
            "osago_expiry": user["osago_expiry"].isoformat() if user.get("osago_expiry") else None,
        }
    }


async def _send_tg_safe(tg_id: int, text: str):
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": str(tg_id), "text": text, "parse_mode": "HTML"},
                timeout=aiohttp.ClientTimeout(total=5))
    except Exception:
        pass


async def _notify_new_site_user(first_name: str, phone: str, method: str):
    """Уведомляет группу и персональных сотрудников о новой регистрации."""
    if not BOT_TOKEN:
        return
    from datetime import datetime
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    method_icon = "✈️ Telegram" if method == "tg" else "📱 SMS"
    text = (
        f"👤 {first_name}, 📞 <code>{phone}</code>, 🔐 {method_icon}, 🌐\n"
        f"📅 {now}"
    )
    targets = []
    group_id = await _get_cfg("new_clients_group_id")
    if group_id:
        targets.append(group_id)
    try:
        staff_ids = await db.get_staff_notify_new_users()
        targets.extend(str(tid) for tid in staff_ids)
    except Exception:
        pass
    async with aiohttp.ClientSession() as s:
        for chat_id in targets:
            try:
                await s.post(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                    json={"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=5))
            except Exception:
                pass


@app.delete("/api/admin/site-users/{user_id}")
async def admin_delete_site_user(user_id: int, body: dict, _=Depends(_get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ok = await db.delete_site_user(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return {"ok": True}

# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/admin/prices")
async def admin_get_prices(_=Depends(get_admin)):
    prices = await db.get_all_prices()
    return {"ok": True, "prices": prices}

@app.put("/api/admin/prices")
async def admin_set_price(req: SetPriceRequest, _=Depends(get_admin)):
    SERVICE_KEYS = ["carpet","carpet_home","sofa","mattress","curtains"]
    TYPE_KEYS    = ["standard","express"]
    if req.service_key not in SERVICE_KEYS:
        raise HTTPException(status_code=400, detail=f"Неверная услуга: {req.service_key}")
    if req.type_key not in TYPE_KEYS:
        raise HTTPException(status_code=400, detail=f"Неверный тип: {req.type_key}")
    if req.price <= 0:
        raise HTTPException(status_code=400, detail="Цена должна быть > 0")
    if req.min_order is not None and req.min_order <= 0:
        raise HTTPException(status_code=400, detail="Минимальный заказ должен быть > 0")
    await db.set_price(req.service_key, req.type_key, req.price, unit_key=req.unit_key, min_order=req.min_order)
    return {"ok": True}

@app.get("/api/units")
async def get_units_public():
    """Публичный эндпоинт — список единиц измерения для сайта"""
    units = await db.get_all_units()
    return {"ok": True, "units": [dict(u) for u in units]}

@app.get("/api/admin/units")
async def admin_get_units(_=Depends(get_admin)):
    units = await db.get_all_units()
    return {"ok": True, "units": [dict(u) for u in units]}

@app.put("/api/admin/units")
async def admin_set_unit(req: UnitRequest, _=Depends(get_admin)):
    if not req.key.strip():
        raise HTTPException(status_code=400, detail="Укажите ключ единицы измерения")
    await db.add_unit(req.key.strip(), req.name_ru.strip(), req.name_uz.strip(),
                       req.symbol_ru.strip(), req.symbol_uz.strip())
    return {"ok": True}

@app.delete("/api/admin/units/{key}")
async def admin_delete_unit(key: str, _=Depends(get_admin)):
    ok = await db.delete_unit(key)
    if not ok:
        raise HTTPException(status_code=404, detail="Единица измерения не найдена")
    return {"ok": True}

@app.get("/api/admin/orders")
async def admin_get_orders(_=Depends(get_admin), status: str = None, limit: int = 50):
    prices = await db.get_admin_orders(status=status, limit=limit)
    return {"ok": True, "orders": [dict(o) for o in prices]}

@app.get("/api/admin/orders/{order_id}")
async def admin_get_order(order_id: int, _=Depends(get_current_staff)):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    return {"ok": True, "order": order}

_ORDER_EDITABLE_STATUSES = {"new","confirmed","pickup","received","washing","drying","packing","ready"}

@app.patch("/api/admin/orders/{order_id}")
async def update_order_data(order_id: int, body: dict = Body(...), staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, [])
    if "orders" not in perms and staff.get("sub") != "admin":
        raise HTTPException(status_code=403, detail="Нет доступа")
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    can_edit_delivery = staff.get("can_edit_delivery", False)
    if (staff.get("sub") != "admin"
            and order.get("status") not in _ORDER_EDITABLE_STATUSES
            and not (order.get("status") == "delivery" and can_edit_delivery)):
        raise HTTPException(status_code=400, detail="Нельзя редактировать заказ в этом статусе")
    allowed = {"client_first_name","client_last_name","client_phone",
               "branch","address","short_address","location","location_address","note","deadline","service_type",
               "pickup_type","self_pickup_discount","discount_sum","manual_discount",
               "delivery_type","delivery_discount","delivery_discount_pct"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    # asyncpg требует объект date, а не строку
    if "deadline" in updates and isinstance(updates["deadline"], str):
        from datetime import date
        try:
            updates["deadline"] = date.fromisoformat(updates["deadline"])
        except ValueError:
            updates["deadline"] = None
    try:
        updated = await db.update_order(order_id, **updates)
        return {"ok": True, "order": {k: str(v) if hasattr(v, 'isoformat') else v
                                      for k, v in updated.items()}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка обновления: {str(e)}")

@app.get("/api/staff/orders/{order_id}/history")
async def get_order_history(order_id: int, _=Depends(get_current_staff)):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    rows = await db.get_order_status_history(order.get("order_num", ""))
    return {"ok": True, "history": [
        {k: str(v) if hasattr(v, 'isoformat') else v for k, v in r.items()}
        for r in rows
    ]}

@app.get("/api/staff/check-phone")
async def check_phone(phone: str, _=Depends(get_current_staff)):
    result = await db.check_phone_duplicate(phone)
    return {"ok": True, **result}

@app.post("/api/staff/leads/{lead_id}/convert")
async def convert_lead_to_order(lead_id: int, body: dict = Body({}),
                                 staff=Depends(require_perm("orders"))):
    lead = await db.get_lead_by_id(lead_id)
    if not lead:
        raise HTTPException(status_code=404, detail="Лид не найден")
    name_parts = (lead.get("name") or "").split(maxsplit=1)
    first = name_parts[0] if name_parts else ""
    last  = name_parts[1] if len(name_parts) > 1 else ""
    order_num = await db.get_next_order_num()
    lead_note = lead.get("note") or ""
    note_text = f"Конвертирован из лида #{lead_id}" + (f". {lead_note}" if lead_note else "")
    await db.save_site_order({
        "order_num":     order_num,
        "first_name":    first,
        "last_name":     last,
        "phone":         lead.get("phone", ""),
        "branch":        lead.get("branch") or body.get("branch", ""),
        "city":          "",
        "address":       lead.get("address", ""),
        "short_address": lead.get("short_address", ""),
        "location":      lead.get("location", ""),
        "service":       "",
        "pickup_type":   lead.get("pickup_type", "courier"),
        "delivery_type": lead.get("delivery_type", "courier"),
        "pickup_date":   "",
        "pickup_time":   "",
        "note":          note_text,
        "total_price":   None,
    }, source="staff")
    await db.update_lead_status(lead_id, "converted")
    return {"ok": True, "order_num": order_num}

@app.patch("/api/admin/orders/{order_id}/status")
async def admin_change_order_status(order_id: int, staff=Depends(get_current_staff),
                                     status: str = Body(..., embed=True),
                                     note: str = Body("", embed=True)):
    role = staff.get("role", "")
    if role == "washer":
        order = await db.get_order_by_id(order_id)
        if not order:
            raise HTTPException(status_code=404, detail="Заказ не найден")
        allowed = WASHER_STATUS_FLOW.get(order.get("status", ""))
        if status != allowed:
            raise HTTPException(status_code=403, detail=f"Мойщик может изменить статус только на: {allowed}")
        # Перед началом мойки — все позиции должны быть замерены
        if status == "washing":
            items = await db.get_order_items(order_id)
            pending = [i for i in items if i.get("measure_status", "pending") == "pending"]
            if pending:
                raise HTTPException(status_code=400, detail=f"Не все позиции замерены: осталось {len(pending)}")
    elif "status" not in ROLE_PERMISSIONS.get(role, []) and role != "admin":
        # Любой с orders может подтвердить заказ (new → confirmed), если есть can_confirm_order
        perms = ROLE_PERMISSIONS.get(role, [])
        if "orders" in perms and status == "confirmed":
            if not staff.get("can_confirm_order", True):
                raise HTTPException(status_code=403, detail="Нет права подтверждать заказы")
            order = await db.get_order_by_id(order_id)
            if not order or order.get("status") != "new":
                raise HTTPException(status_code=403, detail="Можно подтвердить только новый заказ")
        else:
            raise HTTPException(status_code=403, detail="Нет прав для смены статуса")
    if status not in ALL_ORDER_STATUSES:
        raise HTTPException(status_code=400, detail="Неизвестный статус")
    order = await db.update_order_status(order_id, status,
                                          note=note or f"Статус изменён сотрудником {staff.get('login','')}")
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")

    # ── Telegram уведомление клиенту ──────────────────────────────────────
    tg_id = order.get("client_tg_id")
    if tg_id and BOT_TOKEN:
        try:
            tmpl = await db.get_tg_status_message(status)
            if tmpl and tmpl.get("enabled"):
                # Определяем язык клиента — пробуем найти в таблице clients
                lang = "ru"
                try:
                    async with db.pool.acquire() as _c:
                        row = await _c.fetchrow(
                            "SELECT language FROM clients WHERE tg_id=$1", int(tg_id))
                        if row and row["language"] in ("uz", "ru"):
                            lang = row["language"]
                except Exception:
                    pass

                raw = tmpl.get(f"message_{lang}") or tmpl.get("message_ru") or ""
                if raw:
                    STATUS_EMOJI = {
                        "new":"🆕","confirmed":"✅","pickup":"🚗","received":"📦",
                        "washing":"🧼","drying":"💨","packing":"📦","ready":"✅",
                        "delivery":"🚚","delivered":"✅","cancelled":"❌",
                    }
                    STATUS_NAME_RU = {
                        "new":"Новый","confirmed":"Подтверждён","pickup":"Вывоз",
                        "received":"В мастерской","washing":"Мойка","drying":"Сушка",
                        "packing":"Упаковка","ready":"Готов","delivery":"Доставка",
                        "delivered":"Доставлен","cancelled":"Отменён",
                    }
                    text = raw.format(
                        order_num  = order.get("order_num", ""),
                        status     = STATUS_NAME_RU.get(status, status),
                        status_emoji = STATUS_EMOJI.get(status, ""),
                        client_name  = order.get("client_first_name", ""),
                        service      = order.get("service", ""),
                        branch       = order.get("branch", ""),
                        pickup_date  = str(order.get("pickup_date", "") or ""),
                        phone        = order.get("client_phone", ""),
                    )
                    async with aiohttp.ClientSession() as session:
                        await session.post(
                            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                            json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
                            timeout=aiohttp.ClientTimeout(total=8),
                        )
        except Exception as e:
            logging.warning(f"TG notify failed for order {order_id}: {e}")

    # ── Обновить кнопки в канале водителей ───────────────────────────────────
    try:
        branch, ch_msg_id = await db.get_channel_msg_for_order(order_id)
        logging.info(f"[channel_kb] order={order_id} status={status} branch={branch!r} msg_id={ch_msg_id}")
        if ch_msg_id and BOT_TOKEN:
            ch_key = "delivery_channel_navoi_id" if branch == "navoi" else "delivery_channel_zarafshan_id"
            ch_id_str = await _get_cfg(ch_key)
            logging.info(f"[channel_kb] ch_key={ch_key} ch_id={ch_id_str!r}")
            if ch_id_str:
                new_kb = _route_pickup_kb(order_id, status)
                async with aiohttp.ClientSession() as _sess:
                    resp = await _sess.post(
                        f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                        json={"chat_id": ch_id_str, "message_id": int(ch_msg_id),
                              "reply_markup": new_kb},
                        timeout=aiohttp.ClientTimeout(total=5))
                    resp_json = await resp.json()
                    if not resp_json.get("ok"):
                        logging.warning(f"[channel_kb] TG error: {resp_json}")
    except Exception as e:
        logging.warning(f"[channel_kb] failed order={order_id}: {e}", exc_info=True)

    return {"ok": True, "order": order}

@app.get("/api/admin/orders/{order_id}/items")
async def admin_get_order_items(order_id: int, _=Depends(get_current_staff)):
    items = await db.get_order_items(order_id)
    return {"ok": True, "items": items}

class OrderItemRequest(BaseModel):
    service: str
    sqm: float | None = None
    width_cm: float | None = None
    length_cm: float | None = None
    price_per_sqm: float = 0

@app.post("/api/admin/orders/{order_id}/items")
async def admin_create_order_item(order_id: int, req: OrderItemRequest, _=Depends(get_current_staff)):
    sqm = req.sqm
    if not sqm and req.width_cm and req.length_cm:
        sqm = round(req.width_cm * req.length_cm / 10000, 3)
    item = await db.create_order_item(
        order_id=order_id, service=req.service, sqm=sqm or 0,
        price_per_sqm=req.price_per_sqm,
        width_cm=req.width_cm, length_cm=req.length_cm)
    return {"ok": True, "item": item}

@app.post("/api/admin/orders/{order_id}/items/bulk")
async def admin_bulk_create_items(order_id: int, count: int = Body(..., embed=True),
                                   _=Depends(get_current_staff)):
    if count < 1 or count > 50:
        raise HTTPException(status_code=400, detail="Количество от 1 до 50")
    items = await db.create_empty_items(order_id, count)
    return {"ok": True, "items": items, "count": len(items)}

@app.put("/api/admin/orders/{order_id}/items/{item_id}")
async def admin_update_order_item(order_id: int, item_id: int,
                                   req: OrderItemRequest, _=Depends(get_current_staff)):
    sqm = req.sqm
    if not sqm and req.width_cm and req.length_cm:
        sqm = round(req.width_cm * req.length_cm / 10000, 3)
    updates = {"service": req.service, "price_per_sqm": req.price_per_sqm}
    if sqm: updates["sqm"] = sqm
    if req.width_cm: updates["width_cm"] = req.width_cm
    if req.length_cm: updates["length_cm"] = req.length_cm
    item = await db.update_order_item(item_id, **updates)
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True, "item": item}

@app.delete("/api/admin/orders/{order_id}/items/{item_id}")
async def admin_delete_order_item(order_id: int, item_id: int, _=Depends(get_current_staff)):
    ok = await db.delete_order_item(item_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True}

@app.get("/api/admin/orders/{order_id}/photos")
async def get_order_photos(order_id: int, _=Depends(get_current_staff)):
    photos = await db.get_order_photos(order_id)
    return {"ok": True, "photos": photos}

@app.post("/api/admin/orders/{order_id}/photos")
async def upload_order_photo(
    order_id: int,
    file: UploadFile = File(...),
    photo_type: str = Form("before"),
    note: str = Form(""),
    staff=Depends(get_current_staff),
):
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or ""
    if content_type.startswith("video/"):
        tg_method, tg_field, tg_type = "sendVideo",    "video",    "video"
    elif content_type.startswith("image/"):
        tg_method, tg_field, tg_type = "sendPhoto",    "photo",    "photo"
    else:
        tg_method, tg_field, tg_type = "sendDocument", "document", "document"

    # Получаем номер заказа для подписи
    order_row = await db.get_order_by_id(order_id)
    order_num = order_row.get("order_num", f"#{order_id}") if order_row else f"#{order_id}"
    type_labels = {"before": "До", "after": "После", "damage": "Повреждение"}
    type_label  = type_labels.get(photo_type, photo_type)
    staff_name  = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login","")
    caption = f"📷 {type_label}\n🧾 Заказ: {order_num}\n👤 {staff_name}"

    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename, content_type=content_type)
    form.add_field("caption", caption)

    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()

    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")

    msg = result["result"]
    if tg_type == "photo":
        file_id = msg["photo"][-1]["file_id"]
    else:
        file_id = msg[tg_type]["file_id"]

    name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login","")
    photo = await db.save_order_photo(order_id, file_id, tg_type, photo_type, note, name)
    return {"ok": True, "photo": photo}

@app.delete("/api/admin/orders/{order_id}/photos/{photo_id}")
async def delete_order_photo(order_id: int, photo_id: int, _=Depends(get_current_staff)):
    await db.delete_order_photo(photo_id)
    return {"ok": True}

# ── Платежи заказа ────────────────────────────────────────────────────────────

@app.get("/api/admin/orders/{order_id}/payments")
async def get_order_payments(order_id: int, _=Depends(get_current_staff)):
    rows = await db.get_order_payments(order_id)
    return {"ok": True, "payments": rows}

@app.post("/api/admin/orders/{order_id}/payments")
async def add_order_payment(
    order_id: int,
    amount:   float = Body(..., embed=False),
    method:   str   = Body(..., embed=False),
    purpose:  str   = Body("payment", embed=False),
    note:     str   = Body("", embed=False),
    staff=Depends(get_current_staff),
):
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.add_order_payment(order_id, amount, method, purpose, note, name, None, staff.get("id"))
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт"}
    details = f"{pLabel.get(purpose,purpose)}: {int(amount):,} сум ({mLabel.get(method,method)})"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_added", details)
    # Порядковый номер платежа в заказе
    pay_num = row.get("id", payment_id if 'payment_id' in dir() else "?")
    try:
        async with db.pool.acquire() as conn:
            pay_num = await conn.fetchval(
                "SELECT COUNT(*) FROM order_payments WHERE order_id=$1 AND id<=$2",
                order_id, row["id"]) or 1
    except Exception:
        pass
    # Уведомление в канал кассы
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"💰 <b>Новый платёж</b> · Заказ #{order_id} · №{pay_num}\n"
                f"{pLabel.get(purpose, purpose)} · {mLabel.get(method, method)}\n"
                f"<b>{int(amount):,} сум</b>\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🟢 Проверить", btn_cb=f"chk:g:{order_id}"))
    # Пуш-уведомление всем ответственным за кассу (только для карты/перевода)
    if method in ("card", "transfer"):
        cashiers = await db.get_all_cashiers_for_push()
        push_title = "💳 Оплата на проверку"
        push_body  = f"Заказ #{order_id} · {pLabel.get(purpose,purpose)} · {int(amount):,} сум · {name}"
        for c in cashiers:
            if c["id"] != staff.get("id"):
                asyncio.create_task(send_web_push(c["id"], push_title, push_body,
                                                  order_id=order_id, push_type="payment_review"))
    return {"ok": True, "payment": row}


@app.patch("/api/admin/orders/{order_id}/payments/{payment_id}")
async def edit_order_payment(
    order_id:   int,
    payment_id: int,
    amount:  float = Body(..., embed=True),
    method:  str   = Body(..., embed=True),
    purpose: str   = Body(..., embed=True),
    staff=Depends(get_current_staff),
):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    # Только создатель или admin
    if staff.get("sub") != "admin" and existing.get("created_by_staff_id") != staff.get("id"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.edit_order_payment(payment_id, amount, method, purpose)
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт"}
    details = f"Изменён платёж: {int(amount):,} сум ({mLabel.get(method,method)}, {pLabel.get(purpose,purpose)})"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_edited", details)
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"✏️ <b>Платёж изменён</b> · Заказ #{order_id}\n"
                f"{pLabel.get(purpose, purpose)} · {mLabel.get(method, method)}\n"
                f"<b>{int(amount):,} сум</b>\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🟢 Проверить", btn_cb=f"chk:g:{order_id}"))
    return {"ok": True, "payment": row}


@app.get("/api/admin/staff/cashiers")
async def get_cashiers(_=Depends(get_current_staff)):
    rows = await db.get_cashiers()
    return {"ok": True, "cashiers": rows}


@app.get("/api/admin/cash/balance")
async def get_cash_balance(_=Depends(_get_admin)):
    rows = await db.get_cash_balance()
    return {"ok": True, "balances": rows}


@app.get("/api/admin/cash/handovers")
async def list_cash_handovers(_=Depends(_get_admin)):
    rows = await db.get_cash_handovers()
    return {"ok": True, "handovers": rows}


@app.post("/api/admin/cash/handover")
async def create_cash_handover(
    from_staff_id: int   = Body(..., embed=True),
    to_staff_id:   int   = Body(..., embed=True),
    amount:        float = Body(..., embed=True),
    note:          str   = Body("", embed=True),
    _=Depends(_get_admin),
):
    row = await db.add_cash_handover(from_staff_id, to_staff_id, amount, note)
    return {"ok": True, "handover": row}


async def _set_tg_webhook():
    """Установить webhook Telegram при старте."""
    await asyncio.sleep(3)
    url = f"{APP_URL.rstrip('/')}/api/tg/webhook"
    try:
        async with aiohttp.ClientSession() as s:
            r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
                             json={"url": url}, timeout=aiohttp.ClientTimeout(total=10))
            body = await r.json()
            logging.info(f"setWebhook → {body.get('description','')}")
    except Exception as e:
        logging.warning(f"setWebhook error: {e}")


async def _send_tg_cash(chat_id, text: str, photo_bytes: bytes = None, filename: str = None,
                        phone: str = None, btn_label: str = None, btn_cb: str = None):
    """Отправить сообщение (или фото) в ТГ-канал кассы."""
    if not BOT_TOKEN or not chat_id:
        logging.warning(f"_send_tg_cash skip: BOT_TOKEN={bool(BOT_TOKEN)} chat_id={repr(chat_id)}")
        return
    phone_clean = (phone or "").strip()
    if phone_clean:
        text += f"\n📞 {phone_clean}"
    reply_markup = None
    if btn_label and btn_cb:
        reply_markup = {"inline_keyboard": [[{"text": btn_label, "callback_data": btn_cb}]]}
    try:
        async with aiohttp.ClientSession() as s:
            if photo_bytes:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(chat_id))
                form.add_field("photo", photo_bytes, filename=filename or "receipt.jpg", content_type="image/jpeg")
                form.add_field("caption", text)
                if reply_markup:
                    form.add_field("reply_markup", _json.dumps(reply_markup))
                r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto", data=form,
                                 timeout=aiohttp.ClientTimeout(total=10))
                logging.info(f"_send_tg_cash photo → {r.status}")
            else:
                payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
                if reply_markup:
                    payload["reply_markup"] = reply_markup
                r = await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                                 json=payload, timeout=aiohttp.ClientTimeout(total=5))
                body = await r.json()
                logging.info(f"_send_tg_cash msg → {r.status} {body.get('description','')}")
    except Exception as e:
        logging.warning(f"_send_tg_cash error: {e}")


@app.post("/api/tg/webhook")
async def tg_webhook(request: Request):
    """Единый обработчик всех callback-кнопок Telegram (take_lead, chk:, accept_, reject_)."""
    try:
        data = await request.json()
    except Exception:
        return {"ok": True}

    # ── Обычные сообщения (text / contact) ──────────────────────────
    msg = data.get("message") or data.get("edited_message")
    if msg:
        chat_id_msg  = msg.get("chat", {}).get("id")
        tg_uid_msg   = msg.get("from", {}).get("id")
        text_msg     = (msg.get("text") or "").strip()
        contact      = msg.get("contact")
        if text_msg == "/start" and chat_id_msg:
            await _tg_send_reply_keyboard(
                chat_id_msg,
                "👋 <b>Добро пожаловать в ARTEZ!</b>\n\n"
                "Нажмите кнопку ниже, чтобы привязать ваш номер телефона.\n"
                "После этого при регистрации на сайте вы сможете получить код подтверждения через Telegram."
            )
            return {"ok": True}
        if contact and tg_uid_msg and chat_id_msg:
            phone_raw = contact.get("phone_number", "")
            phone = phone_raw if phone_raw.startswith("+") else "+" + phone_raw
            owner_tg = contact.get("user_id")
            if owner_tg and int(owner_tg) != int(tg_uid_msg):
                await _tg_remove_keyboard(chat_id_msg, "❌ Пожалуйста, поделитесь <b>своим</b> номером.")
                return {"ok": True}
            await db.save_tg_phone_link(phone, int(tg_uid_msg))
            await _tg_remove_keyboard(
                chat_id_msg,
                f"✅ <b>Номер привязан!</b>\n\n"
                f"📱 <code>{phone}</code>\n\n"
                f"Теперь при регистрации на сайте ARTEZ вы можете выбрать "
                f"«Получить код через Telegram»."
            )
            return {"ok": True}

    cq = data.get("callback_query")
    if not cq:
        return {"ok": True}
    cq_id      = cq["id"]
    cb_data    = cq.get("data", "")
    msg        = cq.get("message", {})
    chat_id    = msg.get("chat", {}).get("id")
    msg_id     = msg.get("message_id")
    orig_text  = msg.get("text", "")
    tg_user_id = cq["from"]["id"]
    uname      = cq["from"].get("username")
    fname      = cq["from"].get("first_name", "")
    lname      = cq["from"].get("last_name", "")
    display    = f"@{uname}" if uname else " ".join(filter(None, [fname, lname])) or "кто-то"

    # ── Взять лид ─────────────────────────────────────────────────
    if cb_data.startswith("take_lead_"):
        try:
            lead_id = int(cb_data.split("_")[2])

            if not db.pool:
                await _tg_answer_callback(cq_id, "❌ Ошибка базы данных", alert=True)
                return {"ok": True}

            staff = await db.get_staff_by_tg_id(tg_user_id)
            if not staff:
                await _tg_answer_callback(cq_id,
                    "❌ Ваш Telegram не привязан к аккаунту сотрудника ARTEZ.\n"
                    "Обратитесь к администратору.", alert=True)
                return {"ok": True}
            if staff.get("role") == "agent":
                await _tg_answer_callback(cq_id,
                    "❌ Агенты не могут брать лиды через Telegram.", alert=True)
                return {"ok": True}

            staff_id   = staff["id"]
            staff_name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or staff.get("login","")
            took_verb  = "Взяла" if staff.get("gender") == "F" else "Взял"

            async with db.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT assigned_to, lead_code FROM leads WHERE id=$1", lead_id)
                if not row:
                    await _tg_answer_callback(cq_id, "❌ Лид не найден", alert=True)
                    return {"ok": True}

                if row["assigned_to"] and row["assigned_to"] != staff_id:
                    taker = await db.get_staff_by_id(row["assigned_to"])
                    taker_name = ""
                    taker_verb = "Взяла" if taker and taker.get("gender") == "F" else "Взял"
                    if taker:
                        taker_name = f"{taker.get('first_name','')} {taker.get('last_name','')}".strip()
                    await _tg_answer_callback(cq_id,
                        f"❌ Лид уже взят: {taker_name or 'другой сотрудник'}", alert=True)
                    new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {taker_verb}: {taker_name or 'другой сотрудник'}"
                    await _tg_edit_message(chat_id, msg_id, new_text)
                    return {"ok": True}

                if row["assigned_to"] == staff_id:
                    await _tg_answer_callback(cq_id, "✅ Этот лид уже ваш!")
                    return {"ok": True}

                await conn.execute(
                    "UPDATE leads SET assigned_to=$1 WHERE id=$2", staff_id, lead_id)

            await db.add_lead_call(lead_id, staff_id, action="note",
                                   note=f"Лид взят через Telegram: {staff_name}")
            await _tg_answer_callback(cq_id, "✅ Лид взят! Откройте приложение.")
            new_text = orig_text.rstrip("━━━━━━━━━━").rstrip() + f"\n━━━━━━━━━━\n✅ {took_verb}: {staff_name}"
            await _tg_edit_message(chat_id, msg_id, new_text)

        except Exception as e:
            logging.warning(f"take_lead handler error: {e}")
            try:
                await _tg_answer_callback(cq_id, "❌ Ошибка сервера. Попробуйте ещё раз.", alert=True)
            except Exception:
                pass
        return {"ok": True}

    # ── Маршрут: забор/сдача (rp:) ────────────────────────────────
    if cb_data.startswith("rp:"):
        try:
            parts   = cb_data.split(":")
            order_id = int(parts[1])
            action   = parts[2]  # take | undo | deliver

            order_row = await db.get_order_by_id(order_id)
            if not order_row:
                await _tg_answer_callback(cq_id, "❌ Заказ не найден", alert=True)
                return {"ok": True}

            cur_status = order_row.get("status", "")

            if action == "take":
                if cur_status != "confirmed":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "pickup"
                toast = "✅ Забрал — статус: Вывоз"

            elif action == "undo":
                if cur_status != "pickup":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "confirmed"
                toast = "↩️ Отменено — статус: Подтверждён"

            elif action == "deliver":
                if cur_status != "pickup":
                    await _tg_answer_callback(cq_id,
                        f"ℹ️ Статус уже: {_ORDER_STATUS_RU.get(cur_status, cur_status)}", alert=False)
                    return {"ok": True}
                new_status = "received"
                toast = "🏭 Сдан в мастерскую"

            else:
                return {"ok": True}

            await db.update_order_status(order_id, new_status)

            # Обновить клавиатуру сообщения
            new_kb = _route_pickup_kb(order_id, new_status)
            # Обновить текст: поменять строку статуса
            new_text = orig_text
            for old_s, new_s in _ORDER_STATUS_RU.items():
                new_text = new_text.replace(f"📌 Статус: {_ORDER_STATUS_RU[old_s]}", f"📌 Статус: {_ORDER_STATUS_RU.get(new_status, new_status)}")
            # Простая замена последней строки статуса
            lines_t = orig_text.rsplit("📌 Статус:", 1)
            if len(lines_t) == 2:
                new_text = lines_t[0] + "📌 Статус: " + _ORDER_STATUS_RU.get(new_status, new_status)

            await _edit_tg_with_kb(chat_id, msg_id, new_text, new_kb)
            await _tg_answer_callback(cq_id, toast)

        except Exception as e:
            logging.warning(f"rp: callback error: {e}")
            await _tg_answer_callback(cq_id, "❌ Ошибка сервера", alert=True)
        return {"ok": True}

    # ── Проверка оплаты (chk:) ─────────────────────────────────────
    if cb_data.startswith("chk:"):
        parts  = cb_data.split(":")
        color  = parts[1] if len(parts) > 1 else "g"
        icon   = "🟢" if color == "g" else "🔴"
        new_kb = {"inline_keyboard": [[{"text": f"{icon} ✅ Проверено · {display}", "callback_data": "done"}]]}
        try:
            async with aiohttp.ClientSession() as s:
                await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/editMessageReplyMarkup",
                             json={"chat_id": chat_id, "message_id": msg_id, "reply_markup": new_kb},
                             timeout=aiohttp.ClientTimeout(total=5))
                await s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/answerCallbackQuery",
                             json={"callback_query_id": cq_id, "text": "✅ Отмечено"},
                             timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            logging.warning(f"webhook callback error: {e}")

    return {"ok": True}


# ── Передача наличных (staff → ответственный) ─────────────────────────────────

@app.post("/api/admin/cash/staff-handover")
async def staff_cash_handover(
    to_staff_id: int   = Body(..., embed=True),
    amount:      float = Body(..., embed=True),
    note:        str   = Body("", embed=True),
    staff=Depends(get_current_staff),
):
    from_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.add_cash_handover(staff["id"], to_staff_id, amount, note)
    handover_id = row["id"]

    # Пуш получателю
    asyncio.create_task(send_web_push(
        to_staff_id,
        title="💵 Сдача наличных",
        body=f"{from_name} сдаёт {int(amount):,} сум",
        extra={"type": "cash_handover", "handover_id": handover_id},
    ))

    # ТГ канал кассы
    ch = await db.get_cash_tg_channel()
    to_staff = await db.get_staff_by_id(to_staff_id)
    to_name = " ".join(filter(None,[to_staff.get("last_name",""),to_staff.get("first_name","")])).strip() if to_staff else f"#{to_staff_id}"
    text = (f"💵 <b>Передача наличных</b>\n"
            f"От: {from_name}\n"
            f"Кому: {to_name}\n"
            f"Сумма: <b>{int(amount):,} сум</b>"
            + (f"\nПримечание: {note}" if note else ""))
    asyncio.create_task(_send_tg_cash(ch, text))

    return {"ok": True, "handover": row}


@app.post("/api/admin/cash/staff-handover/{handover_id}/confirm")
async def confirm_staff_handover(handover_id: int, staff=Depends(get_current_staff)):
    row = await db.confirm_cash_handover(handover_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Не найдено")
    confirmer_name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    amount = int(float(row.get("amount", 0)))

    # Пуш отправителю
    asyncio.create_task(send_web_push(
        row["from_staff_id"],
        title="✅ Наличные получены",
        body=f"{confirmer_name} подтвердил получение {amount:,} сум",
        extra={"type": "cash_confirmed", "handover_id": handover_id},
    ))

    # ТГ канал кассы
    ch = await db.get_cash_tg_channel()
    text = f"✅ <b>Наличные получены</b>\nПолучил: {confirmer_name}\nСумма: <b>{amount:,} сум</b>"
    asyncio.create_task(_send_tg_cash(ch, text))

    return {"ok": True, "handover": row}


@app.get("/api/admin/cash/pending-handovers")
async def get_pending_handovers(staff=Depends(get_current_staff)):
    rows = await db.get_pending_handovers_for(staff["id"])
    return {"ok": True, "handovers": rows}


# ── Подтверждение оплат картой/переводом ──────────────────────────────────────

@app.get("/api/admin/cash/unconfirmed-payments")
async def get_unconfirmed_payments(_=Depends(get_current_staff)):
    rows = await db.get_unconfirmed_payments()
    return {"ok": True, "payments": rows}


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/confirm")
async def confirm_payment(order_id: int, payment_id: int, staff=Depends(get_current_staff)):
    if staff.get("sub") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.confirm_payment(payment_id, staff["id"])
    if not row:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    details = f"Подтверждён платёж: {int(float(row['amount'])):,} сум ({mLabel.get(row['method'],'')})"
    await db.add_order_activity(order_id, staff["id"], name, "payment_confirmed", details)
    ch = await db.get_cash_tg_channel()
    text = (f"✅ <b>Платёж подтверждён</b> #{order_id}\n"
            f"{mLabel.get(row['method'],'')} · <b>{int(float(row['amount'])):,} сум</b>\n"
            f"Подтвердил: {name}")
    asyncio.create_task(_send_tg_cash(ch, text))
    return {"ok": True, "payment": row}


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/reject")
async def reject_payment(order_id: int, payment_id: int,
                         note: str = Body("", embed=True),
                         staff=Depends(get_current_staff)):
    if staff.get("sub") != "admin" and not staff.get("can_manage_cash"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    row = await db.reject_payment(payment_id, staff["id"], note)
    if not row:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    details = f"Платёж отклонён: {int(float(row['amount'])):,} сум ({mLabel.get(row['method'],'')})"
    await db.add_order_activity(order_id, staff["id"], name, "payment_rejected", details)
    ch = await db.get_cash_tg_channel()
    text = (f"❌ <b>Платёж отклонён</b> · Заказ #{order_id}\n"
            f"{mLabel.get(row['method'],'')} · <b>{int(float(row['amount'])):,} сум</b>\n"
            f"Отклонил: {name}"
            + (f"\n📝 {note}" if note else ""))
    asyncio.create_task(_send_tg_cash(ch, text))
    return {"ok": True, "payment": row}


@app.get("/api/admin/orders/{order_id}/payments/{payment_id}/receipt-file")
async def get_receipt_file(order_id: int, payment_id: int, staff=Depends(get_current_staff)):
    """Возвращает URL для просмотра чека через TG."""
    if not db.pool:
        raise HTTPException(status_code=503)
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT receipt_url FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not row or not row["receipt_url"]:
        raise HTTPException(status_code=404, detail="Чек не найден")
    file_id = row["receipt_url"]
    if not BOT_TOKEN:
        raise HTTPException(status_code=503, detail="Бот не настроен")
    try:
        from fastapi.responses import StreamingResponse
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile",
                             params={"file_id": file_id},
                             timeout=aiohttp.ClientTimeout(total=10)) as r:
                data = await r.json()
            if not data.get("ok"):
                raise HTTPException(status_code=404, detail="Файл не найден в TG")
            file_path = data["result"]["file_path"]
            file_url  = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            # Проксируем содержимое — браузер не может напрямую читать TG-файлы (CORS)
            ext = file_path.rsplit(".", 1)[-1].lower() if "." in file_path else ""
            ctype = ("video/mp4" if ext in ("mp4","mov","avi") else
                     "image/jpeg" if ext in ("jpg","jpeg") else
                     "image/png"  if ext == "png" else
                     "application/octet-stream")
            async with s.get(file_url, timeout=aiohttp.ClientTimeout(total=30)) as fr:
                content = await fr.read()
        return StreamingResponse(iter([content]), media_type=ctype,
                                 headers={"Content-Disposition": "inline"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/orders/{order_id}/payments/{payment_id}/receipt")
async def upload_payment_receipt(
    order_id:   int,
    payment_id: int,
    file: UploadFile = File(...),
    staff=Depends(get_current_staff),
):
    content = await file.read()
    ct = file.content_type or "image/jpeg"
    tg_method = "sendDocument" if not ct.startswith("image/") else "sendPhoto"
    field     = "document" if tg_method == "sendDocument" else "photo"
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")

    # Порядковый номер платежа внутри заказа
    pay_num = 1
    if db.pool:
        try:
            async with db.pool.acquire() as conn:
                pay_num = await conn.fetchval(
                    "SELECT COUNT(*) FROM order_payments WHERE order_id=$1 AND id<=$2",
                    order_id, payment_id) or 1
        except Exception:
            pass

    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    pLabel = {"prepayment":"Предоплата","partial":"Частичная оплата","final":"Окончательный расчёт"}
    # Получаем данные платежа для caption
    pay_row = None
    if db.pool:
        try:
            async with db.pool.acquire() as conn:
                pay_row = await conn.fetchrow("SELECT amount, method, purpose FROM order_payments WHERE id=$1", payment_id)
        except Exception:
            pass

    amount_str = f"{int(float(pay_row['amount'])):,} сум" if pay_row else ""
    method_str = mLabel.get(pay_row['method'], '') if pay_row else ""
    purpose_str = pLabel.get(pay_row['purpose'], '') if pay_row else ""
    caption = (f"🧾 Чек · Заказ #{order_id} · Платёж №{pay_num}\n"
               f"{purpose_str} · {method_str}\n"
               f"💰 {amount_str}\n"
               f"👤 {name}")

    reply_markup = _json.dumps({
        "inline_keyboard": [[{"text": "🟢 Проверить", "callback_data": f"chk:g:{order_id}"}]]
    })

    receipt_url = None
    cash_ch = await db.get_cash_tg_channel()
    upload_ch = cash_ch or await _get_media_channel()
    if BOT_TOKEN and upload_ch:
        try:
            async with aiohttp.ClientSession() as s:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(upload_ch))
                form.add_field(field, content, filename=file.filename or "receipt.jpg", content_type=ct)
                form.add_field("caption", caption)
                form.add_field("reply_markup", reply_markup)
                async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form,
                                  timeout=aiohttp.ClientTimeout(total=15)) as r:
                    res = await r.json()
                if res.get("ok"):
                    msg = res["result"]
                    receipt_url = msg["photo"][-1]["file_id"] if tg_method == "sendPhoto" else msg["document"]["file_id"]
        except Exception as e:
            logging.warning(f"receipt upload error: {e}")

    row = await db.save_payment_receipt(payment_id, receipt_url or file.filename)
    return {"ok": True, "receipt_url": receipt_url}


# ── Plans (roadmap) ─────────────────────────────────────────────────────────

class PlanBody(BaseModel):
    title: str
    description: str = ""
    priority: str = "normal"

class PlanUpdateBody(BaseModel):
    title: str | None = None
    description: str | None = None
    priority: str | None = None
    status: str | None = None

@app.get("/api/admin/plans")
async def list_plans(_=Depends(_get_admin)):
    return {"plans": await db.get_plans()}

@app.post("/api/admin/plans")
async def create_plan(body: PlanBody, _=Depends(_get_admin)):
    plan = await db.create_plan(body.title, body.description, body.priority)
    return {"ok": True, "plan": plan}

@app.put("/api/admin/plans/{plan_id}")
async def update_plan(plan_id: int, body: PlanUpdateBody, _=Depends(_get_admin)):
    from datetime import datetime, timezone
    kwargs = {k: v for k, v in body.model_dump().items() if v is not None}
    if kwargs.get("status") == "done" and "done_at" not in kwargs:
        kwargs["done_at"] = datetime.now(timezone.utc)
    if kwargs.get("status") == "pending":
        kwargs["done_at"] = None
    plan = await db.update_plan(plan_id, **kwargs)
    return {"ok": True, "plan": plan}

@app.delete("/api/admin/plans/{plan_id}")
async def delete_plan(plan_id: int, _=Depends(_get_admin)):
    await db.delete_plan(plan_id)
    return {"ok": True}

# ── TEMP: чистка БД от мусора (удалить группу после чистки) ─────────────────
_TEMP_TABLES = {
    'order_payments':   'Оплаты',
    'order_items':      'Позиции заказов',
    'order_activity':   'Активность / статусы',
    'order_photos':     'Фото заказов',
    'order_item_media': 'Медиа позиций',
}

class _TempDeleteBody(BaseModel):
    ids: list[int]

@app.get("/api/admin/temp/orphan-counts")
async def temp_orphan_counts(_=Depends(_get_admin)):
    if not db.pool: raise HTTPException(503)
    result = {}
    async with db.pool.acquire() as conn:
        for tbl in _TEMP_TABLES:
            result[tbl] = int(await conn.fetchval(
                f"SELECT COUNT(*) FROM {tbl} t LEFT JOIN orders o ON o.id=t.order_id WHERE o.id IS NULL"
            ))
    return result

@app.get("/api/admin/temp/records/{table}")
async def temp_get_records(table: str, orphans_only: bool = True, _=Depends(_get_admin)):
    if table not in _TEMP_TABLES: raise HTTPException(404, "Unknown table")
    if not db.pool: raise HTTPException(503)
    where = "WHERE o.id IS NULL" if orphans_only else ""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT t.*, (o.id IS NULL) AS is_orphan
            FROM {table} t
            LEFT JOIN orders o ON o.id = t.order_id
            {where}
            ORDER BY t.id DESC LIMIT 1000
        """)
    return {"rows": [dict(r) for r in rows], "table": table, "label": _TEMP_TABLES[table]}

@app.delete("/api/admin/temp/records/{table}")
async def temp_delete_records(table: str, body: _TempDeleteBody, _=Depends(_get_admin)):
    if table not in _TEMP_TABLES: raise HTTPException(404, "Unknown table")
    if not body.ids: return {"ok": True, "deleted": 0}
    if not db.pool: raise HTTPException(503)
    async with db.pool.acquire() as conn:
        result = await conn.execute(f"DELETE FROM {table} WHERE id = ANY($1::int[])", body.ids)
    deleted = int(result.split()[-1]) if result else 0
    return {"ok": True, "deleted": deleted}

# ── История чатов (admin) ─────────────────────────────────────────────────────

class _ChatHistoryDeleteBody(BaseModel):
    date_from: str   # YYYY-MM-DD
    date_to:   str   # YYYY-MM-DD

@app.get("/api/admin/chat/history/stats")
async def chat_history_stats(date_from: str, date_to: str, _=Depends(_get_admin)):
    if not db.pool: raise HTTPException(503)
    try:
        df = datetime.strptime(date_from, "%Y-%m-%d").date()
        dt = datetime.strptime(date_to,   "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "date_from / date_to must be YYYY-MM-DD")
    async with db.pool.acquire() as conn:
        sessions = int(await conn.fetchval(
            "SELECT COUNT(*) FROM chat_sessions WHERE status='closed' AND created_at::date BETWEEN $1 AND $2",
            df, dt
        ))
        messages = int(await conn.fetchval(
            """SELECT COUNT(*) FROM chat_messages cm
               JOIN chat_sessions cs ON cs.id = cm.session_id
               WHERE cs.status='closed' AND cs.created_at::date BETWEEN $1 AND $2""",
            df, dt
        ))
    return {"ok": True, "sessions": sessions, "messages": messages, "date_from": date_from, "date_to": date_to}

@app.delete("/api/admin/chat/history")
async def chat_history_delete(body: _ChatHistoryDeleteBody, _=Depends(_get_admin)):
    if not db.pool: raise HTTPException(503)
    try:
        df = datetime.strptime(body.date_from, "%Y-%m-%d").date()
        dt = datetime.strptime(body.date_to,   "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "date_from / date_to must be YYYY-MM-DD")
    async with db.pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM chat_sessions WHERE status='closed' AND created_at::date BETWEEN $1 AND $2",
            df, dt
        )
    deleted = int(result.split()[-1]) if result else 0
    return {"ok": True, "deleted_sessions": deleted}

# ── Настройки кассы (admin) ───────────────────────────────────────────────────

@app.get("/api/admin/settings/cash-channel")
async def get_cash_channel(_=Depends(_get_admin)):
    ch = await db.get_cash_tg_channel()
    return {"ok": True, "cash_tg_channel_id": ch}

@app.put("/api/admin/settings/cash-channel")
async def set_cash_channel(cash_tg_channel_id: str = Body(..., embed=True), _=Depends(_get_admin)):
    if not db.pool: raise HTTPException(503)
    await _upsert_setting("cash_tg_channel_id", cash_tg_channel_id)
    return {"ok": True}

@app.get("/api/admin/settings/media-channel")
async def get_media_channel(_=Depends(_get_admin)):
    ch = await db.get_media_channel_id()
    return {"ok": True, "media_channel_id": ch or MEDIA_CHANNEL_ID}

@app.put("/api/admin/settings/media-channel")
async def set_media_channel(media_channel_id: str = Body(..., embed=True), _=Depends(_get_admin)):
    if not db.pool: raise HTTPException(503)
    await _upsert_setting("media_channel_id", media_channel_id)
    return {"ok": True}

async def _upsert_setting(col: str, val: str):
    """Обновить настройку — гарантирует наличие строки в settings."""
    async with db.pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM settings")
        if count == 0:
            await conn.execute(f"INSERT INTO settings({col}) VALUES($1)", val)
        else:
            await conn.execute(f"UPDATE settings SET {col}=$1", val)
    logging.info(f"_upsert_setting {col}={repr(val)}")


@app.get("/api/admin/cash/my-balance")
async def get_my_cash_balance(staff=Depends(get_current_staff)):
    """Баланс наличных текущего сотрудника."""
    bal = await db.get_my_cash_balance(staff["id"])
    return {"ok": True, **bal}

@app.get("/api/admin/cash/debug")
async def cash_debug(staff=Depends(get_current_staff)):
    """Диагностика: что в БД по наличным для текущего сотрудника."""
    if not db.pool: return {"ok": False}
    sid = staff["id"]
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, order_id, amount, method, purpose, created_by, created_by_staff_id, handed_to_staff_id, created_at FROM order_payments WHERE created_by_staff_id=$1 OR handed_to_staff_id=$1 ORDER BY created_at DESC LIMIT 20",
            sid)
        name_rows = await conn.fetch(
            "SELECT id, order_id, amount, method, purpose, created_by, created_by_staff_id, handed_to_staff_id FROM order_payments WHERE method='cash' AND created_by_staff_id IS NULL ORDER BY created_at DESC LIMIT 10")
        staff_row = await conn.fetchrow("SELECT id, first_name, last_name, login FROM staff WHERE id=$1", sid)
    return {
        "staff_id": sid,
        "staff_name": f"{staff_row['last_name'] or ''} {staff_row['first_name'] or ''}".strip() if staff_row else None,
        "payments_by_id": [dict(r) for r in rows],
        "recent_null_staff_cash": [dict(r) for r in name_rows],
    }

@app.get("/api/admin/cash/my-payments")
async def get_my_cash_payments(staff=Depends(get_current_staff)):
    """Наличные платежи где текущий сотрудник создал платёж или указан получателем."""
    if not db.pool: return {"ok": True, "payments": []}
    my_id = staff["id"]
    s = await db.get_staff_by_id(my_id)
    my_name = " ".join(filter(None, [s.get("last_name",""), s.get("first_name","")])).strip() if s else ""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*, o.client_first_name AS client_name
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            WHERE p.method='cash'
              AND (p.created_by_staff_id=$1
                   OR p.handed_to_staff_id=$1
                   OR (p.created_by_staff_id IS NULL AND p.created_by=$2))
            ORDER BY p.created_at DESC LIMIT 100
        """, my_id, my_name)
        return {"ok": True, "payments": [dict(r) for r in rows]}


@app.delete("/api/admin/orders/{order_id}/payments/{payment_id}")
async def delete_order_payment(
    order_id:   int,
    payment_id: int,
    reason: str = Body("", embed=True),
    staff=Depends(get_current_staff),
):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1 AND order_id=$2", payment_id, order_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Платёж не найден")
    if staff.get("sub") != "admin" and existing.get("created_by_staff_id") != staff.get("id"):
        raise HTTPException(status_code=403, detail="Нет доступа")
    deleted = await db.delete_order_payment(payment_id)
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    mLabel = {"cash":"💵 Нал","card":"💳 Карта","transfer":"📲 Перевод"}
    amt = int(float(deleted.get('amount', 0)))
    mth = deleted.get('method', '')
    details = f"Удалён платёж: {amt:,} сум ({mLabel.get(mth,'')}) — Причина: {reason or '—'}"
    await db.add_order_activity(order_id, staff.get("id"), name, "payment_deleted", details)
    ch = await db.get_cash_tg_channel()
    if ch:
        phone = staff.get("phone") or ""
        text = (f"🗑 <b>Платёж удалён</b> · Заказ #{order_id}\n"
                f"{mLabel.get(mth, mth)} · <b>{amt:,} сум</b>\n"
                f"Причина: {reason or '—'}\n"
                f"👤 {name}")
        asyncio.create_task(_send_tg_cash(ch, text, phone=phone,
                                          btn_label="🔴 Проверить", btn_cb=f"chk:r:{order_id}"))
    return {"ok": True}


@app.get("/api/admin/orders/{order_id}/activity")
async def get_order_activity(order_id: int, _=Depends(get_current_staff)):
    rows = await db.get_order_activity(order_id)
    return {"ok": True, "activity": rows}

# ── Касса ─────────────────────────────────────────────────────────────────────

@app.get("/api/admin/cash/summary")
async def cash_summary(
    date_from: str = None,
    date_to:   str = None,
    _=Depends(get_current_staff),
):
    from datetime import date
    today = date.today().isoformat()
    data = await db.get_cash_summary(date_from or today, date_to or today)
    return {"ok": True, **data}

@app.post("/api/admin/cash/close-shift")
async def close_shift(
    shift_date: str  = Body(None, embed=False),
    note:       str  = Body("",  embed=False),
    staff=Depends(get_current_staff),
):
    from datetime import date
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.close_cash_shift(shift_date or date.today().isoformat(), name, note)
    return {"ok": True, "shift": row}

@app.get("/api/admin/cash/shifts")
async def get_shifts(_=Depends(get_current_staff)):
    rows = await db.get_cash_shifts()
    return {"ok": True, "shifts": rows}

@app.get("/api/media/{photo_id}")
async def serve_order_photo(
    photo_id: int,
    t: str = None,
    authorization: str = Header(None),
):
    token = t or (authorization[7:] if authorization and authorization.startswith("Bearer ") else None)
    if not token:
        raise HTTPException(status_code=401)
    try:
        jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401)

    row = await db.get_photo_by_id(photo_id)
    if not row:
        raise HTTPException(status_code=404)

    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={row['tg_file_id']}") as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=502, detail="Не удалось получить файл")

    from fastapi.responses import RedirectResponse
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{data['result']['file_path']}"
    return RedirectResponse(url=file_url)

@app.get("/api/item-media/{media_id}")
async def serve_item_media(
    media_id: int,
    t: str = None,
    authorization: str = Header(None),
):
    token = t or (authorization[7:] if authorization and authorization.startswith("Bearer ") else None)
    if not token:
        raise HTTPException(status_code=401)
    try:
        jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except Exception:
        raise HTTPException(status_code=401)

    row = await db.get_item_media_by_id(media_id)
    if not row:
        raise HTTPException(status_code=404)

    async with aiohttp.ClientSession() as s:
        async with s.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={row['tg_file_id']}") as r:
            data = await r.json()
    if not data.get("ok"):
        raise HTTPException(status_code=502, detail="Не удалось получить файл")

    from fastapi.responses import RedirectResponse
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{data['result']['file_path']}"
    return RedirectResponse(url=file_url)

@app.patch("/api/admin/orders/{order_id}/discount")
async def admin_set_order_discount(order_id: int, staff=Depends(get_current_staff),
                                    discount_sum: float = Body(0, embed=True)):
    role = staff.get("role", "")
    if role not in ("admin", "manager") and "status" not in ROLE_PERMISSIONS.get(role, []):
        raise HTTPException(status_code=403, detail="Нет прав")
    order = await db.update_order_discount(order_id, discount_sum)
    if not order:
        raise HTTPException(status_code=404, detail="Заказ не найден")
    return {"ok": True, "order": order}

@app.patch("/api/admin/orders/{order_id}/items/{item_id}/measure")
async def admin_measure_item(order_id: int, item_id: int, staff=Depends(get_current_staff),
                              action: str = Body(..., embed=True),
                              actual_width_cm: float = Body(None, embed=True),
                              actual_length_cm: float = Body(None, embed=True),
                              note: str = Body("", embed=True)):
    if action == "submit":
        if not actual_width_cm or not actual_length_cm:
            raise HTTPException(status_code=400, detail="Укажите ширину и длину")
        await db.save_measure_dims(item_id, actual_width_cm, actual_length_cm)
        media = await db.get_item_media(item_id)
        if not media:
            raise HTTPException(status_code=400, detail="Добавьте фото или видео замера")
        item = await db.submit_item_measure(item_id)
        if not item:
            raise HTTPException(status_code=400, detail="Ошибка при отправке на проверку")
        # Push всем кто может проверять замеры
        try:
            approvers = await db.get_all_approvers()
            order_row = await db.get_order_by_id(order_id)
            order_num = (order_row or {}).get("order_num") or f"#{order_id}"
            svc       = item.get("service") or "позиция"
            for ap in approvers:
                asyncio.create_task(send_web_push(
                    ap["id"],
                    f"📐 Новый замер — {order_num}",
                    f"Замер «{svc}» ожидает вашего утверждения",
                    order_id=order_id, item_id=item_id, push_type="measure"
                ))
        except Exception as _pe:
            logging.warning(f"measure push error: {_pe}")
    elif action == "approve":
        item = await db.approve_item_measure(item_id)
        try:
            washer_login = item.get("washer_login")
            if washer_login:
                washer = await db.get_staff_by_login(washer_login)
                if washer:
                    order_row = await db.get_order_by_id(order_id)
                    order_num = (order_row or {}).get("order_num") or f"#{order_id}"
                    svc       = item.get("service") or "позиция"
                    push_body = f"«{svc}» — замер принят. Отличная работа!"
                    asyncio.create_task(send_web_push(
                        washer["id"],
                        f"✅ Замер утверждён — {order_num}",
                        push_body,
                        order_id=order_id, item_id=item_id, push_type="measure_approved"
                    ))
                    await db.create_washer_notification(
                        washer["id"], order_id, order_num, push_body,
                        item_id=item_id, notification_type="measure_approved"
                    )
        except Exception as _pe:
            logging.warning(f"measure approved push error: {_pe}")
    elif action == "reject":
        if not note:
            raise HTTPException(status_code=400, detail="Укажите причину отклонения")
        item = await db.reject_item_measure(item_id, note)
        try:
            washer_login = item.get("washer_login")
            if washer_login:
                washer = await db.get_staff_by_login(washer_login)
                if washer:
                    order_row = await db.get_order_by_id(order_id)
                    order_num = (order_row or {}).get("order_num") or f"#{order_id}"
                    svc = item.get("service") or "позиция"
                    push_body = f"«{svc}» — {note}"
                    asyncio.create_task(send_web_push(
                        washer["id"],
                        f"❌ Замер отклонён — {order_num}",
                        push_body,
                        order_id=order_id, item_id=item_id, push_type="measure_rejected"
                    ))
                    await db.create_washer_notification(
                        washer["id"], order_id, order_num, push_body,
                        item_id=item_id, notification_type="measure_rejected"
                    )
        except Exception as _pe:
            logging.warning(f"measure reject push error: {_pe}")
    else:
        raise HTTPException(status_code=400, detail="Неверное действие")
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True, "item": item}

@app.post("/api/admin/orders/{order_id}/items/{item_id}/measure/claim")
async def claim_measure_review(order_id: int, item_id: int, staff=Depends(get_current_staff)):
    if not staff.get("can_approve_measure"):
        raise HTTPException(status_code=403, detail="Нет прав для проверки замеров")
    item = await db.claim_measure_review(item_id, staff["id"])
    if not item:
        raise HTTPException(status_code=404, detail="Замер не найден или уже утверждён")
    return {"ok": True, "item": item}

@app.get("/api/staff/pending-payment-reviews")
async def get_pending_payment_reviews(staff=Depends(get_current_staff)):
    if not staff.get("can_manage_cash") and staff.get("sub") != "admin":
        return {"ok": True, "payments": []}
    payments = await db.get_unconfirmed_payments()
    return {"ok": True, "payments": payments}

@app.get("/api/staff/pending-position-requests")
async def get_pending_position_requests(staff=Depends(get_current_staff)):
    """Список заказов с активным (не принятым) запросом позиции — для поллинга."""
    if not db.pool:
        return {"ok": True, "order_ids": []}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM orders WHERE pos_request_pending=TRUE"
        )
    return {"ok": True, "order_ids": [r["id"] for r in rows]}

# ── Контакты филиалов (публичный GET) ────────────────────────────────────────
@app.get("/api/site-contacts")
async def get_site_contacts():
    if not db.pool:
        return {"ok": True, "contacts": []}
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM site_contacts ORDER BY branch")
    return {"ok": True, "contacts": [dict(r) for r in rows]}

# ── Обновление контактов (только админ) ──────────────────────────────────────
class SiteContactsIn(BaseModel):
    branch_name: str = ""
    phones:      list = []
    telegram:    str  = ""
    whatsapp:    str  = ""
    instagram:   str  = ""

@app.put("/api/admin/site-contacts/{branch}")
async def update_site_contacts(branch: str, data: SiteContactsIn, admin=Depends(get_admin)):
    if not db.pool:
        raise HTTPException(503)
    import json
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO site_contacts (branch, branch_name, phones, telegram, whatsapp, instagram)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6)
            ON CONFLICT (branch) DO UPDATE SET
                branch_name=$2, phones=$3::jsonb,
                telegram=$4, whatsapp=$5, instagram=$6
        """, branch, data.branch_name, json.dumps(data.phones, ensure_ascii=False),
             data.telegram, data.whatsapp, data.instagram)
    return {"ok": True}

@app.get("/api/staff/pending-reviews")
async def get_pending_reviews(staff=Depends(get_current_staff)):
    if not staff.get("can_approve_measure"):
        return {"ok": True, "reviews": []}
    reviews = await db.get_pending_measure_reviews()
    # Вернуть только те, которые не приняты другим сотрудником (или приняты мной)
    my_id = staff["id"]
    visible = [r for r in reviews if not r["review_claimed_by"] or r["review_claimed_by"] == my_id]
    return {"ok": True, "reviews": visible}

@app.post("/api/staff/orders/{order_id}/request-position")
async def request_position(
    order_id: int,
    note:  str = Body(..., embed=True),
    count: int = Body(1,  embed=True),
    staff=Depends(get_current_staff)
):
    """Мойщик просит добавить позицию — пуш всем с can_approve_measure."""
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    washer_name  = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "Мойщик")
    order_num    = order.get("order_number") or f"#{order_id}"
    items        = await db.get_order_items(order_id)
    current_cnt  = len(items)
    add_str      = f"+{count}" if count > 1 else "+1"
    title = f"📋 {order_num} — сейчас {current_cnt} поз., нужно {add_str}"
    body  = f"{washer_name}: {note}" if note else washer_name
    approvers = await db.get_all_approvers()
    for a in approvers:
        asyncio.create_task(send_web_push(
            a["id"], title, body,
            order_id=order_id, push_type="position_request"
        ))
    # Ставим флаг pending в БД
    if db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET pos_request_pending=TRUE, pos_request_at=NOW() WHERE id=$1",
                order_id
            )
    return {"ok": True}

@app.post("/api/staff/orders/{order_id}/claim-position-request")
async def claim_position_request(order_id: int, staff=Depends(get_current_staff)):
    """Менеджер принимает запрос — уведомляем остальных чтобы не дублировали."""
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    my_id       = staff["id"]
    order_num   = order.get("order_number") or f"#{order_id}"
    my_name     = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "")
    title = f"✅ {order_num} — принято"
    body  = f"{my_name} принял запрос на добавление позиции"
    # Сбрасываем флаг pending
    if db.pool:
        async with db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE orders SET pos_request_pending=FALSE WHERE id=$1", order_id
            )
    approvers = await db.get_all_approvers()
    for a in approvers:
        if a["id"] == my_id:
            continue
        asyncio.create_task(send_web_push(
            a["id"], title, body,
            order_id=order_id, push_type="position_claimed"
        ))
    return {"ok": True}

@app.post("/api/staff/orders/{order_id}/notify-washer")
async def notify_washer_new_item(
    order_id: int,
    washer_id: int = Body(None, embed=True),   # None = всем мойщикам
    item_id: int   = Body(None, embed=True),
    staff=Depends(get_current_staff),
):
    order = await db.get_order_by_id(order_id)
    if not order:
        raise HTTPException(404, "Заказ не найден")
    order_num   = order.get("order_num") or f"#{order_id}"
    items       = await db.get_order_items(order_id)
    item_count  = len(items)
    sender      = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login", "Менеджер")
    title = f"📋 Новая позиция — {order_num}"
    body  = f"Сейчас {item_count} поз. в заказе {order_num}. {sender} добавил позицию."

    if washer_id:
        target_ids = [washer_id]
    else:
        all_staff  = await db.get_all_staff()
        target_ids = [s["id"] for s in all_staff if s.get("role") == "washer" and s.get("active")]

    sent = 0
    no_sub = 0
    for wid in target_ids:
        await db.create_washer_notification(wid, order_id, order_num, body)
        subs = await db.get_push_subscriptions(wid)
        if subs:
            asyncio.create_task(send_web_push(wid, title, body, order_id=order_id, push_type="new_item"))
            sent += 1
        else:
            no_sub += 1
            logging.warning(f"notify_washer: no push sub for staff_id={wid}")

    return {"ok": True, "sent": sent, "no_subscription": no_sub}


@app.get("/api/staff/my-order-notifications")
async def get_my_order_notifications(staff=Depends(get_current_staff)):
    rows = await db.get_washer_notifications(staff["id"])
    return {"ok": True, "notifications": rows}

@app.get("/api/staff/my-order-notifications/unread-count")
async def get_order_notif_unread(staff=Depends(get_current_staff)):
    count = await db.count_unread_washer_notifications(staff["id"])
    return {"ok": True, "count": count}

@app.post("/api/staff/my-order-notifications/read")
async def mark_order_notifs_read(staff=Depends(get_current_staff)):
    await db.mark_washer_notifications_read(staff["id"])
    return {"ok": True}

@app.patch("/api/staff/my-order-notifications/{notif_id}/read")
async def mark_order_notif_read(notif_id: int, staff=Depends(get_current_staff)):
    await db.mark_washer_notification_read(notif_id, staff["id"])
    return {"ok": True}

@app.get("/api/admin/orders/{order_id}/items/{item_id}/media")
async def get_item_media(order_id: int, item_id: int, _=Depends(get_current_staff)):
    media = await db.get_item_media(item_id)
    return {"ok": True, "media": media}

@app.post("/api/admin/orders/{order_id}/items/{item_id}/media")
async def upload_item_media(
    order_id: int, item_id: int,
    file: UploadFile = File(...),
    staff=Depends(get_current_staff),
):
    media_ch = await _get_media_channel()
    if not BOT_TOKEN or not media_ch:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or ""
    if content_type.startswith("video/"):
        tg_method, tg_field, tg_type = "sendVideo", "video", "video"
    else:
        tg_method, tg_field, tg_type = "sendPhoto", "photo", "photo"

    order_row = await db.get_order_by_id(order_id)
    order_num = order_row.get("order_num", f"#{order_id}") if order_row else f"#{order_id}"
    staff_name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "")
    # Порядковый номер позиции внутри заказа (1-based)
    order_items = await db.get_order_items(order_id)
    item_ids = [i["id"] for i in order_items]
    item_pos = item_ids.index(item_id) + 1 if item_id in item_ids else item_id
    caption = f"📐 Замер\n🧾 Заказ: {order_num} | Позиция #{item_pos}\n👤 {staff_name}"

    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(media_ch))
    form.add_field(tg_field, file_bytes, filename=file.filename, content_type=content_type)
    form.add_field("caption", caption)

    async with aiohttp.ClientSession() as s:
        async with s.post(f"https://api.telegram.org/bot{BOT_TOKEN}/{tg_method}", data=form) as r:
            result = await r.json()

    if not result.get("ok"):
        raise HTTPException(status_code=502, detail=f"Telegram: {result.get('description','upload failed')}")

    msg = result["result"]
    file_id = msg["photo"][-1]["file_id"] if tg_type == "photo" else msg[tg_type]["file_id"]
    row = await db.add_item_media(item_id, order_id, file_id, tg_type, staff_name)
    return {"ok": True, "media": row}

@app.delete("/api/admin/orders/{order_id}/items/{item_id}/media/{media_id}")
async def delete_item_media(order_id: int, item_id: int, media_id: int, _=Depends(get_current_staff)):
    await db.delete_item_media(media_id)
    return {"ok": True}

@app.patch("/api/admin/orders/{order_id}/items/{item_id}/washer")
async def admin_set_item_washer(order_id: int, item_id: int, staff=Depends(get_current_staff),
                                 washer_login: str = Body("", embed=True)):
    item = await db.update_item_washer(item_id, washer_login or None)
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True, "item": item}

@app.patch("/api/admin/staff/{staff_id}/can-edit-items")
async def admin_set_can_edit_items(staff_id: int, _staff=Depends(_get_admin),
                                    can_edit_items: bool = Body(..., embed=True)):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE staff SET can_edit_items=$2 WHERE id=$1 RETURNING id, can_edit_items",
            staff_id, can_edit_items)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, **dict(row)}

@app.patch("/api/admin/staff/{staff_id}/permissions")
async def admin_set_staff_permissions(staff_id: int, _staff=Depends(_get_admin),
    can_edit_items:      bool = Body(True,  embed=True),
    can_measure:         bool = Body(False, embed=True),
    can_approve_measure: bool = Body(False, embed=True),
    can_create_order:    bool = Body(True,  embed=True),
    can_confirm_order:   bool = Body(True,  embed=True),
    can_edit_confirmed:  bool = Body(False, embed=True),
    can_send_pickup:     bool = Body(False, embed=True),
    can_edit_delivery:   bool = Body(False, embed=True),
    can_accept_payment:  bool = Body(False, embed=True),
    can_manage_cash:     bool = Body(False, embed=True),
    notify_new_users:    bool = Body(False, embed=True),
    order_stages:        str  = Body(None,  embed=True)):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE staff
               SET can_edit_items=$2, can_measure=$3, can_approve_measure=$4,
                   can_create_order=$5, can_confirm_order=$6, order_stages=$7,
                   can_edit_confirmed=$8, can_send_pickup=$9, can_edit_delivery=$10,
                   can_accept_payment=$11, can_manage_cash=$12, notify_new_users=$13
               WHERE id=$1
               RETURNING id, can_edit_items, can_measure, can_approve_measure,
                         can_create_order, can_confirm_order, order_stages,
                         can_edit_confirmed, can_send_pickup, can_edit_delivery,
                         can_accept_payment, can_manage_cash, notify_new_users""",
            staff_id, can_edit_items, can_measure, can_approve_measure,
            can_create_order, can_confirm_order, order_stages or None,
            can_edit_confirmed, can_send_pickup, can_edit_delivery,
            can_accept_payment, can_manage_cash, notify_new_users)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, **dict(row)}


OSAGO_DEFAULT = {"tier1": 200000, "tier2": 400000, "tier3": 700000,
                  "pct1": 5, "pct2": 10, "pct3": 20}

@app.get("/api/settings/osago")
async def get_osago_settings():
    import json
    raw = await db.get_config("osago_tiers")
    if raw:
        try:
            return {"ok": True, "tiers": json.loads(raw)}
        except Exception:
            pass
    return {"ok": True, "tiers": OSAGO_DEFAULT}


class OsagoSettings(BaseModel):
    tier1: int
    tier2: int
    tier3: int
    pct1: int
    pct2: int
    pct3: int

@app.put("/api/admin/settings/osago")
async def save_osago_settings(body: OsagoSettings, _=Depends(get_admin)):
    import json
    await db.set_config("osago_tiers", json.dumps(body.dict()))
    return {"ok": True}


# ── Скидка при самовывозе ─────────────────────────────────────
@app.get("/api/admin/settings/self-pickup-discount")
async def get_self_pickup_discount(_=Depends(get_admin)):
    val = await db.get_config("self_pickup_discount")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.get("/api/settings/self-pickup-discount")
async def get_self_pickup_discount_public():
    val = await db.get_config("self_pickup_discount")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.put("/api/admin/settings/self-pickup-discount")
async def save_self_pickup_discount(discount: float = Body(..., embed=True), _=Depends(get_admin)):
    await db.set_config("self_pickup_discount", str(discount))
    return {"ok": True, "discount": discount}

# ── Скидка при самовывозе (клиент забирает) ──────────────────
@app.get("/api/admin/settings/delivery-discount")
async def get_delivery_discount(_=Depends(get_admin)):
    val = await db.get_config("delivery_discount_pct")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.get("/api/settings/delivery-discount")
async def get_delivery_discount_public():
    val = await db.get_config("delivery_discount_pct")
    return {"ok": True, "discount": float(val) if val else 0.0}

@app.put("/api/admin/settings/delivery-discount")
async def save_delivery_discount(discount: float = Body(..., embed=True), _=Depends(get_admin)):
    await db.set_config("delivery_discount_pct", str(discount))
    return {"ok": True, "discount": discount}


# ── Настройки сайта ──────────────────────────────────────────
# Fallback: если в БД пусто — берём env-переменную, затем хардкод
SITE_SETTINGS_DEFAULTS = {
    # Соцсети
    "social_instagram":    "https://www.instagram.com/ziyoboboev/",
    "social_tg_bot":       "https://t.me/artez_orders_bot",
    "social_tg_group":     "https://t.me/artez_gilam_yuvish",
    # Контакты
    "contact_short":       "1221",
    "contact_main":        "+998792221221",
    "contact_zarafshan_1":         "+998882001221",
    "contact_zarafshan_2":         "",
    "contact_zarafshan_telegram":  "",
    "contact_zarafshan_admin_tg":  "",
    "contact_zarafshan_whatsapp":  "",
    "contact_zarafshan_instagram": "",
    "branch_zarafshan_location":   "",
    "contact_navoi_1":             "+998997500020",
    "contact_navoi_2":             "",
    "contact_navoi_telegram":      "",
    "contact_navoi_admin_tg":      "",
    "contact_navoi_whatsapp":      "",
    "contact_navoi_instagram":     "",
    "branch_navoi_location":       "",
    # Telegram бот — fallback из env
    "tg_bot_token":        BOT_TOKEN,
    "tg_group_id":         GROUP_ID,
    "tg_group_zarafshan":  GROUP_ID_ZARAFSHAN,
    "tg_group_navoi":      GROUP_ID_NAVOI,
    "tg_group_sms_id":     os.getenv("GROUP_SMS_ID", ""),
    # Яндекс Карты — fallback из env
    "yandex_maps_key":     os.getenv("YANDEX_MAPS_KEY", ""),
    # Eskiz SMS — fallback из env
    "eskiz_email":         ESKIZ_EMAIL,
    "eskiz_password":      ESKIZ_PASSWORD,
    "eskiz_from":          ESKIZ_FROM,
    "sms_text_register":   "Kod podtverzhdeniya dlya registracii na sayte ARTEZ.uz: {code}",
    "sms_text_login":      "Kod podtverzhdeniya dlya vhoda na sayt ARTEZ.uz: {code}",
    "sms_text_reset":      "Kod vosstanovleniya parolya dlya vhoda na sayt ARTEZ.uz: {code}",
    # ОСАГО партнёр
    "osago_partner_phone": "+998936121300",
    "osago_partner_promo": "ARTEZ",
    # Google Sheets
    "sheets_url":          SHEETS_URL,
    # Новые пользователи сайта — группа уведомлений
    "new_clients_group_id":    GROUP_NEW_CLIENTS_ID,
    # Группа водителей/доставщиков (маршруты)
    "delivery_group_id":              GROUP_DELIVERY_ID,
    "delivery_group_zarafshan_id":      GROUP_DELIVERY_ZARAFSHAN_ID,
    "delivery_group_navoi_id":          GROUP_DELIVERY_NAVOI_ID,
    "delivery_channel_zarafshan_id":    GROUP_DELIVERY_ZARAFSHAN_CHANNEL,
    "delivery_channel_navoi_id":        GROUP_DELIVERY_NAVOI_CHANNEL,
    "delivery_channel_zarafshan_link":  "https://t.me/+NmPO9-2PDYVlNzQy",
    "delivery_channel_navoi_link":      "",
    "delivery_group_template": "🚗 {route_name}-{count} — {route_type} · {date} {time}",
    # Лиды — группы и шаблон уведомлений
    "leads_group_id":          LEADS_GROUP_ID,
    "leads_group_zarafshan":   "",
    "leads_group_navoi":       "",
    "leads_group_enabled": "0",
    "lead_notify_ru": (
        "🎯 {lead_code} · {source_full}\n"
        "👤 {client_name}  📞 {client_phone}\n"
        "🏢 {branch}{note_inline}\n"
        "{location_link}"
    ),
    "lead_notify_uz": (
        "🎯 {lead_code} · {source_full}\n"
        "👤 {client_name}  📞 {client_phone}\n"
        "🏢 {branch}{note_inline}\n"
        "{location_link}"
    ),
    "callback_overdue_minutes": "10",
}

async def _get_cfg(key: str) -> str:
    """БД → env-fallback из SITE_SETTINGS_DEFAULTS."""
    val = await db.get_config(key)
    if val:
        return val
    return SITE_SETTINGS_DEFAULTS.get(key, "")

@app.get("/api/settings/site")
async def get_site_settings():
    # Публичный эндпоинт — соцсети, контакты и ключ карты (не секреты)
    PUBLIC_KEYS = [
        "social_instagram", "social_tg_bot", "social_tg_group",
        "contact_short", "contact_main",
        "contact_zarafshan_1", "contact_zarafshan_2", "contact_zarafshan_telegram", "contact_zarafshan_admin_tg", "contact_zarafshan_whatsapp", "contact_zarafshan_instagram",
        "contact_navoi_1", "contact_navoi_2", "contact_navoi_telegram", "contact_navoi_admin_tg", "contact_navoi_whatsapp", "contact_navoi_instagram",
        "yandex_maps_key",
        "branch_zarafshan_location", "branch_navoi_location",
        "osago_partner_phone", "osago_partner_promo",
    ]
    result = {}
    for key in PUBLIC_KEYS:
        result[key] = await _get_cfg(key)
    return {"ok": True, "settings": result}


class SiteSettings(BaseModel):
    social_instagram:    str | None = None
    social_tg_bot:       str | None = None
    social_tg_group:     str | None = None
    contact_short:       str | None = None
    contact_main:        str | None = None
    contact_zarafshan_1:        str | None = None
    contact_zarafshan_2:        str | None = None
    contact_zarafshan_telegram: str | None = None
    contact_zarafshan_admin_tg: str | None = None
    contact_zarafshan_whatsapp: str | None = None
    contact_zarafshan_instagram:str | None = None
    branch_zarafshan_location:  str | None = None
    contact_navoi_1:            str | None = None
    contact_navoi_2:            str | None = None
    contact_navoi_telegram:     str | None = None
    contact_navoi_admin_tg:     str | None = None
    contact_navoi_whatsapp:     str | None = None
    contact_navoi_instagram:    str | None = None
    branch_navoi_location:      str | None = None
    delivery_group_id:              str | None = None
    delivery_group_zarafshan_id:      str | None = None
    delivery_group_navoi_id:          str | None = None
    delivery_channel_zarafshan_id:    str | None = None
    delivery_channel_navoi_id:        str | None = None
    delivery_channel_zarafshan_link:  str | None = None
    delivery_channel_navoi_link:      str | None = None
    delivery_group_template:          str | None = None
    tg_bot_token:        str | None = None
    tg_group_id:         str | None = None
    tg_group_zarafshan:  str | None = None
    tg_group_navoi:      str | None = None
    tg_group_sms_id:     str | None = None
    yandex_maps_key:     str | None = None
    eskiz_email:         str | None = None
    eskiz_password:      str | None = None
    eskiz_from:          str | None = None
    sms_text_register:   str | None = None
    sms_text_login:      str | None = None
    sms_text_reset:      str | None = None
    osago_partner_phone: str | None = None
    osago_partner_promo: str | None = None
    sheets_url:          str | None = None
    leads_group_id:          str | None = None
    leads_group_zarafshan:   str | None = None
    leads_group_navoi:       str | None = None
    leads_group_enabled:         str | None = None
    lead_notify_ru:              str | None = None
    callback_overdue_minutes:    str | None = None

@app.get("/api/admin/settings/site")
async def get_admin_site_settings(_=Depends(get_admin)):
    result = {key: await _get_cfg(key) for key in SITE_SETTINGS_DEFAULTS}
    return {"ok": True, "settings": result}

@app.put("/api/admin/settings/site")
async def save_site_settings(body: SiteSettings, _=Depends(get_admin)):
    data = {k: v for k, v in body.dict().items() if v is not None}
    for key, val in data.items():
        await db.set_config(key, val)
    return {"ok": True}


# ── Telegram: шаблоны уведомлений ──────────────────────────────────────
@app.get("/api/admin/settings/tg-messages")
async def get_tg_messages(_=Depends(get_admin)):
    rows = await db.get_tg_status_messages()
    return rows

@app.put("/api/admin/settings/tg-messages/{status}")
async def save_tg_message(status: str, body: dict, _=Depends(get_admin)):
    ALL_STATUSES = {"new","confirmed","pickup","received","washing","drying","packing","ready","delivery","delivered","cancelled"}
    if status not in ALL_STATUSES:
        raise HTTPException(status_code=400, detail="Неизвестный статус")
    row = await db.upsert_tg_status_message(
        status=status,
        enabled=bool(body.get("enabled", True)),
        message_ru=body.get("message_ru", ""),
        message_uz=body.get("message_uz", ""),
    )
    return row

@app.get("/api/admin/tg-clients")
async def get_tg_clients(search: str = "", _=Depends(get_admin)):
    rows = await db.get_tg_clients(search=search)
    return {"clients": rows, "total": len(rows)}

@app.patch("/api/admin/tg-clients/{tg_id}")
async def tg_client_update(tg_id: int, body: dict, _=Depends(get_admin)):
    allowed = {"first_name", "last_name", "phone"}
    data = {k: v for k, v in body.items() if k in allowed}
    if not data:
        raise HTTPException(status_code=400, detail="Нет полей для обновления")
    await db.update_tg_client(tg_id, data)
    return {"ok": True}

@app.patch("/api/admin/tg-clients/{tg_id}/block")
async def tg_client_block(tg_id: int, body: dict, _=Depends(get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    blocked = bool(body.get("blocked", True))
    await db.block_tg_client(tg_id, blocked)
    return {"ok": True, "blocked": blocked}

@app.delete("/api/admin/tg-clients/{tg_id}")
async def tg_client_delete(tg_id: int, body: dict, _=Depends(get_admin)):
    if not (apass := await get_admin_pass()) or body.get("admin_password") != apass:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    await db.delete_tg_client(tg_id)
    return {"ok": True}


@app.post("/api/orders")
async def create_order_from_site(order: OrderRequest, user=Depends(get_optional_user)):
    """Заявка с сайта/бота → сохраняется как лид для обработки сотрудниками."""
    full_name = f"{order.first_name} {order.last_name}".strip()
    note_parts = []
    if order.service_type: note_parts.append(f"Тип: {order.service_type}")
    if order.pickup_date:  note_parts.append(f"Дата: {order.pickup_date}")
    if order.pickup_time:  note_parts.append(f"Время: {order.pickup_time}")
    if order.is_quick:     note_parts.append("Быстрая заявка")
    note = " · ".join(note_parts) if note_parts else None

    # Определяем агента: сначала по авторизованному пользователю, затем по телефону
    volunteer_id = None
    agent_staff = None
    if user:
        agent_staff = await db.get_staff_by_site_user(user["id"])
        if not agent_staff:
            agent_staff = await db.get_staff_by_login(user["phone"])
    if not agent_staff:
        agent_staff = await db.get_staff_by_login(order.phone)
    if agent_staff and agent_staff.get("role") == "agent" and agent_staff.get("active"):
        volunteer_id = agent_staff["id"]

    lead_source = order.source if order.source in ("site", "bot") else "site"
    lead = await db.create_lead({
        "client_name":   full_name,
        "client_phone":  order.phone,
        "service":       order.service,
        "branch":        order.branch,
        "city":          order.city,
        "address":       order.address,
        "short_address": order.address,
        "note":          note,
        "status":        "new",
        "created_by":    None,
        "volunteer_id":  volunteer_id,
        "location":      order.location,
        "location_address": order.location_address,
        "source":        lead_source,
        "client_tg_id":  order.client_tg_id,
    })
    lead_code = (lead or {}).get("lead_code") or f"#{(lead or {}).get('id','?')}"
    if lead:
        src_label = "Telegram-бот" if lead_source == "bot" else "сайта"
        await db.add_lead_call(lead["id"], None, action="created",
                               note=f"Лид создан с {src_label} ({lead_code})")

    creator_role = lead_source if lead_source in ("site", "bot") else "site"
    creator_staff = {"role": creator_role, "first_name": "Сайт" if creator_role == "site" else "Telegram", "last_name": "", "login": creator_role}
    asyncio.create_task(_notify_new_lead(lead or {}, creator_staff))
    await notify_sheets_new_order(lead_code, order)

    await db.upsert_crm_client(
        phone=order.phone,
        first_name=order.first_name,
        last_name=order.last_name,
        source="site",
    )
    await db.refresh_crm_client_stats(order.phone)

    return {"ok": True, "order_num": lead_code}


class BotLeadRequest(BaseModel):
    client_name: str
    client_phone: str
    branch: str = ""
    city: str = ""
    address: str = ""
    service: str = ""
    service_type: str = ""
    pickup_date: str = ""
    pickup_time: str = ""
    note: str = ""
    location: str = ""
    location_address: str = ""
    client_tg_id: int | None = None
    is_quick: bool = False

    @field_validator("client_phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

@app.post("/api/bot/lead")
async def create_bot_lead(req: BotLeadRequest, x_bot_token: str = Header(None, alias="X-Bot-Token")):
    """Заявка из Telegram-бота → лид."""
    if not BOT_TOKEN or x_bot_token != BOT_TOKEN:
        raise HTTPException(status_code=403, detail="Нет доступа")

    note_parts = []
    if req.is_quick:        note_parts.append("Быстрая заявка (бот)")
    if req.service_type:    note_parts.append(f"Тип: {req.service_type}")
    if req.pickup_date:     note_parts.append(f"Дата: {req.pickup_date}")
    if req.pickup_time:     note_parts.append(f"Время: {req.pickup_time}")
    if req.note:            note_parts.append(req.note)
    note = " · ".join(note_parts) if note_parts else None

    lead = await db.create_lead({
        "client_name":     req.client_name,
        "client_phone":    req.client_phone,
        "service":         req.service,
        "branch":          req.branch,
        "city":            req.city,
        "address":         req.address,
        "short_address":   req.address,
        "note":            note,
        "status":          "new",
        "created_by":      None,
        "volunteer_id":    None,
        "location":        req.location,
        "location_address": req.location_address,
        "source":          "bot",
        "client_tg_id":    req.client_tg_id,
    })
    lead_code = (lead or {}).get("lead_code") or f"#{(lead or {}).get('id','?')}"
    if lead:
        await db.add_lead_call(lead["id"], None, action="created",
                               note=f"Лид создан через Telegram-бот ({lead_code})")

    bot_staff = {"role": "bot", "first_name": "Telegram", "last_name": "", "login": "bot"}
    asyncio.create_task(_notify_new_lead(lead or {}, bot_staff))

    if req.client_phone:
        await db.upsert_crm_client(
            phone=req.client_phone,
            first_name=req.client_name,
            last_name="",
            tg_id=req.client_tg_id,
            source="bot",
        )
        await db.refresh_crm_client_stats(req.client_phone)

    return {"ok": True, "lead_code": lead_code, "lead_id": (lead or {}).get("id")}


class CallbackRequest(BaseModel):
    phone: str
    branch: str = ""
    name: str = ""
    profile_phone: str = ""  # зарегистрированный телефон пользователя

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        v = normalize_phone(v)
        if not PHONE_RE.match(v):
            raise ValueError("Неверный формат номера. Используйте +998XXXXXXXXX")
        return v

@app.post("/api/callback")
async def request_callback(req: CallbackRequest, user=Depends(get_optional_user)):
    """Обратный звонок с сайта → лид в группу лидов филиала."""
    client_name = req.name.strip() or (user.get("first_name", "") if user else "") or req.phone
    note_parts = ["🔔 Обратный звонок с сайта"]
    if req.profile_phone and req.profile_phone != req.phone:
        note_parts.append(f"Тел. в профиле: {req.profile_phone}")

    lead = await db.create_lead({
        "client_name":  client_name,
        "client_phone": req.phone,
        "service":      "callback",
        "branch":       req.branch,
        "note":         " · ".join(note_parts),
        "status":       "new",
        "created_by":   None,
        "volunteer_id": None,
    })
    lead_code = (lead or {}).get("lead_code") or f"#{(lead or {}).get('id','?')}"
    if lead:
        await db.add_lead_call(lead["id"], None, action="created",
                               note=f"Обратный звонок с сайта ({lead_code})")

    site_staff = {"role": "site", "first_name": "Сайт", "last_name": "", "login": "site"}
    asyncio.create_task(_notify_new_lead(lead or {}, site_staff))

    await db.upsert_crm_client(phone=req.phone, first_name=client_name, last_name="", source="callback")
    await db.refresh_crm_client_stats(req.phone)

    return {"ok": True, "lead_code": lead_code}


async def _notify_group_site_lead(lead_code: str, data: "OrderRequest", lead_id: int = None):
    """Telegram: новый лид с сайта — кнопка Взять лид прямо в группе."""
    if not BOT_TOKEN:
        return
    chat_id = await _group_id_for_branch(data.branch or "")
    if not chat_id:
        return

    def he(s):
        return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") if s else "—"

    full_name = f"{data.first_name} {data.last_name}".strip()

    if data.is_quick:
        text = (
            f"🎯 Новый лид <b>{lead_code}</b> — быстрая заявка (сайт)\n"
            f"━━━━━━━━━━\n"
            f"👤 {he(full_name)}\n"
            f"📞 {he(data.phone)}\n"
            f"━━━━━━━━━━"
        )
    else:
        lines = [
            f"🎯 Новый лид <b>{lead_code}</b> (сайт)",
            f"━━━━━━━━━━",
            f"👤 {he(full_name)}",
            f"📞 {he(data.phone)}",
        ]
        if data.branch:      lines.append(f"🏢 {he(branch_ru(data.branch))}")
        if data.city:        lines.append(f"📍 {he(data.city)}")
        if data.address:     lines.append(f"🏠 {he(data.address)}")
        if data.service:     lines.append(f"🧺 {he(data.service)}")
        if data.pickup_date: lines.append(f"📅 {he(data.pickup_date)} {he(data.pickup_time)}".rstrip())
        lines.append("━━━━━━━━━━")
        text = "\n".join(lines)

    keyboard = None
    if lead_id:
        keyboard = {"inline_keyboard": [[
            {"text": "✋ Взять лид", "callback_data": f"take_lead_{lead_id}"}
        ]]}

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": str(chat_id), "text": text, "parse_mode": "HTML"}
        if keyboard:
            payload["reply_markup"] = keyboard
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logging.warning(f"_notify_group_site_lead error: {e}")


# ── ОБСЛУЖИВАНИЕ БД ──────────────────────────────────────────────────────────
@app.post("/api/admin/db-maintenance")
async def db_maintenance(op: str = Body(..., embed=True), _=Depends(_get_admin)):
    if not db.pool:
        raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        if op == "purge_deleted_leads_data":
            r1 = await conn.execute("DELETE FROM agent_notifications WHERE lead_id NOT IN (SELECT id FROM leads)")
            r2 = await conn.execute("DELETE FROM lead_reminders       WHERE lead_id NOT IN (SELECT id FROM leads)")
            r3 = await conn.execute("DELETE FROM lead_calls           WHERE lead_id NOT IN (SELECT id FROM leads)")
            total = sum(int(r.split()[-1]) for r in [r1, r2, r3] if r)
            return {"ok": True, "message": f"Удалено {total} записей (уведомления: {r1.split()[-1]}, напоминания: {r2.split()[-1]}, журнал: {r3.split()[-1]})"}

        elif op == "purge_deleted_history":
            result = await conn.execute("""
                DELETE FROM order_status_history
                WHERE order_num NOT IN (
                    SELECT order_num FROM orders WHERE order_num IS NOT NULL
                )
            """)
            count = result.split()[-1] if result else "0"
            return {"ok": True, "message": f"Удалено {count} записей истории удалённых заказов"}

        elif op == "truncate_history":
            await conn.execute("TRUNCATE TABLE order_status_history")
            return {"ok": True, "message": "Таблица order_status_history очищена"}

        elif op == "vacuum":
            await conn.execute("VACUUM ANALYZE orders")
            await conn.execute("VACUUM ANALYZE order_items")
            await conn.execute("VACUUM ANALYZE order_status_history")
            return {"ok": True, "message": "VACUUM ANALYZE выполнен для orders, order_items, order_status_history"}

        elif op == "purge_old_leads":
            result = await conn.execute("""
                DELETE FROM leads
                WHERE status IN ('closed','cancelled')
                  AND created_at < NOW() - INTERVAL '90 days'
            """)
            count = result.split()[-1] if result else "0"
            return {"ok": True, "message": f"Удалено {count} старых лидов"}

        else:
            raise HTTPException(status_code=400, detail=f"Неизвестная операция: {op}")


# ══════════════════════════════════════════════════════════════════════════════
# CHAT
# ══════════════════════════════════════════════════════════════════════════════

def _tpl(text: str, session: dict) -> str:
    """Подставляет {name} → первое слово имени клиента. Без имени — убирает {name} вместе с соседней запятой."""
    if not text:
        return text
    raw = (session.get('client_name') or '').strip()
    first = raw.split()[0] if raw else ''
    if first:
        return text.replace('{name}', first)
    # убираем «, {name}», «{name},» и просто «{name}»
    for pat in (', {name}', ' {name},', '{name}, ', '{name}'):
        text = text.replace(pat, '')
    return text

class _ChatMgr:
    def __init__(self):
        self.clients: dict[str, set] = {}   # code → set of WebSocket (multi-device)
        self.staff:   dict[int,  WebSocket] = {}   # staff_id → ws

    async def connect_client(self, code: str, ws: WebSocket):
        await ws.accept()
        if code not in self.clients:
            self.clients[code] = set()
        self.clients[code].add(ws)

    async def connect_staff(self, staff_id: int, ws: WebSocket):
        await ws.accept()
        self.staff[staff_id] = ws

    def disconnect_client(self, code: str, ws: WebSocket = None):
        if ws is not None:
            self.clients.get(code, set()).discard(ws)
            if not self.clients.get(code):
                self.clients.pop(code, None)
        else:
            self.clients.pop(code, None)

    def disconnect_staff(self, staff_id: int):
        self.staff.pop(staff_id, None)

    async def send_client(self, code: str, data: dict):
        dead = []
        for ws in list(self.clients.get(code, set())):
            try: await ws.send_json(data)
            except: dead.append(ws)
        for ws in dead: self.disconnect_client(code, ws)

    async def send_staff(self, staff_id: int, data: dict):
        ws = self.staff.get(staff_id)
        if ws:
            try: await ws.send_json(data)
            except: self.disconnect_staff(staff_id)

    async def broadcast_staff(self, data: dict, exclude: int = None):
        dead = []
        for sid, ws in list(self.staff.items()):
            if sid == exclude: continue
            try: await ws.send_json(data)
            except: dead.append(sid)
        for sid in dead: self.disconnect_staff(sid)

    def staff_online_ids(self) -> set:
        return set(self.staff.keys())

_chat = _ChatMgr()


async def _chat_timeout_worker():
    """Каждые 60 сек проверяет неактивные чаты и закрывает их."""
    await asyncio.sleep(60)
    while True:
        try:
            # 1. Предупредить
            to_warn = await db.get_sessions_to_warn()
            for s in to_warn:
                lang = s.get('lang') or 'uz'
                warn_text = _tpl(await db.get_chat_template_text('warn_timeout', lang) or \
                    "⏰ Вы давно не отвечаете. Чат будет автоматически закрыт через 1 минуту.", s)
                msg = await db.add_chat_message(s['id'], 'bot', 'ARTEZ', warn_text)
                if msg:
                    await _chat.send_client(s['code'], {"type": "message", "msg": _msg_json(msg)})
                    claimed = s.get('claimed_by')
                    if claimed:
                        await _chat.send_staff(claimed, {"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                    else:
                        await _chat.broadcast_staff({"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                await db.set_chat_warned(s['code'])

            # 2. Закрыть
            to_close = await db.get_sessions_to_close()
            for s in to_close:
                lang   = s.get('lang') or 'uz'
                gender = (s.get('staff_gender') or 'M').upper()
                bye_key = 'bye_f' if gender == 'F' else 'bye_m'
                bye = _tpl(await db.get_chat_template_text(bye_key, lang) or \
                    ("Я рада, что смогла вам помочь! 😊" if gender == 'F' else "Я рад, что смог вам помочь! 😊"), s)
                msg = await db.add_chat_message(s['id'], 'bot', 'ARTEZ', bye)
                if msg:
                    await _chat.send_client(s['code'], {"type": "message", "msg": _msg_json(msg)})
                    claimed = s.get('claimed_by')
                    if claimed:
                        await _chat.send_staff(claimed, {"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                    else:
                        await _chat.broadcast_staff({"type": "message", "code": s['code'], "msg": _msg_json(msg)})
                await asyncio.sleep(1)
                closed = await db.close_chat_session(s['code'])
                if closed:
                    await _chat.send_client(s['code'], {"type": "chat_closed"})
                    await _chat.broadcast_staff({"type": "chat_closed", "code": s['code']})
        except Exception as e:
            logging.warning(f"_chat_timeout_worker error: {e}")
        await asyncio.sleep(60)

def _gen_chat_code() -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def _msg_json(msg: dict) -> dict:
    m = dict(msg)
    if hasattr(m.get('created_at'), 'isoformat'):
        m['created_at'] = m['created_at'].isoformat()
    return m


@app.post("/api/chat/start")
async def chat_start(body: dict = Body(...)):
    client_phone = (body.get("client_phone") or "").strip()
    client_name  = (body.get("client_name")  or "").strip()
    branch       = (body.get("branch")       or "").strip()
    lang         = (body.get("lang")         or "uz").strip().lower()[:5]

    code = _gen_chat_code()
    session = await db.create_chat_session(code, client_phone, client_name, branch, lang)
    if not session:
        raise HTTPException(500, "Не удалось создать сессию")

    welcome = _tpl(await db.get_chat_template_text('welcome', lang) or \
        "Здравствуйте! 👋 Спасибо, что обратились в ARTEZ. Менеджер ответит вам в ближайшее время.", session)
    await db.add_chat_message(session['id'], 'bot', 'ARTEZ', welcome)

    # Уведомить подключённых сотрудников через WS
    await _chat.broadcast_staff({
        "type": "new_chat",
        "code": code,
        "client_name": client_name or client_phone or "Клиент",
        "client_phone": client_phone,
        "branch": branch,
        "created_at": session['created_at'].isoformat() if hasattr(session.get('created_at'), 'isoformat') else str(session.get('created_at','')),
    })

    # Push сотрудникам, которые не подключены
    staff_ids = await db.get_staff_for_chat_push()
    online    = _chat.staff_online_ids()
    for sid in staff_ids:
        if sid not in online:
            asyncio.create_task(send_web_push(
                sid,
                title="💬 Новый чат",
                body=f"Клиент {client_name or client_phone or 'с сайта'} ждёт ответа",
                push_type="new_chat",
            ))

    return {"ok": True, "code": code}


@app.get("/api/chat/sessions")
async def chat_sessions(staff=Depends(get_current_staff)):
    sessions = await db.get_active_chat_sessions()
    result = []
    for s in sessions:
        msgs = await db.get_chat_messages(s['id'])
        s2 = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in s.items()}
        s2['message_count'] = len(msgs)
        s2['last_text'] = msgs[-1]['text'] if msgs else ''
        result.append(s2)
    return result


@app.get("/api/chat/{code}/messages")
async def chat_get_messages(code: str, staff=Depends(get_current_staff)):
    session = await db.get_chat_session(code)
    if not session:
        raise HTTPException(404, "Сессия не найдена")
    msgs = await db.get_chat_messages(session['id'])
    s = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in session.items()}
    return {"session": s, "messages": [_msg_json(m) for m in msgs]}


@app.post("/api/chat/{code}/claim")
async def chat_claim(code: str, staff=Depends(get_current_staff)):
    name = f"{staff.get('first_name','')} {staff.get('last_name','')}".strip() or "Менеджер"
    session = await db.claim_chat_session(code, staff['id'], name)
    if not session:
        raise HTTPException(400, "Чат уже занят другим сотрудником")

    await _chat.broadcast_staff({"type": "chat_claimed", "code": code,
                                  "claimed_by": staff['id'], "claimed_name": name})
    await _chat.send_client(code, {"type": "staff_joined", "staff_name": name})
    s = {k: (v.isoformat() if hasattr(v, 'isoformat') else v) for k, v in session.items()}
    return {"ok": True, "session": s}


@app.post("/api/chat/{code}/close")
async def chat_close(code: str, staff=Depends(get_current_staff)):
    session = await db.close_chat_session(code)
    if not session:
        raise HTTPException(404, "Сессия не найдена")
    await _chat.broadcast_staff({"type": "chat_closed", "code": code})
    await _chat.send_client(code, {"type": "chat_closed",
                                    "text": "Чат завершён. Спасибо, что обратились в ARTEZ!"})
    return {"ok": True}


@app.post("/api/chat/templates/seed")
async def seed_templates(staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin',):
        raise HTTPException(403, "Только для admin")
    # принудительно засеять (даже если таблица не пустая)
    await db.seed_chat_templates_forced()
    return {"ok": True}

@app.get("/api/chat/active-by-phone")
async def chat_active_by_phone(phone: str):
    """Публичный эндпоинт — проверить есть ли активный чат для этого номера."""
    if not phone:
        return {"session": None}
    session = await db.get_active_chat_by_phone(phone)
    if not session:
        return {"session": None}
    for k, v in session.items():
        if hasattr(v, 'isoformat'): session[k] = v.isoformat()
    return {"session": {"code": session["code"], "status": session["status"]}}

@app.get("/api/chat/history")
async def chat_history(limit: int = 50, offset: int = 0, filter: str = "own",
                       staff=Depends(get_current_staff)):
    role = staff.get("role", "")
    can_see_all = role in ("admin", "manager")
    own_only = not can_see_all or filter == "own"
    rows = await db.get_closed_chat_sessions(limit, offset,
                                              staff_id=staff["id"], own_only=own_only)
    for r in rows:
        for k, v in r.items():
            if hasattr(v, 'isoformat'): r[k] = v.isoformat()
    return {"rows": rows, "can_see_all": can_see_all}

@app.get("/api/chat/templates")
async def get_templates(staff=Depends(get_current_staff)):
    rows = await db.get_all_chat_templates()
    return rows

@app.get("/api/chat/templates/quick")
async def get_quick_templates(lang: str = "uz", staff=Depends(get_current_staff)):
    rows = await db.get_chat_templates(lang=lang, key='quick')
    return rows

@app.post("/api/chat/templates")
async def create_template(body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    row = await db.upsert_chat_template(body)
    return row or {}

@app.put("/api/chat/templates/{tid}")
async def update_template(tid: int, body: dict = Body(...), staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    body['id'] = tid
    row = await db.upsert_chat_template(body)
    return row or {}

@app.delete("/api/chat/templates/{tid}")
async def del_template(tid: int, staff=Depends(get_current_staff)):
    if staff.get('role') not in ('admin', 'manager'):
        raise HTTPException(403, "Недостаточно прав")
    await db.delete_chat_template(tid)
    return {"ok": True}


@app.post("/api/admin/bot/broadcast-restart")
async def bot_broadcast_restart(_=Depends(_get_admin)):
    """Рассылает всем клиентам бота сообщение «Нажмите /start»."""
    token = await _get_cfg("tg_bot_token") or BOT_TOKEN
    if not token:
        raise HTTPException(status_code=503, detail="BOT_TOKEN не настроен")
    tg_ids = await db.get_all_bot_client_tg_ids()
    if not tg_ids:
        return {"ok": True, "sent": 0, "failed": 0, "total": 0}
    text = (
        "🔄 <b>Бот ARTEZ обновлён!</b>\n\n"
        "Для продолжения нажмите /start"
    )
    sent = failed = 0
    async with aiohttp.ClientSession() as s:
        for tg_id in tg_ids:
            try:
                r = await s.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": tg_id, "text": text, "parse_mode": "HTML"},
                    timeout=aiohttp.ClientTimeout(total=5))
                if (await r.json()).get("ok"):
                    sent += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
            await asyncio.sleep(0.05)
    logging.info(f"Bot broadcast-restart: sent={sent}, failed={failed}, total={len(tg_ids)}")
    return {"ok": True, "sent": sent, "failed": failed, "total": len(tg_ids)}


@app.websocket("/ws/chat/client/{code}")
async def ws_chat_client(websocket: WebSocket, code: str):
    session = await db.get_chat_session(code)
    if not session or session['status'] == 'closed':
        await websocket.accept()
        await websocket.send_json({"type": "chat_closed"})
        await websocket.close()
        return

    await _chat.connect_client(code, websocket)  # accept() внутри
    msgs = await db.get_chat_messages(session['id'])
    await websocket.send_json({"type": "history", "messages": [_msg_json(m) for m in msgs]})

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") != "message":
                continue
            text = (data.get("text") or "").strip()
            if not text:
                continue
            # Перечитать сессию (могли claim)
            session = await db.get_chat_session(code)
            if not session or session['status'] == 'closed':
                break
            cname = session.get('client_name') or session.get('client_phone') or "Клиент"
            is_first = await db.is_first_client_message(session['id'])
            msg = await db.add_chat_message(session['id'], 'client', cname, text)
            if not msg:
                continue
            asyncio.create_task(db.touch_chat_client_activity(code))
            payload = {"type": "message", "code": code, "msg": _msg_json(msg)}
            await websocket.send_json(payload)
            claimed = session.get('claimed_by')
            if claimed:
                await _chat.send_staff(claimed, payload)
            else:
                await _chat.broadcast_staff(payload)
            # Авто-ответ на первое сообщение клиента (через 3 сек)
            if is_first:
                async def _send_auto_reply(c=code, sess=dict(session), cl=claimed):
                    await asyncio.sleep(3)
                    lang = sess.get('lang') or 'uz'
                    auto_text = _tpl(await db.get_chat_template_text('auto_reply', lang), sess)
                    if not auto_text:
                        return
                    auto_msg = await db.add_chat_message(sess['id'], 'bot', 'ARTEZ', auto_text)
                    if not auto_msg:
                        return
                    auto_payload = {"type": "message", "code": c, "msg": _msg_json(auto_msg)}
                    await _chat.send_client(c, auto_payload)
                    if cl:
                        await _chat.send_staff(cl, auto_payload)
                    else:
                        await _chat.broadcast_staff(auto_payload)
                asyncio.create_task(_send_auto_reply())
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _chat.disconnect_client(code, websocket)


@app.websocket("/ws/chat/staff/{token}")
async def ws_chat_staff(websocket: WebSocket, token: str):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        staff_id = int(payload.get("sub"))
    except Exception:
        await websocket.close(code=4001)
        return

    await _chat.connect_staff(staff_id, websocket)
    staff_row = await db.get_staff_by_id(staff_id)
    sname = f"{(staff_row or {}).get('first_name','')} {(staff_row or {}).get('last_name','')}".strip() or "Менеджер"

    try:
        while True:
            data = await websocket.receive_json()
            if data.get("type") != "message":
                continue
            code = (data.get("code") or "").strip()
            text = (data.get("text") or "").strip()
            if not code or not text:
                continue
            session = await db.get_chat_session(code)
            if not session or session['status'] == 'closed':
                continue
            msg = await db.add_chat_message(session['id'], 'staff', sname, text)
            if not msg:
                continue
            payload = {"type": "message", "code": code, "msg": _msg_json(msg)}
            await _chat.send_client(code, {"type": "message", "msg": _msg_json(msg)})
            await _chat.broadcast_staff(payload, exclude=staff_id)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _chat.disconnect_staff(staff_id)
