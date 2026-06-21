import os
import re
import secrets
import logging
import asyncio
from datetime import datetime, timedelta, timezone

import json as _json
import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header, Body, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
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
VAPID_PRIVATE  = os.getenv("VAPID_PRIVATE", "")
VAPID_PUBLIC   = os.getenv("VAPID_PUBLIC", "")

BOT_TOKEN          = os.getenv("BOT_TOKEN", "")
GROUP_ID           = os.getenv("GROUP_ID", "")
GROUP_ID_ZARAFSHAN = os.getenv("GROUP_ID_ZARAFSHAN", "")
LEADS_GROUP_ID     = os.getenv("LEADS_GROUP_ID", "-1004486597965")
GROUP_ID_NAVOI     = os.getenv("GROUP_ID_NAVOI", "")
MEDIA_CHANNEL_ID   = os.getenv("MEDIA_CHANNEL_ID", "-1004453880659")
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
    asyncio.create_task(_tg_reminder_worker())

async def send_web_push(staff_id: int, title: str, body: str, lead_id: int = None, phone: str = None):
    if not VAPID_PRIVATE or not VAPID_PUBLIC:
        return
    try:
        from pywebpush import webpush, WebPushException
        subs = await db.get_push_subscriptions(staff_id)
        for sub in subs:
            try:
                payload = _json.dumps({"title": title, "body": body, "lead_id": lead_id, "phone": phone})
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
                    # Web Push + уведомление в приложении
                    if r.get("staff_id"):
                        push_body = f"📞 {r['client_phone']}" + (f"\n{msg}" if msg != "Запланированный звонок" else "")
                        asyncio.create_task(send_web_push(
                            r["staff_id"],
                            f"🔔 Перезвонить: {client}",
                            push_body,
                            r["lead_id"],
                            r["client_phone"]
                        ))
                        # Пишем в Уведомления приложения
                        try:
                            await db.create_agent_notification(
                                r["staff_id"], r["lead_id"],
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
    enabled = await _get_cfg("leads_group_enabled")
    if enabled not in ("1", "true"):
        return
    group_id = await _get_cfg("leads_group_id")
    if not group_id:
        return
    template_ru = await _get_cfg("lead_notify_ru")
    template_uz = await _get_cfg("lead_notify_uz")

    role    = staff.get("role", "")
    source  = "🤝 Агент" if role == "agent" else "👤 Сотрудник"
    creator = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "—")

    vars_ = {
        "lead_code":    lead.get("lead_code") or f"#{lead.get('id')}",
        "client_name":  lead.get("client_name") or "—",
        "client_phone": lead.get("client_phone") or "—",
        "branch":       lead.get("branch") or "—",
        "note":         lead.get("note") or "—",
        "source":       source,
        "creator":      creator,
    }

    text = template_ru or template_uz
    if text:
        await send_tg(group_id, text.format_map(vars_))


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

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v):
        return normalize_phone(v)


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
        if not v.strip():
            raise ValueError("Укажите адрес")
        return v.strip()


class StaffOrderRequest(BaseModel):
    first_name: str
    phone: str
    service: str = ""
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
    "driver":     ["orders", "status_delivery"],
    "logistics":  ["orders", "status"],
    "washer":     ["orders_workshop", "status_wash"],
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
        # orders_workshop даёт доступ к эндпоинту orders (но с фильтрацией)
        if permission == "orders" and "orders_workshop" in perms:
            return staff
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
    tg_id: str | None = None
    tg_username: str | None = None
    salary_type: str | None = None
    salary_rate: float | None = None
    hire_date: str | None = None
    note: str | None = None

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
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"DB error: {type(e).__name__}: {e}")
    return {"ok": True, "id": sid}

@app.patch("/api/staff/{staff_id}")
async def staff_update(staff_id: int, body: dict, _=Depends(require_perm("staff"))):
    allowed = {"first_name","last_name","middle_name","phone","login","role","branch","position","active","is_active","note","hire_date","salary_type","salary_rate","tg_id","tg_username"}
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="Нет данных для обновления")
    if "tg_id" in updates and updates["tg_id"] is not None:
        try:
            updates["tg_id"] = int(updates["tg_id"])
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="tg_id должен быть числом")
    await db.update_staff(staff_id, **updates)
    row = await db.get_staff_by_id(staff_id)
    if not row:
        raise HTTPException(status_code=404, detail="Сотрудник не найден")
    return {"ok": True, "staff": _staff_public(dict(row))}

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
    lead_num = await db.get_next_lead_num()
    lead_code = await db.generate_lead_code()
    creator_id = None if staff.get("sub") == "admin" else staff.get("id")
    # агент автоматически становится agent_id лида
    agent_id = req.volunteer_id
    if role == "agent" and not agent_id:
        agent_id = creator_id
    lead = await db.create_lead({
        "lead_num": lead_num, "client_name": req.client_name,
        "client_phone": req.client_phone, "service": req.service,
        "branch": req.branch, "city": req.city, "address": req.address,
        "short_address": req.short_address, "note": req.note,
        "assigned_to": req.assigned_to, "created_by": creator_id,
        "volunteer_id": agent_id, "lead_code": lead_code,
    })
    if lead:
        await db.add_lead_call(lead["id"], creator_id, action="created",
                               note=f"Лид создан ({lead_code})")
        asyncio.create_task(_notify_new_lead(lead, staff))
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
    allowed = {"client_name","client_phone","branch","address","short_address","note","volunteer_id"}
    fields = {k: v for k, v in body.items() if k in allowed}
    lead = await db.update_lead(lead_id, **fields)
    operator_id = None if staff.get("sub") == "admin" else staff.get("id")
    await db.add_lead_call(lead_id, operator_id, action="edited", note="Лид отредактирован")
    return {"ok": True, "lead": lead}

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
        await db.update_lead_status(lead_id, status)
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
    if not body.get("admin_password") or body["admin_password"] != ADMIN_PASS:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/staff/leads/bulk-delete")
async def bulk_delete_leads(body: dict, _=Depends(require_perm("leads"))):
    if not body.get("admin_password") or body["admin_password"] != ADMIN_PASS:
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
    if not body.get("admin_password") or body["admin_password"] != ADMIN_PASS:
        raise HTTPException(status_code=403, detail="Неверный пароль администратора")
    ids = body.get("ids", [])
    if not ids:
        raise HTTPException(status_code=400, detail="Нет ID заказов")
    deleted = 0
    for order_id in ids:
        ok = await db.delete_order(int(order_id))
        if ok: deleted += 1
    return {"ok": True, "deleted": deleted}


# ══════════════════════════════════════
#  ЗАЯВКИ — для сотрудников
# ══════════════════════════════════════
_WORKSHOP_STATUSES = {"received", "washing", "drying", "packing", "ready"}

@app.get("/api/staff/orders")
async def staff_orders(status: str = None, branch: str = None,
                       staff=Depends(require_perm("orders"))):
    rows = await db.get_admin_orders(status=status, limit=200)
    result = [dict(r) for r in rows]
    # Если у сотрудника разрешение orders_workshop — показывать только мастерские статусы
    role  = staff.get("role", "")
    perms = ROLE_PERMISSIONS.get(role, []) + (staff.get("extra_permissions") or [])
    if "orders_workshop" in perms and "orders" not in perms:
        result = [o for o in result if o.get("status") in _WORKSHOP_STATUSES]
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
            "service":     req.service,
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
    if not ADMIN_PASS or req.password != ADMIN_PASS:
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
    if not ADMIN_PASS or req.password != ADMIN_PASS:
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
    if not ADMIN_PASS or req.password != ADMIN_PASS:
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


@app.post("/api/register")
async def register(req: RegisterRequest):
    existing = await db.get_user_by_phone(req.phone)
    if existing and existing["is_verified"]:
        raise HTTPException(status_code=400, detail=bi("Этот номер уже зарегистрирован","Bu raqam allaqachon ro'yxatdan o'tgan"))

    ok, err = await db.check_sms_rate_limit(req.phone, "register")
    if not ok:
        raise HTTPException(status_code=429, detail=err)

    password_hash = pwd_context.hash(req.password[:72])
    await db.create_user(req.phone, password_hash, req.first_name)

    code = generate_code()
    expires_at = datetime.utcnow() + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, "register", expires_at)
    await send_sms(req.phone, await sms_text(code, "register"))

    return {"ok": True, "message": "Код подтверждения отправлен", "phone": req.phone}


@app.post("/api/verify")
async def verify(req: VerifyRequest):
    ok = await db.check_sms_code(req.phone, req.code, "register")
    if not ok:
        raise HTTPException(status_code=400, detail=bi("Неверный или просроченный код","Noto'g'ri yoki muddati o'tgan kod"))

    await db.verify_user(req.phone)
    user = await db.get_user_by_phone(req.phone)
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
    await send_sms(req.phone, await sms_text(code, req.purpose))

    return {"ok": True, "message": "Код отправлен повторно"}


@app.post("/api/login")
async def login(req: LoginRequest):
    user = await db.get_user_by_phone(req.phone)
    if not user or not pwd_context.verify(req.password[:72], user["password_hash"]):
        raise HTTPException(status_code=401, detail=bi("Неверный номер или пароль","Noto'g'ri telefon yoki parol"))

    if not user["is_verified"]:
        raise HTTPException(status_code=403, detail=bi("Номер не подтверждён. Запросите код заново","Raqam tasdiqlanmagan. Kodni qayta so'rang"))

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
    "zarafshan": "Зарафшан",
    "navoi":     "Навои",
}

def branch_ru(branch: str) -> str:
    return BRANCH_RU.get(branch, branch) if branch else "—"

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
        "exp": datetime.now(timezone.utc) + timedelta(days=1),
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
    if not ADMIN_PASS or req.password != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Неверный пароль")
    return {"ok": True, "token": create_admin_token()}

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
    if not ADMIN_PASS or req.password != ADMIN_PASS:
        raise HTTPException(status_code=403, detail="Неверный пароль")
    ok = await db.delete_lead(lead_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Лид не найден")
    return {"ok": True}

@app.post("/api/admin/leads")
async def admin_create_lead(req: LeadCreateRequest, _=Depends(_get_admin)):
    lead_num = await db.get_next_lead_num()
    lead = await db.create_lead({
        "lead_num": lead_num, "client_name": req.client_name,
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
async def agent_apply(user=Depends(get_current_user)):
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

    staff_id = await db.create_agent_from_user(user_data, password_hash)
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
    return {"ok": True, "users": [dict(r) for r in rows]}

@app.post("/api/admin/site-users/{user_id}/reset-password")
async def admin_reset_site_user_password(user_id: int, body: dict, _=Depends(_get_admin)):
    new_password = (body.get("new_password") or "").strip()
    if len(new_password) < 4:
        raise HTTPException(status_code=400, detail="Пароль минимум 4 символа")
    hashed = pwd_context.hash(new_password[:72])
    await db.update_user_password(user_id, hashed)
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
    if staff.get("sub") != "admin" and order.get("status") not in _ORDER_EDITABLE_STATUSES:
        raise HTTPException(status_code=400, detail="Нельзя редактировать заказ в этом статусе")
    allowed = {"client_first_name","client_last_name","client_phone",
               "branch","address","short_address","location","location_address","note","deadline"}
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
    if not sqm or sqm <= 0:
        raise HTTPException(status_code=400, detail="Укажите площадь или ширину и длину")
    item = await db.create_order_item(
        order_id=order_id, service=req.service, sqm=sqm,
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
    if not BOT_TOKEN or not MEDIA_CHANNEL_ID:
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
    form.add_field("chat_id", str(MEDIA_CHANNEL_ID))
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
    amount:  float = Body(..., embed=False),
    method:  str   = Body(..., embed=False),
    purpose: str   = Body("payment", embed=False),
    note:    str   = Body("", embed=False),
    staff=Depends(get_current_staff),
):
    name = " ".join(filter(None,[staff.get("last_name"),staff.get("first_name")])) or staff.get("login","")
    row = await db.add_order_payment(order_id, amount, method, purpose, note, name)
    return {"ok": True, "payment": row}

@app.delete("/api/admin/orders/{order_id}/payments/{payment_id}")
async def delete_order_payment(order_id: int, payment_id: int, _=Depends(get_current_staff)):
    await db.delete_order_payment(payment_id)
    return {"ok": True}

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
    elif action == "approve":
        item = await db.approve_item_measure(item_id)
    elif action == "reject":
        if not note:
            raise HTTPException(status_code=400, detail="Укажите причину отклонения")
        item = await db.reject_item_measure(item_id, note)
    else:
        raise HTTPException(status_code=400, detail="Неверное действие")
    if not item:
        raise HTTPException(status_code=404, detail="Позиция не найдена")
    return {"ok": True, "item": item}

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
    if not BOT_TOKEN or not MEDIA_CHANNEL_ID:
        raise HTTPException(status_code=503, detail="Медиа-хранилище не настроено")
    content_type = file.content_type or ""
    if content_type.startswith("video/"):
        tg_method, tg_field, tg_type = "sendVideo", "video", "video"
    else:
        tg_method, tg_field, tg_type = "sendPhoto", "photo", "photo"

    order_row = await db.get_order_by_id(order_id)
    order_num = order_row.get("order_num", f"#{order_id}") if order_row else f"#{order_id}"
    staff_name = " ".join(filter(None, [staff.get("last_name"), staff.get("first_name")])) or staff.get("login", "")
    caption = f"📐 Замер\n🧾 Заказ: {order_num} | Позиция #{item_id}\n👤 {staff_name}"

    file_bytes = await file.read()
    form = aiohttp.FormData()
    form.add_field("chat_id", str(MEDIA_CHANNEL_ID))
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
    can_approve_measure: bool = Body(False, embed=True)):
    if not db.pool: raise HTTPException(status_code=503, detail="DB unavailable")
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE staff SET can_edit_items=$2, can_measure=$3, can_approve_measure=$4
               WHERE id=$1 RETURNING id, can_edit_items, can_measure, can_approve_measure""",
            staff_id, can_edit_items, can_measure, can_approve_measure)
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
    "contact_zarafshan_1": "+998882001221",
    "contact_zarafshan_2": "+998947380444",
    "contact_navoi_1":     "+998997500020",
    "contact_navoi_2":     "+998991124848",
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
    # Лиды — группа и шаблоны уведомлений
    "leads_group_id":      LEADS_GROUP_ID,
    "leads_group_enabled": "0",
    "lead_notify_ru": (
        "🎯 Новый лид {lead_code}\n\n"
        "👤 {client_name}\n"
        "📞 {client_phone}\n"
        "🏢 Филиал: {branch}\n"
        "💬 {note}\n\n"
        "📌 {source}: {creator}"
    ),
    "lead_notify_uz": (
        "🎯 Yangi lid {lead_code}\n\n"
        "👤 {client_name}\n"
        "📞 {client_phone}\n"
        "🏢 Filial: {branch}\n"
        "💬 {note}\n\n"
        "📌 {source}: {creator}"
    ),
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
        "contact_zarafshan_1", "contact_zarafshan_2",
        "contact_navoi_1", "contact_navoi_2",
        "yandex_maps_key",
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
    contact_zarafshan_1: str | None = None
    contact_zarafshan_2: str | None = None
    contact_navoi_1:     str | None = None
    contact_navoi_2:     str | None = None
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
    leads_group_id:       str | None = None
    leads_group_enabled:  str | None = None
    lead_notify_ru:       str | None = None
    lead_notify_uz:       str | None = None

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


@app.post("/api/orders")
async def create_order(order: OrderRequest):
    order_num = await db.get_next_order_num()
    await db.save_site_order({
        "order_num":    order_num,
        "first_name":   order.first_name,
        "last_name":    order.last_name,
        "phone":        order.phone,
        "branch":       order.branch,
        "city":         order.city,
        "address":      order.address,
        "location":     order.location,
        "service":      order.service,
        "pickup_date":  order.pickup_date,
        "pickup_time":  order.pickup_time,
        "note":         f"Тип услуги: {order.service_type}" if order.service_type else "",
        "total_price":  order.total_price,
    })

    await notify_group_new_order(order_num, order)
    await notify_sheets_new_order(order_num, order)

    # Авто-регистрация клиента в CRM
    await db.upsert_crm_client(
        phone=order.phone,
        first_name=order.first_name,
        last_name=order.last_name,
        source="site",
    )
    await db.refresh_crm_client_stats(order.phone)

    return {"ok": True, "order_num": order_num}
