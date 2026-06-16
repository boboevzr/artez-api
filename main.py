import os
import re
import random
import logging
from datetime import datetime, timedelta, timezone

import aiohttp
from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
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

BOT_TOKEN  = os.getenv("BOT_TOKEN", "")
GROUP_ID   = os.getenv("GROUP_ID", "")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI(title="ARTEZ API")

# CORS — разрешаем запросы с сайта (уточните домен в проде)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # на проде заменить на ["https://artez.uz"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await db.init_db()


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
    service: str = ""
    service_type: str = ""
    pickup_date: str = ""
    pickup_time: str = ""

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


# ══════════════════════════════════════
#  SMS (заглушка — подключить Eskiz позже)
# ══════════════════════════════════════
async def send_sms(phone: str, message: str):
    """
    TODO: подключить реального провайдера (Eskiz.uz).
    Пока код выводится в логи сервера для тестирования.
    """
    logging.info(f"📲 [SMS->{phone}] {message}")
    # Пример будущей интеграции с Eskiz:
    # async with aiohttp.ClientSession() as session:
    #     await session.post(
    #         "https://notify.eskiz.uz/api/message/sms/send",
    #         headers={"Authorization": f"Bearer {ESKIZ_TOKEN}"},
    #         data={"mobile_phone": phone.lstrip("+"), "message": message, "from": "4546"}
    #     )


def generate_code() -> str:
    return f"{random.randint(0, 9999):04d}"


# ══════════════════════════════════════
#  JWT
# ══════════════════════════════════════
def create_token(user_id: int, phone: str) -> str:
    payload = {
        "sub": str(user_id),
        "phone": phone,
        "exp": datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


async def get_current_user(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Не авторизован")
    token = authorization.removeprefix("Bearer ").strip()
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(status_code=401, detail="Недействительный токен")
    user = await db.get_user_by_id(int(payload["sub"]))
    if not user:
        raise HTTPException(status_code=401, detail="Пользователь не найден")
    return user


# ══════════════════════════════════════
#  ENDPOINTS
# ══════════════════════════════════════
@app.get("/api/health")
async def health():
    return {"ok": True}


@app.get("/api/prices")
async def get_prices():
    """Возвращает актуальные цены из БД для калькулятора и прайс-листа на сайте"""
    prices = await db.get_all_prices()
    if not prices:
        # Дефолты на случай пустой таблицы
        prices = {
            "carpet":      {"standard": 13000, "express": 18000},
            "carpet_home": {"standard": 15000, "express": 20000},
            "sofa":        {"standard": 100000, "express": 150000},
            "mattress":    {"standard": 30000, "express": 40000},
            "curtains":    {"standard": 5000,  "express": 8000},
        }
    return {"ok": True, "prices": prices}


@app.post("/api/register")
async def register(req: RegisterRequest):
    existing = await db.get_user_by_phone(req.phone)
    if existing and existing["is_verified"]:
        raise HTTPException(status_code=400, detail="Этот номер уже зарегистрирован")

    password_hash = pwd_context.hash(req.password[:72])
    await db.create_user(req.phone, password_hash, req.first_name)

    code = generate_code()
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, "register", expires_at)
    await send_sms(req.phone, f"ARTEZ: код подтверждения — {code}")

    return {"ok": True, "message": "Код подтверждения отправлен", "phone": req.phone}


@app.post("/api/verify")
async def verify(req: VerifyRequest):
    ok = await db.check_sms_code(req.phone, req.code, "register")
    if not ok:
        raise HTTPException(status_code=400, detail="Неверный или просроченный код")

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
        }
    }


@app.post("/api/resend-code")
async def resend_code(req: ResendCodeRequest):
    user = await db.get_user_by_phone(req.phone)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    code = generate_code()
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=SMS_CODE_TTL_MIN)
    await db.save_sms_code(req.phone, code, req.purpose, expires_at)
    await send_sms(req.phone, f"ARTEZ: код подтверждения — {code}")

    return {"ok": True, "message": "Код отправлен повторно"}


@app.post("/api/login")
async def login(req: LoginRequest):
    user = await db.get_user_by_phone(req.phone)
    if not user or not pwd_context.verify(req.password[:72], user["password_hash"]):
        raise HTTPException(status_code=401, detail="Неверный номер или пароль")

    if not user["is_verified"]:
        raise HTTPException(status_code=403, detail="Номер не подтверждён. Запросите код заново")

    token = create_token(user["id"], user["phone"])
    return {
        "ok": True,
        "token": token,
        "user": {
            "id": user["id"],
            "phone": user["phone"],
            "first_name": user["first_name"],
        }
    }


@app.get("/api/me")
async def me(user = Depends(get_current_user)):
    return {
        "id": user["id"],
        "phone": user["phone"],
        "first_name": user["first_name"],
        "is_verified": user["is_verified"],
    }


@app.get("/api/orders")
async def my_orders(user = Depends(get_current_user)):
    orders = await db.get_orders_by_phone(user["phone"])
    return {"orders": [dict(o) for o in orders]}


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


async def notify_group_new_order(order_num: str, data: "OrderRequest"):
    if not BOT_TOKEN or not GROUP_ID:
        logging.warning("BOT_TOKEN/GROUP_ID not set — skipping group notification")
        return

    full_name = f"{data.first_name} {data.last_name}".strip()
    text = (
        f"🌐 Новая заявка {order_num} (сайт)\n"
        f"━━━━━━━━━━━━━━━\n"
        f"👤 {full_name}\n"
        f"📞 {data.phone}\n"
        f"🏢 {data.branch}\n"
        f"📍 {data.city}\n"
        f"🏠 {data.address}\n"
        f"🗺 {data.location or '—'}\n"
        f"🧺 {data.service}\n"
        f"⚙️ {data.service_type}\n"
        f"📅 {data.pickup_date}\n"
        f"🕐 {data.pickup_time}\n"
        f"━━━━━━━━━━━━━━━"
    )

    tel_phone = (data.phone or "").replace("+", "%2B")
    keyboard = {
        "inline_keyboard": [
            [
                {"text": "✅ Принять заказ", "callback_data": f"accept_{order_num}_0"},
                {"text": "📞 Позвонить", "url": f"tel:{tel_phone}"},
            ],
            [
                {"text": "🚗 Назначить водителя", "callback_data": f"driver_{order_num}_0"},
                {"text": "❌ Отклонить", "callback_data": f"reject_{order_num}_0"},
            ],
        ]
    }

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": GROUP_ID, "text": text, "reply_markup": keyboard}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.warning(f"Telegram notify failed: {resp.status} {body}")
    except Exception as e:
        logging.warning(f"Telegram notify error: {e}")


# ══════════════════════════════════════
#  ADMIN
# ══════════════════════════════════════
class AdminLoginRequest(BaseModel):
    password: str

class SetPriceRequest(BaseModel):
    service_key: str
    type_key: str
    price: int

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
    await db.set_price(req.service_key, req.type_key, req.price)
    return {"ok": True}

@app.get("/api/admin/orders")
async def admin_get_orders(_=Depends(get_admin), status: str = None, limit: int = 50):
    prices = await db.get_admin_orders(status=status, limit=limit)
    return {"ok": True, "orders": [dict(o) for o in prices]}


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
    })

    await notify_group_new_order(order_num, order)

    return {"ok": True, "order_num": order_num}
