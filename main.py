import os
import re
import random
import logging
from datetime import datetime, timedelta, timezone

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


@app.post("/api/register")
async def register(req: RegisterRequest):
    existing = await db.get_user_by_phone(req.phone)
    if existing and existing["is_verified"]:
        raise HTTPException(status_code=400, detail="Этот номер уже зарегистрирован")

    password_hash = pwd_context.hash(req.password)
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
    if not user or not pwd_context.verify(req.password, user["password_hash"]):
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
