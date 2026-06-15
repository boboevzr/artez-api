import os
import asyncpg
import logging

DB_URL = os.getenv("DATABASE_URL", "")

pool = None

async def init_db():
    global pool
    if not DB_URL:
        logging.warning("DATABASE_URL not set, DB disabled")
        return
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
    await create_tables()
    logging.info("✅ API: Database connected")


async def create_tables():
    async with pool.acquire() as conn:
        await conn.execute("""
        -- ══════════════════════════════════════
        --  ПОЛЬЗОВАТЕЛИ САЙТА (личный кабинет)
        -- ══════════════════════════════════════
        CREATE TABLE IF NOT EXISTS users (
            id              SERIAL PRIMARY KEY,
            phone           VARCHAR(20) UNIQUE NOT NULL,
            password_hash   VARCHAR(255) NOT NULL,
            first_name      VARCHAR(100),
            tg_id           BIGINT,                    -- связь с Telegram-клиентом (если есть)
            is_verified     BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        );

        -- ══════════════════════════════════════
        --  SMS-КОДЫ ПОДТВЕРЖДЕНИЯ
        -- ══════════════════════════════════════
        CREATE TABLE IF NOT EXISTS sms_codes (
            id              SERIAL PRIMARY KEY,
            phone           VARCHAR(20) NOT NULL,
            code            VARCHAR(6) NOT NULL,
            purpose         VARCHAR(20) DEFAULT 'register'
                            CHECK (purpose IN ('register','login','reset')),
            expires_at      TIMESTAMP NOT NULL,
            used            BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_users_phone     ON users(phone);
        CREATE INDEX IF NOT EXISTS idx_sms_codes_phone ON sms_codes(phone);
        """)
    logging.info("✅ API: Tables created/verified")


# ══════════════════════════════════════
#  ПОЛЬЗОВАТЕЛИ
# ══════════════════════════════════════
async def get_user_by_phone(phone: str):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE phone=$1", phone)


async def get_user_by_id(user_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE id=$1", user_id)


async def create_user(phone: str, password_hash: str, first_name: str):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            INSERT INTO users (phone, password_hash, first_name, is_verified)
            VALUES ($1, $2, $3, FALSE)
            ON CONFLICT (phone) DO UPDATE SET
                password_hash = EXCLUDED.password_hash,
                first_name    = EXCLUDED.first_name,
                updated_at    = NOW()
            RETURNING *
        """, phone, password_hash, first_name)


async def verify_user(phone: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET is_verified = TRUE, updated_at = NOW()
            WHERE phone = $1
        """, phone)


async def link_user_tg_id(phone: str, tg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET tg_id = $2, updated_at = NOW()
            WHERE phone = $1
        """, phone, tg_id)


# ══════════════════════════════════════
#  SMS-КОДЫ
# ══════════════════════════════════════
async def save_sms_code(phone: str, code: str, purpose: str, expires_at):
    if not pool: return
    async with pool.acquire() as conn:
        # Деактивируем старые коды для этого номера и цели
        await conn.execute("""
            UPDATE sms_codes SET used = TRUE
            WHERE phone=$1 AND purpose=$2 AND used = FALSE
        """, phone, purpose)
        await conn.execute("""
            INSERT INTO sms_codes (phone, code, purpose, expires_at)
            VALUES ($1, $2, $3, $4)
        """, phone, code, purpose, expires_at)


async def check_sms_code(phone: str, code: str, purpose: str) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id FROM sms_codes
            WHERE phone=$1 AND code=$2 AND purpose=$3
              AND used = FALSE AND expires_at > NOW()
            ORDER BY id DESC LIMIT 1
        """, phone, code, purpose)
        if not row:
            return False
        await conn.execute("UPDATE sms_codes SET used = TRUE WHERE id=$1", row["id"])
        return True


# ══════════════════════════════════════
#  ЗАКАЗЫ КЛИЕНТА (для личного кабинета)
# ══════════════════════════════════════
async def get_orders_by_phone(phone: str):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT order_num, service, branch, city, address, status,
                   pickup_date, pickup_time, created_at
            FROM orders
            WHERE client_phone = $1
            ORDER BY created_at DESC
            LIMIT 50
        """, phone)


async def get_orders_by_tg_id(tg_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT order_num, service, branch, city, address, status,
                   pickup_date, pickup_time, created_at
            FROM orders
            WHERE client_tg_id = $1
            ORDER BY created_at DESC
            LIMIT 50
        """, tg_id)
