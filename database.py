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

        -- ══════════════════════════════════════
        --  СИСТЕМНЫЕ НАСТРОЙКИ (кэш токенов и т.п.)
        -- ══════════════════════════════════════
        CREATE TABLE IF NOT EXISTS config (
            key        VARCHAR(100) PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- Заявки с сайта не имеют Telegram ID — снимаем NOT NULL, если ещё установлен
        ALTER TABLE orders ALTER COLUMN client_tg_id DROP NOT NULL;

        -- ══════════════════════════════════════
        --  ЕДИНИЦЫ ИЗМЕРЕНИЯ (общая таблица с ботом)
        -- ══════════════════════════════════════
        CREATE TABLE IF NOT EXISTS units (
            id          SERIAL PRIMARY KEY,
            key         VARCHAR(20) UNIQUE NOT NULL,
            name_ru     VARCHAR(50) NOT NULL,
            name_uz     VARCHAR(50) NOT NULL,
            symbol_ru   VARCHAR(10) NOT NULL,
            symbol_uz   VARCHAR(10) NOT NULL,
            created_at  TIMESTAMP DEFAULT NOW()
        );

        -- ══════════════════════════════════════
        --  ЦЕНЫ (общая таблица с ботом)
        -- ══════════════════════════════════════
        CREATE TABLE IF NOT EXISTS prices (
            id              SERIAL PRIMARY KEY,
            service_key     VARCHAR(30) NOT NULL,
            type_key        VARCHAR(20) NOT NULL,
            price           INT NOT NULL,
            unit            VARCHAR(20) DEFAULT 'sum/m2',
            unit_key        VARCHAR(20) DEFAULT 'm2',
            min_order       NUMERIC(10,2) DEFAULT NULL,
            updated_at      TIMESTAMP DEFAULT NOW(),
            UNIQUE(service_key, type_key)
        );
        ALTER TABLE prices ADD COLUMN IF NOT EXISTS unit_key VARCHAR(20) DEFAULT 'm2';
        ALTER TABLE prices ADD COLUMN IF NOT EXISTS min_order NUMERIC(10,2) DEFAULT NULL;
        """)

        # Дефолтные единицы измерения (если таблица пуста)
        units_count = await conn.fetchval("SELECT COUNT(*) FROM units")
        if units_count == 0:
            default_units = [
                ("m2",  "Квадратный метр", "Kvadrat metr",  "м²",  "m²"),
                ("m",   "Метр",            "Metr",          "м",   "m"),
                ("pcs", "Штука",           "Dona",          "шт",  "dona"),
                ("cm",  "Сантиметр",       "Santimetr",     "см",  "sm"),
                ("cm2", "Кв. сантиметр",   "Kv. santimetr", "см²", "sm²"),
                ("kg",  "Килограмм",       "Kilogramm",     "кг",  "kg"),
            ]
            await conn.executemany("""
                INSERT INTO units (key, name_ru, name_uz, symbol_ru, symbol_uz)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (key) DO NOTHING
            """, default_units)
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


async def check_sms_rate_limit(phone: str, purpose: str) -> tuple[bool, str]:
    """Возвращает (ok, сообщение_об_ошибке). 60 сек между отправками, макс 5 за час."""
    if not pool: return True, ""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))::INT AS seconds_since_last,
                COUNT(*) AS count_hour
            FROM sms_codes
            WHERE phone=$1 AND purpose=$2 AND created_at > NOW() - INTERVAL '1 hour'
        """, phone, purpose)
        if row and row["count_hour"] and row["count_hour"] > 0:
            secs = row["seconds_since_last"]
            if secs is not None and secs < 60:
                return False, f"Подождите {60 - secs} сек. перед повторной отправкой"
            if row["count_hour"] >= 5:
                return False, "Превышен лимит отправки кодов. Попробуйте через час"
    return True, ""


async def get_config(key: str) -> str | None:
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT value FROM config WHERE key=$1", key)


async def set_config(key: str, value: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO config (key, value, updated_at) VALUES ($1, $2, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, key, value)


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


# ══════════════════════════════════════
#  СОЗДАНИЕ ЗАЯВКИ С САЙТА
# ══════════════════════════════════════
async def get_next_order_num(prefix: str = "ARTEZ") -> str:
    """Возвращает следующий номер заказа на основе данных в БД (общий с ботом счётчик)"""
    if not pool:
        return f"{prefix}-1001"
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT order_num FROM orders
            WHERE order_num LIKE $1
            ORDER BY id DESC
            LIMIT 1
        """, f"{prefix}-%")
        if row and row["order_num"]:
            try:
                last_num = int(row["order_num"].split("-")[-1])
            except (ValueError, IndexError):
                last_num = 1000
        else:
            last_num = 1000
        return f"{prefix}-{last_num + 1}"


async def save_site_order(data: dict) -> str:
    """Сохраняет заявку, оформленную на сайте (source='site'), без обязательного Telegram ID"""
    if not pool:
        return data.get("order_num", "")
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders (
                order_num, source,
                client_tg_id, client_first_name, client_last_name, client_phone,
                branch, city, address, location, service, pickup_date, pickup_time, note,
                status
            ) VALUES (
                $1, 'site',
                NULL, $2, $3, $4,
                $5, $6, $7, $8, $9, $10, $11, $12,
                'new'
            )
            ON CONFLICT (order_num) DO NOTHING
        """,
            data.get("order_num"),
            data.get("first_name"),
            data.get("last_name", ""),
            data.get("phone"),
            data.get("branch"),
            data.get("city"),
            data.get("address"),
            data.get("location"),
            data.get("service"),
            data.get("pickup_date"),
            data.get("pickup_time"),
            data.get("note"),
        )
        await conn.execute("""
            INSERT INTO order_status_history (order_num, new_status, note)
            VALUES ($1, 'new', 'Заявка создана через сайт')
        """, data.get("order_num"))
    return data.get("order_num", "")


# ══════════════════════════════════════
#  ЦЕНЫ (общая таблица с ботом)
# ══════════════════════════════════════
async def get_all_prices() -> dict:
    """Возвращает все цены из таблицы prices: {service_key: {type_key: {price, unit_key, min_order}}}"""
    if not pool:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT service_key, type_key, price, unit_key, min_order FROM prices ORDER BY service_key, type_key"
        )
    result = {}
    for r in rows:
        result.setdefault(r["service_key"], {})[r["type_key"]] = {
            "price": r["price"],
            "unit_key": r["unit_key"],
            "min_order": float(r["min_order"]) if r["min_order"] is not None else None,
        }
    return result


async def set_price(service_key: str, type_key: str, price: int,
                     unit_key: str = None, min_order=None) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO prices (service_key, type_key, price, unit_key, min_order, updated_at)
            VALUES ($1, $2, $3, COALESCE($4, 'm2'), $5, NOW())
            ON CONFLICT (service_key, type_key) DO UPDATE SET
                price      = EXCLUDED.price,
                unit_key   = COALESCE($4, prices.unit_key),
                min_order  = $5,
                updated_at = NOW()
        """, service_key, type_key, price, unit_key, min_order)
    return True


# ══════════════════════════════════════
#  ЕДИНИЦЫ ИЗМЕРЕНИЯ (общая таблица с ботом)
# ══════════════════════════════════════
async def get_all_units():
    if not pool:
        return []
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT * FROM units ORDER BY id")


async def add_unit(key: str, name_ru: str, name_uz: str, symbol_ru: str, symbol_uz: str) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO units (key, name_ru, name_uz, symbol_ru, symbol_uz)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (key) DO UPDATE SET
                name_ru = EXCLUDED.name_ru,
                name_uz = EXCLUDED.name_uz,
                symbol_ru = EXCLUDED.symbol_ru,
                symbol_uz = EXCLUDED.symbol_uz
        """, key, name_ru, name_uz, symbol_ru, symbol_uz)
    return True


async def delete_unit(key: str) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM units WHERE key=$1", key)
    return result != "DELETE 0"


async def get_admin_orders(status: str = None, limit: int = 50):
    if not pool:
        return []
    async with pool.acquire() as conn:
        if status:
            return await conn.fetch(
                "SELECT * FROM orders WHERE status=$1 ORDER BY created_at DESC LIMIT $2",
                status, limit
            )
        return await conn.fetch(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT $1", limit
        )
