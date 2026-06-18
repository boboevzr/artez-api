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
    # ── Шаг 1: основные таблицы ──────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id              SERIAL PRIMARY KEY,
            phone           VARCHAR(20) UNIQUE NOT NULL,
            password_hash   VARCHAR(255) NOT NULL,
            first_name      VARCHAR(100),
            tg_id           BIGINT,
            is_verified     BOOLEAN DEFAULT FALSE,
            created_at      TIMESTAMP DEFAULT NOW(),
            updated_at      TIMESTAMP DEFAULT NOW()
        );
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
        CREATE TABLE IF NOT EXISTS config (
            key        VARCHAR(100) PRIMARY KEY,
            value      TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS units (
            id          SERIAL PRIMARY KEY,
            key         VARCHAR(20) UNIQUE NOT NULL,
            name_ru     VARCHAR(50) NOT NULL,
            name_uz     VARCHAR(50) NOT NULL,
            symbol_ru   VARCHAR(10) NOT NULL,
            symbol_uz   VARCHAR(10) NOT NULL,
            created_at  TIMESTAMP DEFAULT NOW()
        );
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
        CREATE TABLE IF NOT EXISTS staff (
            id              SERIAL PRIMARY KEY,
            first_name      VARCHAR(100) NOT NULL,
            last_name       VARCHAR(100),
            middle_name     VARCHAR(100),
            phone           VARCHAR(20),
            login           VARCHAR(50),
            password_hash   TEXT,
            role            VARCHAR(30) DEFAULT 'callcenter',
            position        VARCHAR(100),
            branch          VARCHAR(50),
            tg_id           VARCHAR(50),
            tg_username     VARCHAR(100),
            salary_type     VARCHAR(20),
            salary_rate     NUMERIC(10,2),
            hire_date       DATE,
            note            TEXT,
            active          BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_users_phone     ON users(phone);
        CREATE INDEX IF NOT EXISTS idx_sms_codes_phone ON sms_codes(phone);
        """)

    # Опциональные миграции других таблиц — каждый отдельно чтобы не блокировать
    other_migrations = [
        "ALTER TABLE orders ALTER COLUMN client_tg_id DROP NOT NULL",
        "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_source_check",
        "ALTER TABLE orders ADD CONSTRAINT orders_source_check CHECK (source IN ('bot','site','staff'))",
        "ALTER TABLE prices  ADD COLUMN IF NOT EXISTS unit_key  VARCHAR(20)   DEFAULT 'm2'",
        "ALTER TABLE prices  ADD COLUMN IF NOT EXISTS min_order NUMERIC(10,2) DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS address   VARCHAR(200)  DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS car_plate VARCHAR(20)   DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS osago_expiry DATE       DEFAULT NULL",
        "ALTER TABLE orders  ADD COLUMN IF NOT EXISTS total_price INT         DEFAULT NULL",
    ]
    async with pool.acquire() as c:
        for sql in other_migrations:
            try:
                await c.execute(sql)
            except Exception:
                pass

    # ── Шаг 2: миграции staff (добавляем недостающие колонки) ────────────
    staff_migrations = [
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS phone         VARCHAR(20)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS last_name     VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS login         VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS password_hash TEXT",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS role          VARCHAR(30) DEFAULT 'callcenter'",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS position      VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS branch        VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS tg_id         VARCHAR(50)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS tg_username   VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_type   VARCHAR(20)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_rate   NUMERIC(10,2)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS hire_date     DATE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS note          TEXT",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS middle_name   VARCHAR(100)",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS active        BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS updated_at    TIMESTAMPTZ DEFAULT NOW()",
        "CREATE INDEX IF NOT EXISTS idx_staff_login ON staff(login)",
        # Снять CHECK constraints на role — бот мог ограничить список ролей
        """DO $$ DECLARE r RECORD;
           BEGIN
             FOR r IN SELECT conname FROM pg_constraint
                      WHERE conrelid='staff'::regclass AND contype='c'
             LOOP EXECUTE format('ALTER TABLE staff DROP CONSTRAINT %I', r.conname);
             END LOOP;
           END $$""",
        # Снять NOT NULL с role и tg_id если был
        "ALTER TABLE staff ALTER COLUMN role   DROP NOT NULL",
        "ALTER TABLE staff ALTER COLUMN tg_id  DROP NOT NULL",
    ]
    async with pool.acquire() as c:
        for sql in staff_migrations:
            try:
                await c.execute(sql)
            except Exception:
                pass  # колонка или индекс уже существует

    # ── Шаг 3: таблица leads ─────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id              SERIAL PRIMARY KEY,
            lead_num        VARCHAR(20) UNIQUE,
            client_name     VARCHAR(200),
            client_phone    VARCHAR(20) NOT NULL,
            service         VARCHAR(100),
            branch          VARCHAR(50),
            city            VARCHAR(100),
            address         TEXT,
            note            TEXT,
            status          VARCHAR(30) DEFAULT 'new'
                            CHECK (status IN ('new','contacted','qualified','converted','lost')),
            assigned_to     INTEGER REFERENCES staff(id),
            created_by      INTEGER REFERENCES staff(id),
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_leads_phone  ON leads(client_phone);
        CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
        """)

    # ── Шаг 4: дефолтные единицы измерения ───────────────────────────────
    async with pool.acquire() as c:
        units_count = await c.fetchval("SELECT COUNT(*) FROM units")
        if units_count == 0:
            default_units = [
                ("m2",  "Квадратный метр", "Kvadrat metr",  "м²",  "m²"),
                ("m",   "Метр",            "Metr",          "м",   "m"),
                ("pcs", "Штука",           "Dona",          "шт",  "dona"),
                ("cm",  "Сантиметр",       "Santimetr",     "см",  "sm"),
                ("cm2", "Кв. сантиметр",   "Kv. santimetr", "см²", "sm²"),
                ("kg",  "Килограмм",       "Kilogramm",     "кг",  "kg"),
            ]
            await c.executemany("""
                INSERT INTO units (key, name_ru, name_uz, symbol_ru, symbol_uz)
                VALUES ($1, $2, $3, $4, $5) ON CONFLICT (key) DO NOTHING
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


async def update_user_name(user_id: int, first_name: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET first_name=$2, updated_at=NOW() WHERE id=$1
        """, user_id, first_name)


async def update_user_password(user_id: int, password_hash: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET password_hash=$2, updated_at=NOW() WHERE id=$1
        """, user_id, password_hash)


async def update_user_profile(user_id: int, first_name: str, address: str = None,
                               car_plate: str = None, osago_expiry=None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET first_name=$2, address=$3, car_plate=$4, osago_expiry=$5, updated_at=NOW() WHERE id=$1
        """, user_id, first_name, address, car_plate, osago_expiry)


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
                   pickup_date, pickup_time, total_price, created_at
            FROM orders
            WHERE client_phone = $1
            ORDER BY created_at DESC
            LIMIT 50
        """, phone)


async def cancel_order_by_phone(order_num: str, phone: str):
    """Отменяет заказ со статусом 'new', принадлежащий этому номеру.
    Возвращает dict с данными заказа или None если не найден/нельзя отменить."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE orders SET status='cancelled'
            WHERE order_num=$1 AND client_phone=$2 AND status='new'
            RETURNING order_num, client_first_name, client_last_name, client_phone, service, branch
        """, order_num, phone)
        if not row:
            return None
        r = dict(row)
        r['client_name'] = f"{r.pop('client_first_name') or ''} {r.pop('client_last_name') or ''}".strip()
        return r


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


async def save_site_order(data: dict, source: str = "site") -> str:
    """Сохраняет заявку без обязательного Telegram ID. source: 'site' | 'staff'"""
    if not pool:
        return data.get("order_num", "")
    source_note = {"site": "Заявка создана через сайт", "staff": "Заявка создана сотрудником"}.get(source, "Заявка создана")
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO orders (
                order_num, source,
                client_tg_id, client_first_name, client_last_name, client_phone,
                branch, city, address, location, service, pickup_date, pickup_time, note,
                total_price, status
            ) VALUES (
                $1, $2,
                NULL, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12, $13,
                $14, 'new'
            )
            ON CONFLICT (order_num) DO NOTHING
        """,
            data.get("order_num"),
            source,
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
            data.get("total_price"),
        )
        await conn.execute("""
            INSERT INTO order_status_history (order_num, new_status, note)
            VALUES ($1, 'new', $2)
        """, data.get("order_num"), source_note)
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


# ══════════════════════════════════════
#  СОТРУДНИКИ
# ══════════════════════════════════════
async def get_staff_by_login(login: str):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE login=$1 AND active=TRUE", login
        )

async def get_staff_by_id(staff_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM staff WHERE id=$1", staff_id)

async def get_all_staff():
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM staff ORDER BY active DESC, first_name",
        )

async def create_staff(data: dict) -> int:
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO staff (first_name, last_name, middle_name, phone, login, password_hash,
                               role, position, branch, tg_id, tg_username,
                               salary_type, salary_rate, hire_date, note)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            RETURNING id
        """, data["first_name"], data.get("last_name"), data.get("middle_name"),
            data.get("phone"), data["login"], data["password_hash"],
            data.get("role","callcenter"), data.get("position"), data.get("branch"),
            data.get("tg_id"), data.get("tg_username"),
            data.get("salary_type"), data.get("salary_rate"),
            data.get("hire_date"), data.get("note"))

async def update_staff(staff_id: int, **kwargs):
    if not pool or not kwargs: return
    allowed = {"first_name","last_name","middle_name","phone","login","role","position",
               "branch","tg_id","tg_username","salary_type","salary_rate","hire_date",
               "note","active","is_active"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if "is_active" in fields:
        fields["active"] = fields.pop("is_active")
    if not fields: return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE staff SET {sets}, updated_at=NOW() WHERE id=$1",
            staff_id, *vals
        )

async def update_staff_password(staff_id: int, password_hash: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE staff SET password_hash=$2, updated_at=NOW() WHERE id=$1",
            staff_id, password_hash
        )

# ══════════════════════════════════════
#  ЛИДЫ
# ══════════════════════════════════════
async def get_next_lead_num() -> str:
    if not pool: return "LEAD-0001"
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM leads") or 0
        return f"LEAD-{count + 1:04d}"

async def create_lead(data: dict) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO leads (lead_num, client_name, client_phone, service, branch,
                               city, address, note, status, assigned_to, created_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            RETURNING *
        """, data["lead_num"], data.get("client_name"), data["client_phone"],
            data.get("service"), data.get("branch"), data.get("city"),
            data.get("address"), data.get("note"), data.get("status","new"),
            data.get("assigned_to"), data.get("created_by"))
        return dict(row)

async def get_leads(status: str = None, branch: str = None,
                    assigned_to: int = None, limit: int = 100):
    if not pool: return []
    async with pool.acquire() as conn:
        filters, args = ["1=1"], []
        if status:
            args.append(status);   filters.append(f"status=${len(args)}")
        if branch:
            args.append(branch);   filters.append(f"branch=${len(args)}")
        if assigned_to:
            args.append(assigned_to); filters.append(f"assigned_to=${len(args)}")
        args.append(limit)
        return await conn.fetch(
            f"SELECT * FROM leads WHERE {' AND '.join(filters)} "
            f"ORDER BY created_at DESC LIMIT ${len(args)}", *args
        )

async def update_lead_status(lead_id: int, status: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE leads SET status=$2, updated_at=NOW() WHERE id=$1", lead_id, status
        )
