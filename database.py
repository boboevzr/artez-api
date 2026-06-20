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
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS total_price   INT          DEFAULT NULL",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS discount_sum  NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS address       TEXT         DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_items   BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS plain_password   VARCHAR(100) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS washer_login   VARCHAR(50)  DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_width_cm  NUMERIC(8,1) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_length_cm NUMERIC(8,1) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_sqm       NUMERIC(8,3) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS actual_total_sum NUMERIC(12,2) DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS measure_status   VARCHAR(20)  DEFAULT 'pending'",
        # CRM leads расширение
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS converted_by   INTEGER REFERENCES staff(id)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS volunteer_id   INTEGER REFERENCES staff(id)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS converted_order VARCHAR(20)",
        "ALTER TABLE leads ADD COLUMN IF NOT EXISTS lead_code       VARCHAR(20) UNIQUE",
        "ALTER TABLE leads DROP CONSTRAINT IF EXISTS leads_status_check",
        "ALTER TABLE leads ADD CONSTRAINT leads_status_check CHECK (status IN ('new','contacted','callback','converted','lost','no_answer'))",
        # Агент: временный пароль
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS temp_password_hash    VARCHAR(255) DEFAULT NULL",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS temp_password_expires TIMESTAMPTZ  DEFAULT NULL",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS must_change_password  BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS site_user_id          INTEGER REFERENCES users(id)",
        # Бот: верифицированный TG-номер клиента
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS tg_phone  VARCHAR(20) DEFAULT NULL",
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS language  VARCHAR(5)  DEFAULT 'ru'",
        # Users: привязка Telegram ID
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS tg_id BIGINT",
        # Staff: уникальный логин для ON CONFLICT
        "CREATE UNIQUE INDEX IF NOT EXISTS staff_login_unique ON staff(login)",
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

    # ── Шаг 3: таблица crm_clients ───────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS crm_clients (
            id            SERIAL PRIMARY KEY,
            phone         VARCHAR(20) UNIQUE NOT NULL,
            phone2        VARCHAR(20),
            first_name    VARCHAR(100),
            last_name     VARCHAR(100),
            tg_id         BIGINT,
            tg_username   VARCHAR(100),
            source        VARCHAR(20) DEFAULT 'unknown',
            status        VARCHAR(20) DEFAULT 'new',
            note          TEXT,
            orders_count  INT DEFAULT 0,
            total_spent   NUMERIC(12,2) DEFAULT 0,
            last_order_at TIMESTAMPTZ,
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            updated_at    TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_crm_clients_phone  ON crm_clients(phone);
        CREATE INDEX IF NOT EXISTS idx_crm_clients_tg_id  ON crm_clients(tg_id);
        CREATE INDEX IF NOT EXISTS idx_crm_clients_status ON crm_clients(status);
        """)

    # ── Шаг 3б: таблица contacts (справочник) ───────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS contacts (
            id          SERIAL PRIMARY KEY,
            first_name  VARCHAR(100) DEFAULT '',
            last_name   VARCHAR(100) DEFAULT '',
            middle_name VARCHAR(100) DEFAULT '',
            phone       VARCHAR(20) NOT NULL,
            phone2      VARCHAR(20) DEFAULT '',
            address     TEXT DEFAULT '',
            source      VARCHAR(50) DEFAULT 'ARTEZ',
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(phone)
        );
        CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
        CREATE INDEX IF NOT EXISTS idx_contacts_name  ON contacts(first_name, last_name);
        """)
        # Добавляем short_address если ещё нет (миграция)
        await c.execute("""
        ALTER TABLE contacts ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT '';
        CREATE INDEX IF NOT EXISTS idx_contacts_short_addr ON contacts(short_address);
        """)

    # ── Шаг 4: таблица leads ─────────────────────────────────────────────
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

    # ── Шаг 4б: CRM — журнал звонков и напоминания ───────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS lead_calls (
            id          SERIAL PRIMARY KEY,
            lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            operator_id INTEGER REFERENCES staff(id),
            action      VARCHAR(50) NOT NULL,
            note        TEXT,
            scheduled_at TIMESTAMPTZ,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_lead_calls_lead ON lead_calls(lead_id);
        CREATE TABLE IF NOT EXISTS lead_reminders (
            id          SERIAL PRIMARY KEY,
            lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
            staff_id    INTEGER REFERENCES staff(id),
            remind_at   TIMESTAMPTZ NOT NULL,
            message     TEXT,
            sent_browser BOOLEAN DEFAULT FALSE,
            sent_tg      BOOLEAN DEFAULT FALSE,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_lead_reminders_staff ON lead_reminders(staff_id, sent_browser);
        """)

    # ── Шаг 4в: позиции услуг в заказах ─────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER NOT NULL,
            service         VARCHAR(200) NOT NULL,
            width_cm        NUMERIC(8,1),
            length_cm       NUMERIC(8,1),
            sqm             NUMERIC(8,3) NOT NULL,
            price_per_sqm   NUMERIC(10,2) NOT NULL DEFAULT 0,
            total_sum       NUMERIC(12,2) GENERATED ALWAYS AS (sqm * price_per_sqm) STORED,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
        """)

    # ── Шаг 5: дефолтные единицы измерения ───────────────────────────────
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

    # ── Шаг 5: таблица шаблонов Telegram-уведомлений ────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS tg_status_messages (
            status      VARCHAR(30) PRIMARY KEY,
            enabled     BOOLEAN     DEFAULT TRUE,
            message_ru  TEXT        DEFAULT '',
            message_uz  TEXT        DEFAULT ''
        );
        """)
        # Дефолтные шаблоны если таблица пустая
        defaults = [
            ("new",       True,  "🆕 Ваша заявка #{order_num} принята!\n\nУслуга: {service}\nДата вывоза: {pickup_date}\n\nМы свяжемся с вами для подтверждения.",
                                 "🆕 #{order_num} raqamli arizangiz qabul qilindi!\n\nXizmat: {service}\nOlib ketish sanasi: {pickup_date}\n\nTasdiqlash uchun siz bilan bog'lanamiz."),
            ("confirmed", True,  "✅ Ваш заказ #{order_num} подтверждён!\n\nВодитель приедет: {pickup_date}\n📞 По вопросам: 1221",
                                 "✅ #{order_num} raqamli buyurtmangiz tasdiqlandi!\n\nHaydovchi keladi: {pickup_date}\n📞 Savollar uchun: 1221"),
            ("pickup",    False, "🚗 Водитель выехал за вашим ковром #{order_num}.\n\nАдрес: {address}",
                                 "🚗 Haydovchi #{order_num} gilamingiz uchun yo'lga chiqdi.\n\nManzil: {address}"),
            ("received",  True,  "📥 Ваш ковёр #{order_num} доставлен в мастерскую.\n\nНачинаем обработку. Сообщим о готовности!",
                                 "📥 #{order_num} gilamingiz ustaxonaga yetkazildi.\n\nIshlashni boshladik. Tayyor bo'lganda xabar beramiz!"),
            ("washing",   False, "🧼 Ваш ковёр #{order_num} на мойке.",
                                 "🧼 #{order_num} gilamingiz yuvish jarayonida."),
            ("drying",    False, "💨 Ваш ковёр #{order_num} на сушке.",
                                 "💨 #{order_num} gilamingiz quritilmoqda."),
            ("packing",   False, "📦 Ваш ковёр #{order_num} упаковывается.",
                                 "📦 #{order_num} gilamingiz qadoqlanmoqda."),
            ("ready",     True,  "✅ Ваш ковёр #{order_num} готов!\n\nМожем доставить или вы можете забрать сами.\n📞 Позвоните: 1221",
                                 "✅ #{order_num} gilamingiz tayyor!\n\nYetkazib berishimiz yoki o'zingiz olib ketishingiz mumkin.\n📞 Qo'ng'iroq qiling: 1221"),
            ("delivery",  True,  "🚚 Ваш ковёр #{order_num} в пути!\n\nВодитель скоро будет у вас. Ждите звонка.",
                                 "🚚 #{order_num} gilamingiz yo'lda!\n\nHaydovchi tez orada sizga etib keladi. Qo'ng'iroqni kuting."),
            ("delivered", True,  "🎉 Ваш ковёр #{order_num} доставлен!\n\nСпасибо что выбрали ARTEZ. Будем рады видеть вас снова! ⭐",
                                 "🎉 #{order_num} gilamingiz yetkazildi!\n\nARTEZ ni tanlaganingiz uchun rahmat. Yana ko'rishishni xohlaymiz! ⭐"),
            ("cancelled", True,  "❌ Ваш заказ #{order_num} отменён.\n\nЕсли это ошибка — позвоните нам: 1221",
                                 "❌ #{order_num} raqamli buyurtmangiz bekor qilindi.\n\nXato bo'lsa — qo'ng'iroq qiling: 1221"),
        ]
        await c.executemany("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz)
            VALUES ($1, $2, $3, $4) ON CONFLICT (status) DO NOTHING
        """, defaults)

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

async def get_user_by_tg_id(tg_id):
    if not pool: return None
    async with pool.acquire() as conn:
        # Пробуем как целое число, затем как строку
        try:
            return await conn.fetchrow("SELECT * FROM users WHERE tg_id=$1", int(tg_id))
        except Exception:
            try:
                return await conn.fetchrow("SELECT * FROM users WHERE tg_id::text=$1", str(tg_id))
            except Exception:
                return None


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

async def get_all_site_users(search: str = "", limit: int = 200):
    if not pool: return []
    async with pool.acquire() as conn:
        if search:
            return await conn.fetch(
                "SELECT id, phone, first_name, is_verified, tg_id, created_at "
                "FROM users WHERE phone ILIKE $1 OR first_name ILIKE $1 "
                "ORDER BY created_at DESC LIMIT $2",
                f"%{search}%", limit
            )
        return await conn.fetch(
            "SELECT id, phone, first_name, is_verified, tg_id, created_at "
            "FROM users ORDER BY created_at DESC LIMIT $1", limit
        )


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
                wait = 60 - secs
                return False, f"Подождите {wait} сек. / {wait} soniya kuting"
            if row["count_hour"] >= 5:
                return False, "Превышен лимит. Попробуйте через час / Limit oshdi. 1 soatdan keyin urinib ko'ring"
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
                branch, city, address, short_address, location, service, pickup_date, pickup_time, note,
                total_price, status
            ) VALUES (
                $1, $2,
                NULL, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12, $13, $14,
                $15, 'new'
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
            data.get("short_address", ""),
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
        q = """
            SELECT o.*,
                   COALESCE(i.cnt, 0)::int AS item_count,
                   COALESCE(i.corr, 0)::int AS corrected_count
            FROM orders o
            LEFT JOIN (
                SELECT order_id, COUNT(*) AS cnt,
                       COUNT(*) FILTER (WHERE measure_status='corrected') AS corr
                FROM order_items GROUP BY order_id
            ) i ON i.order_id = o.id
            {where}
            ORDER BY o.created_at DESC LIMIT $1
        """
        if status:
            rows = await conn.fetch(
                q.format(where="WHERE o.status=$2"), limit, status)
        else:
            rows = await conn.fetch(q.format(where=""), limit)
        return rows


# ══════════════════════════════════════
#  СОТРУДНИКИ
# ══════════════════════════════════════
async def get_staff_by_login(login: str):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE login=$1 AND active=TRUE", login
        )

async def get_staff_by_site_user(site_user_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM staff WHERE site_user_id=$1 AND active=TRUE", site_user_id
        )

async def link_staff_to_site_user(staff_id: int, site_user_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE staff SET site_user_id=$2 WHERE id=$1", staff_id, site_user_id
        )

async def get_staff_by_tg_id(tg_id):
    if not pool: return None
    async with pool.acquire() as conn:
        try:
            return await conn.fetchrow(
                "SELECT * FROM staff WHERE tg_id=$1 AND active=TRUE", int(tg_id))
        except Exception:
            try:
                return await conn.fetchrow(
                    "SELECT * FROM staff WHERE tg_id::text=$1 AND active=TRUE", str(tg_id))
            except Exception:
                return None

async def create_agent_from_user(user: dict, password_hash: str) -> int:
    """Создаёт staff-аккаунт агента из пользователя сайта."""
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchval("""
            INSERT INTO staff (first_name, phone, login, password_hash, role,
                               tg_id, site_user_id, active)
            VALUES ($1,$2,$3,$4,'agent',$5,$6,TRUE)
            ON CONFLICT (login) DO NOTHING
            RETURNING id
        """, user["first_name"], user["phone"], user["phone"],
            password_hash, user.get("tg_id"), user["id"])

async def set_staff_temp_password(staff_id: int, temp_hash: str, expires_at):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE staff SET temp_password_hash=$2, temp_password_expires=$3,
                             must_change_password=TRUE
            WHERE id=$1
        """, staff_id, temp_hash, expires_at)

async def clear_staff_temp_password(staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE staff SET temp_password_hash=NULL, temp_password_expires=NULL,
                             must_change_password=FALSE
            WHERE id=$1
        """, staff_id)

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
                               plain_password, role, position, branch, tg_id, tg_username,
                               salary_type, salary_rate, hire_date, note)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            RETURNING id
        """, data["first_name"], data.get("last_name"), data.get("middle_name"),
            data.get("phone"), data["login"], data["password_hash"],
            data.get("plain_password"),
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

async def update_staff_password(staff_id: int, password_hash: str, plain: str = None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE staff SET password_hash=$2, plain_password=$3, updated_at=NOW() WHERE id=$1",
            staff_id, password_hash, plain
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
                               city, address, short_address, note, status, assigned_to, created_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            RETURNING *
        """, data["lead_num"], data.get("client_name"), data["client_phone"],
            data.get("service"), data.get("branch"), data.get("city"),
            data.get("address"), data.get("short_address", ""), data.get("note"),
            data.get("status","new"), data.get("assigned_to"), data.get("created_by"))
        return dict(row)

async def get_leads(status: str = None, branch: str = None,
                    assigned_to: int = None, limit: int = 100):
    if not pool: return []
    async with pool.acquire() as conn:
        filters, args = ["1=1"], []
        if status:
            args.append(status);   filters.append(f"l.status=${len(args)}")
        if branch:
            args.append(branch);   filters.append(f"l.branch=${len(args)}")
        if assigned_to:
            args.append(assigned_to); filters.append(f"l.assigned_to=${len(args)}")
        args.append(limit)
        return await conn.fetch(
            f"""SELECT l.*,
                       s.first_name   AS creator_first_name,
                       s.last_name    AS creator_last_name,
                       s.position     AS creator_position,
                       vol.first_name AS volunteer_first_name,
                       vol.last_name  AS volunteer_last_name
                FROM leads l
                LEFT JOIN staff s   ON s.id   = l.created_by
                LEFT JOIN staff vol ON vol.id = l.volunteer_id
                WHERE {' AND '.join(filters)}
                ORDER BY l.created_at DESC LIMIT ${len(args)}""", *args
        )

async def update_lead_status(lead_id: int, status: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE leads SET status=$2, updated_at=NOW() WHERE id=$1", lead_id, status
        )

async def update_lead(lead_id: int, **kwargs) -> dict | None:
    if not pool: return None
    allowed = {"client_name","client_phone","service","branch","city","address","short_address","note","status"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return None
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE leads SET {set_parts}, updated_at=NOW() WHERE id=$1 RETURNING *",
            lead_id, *list(fields.values()))
        return dict(row) if row else None

async def delete_lead(lead_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM leads WHERE id=$1", lead_id)
        return res == "DELETE 1"

async def get_leads_by_agent(agent_id: int, status: str = None):
    if not pool: return []
    async with pool.acquire() as conn:
        filters = ["(l.created_by=$1 OR l.volunteer_id=$1)"]
        args = [agent_id]
        if status:
            args.append(status); filters.append(f"l.status=${len(args)}")
        return await conn.fetch(
            f"""SELECT l.*,
                       s.first_name AS creator_first_name, s.last_name AS creator_last_name,
                       s.position AS creator_position
                FROM leads l LEFT JOIN staff s ON s.id = l.created_by
                WHERE {' AND '.join(filters)} ORDER BY l.created_at DESC LIMIT 200""", *args)

async def generate_lead_code() -> str:
    if not pool: return "L-0001"
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM leads") or 0
        return f"L-{count+1:04d}"

async def set_lead_code(lead_id: int, code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE leads SET lead_code=$2 WHERE id=$1 AND lead_code IS NULL", lead_id, code)

async def get_lead_by_id(lead_id: int):
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT l.*,
                   s.first_name  AS creator_first_name,
                   s.last_name   AS creator_last_name,
                   s.position    AS creator_position,
                   vol.first_name AS volunteer_first_name,
                   vol.last_name  AS volunteer_last_name,
                   conv.first_name AS converted_first_name,
                   conv.last_name  AS converted_last_name
            FROM leads l
            LEFT JOIN staff s    ON s.id   = l.created_by
            LEFT JOIN staff vol  ON vol.id = l.volunteer_id
            LEFT JOIN staff conv ON conv.id = l.converted_by
            WHERE l.id = $1
        """, lead_id)

# ── lead_calls (журнал звонков) ───────────────────────────────────────

async def add_lead_call(lead_id: int, operator_id: int, action: str,
                         note: str = None, scheduled_at=None):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO lead_calls (lead_id, operator_id, action, note, scheduled_at)
            VALUES ($1,$2,$3,$4,$5) RETURNING *
        """, lead_id, operator_id, action, note, scheduled_at)
        return dict(row) if row else None

async def get_lead_calls(lead_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT lc.*, s.first_name, s.last_name, s.position
            FROM lead_calls lc
            LEFT JOIN staff s ON s.id = lc.operator_id
            WHERE lc.lead_id = $1
            ORDER BY lc.created_at DESC
        """, lead_id)

# ── lead_reminders ────────────────────────────────────────────────────

async def add_lead_reminder(lead_id: int, staff_id: int, remind_at, message: str = None):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO lead_reminders (lead_id, staff_id, remind_at, message)
            VALUES ($1,$2,$3,$4) RETURNING *
        """, lead_id, staff_id, remind_at, message)
        return dict(row) if row else None

async def get_due_reminders(staff_id: int):
    """Возвращает напоминания которые уже наступили, ещё не отправлены в браузер."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, l.client_name, l.client_phone, l.lead_code
            FROM lead_reminders r
            JOIN leads l ON l.id = r.lead_id
            WHERE r.staff_id = $1 AND r.remind_at <= NOW() AND r.sent_browser = FALSE
        """, staff_id)

async def mark_reminder_sent(reminder_id: int, channel: str = "browser"):
    if not pool: return
    async with pool.acquire() as conn:
        col = "sent_tg" if channel == "tg" else "sent_browser"
        await conn.execute(f"UPDATE lead_reminders SET {col}=TRUE WHERE id=$1", reminder_id)

async def get_pending_tg_reminders():
    """Для фонового воркера — все напоминания для отправки в Telegram."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, l.client_name, l.client_phone, l.lead_code,
                   s.tg_id AS staff_tg_id,
                   s.first_name AS staff_first_name,
                   s.last_name  AS staff_last_name
            FROM lead_reminders r
            JOIN leads l  ON l.id  = r.lead_id
            JOIN staff s  ON s.id  = r.staff_id
            WHERE r.remind_at <= NOW() AND r.sent_tg = FALSE
        """)

async def convert_lead_to_order(lead_id: int, order_num: str, converted_by: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE leads SET status='converted', converted_by=$2,
                             converted_order=$3, updated_at=NOW()
            WHERE id=$1
        """, lead_id, converted_by, order_num)


# ══════════════════════════════════════
#  CRM КЛИЕНТЫ
# ══════════════════════════════════════
async def upsert_crm_client(phone: str, first_name: str = "", last_name: str = "",
                             tg_id: int = None, tg_username: str = None,
                             source: str = "unknown", address: str = "",
                             short_address: str = "") -> dict:
    """Создаёт или обновляет запись клиента. Статус не понижается."""
    if not pool or not phone:
        return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO crm_clients (phone, first_name, last_name, tg_id, tg_username, source, address, short_address)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (phone) DO UPDATE SET
                first_name    = CASE WHEN $2 != '' THEN $2 ELSE crm_clients.first_name END,
                last_name     = CASE WHEN $3 != '' THEN $3 ELSE crm_clients.last_name END,
                tg_id         = COALESCE($4, crm_clients.tg_id),
                tg_username   = CASE WHEN $5 IS NOT NULL AND $5 != ''
                                     THEN $5 ELSE crm_clients.tg_username END,
                address       = CASE WHEN $7 != '' THEN $7 ELSE crm_clients.address END,
                short_address = CASE WHEN $8 != '' THEN $8 ELSE crm_clients.short_address END,
                updated_at    = NOW()
            RETURNING *
        """, phone, first_name or "", last_name or "", tg_id, tg_username, source,
             address or "", short_address or "")
        return dict(row) if row else {}


async def refresh_crm_client_stats(phone: str):
    """Пересчитывает orders_count и last_order_at из таблицы orders."""
    if not pool or not phone:
        return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE crm_clients SET
                orders_count  = (SELECT COUNT(*) FROM orders WHERE client_phone = $1),
                last_order_at = (SELECT MAX(created_at) FROM orders WHERE client_phone = $1),
                status = CASE
                    WHEN status NOT IN ('vip','inactive')
                         AND (SELECT COUNT(*) FROM orders
                              WHERE client_phone = $1 AND status = 'done') > 0
                    THEN 'active'
                    ELSE status
                END,
                updated_at = NOW()
            WHERE phone = $1
        """, phone)


async def get_crm_client_by_phone(phone: str) -> dict | None:
    if not pool:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM crm_clients WHERE phone = $1", phone)
        return dict(row) if row else None


async def get_crm_client_by_id(client_id: int) -> dict | None:
    if not pool:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM crm_clients WHERE id = $1", client_id)
        return dict(row) if row else None


async def get_crm_clients_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch("""
                SELECT * FROM crm_clients
                WHERE phone ILIKE $1 OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR short_address ILIKE $1 OR address ILIKE $1
                ORDER BY updated_at DESC LIMIT $2 OFFSET $3
            """, f"%{search}%", limit, offset)
        else:
            rows = await conn.fetch("""
                SELECT * FROM crm_clients
                ORDER BY updated_at DESC LIMIT $1 OFFSET $2
            """, limit, offset)
        return [dict(r) for r in rows]


async def update_crm_client(client_id: int, **kwargs) -> dict | None:
    if not pool or not kwargs:
        return None
    allowed = {"first_name", "last_name", "phone2", "status", "note", "address", "short_address"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return None
    set_parts = ", ".join(f"{k} = ${i+2}" for i, k in enumerate(fields))
    vals = [client_id] + list(fields.values())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE crm_clients SET {set_parts}, updated_at=NOW() WHERE id=$1 RETURNING *",
            *vals
        )
        return dict(row) if row else None


async def get_crm_client_orders(phone: str, limit: int = 20) -> list[dict]:
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT order_num, source, service, status, created_at, total_price, branch, address
            FROM orders WHERE client_phone = $1
            ORDER BY created_at DESC LIMIT $2
        """, phone, limit)
        return [dict(r) for r in rows]


async def get_crm_clients_count() -> dict:
    """Возвращает кол-во клиентов по статусам."""
    if not pool:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM crm_clients GROUP BY status"
        )
        return {r["status"]: r["cnt"] for r in rows}


# ══════════════════════════════════════════════════════════════════════════════
# CONTACTS (справочник)
# ══════════════════════════════════════════════════════════════════════════════

async def search_contacts(q: str, limit: int = 10) -> list[dict]:
    """Быстрый поиск по телефону или имени для автодополнения."""
    if not pool or not q:
        return []
    q = q.strip()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, first_name, last_name, middle_name, phone, phone2, address, short_address, source
            FROM contacts
            WHERE phone ILIKE $1
               OR phone2 ILIKE $1
               OR first_name ILIKE $1
               OR last_name ILIKE $1
               OR (first_name || ' ' || last_name) ILIKE $1
               OR (last_name || ' ' || first_name) ILIKE $1
               OR short_address ILIKE $1
            ORDER BY
                CASE WHEN phone ILIKE $2 THEN 0
                     WHEN phone2 ILIKE $2 THEN 1
                     ELSE 2 END,
                last_name, first_name
            LIMIT $3
        """, f"%{q}%", f"{q}%", limit)
        return [dict(r) for r in rows]


async def upsert_contact(phone: str, first_name: str = "", last_name: str = "",
                         middle_name: str = "", phone2: str = "",
                         address: str = "", short_address: str = "",
                         source: str = "ARTEZ") -> dict | None:
    """Добавить или обновить контакт (ON CONFLICT по phone)."""
    if not pool or not phone:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO contacts (phone, first_name, last_name, middle_name, phone2, address, short_address, source)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (phone) DO UPDATE SET
                first_name    = CASE WHEN $2 != '' THEN $2 ELSE contacts.first_name END,
                last_name     = CASE WHEN $3 != '' THEN $3 ELSE contacts.last_name END,
                middle_name   = CASE WHEN $4 != '' THEN $4 ELSE contacts.middle_name END,
                phone2        = CASE WHEN $5 != '' THEN $5 ELSE contacts.phone2 END,
                address       = CASE WHEN $6 != '' THEN $6 ELSE contacts.address END,
                short_address = CASE WHEN $7 != '' THEN $7 ELSE contacts.short_address END,
                updated_at    = NOW()
            RETURNING *
        """, phone, first_name, last_name, middle_name, phone2, address, short_address, source)
        return dict(row) if row else None


async def bulk_insert_contacts(rows: list[dict]) -> dict:
    """Массовая вставка контактов. Возвращает {ok, dup, err}."""
    if not pool:
        return {"ok": 0, "dup": 0, "err": len(rows)}
    ok = dup = err = 0
    async with pool.acquire() as conn:
        for r in rows:
            phone = str(r.get("phone", "")).strip()
            if not phone:
                err += 1
                continue
            try:
                res = await conn.fetchval("""
                    INSERT INTO contacts
                        (phone, first_name, last_name, middle_name, phone2, address, short_address, source)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (phone) DO NOTHING
                    RETURNING id
                """,
                    phone,
                    str(r.get("first_name",    "") or "").strip(),
                    str(r.get("last_name",     "") or "").strip(),
                    str(r.get("middle_name",   "") or "").strip(),
                    str(r.get("phone2",        "") or "").strip(),
                    str(r.get("address",       "") or "").strip(),
                    str(r.get("short_address", "") or "").strip(),
                    str(r.get("source", "Старая база")),
                )
                if res:
                    ok += 1
                else:
                    dup += 1
            except Exception:
                err += 1
    return {"ok": ok, "dup": dup, "err": err}


async def get_contacts_list(search: str = "", limit: int = 50, offset: int = 0) -> list[dict]:
    if not pool:
        return []
    async with pool.acquire() as conn:
        if search:
            rows = await conn.fetch("""
                SELECT * FROM contacts
                WHERE phone ILIKE $1 OR phone2 ILIKE $1
                   OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR middle_name ILIKE $1 OR address ILIKE $1
                   OR short_address ILIKE $1
                ORDER BY id DESC LIMIT $2 OFFSET $3
            """, f"%{search}%", limit, offset)
        else:
            rows = await conn.fetch(
                "SELECT * FROM contacts ORDER BY id DESC LIMIT $1 OFFSET $2", limit, offset)
        return [dict(r) for r in rows]


async def get_contacts_total(search: str = "") -> int:
    if not pool:
        return 0
    async with pool.acquire() as conn:
        if search:
            return await conn.fetchval("""
                SELECT COUNT(*) FROM contacts
                WHERE phone ILIKE $1 OR phone2 ILIKE $1
                   OR first_name ILIKE $1 OR last_name ILIKE $1
                   OR short_address ILIKE $1
            """, f"%{search}%")
        return await conn.fetchval("SELECT COUNT(*) FROM contacts")


async def get_contacts_source_counts() -> dict:
    if not pool:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT source, COUNT(*) cnt FROM contacts GROUP BY source")
        return {r["source"]: r["cnt"] for r in rows}


async def update_contact(contact_id: int, **kwargs) -> dict | None:
    if not pool:
        return None
    allowed = {"phone","first_name","last_name","middle_name","phone2","address","short_address","source"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return None
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    vals = list(fields.values())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE contacts SET {set_parts}, updated_at=NOW() WHERE id=$1 RETURNING *",
            contact_id, *vals)
        return dict(row) if row else None


async def delete_crm_client(client_id: int) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM crm_clients WHERE id=$1", client_id)
        return res == "DELETE 1"


async def delete_contact(contact_id: int) -> bool:
    if not pool:
        return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM contacts WHERE id=$1", contact_id)
        return res == "DELETE 1"


async def delete_all_contacts() -> int:
    """Удалить все записи из contacts. Возвращает количество удалённых."""
    if not pool:
        return 0
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM contacts")
        # res == "DELETE 41234"
        try:
            return int(res.split()[-1])
        except Exception:
            return 0


# ══════════════════════════════════════
#  ПОЗИЦИИ УСЛУГ В ЗАКАЗАХ
# ══════════════════════════════════════
async def update_order_status(order_id: int, status: str, note: str = "") -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET status=$2 WHERE id=$1 RETURNING *", order_id, status)
        if row:
            await conn.execute("""
                INSERT INTO order_status_history (order_num, new_status, note)
                VALUES ($1, $2, $3)
            """, dict(row).get("order_num", ""), status, note)
        return dict(row) if row else {}

async def get_order_items(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_items WHERE order_id=$1 ORDER BY id", order_id)
        return [dict(r) for r in rows]

async def create_order_item(order_id: int, service: str, sqm: float,
                             price_per_sqm: float, width_cm: float = None,
                             length_cm: float = None) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_items (order_id, service, width_cm, length_cm, sqm, price_per_sqm)
            VALUES ($1, $2, $3, $4, $5, $6) RETURNING *
        """, order_id, service, width_cm, length_cm, sqm, price_per_sqm)
        return dict(row) if row else {}

async def update_order_item(item_id: int, **kwargs) -> dict:
    if not pool: return {}
    allowed = {"service", "width_cm", "length_cm", "sqm", "price_per_sqm"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return {}
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE order_items SET {set_parts} WHERE id=$1 RETURNING *",
            item_id, *list(fields.values()))
        return dict(row) if row else {}

async def delete_order_item(item_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM order_items WHERE id=$1", item_id)
        return res == "DELETE 1"

async def get_order_by_id(order_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
        return dict(row) if row else {}

async def update_order_discount(order_id: int, discount_sum: float) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE orders SET discount_sum=$2 WHERE id=$1 RETURNING *", order_id, discount_sum)
        return dict(row) if row else {}

async def confirm_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_items SET measure_status='confirmed' WHERE id=$1 RETURNING *", item_id)
        return dict(row) if row else {}

async def correct_item_measure(item_id: int, actual_width_cm: float, actual_length_cm: float) -> dict:
    if not pool: return {}
    actual_sqm = round(actual_width_cm * actual_length_cm / 10000, 3)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET actual_width_cm=$2, actual_length_cm=$3, actual_sqm=$4,
                actual_total_sum=ROUND($4 * price_per_sqm, 2),
                measure_status='corrected'
            WHERE id=$1 RETURNING *
        """, item_id, actual_width_cm, actual_length_cm, actual_sqm)
        return dict(row) if row else {}

async def update_item_washer(item_id: int, washer_login: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_items SET washer_login=$2 WHERE id=$1 RETURNING *", item_id, washer_login or None)
        return dict(row) if row else {}


# ══════════════════════════════════════
#  TELEGRAM — ШАБЛОНЫ УВЕДОМЛЕНИЙ
# ══════════════════════════════════════

async def get_tg_status_messages() -> list[dict]:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM tg_status_messages ORDER BY status")
        return [dict(r) for r in rows]

async def get_tg_status_message(status: str) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tg_status_messages WHERE status=$1", status)
        return dict(row) if row else None

async def upsert_tg_status_message(status: str, enabled: bool, message_ru: str, message_uz: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO tg_status_messages (status, enabled, message_ru, message_uz)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (status) DO UPDATE
              SET enabled=$2, message_ru=$3, message_uz=$4
            RETURNING *
        """, status, enabled, message_ru, message_uz)
        return dict(row) if row else {}


async def get_client_by_tg_phone(tg_phone: str) -> dict | None:
    """Ищет клиента бота по tg_phone (верифицированный) или phone (запасной)."""
    if not pool: return None
    alt = tg_phone[1:] if tg_phone.startswith("+") else "+" + tg_phone
    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                """SELECT tg_id, tg_phone, phone, first_name FROM clients
                   WHERE tg_phone=$1 OR tg_phone=$2
                      OR phone=$1    OR phone=$2
                   LIMIT 1""",
                tg_phone, alt)
            return dict(row) if row else None
        except Exception:
            return None


async def get_tg_clients(search: str = "", limit: int = 200) -> list[dict]:
    """Клиенты из таблицы бота (clients) — все кто писал в Telegram"""
    if not pool: return []
    async with pool.acquire() as conn:
        # Проверяем что таблица clients существует (создаётся ботом)
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='clients')"
        )
        if not exists:
            return []
        if search:
            rows = await conn.fetch("""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at
                FROM clients
                WHERE phone ILIKE $1 OR first_name ILIKE $1
                   OR last_name ILIKE $1 OR tg_username ILIKE $1
                ORDER BY created_at DESC
                LIMIT $2
            """, f"%{search}%", limit)
        else:
            rows = await conn.fetch("""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at
                FROM clients
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
        return [dict(r) for r in rows]
