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
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_items      BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_measure         BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_approve_measure BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS order_stages        VARCHAR(100) DEFAULT NULL",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_create_order     BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_confirm_order    BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_confirmed   BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_send_pickup      BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_delivery    BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_accept_payment   BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_manage_cash      BOOLEAN DEFAULT FALSE",
        "UPDATE staff SET can_manage_cash=TRUE WHERE can_accept_payment=TRUE",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS handed_to_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS created_by_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        """CREATE TABLE IF NOT EXISTS order_activity (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER NOT NULL,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            staff_name  VARCHAR(100) DEFAULT '',
            action      VARCHAR(50)  NOT NULL,
            details     TEXT         DEFAULT '',
            created_at  TIMESTAMPTZ  DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_order_activity_order ON order_activity(order_id)",
        """CREATE TABLE IF NOT EXISTS cash_handovers (
            id              SERIAL PRIMARY KEY,
            from_staff_id   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            to_staff_id     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            amount          NUMERIC(12,2) NOT NULL,
            note            TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS review_claimed_by    INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS review_claimed_at    TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS gender             VARCHAR(1) DEFAULT 'M'",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS birth_date        DATE DEFAULT NULL",
        """CREATE TABLE IF NOT EXISTS staff_personal (
            staff_id         INTEGER PRIMARY KEY REFERENCES staff(id) ON DELETE CASCADE,
            passport_series  VARCHAR(10),
            passport_number  VARCHAR(20),
            pinfl            VARCHAR(20),
            home_address     TEXT,
            extra_phone      VARCHAR(20),
            children_count   INTEGER DEFAULT 0,
            marital_status   VARCHAR(20) DEFAULT 'single',
            spouse_name      VARCHAR(200),
            spouse_birth_date DATE,
            spouse_phone     VARCHAR(20),
            spouse_workplace VARCHAR(200),
            spouse_position  VARCHAR(200),
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS location           TEXT DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS location_address   TEXT DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS callback_at        TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS assigned_to        INTEGER REFERENCES staff(id) DEFAULT NULL",
        # Заполнить lead_code для лидов у которых он NULL
        """UPDATE leads SET lead_code = 'L-' || LPAD(id::text, 4, '0')
           WHERE lead_code IS NULL""",
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
        # Orders: хранить текстовый адрес геолокации
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS location_address TEXT DEFAULT ''",
        # Orders: дедлайн (дата готовности)
        "ALTER TABLE orders ADD COLUMN IF NOT EXISTS deadline DATE DEFAULT NULL",
        # Замеры: причина отклонения
        "ALTER TABLE order_items ADD COLUMN IF NOT EXISTS reject_note TEXT DEFAULT NULL",
        # Маршруты логистики
        """CREATE TABLE IF NOT EXISTS routes (
            id          SERIAL PRIMARY KEY,
            name        VARCHAR(200) NOT NULL,
            date        DATE NOT NULL,
            driver_id   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            branch      VARCHAR(50),
            type        VARCHAR(20) DEFAULT 'mixed',
            status      VARCHAR(20) DEFAULT 'planned',
            note        TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS route_orders (
            id          SERIAL PRIMARY KEY,
            route_id    INTEGER NOT NULL REFERENCES routes(id) ON DELETE CASCADE,
            order_id    INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            sort_order  INTEGER DEFAULT 0,
            stop_status VARCHAR(20) DEFAULT 'pending',
            note        TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(route_id, order_id)
        )""",
        # Заполнить created_by_staff_id для старых платежей по совпадению имени
        """UPDATE order_payments p
           SET created_by_staff_id = s.id
           FROM staff s
           WHERE p.created_by_staff_id IS NULL
             AND p.created_by IS NOT NULL
             AND p.created_by <> ''
             AND TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) = TRIM(p.created_by)""",
        # Касса: статус передачи наличных
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'pending'",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS confirmed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        # Платежи: подтверждение и фото чека
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS receipt_url TEXT DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS reject_note TEXT DEFAULT NULL",
        # Таблица настроек (создаём если не существует + гарантируем одну строку)
        "CREATE TABLE IF NOT EXISTS settings (id SERIAL PRIMARY KEY)",
        "INSERT INTO settings DEFAULT VALUES ON CONFLICT DO NOTHING",
        # Настройки: ТГ канал кассы
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS cash_tg_channel_id VARCHAR(50) DEFAULT NULL",
        # Настройки: канал медиафайлов (замеры, чеки и т.д.)
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS media_channel_id VARCHAR(50) DEFAULT NULL",
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

    # ── Push subscriptions ──────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS push_subscriptions (
            id         SERIAL PRIMARY KEY,
            staff_id   INTEGER NOT NULL,
            endpoint   TEXT    NOT NULL UNIQUE,
            p256dh     TEXT    NOT NULL,
            auth       TEXT    NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_push_staff_id ON push_subscriptions(staff_id);
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

    await ensure_agent_notifications_table()

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

    # ── Шаг 5: фото/видео заказов ────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_photos (
            id            SERIAL PRIMARY KEY,
            order_id      INTEGER      NOT NULL,
            tg_file_id    TEXT         NOT NULL,
            tg_file_type  VARCHAR(20)  NOT NULL DEFAULT 'photo',
            photo_type    VARCHAR(20)  NOT NULL DEFAULT 'before',
            note          TEXT         DEFAULT '',
            uploaded_by   VARCHAR(100) DEFAULT '',
            created_at    TIMESTAMPTZ  DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_photos_order ON order_photos(order_id);
        """)

    # ── Шаг 6: оплата и касса ────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_method  VARCHAR(20)   DEFAULT NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS prepaid_amount  NUMERIC(12,2) DEFAULT 0;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS payment_status  VARCHAR(20)   DEFAULT 'unpaid';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS paid_at         TIMESTAMPTZ   DEFAULT NULL;
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_payments (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER       NOT NULL,
            amount      NUMERIC(12,2) NOT NULL,
            method      VARCHAR(20)   NOT NULL,
            purpose     VARCHAR(50)   DEFAULT 'payment',
            note        TEXT          DEFAULT '',
            created_by  VARCHAR(100)  DEFAULT '',
            created_at  TIMESTAMPTZ   DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_order_payments_order ON order_payments(order_id);
        ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS purpose VARCHAR(50) DEFAULT 'payment';
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS order_item_media (
            id           SERIAL PRIMARY KEY,
            item_id      INTEGER       NOT NULL,
            order_id     INTEGER       NOT NULL,
            tg_file_id   VARCHAR(200)  NOT NULL,
            tg_file_type VARCHAR(20)   DEFAULT 'photo',
            created_by   VARCHAR(100)  DEFAULT '',
            created_at   TIMESTAMPTZ   DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_item_media_item ON order_item_media(item_id);
        """)
        await c.execute("""
        CREATE TABLE IF NOT EXISTS cash_shifts (
            id             SERIAL PRIMARY KEY,
            shift_date     DATE          NOT NULL,
            closed_by      VARCHAR(100)  DEFAULT '',
            closed_at      TIMESTAMPTZ   DEFAULT NOW(),
            cash_total     NUMERIC(12,2) DEFAULT 0,
            card_total     NUMERIC(12,2) DEFAULT 0,
            transfer_total NUMERIC(12,2) DEFAULT 0,
            grand_total    NUMERIC(12,2) DEFAULT 0,
            orders_count   INTEGER       DEFAULT 0,
            note           TEXT          DEFAULT ''
        );
        """)

    # ── Шаг 7: тип заказа (стандарт/экспресс) ───────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS service_type VARCHAR(20) DEFAULT 'standard';
        """)

    # ── Шаг 8: тип вывоза и скидка при самовывозе ────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pickup_type VARCHAR(10) DEFAULT 'courier';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS self_pickup_discount NUMERIC(5,2) DEFAULT 0;
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS pickup_type VARCHAR(10) DEFAULT 'courier';
        """)

    # ── Шаг 9: ручная скидка на заказ ────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS manual_discount NUMERIC(12,2) DEFAULT 0;
        """)

    # ── Шаг 10: тип вывоза и скидка при самовывозе (из мастерской) ───────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_type VARCHAR(10) DEFAULT 'courier';
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_discount NUMERIC(12,2) DEFAULT 0;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS delivery_discount_pct NUMERIC(5,2) DEFAULT 0;
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS delivery_type VARCHAR(10) DEFAULT 'courier';
        """)

    # ── Шаг 11: флаг pending position request ────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pos_request_pending BOOLEAN DEFAULT FALSE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS pos_request_at TIMESTAMPTZ DEFAULT NULL;
        """)

    # ── Шаг 12: контакты филиалов ────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS site_contacts (
            branch      VARCHAR(50) PRIMARY KEY,
            branch_name VARCHAR(100) NOT NULL,
            phones      JSONB NOT NULL DEFAULT '[]',
            telegram    VARCHAR(200) DEFAULT '',
            whatsapp    VARCHAR(200) DEFAULT '',
            instagram   VARCHAR(200) DEFAULT ''
        );
        INSERT INTO site_contacts (branch, branch_name, phones, telegram, whatsapp, instagram)
        VALUES
          ('navoi',     'Навои',     '["1221","+998792221221"]', '', '', ''),
          ('zarafshan', 'Зарафшан',  '["1221","+998792221221"]', '', '', '')
        ON CONFLICT (branch) DO NOTHING;
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
                branch, city, address, short_address, location, service, service_type, pickup_type, delivery_type, pickup_date, pickup_time, note,
                total_price, status
            ) VALUES (
                $1, $2,
                NULL, $3, $4, $5,
                $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, 'new'
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
            data.get("service_type") or "standard",
            data.get("pickup_type") or "courier",
            data.get("delivery_type") or "courier",
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
                               salary_type, salary_rate, hire_date, note, gender, birth_date)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
            RETURNING id
        """, data["first_name"], data.get("last_name"), data.get("middle_name"),
            data.get("phone"), data["login"], data["password_hash"],
            data.get("plain_password"),
            data.get("role","callcenter"), data.get("position"), data.get("branch"),
            data.get("tg_id"), data.get("tg_username"),
            data.get("salary_type"), data.get("salary_rate"),
            data.get("hire_date"), data.get("note"), data.get("gender","M"),
            data.get("birth_date"))

async def update_staff(staff_id: int, **kwargs):
    if not pool or not kwargs: return
    allowed = {"first_name","last_name","middle_name","phone","login","role","position",
               "branch","tg_id","tg_username","salary_type","salary_rate","hire_date",
               "note","active","is_active","gender","birth_date"}
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

async def get_staff_personal(staff_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM staff_personal WHERE staff_id=$1", staff_id)
        return dict(row) if row else {}

async def upsert_staff_personal(staff_id: int, data: dict) -> None:
    if not pool: return
    fields = ["passport_series","passport_number","pinfl","home_address","extra_phone",
              "children_count","marital_status","spouse_name","spouse_birth_date",
              "spouse_phone","spouse_workplace","spouse_position"]
    filtered = {k: data.get(k) for k in fields}
    cols = ", ".join(filtered.keys())
    placeholders = ", ".join(f"${i+2}" for i in range(len(filtered)))
    updates = ", ".join(f"{k}=${i+2}" for i, k in enumerate(filtered))
    async with pool.acquire() as conn:
        await conn.execute(
            f"""INSERT INTO staff_personal (staff_id, {cols}, updated_at)
                VALUES ($1, {placeholders}, NOW())
                ON CONFLICT (staff_id) DO UPDATE SET {updates}, updated_at=NOW()""",
            staff_id, *list(filtered.values())
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
            INSERT INTO leads (lead_num, lead_code, client_name, client_phone, service, branch,
                               city, address, short_address, note, status, assigned_to,
                               created_by, volunteer_id, location, location_address)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            RETURNING *
        """, data["lead_num"], data.get("lead_code"), data.get("client_name"), data["client_phone"],
            data.get("service"), data.get("branch"), data.get("city"),
            data.get("address"), data.get("short_address", ""), data.get("note"),
            data.get("status","new"), data.get("assigned_to"), data.get("created_by"),
            data.get("volunteer_id"), data.get("location"), data.get("location_address"))
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
                       s.login        AS creator_login,
                       s.phone        AS creator_phone,
                       vol.first_name AS volunteer_first_name,
                       vol.last_name  AS volunteer_last_name,
                       vol.phone      AS volunteer_phone,
                       asgn.first_name AS assigned_first_name,
                       asgn.last_name  AS assigned_last_name,
                       asgn.phone      AS assigned_phone
                FROM leads l
                LEFT JOIN staff s    ON s.id    = l.created_by
                LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
                LEFT JOIN staff asgn ON asgn.id = l.assigned_to
                WHERE {' AND '.join(filters)}
                ORDER BY l.created_at DESC LIMIT ${len(args)}""", *args
        )

async def update_lead_status(lead_id: int, status: str, scheduled_at=None):
    if not pool: return
    async with pool.acquire() as conn:
        if status == "callback" and scheduled_at:
            await conn.execute(
                "UPDATE leads SET status=$2, callback_at=$3, updated_at=NOW() WHERE id=$1",
                lead_id, status, scheduled_at
            )
        else:
            await conn.execute(
                "UPDATE leads SET status=$2, callback_at=NULL, updated_at=NOW() WHERE id=$1",
                lead_id, status
            )

async def update_lead(lead_id: int, **kwargs) -> dict | None:
    if not pool: return None
    allowed = {"client_name","client_phone","service","branch","city","address","short_address","note","status","location","location_address","volunteer_id","pickup_type","delivery_type"}
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
        await conn.execute("DELETE FROM agent_notifications WHERE lead_id=$1", lead_id)
        await conn.execute("DELETE FROM lead_reminders       WHERE lead_id=$1", lead_id)
        await conn.execute("DELETE FROM lead_calls           WHERE lead_id=$1", lead_id)
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
                       s.first_name   AS creator_first_name,
                       s.last_name    AS creator_last_name,
                       s.position     AS creator_position,
                       s.login        AS creator_login,
                       s.phone        AS creator_phone,
                       vol.first_name AS volunteer_first_name,
                       vol.last_name  AS volunteer_last_name,
                       vol.phone      AS volunteer_phone,
                       asgn.first_name AS assigned_first_name,
                       asgn.last_name  AS assigned_last_name,
                       asgn.phone      AS assigned_phone
                FROM leads l
                LEFT JOIN staff s    ON s.id    = l.created_by
                LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
                LEFT JOIN staff asgn ON asgn.id = l.assigned_to
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
                   s.login       AS creator_login,
                   s.phone       AS creator_phone,
                   vol.first_name AS volunteer_first_name,
                   vol.last_name  AS volunteer_last_name,
                   vol.phone      AS volunteer_phone,
                   conv.first_name AS converted_first_name,
                   conv.last_name  AS converted_last_name,
                   asgn.first_name AS assigned_first_name,
                   asgn.last_name  AS assigned_last_name,
                   asgn.phone      AS assigned_phone
            FROM leads l
            LEFT JOIN staff s    ON s.id    = l.created_by
            LEFT JOIN staff vol  ON vol.id  = l.volunteer_id
            LEFT JOIN staff conv ON conv.id = l.converted_by
            LEFT JOIN staff asgn ON asgn.id = l.assigned_to
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
    """Для фонового воркера — все напоминания для отправки в Telegram.
    Получатель = assigned_to (кто взял лид), если взят; иначе staff_id (кто поставил напоминание)."""
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT r.*, l.client_name, l.client_phone, l.lead_code,
                   -- целевой получатель: assigned_to имеет приоритет
                   COALESCE(l.assigned_to, r.staff_id) AS target_staff_id,
                   tgt.tg_id AS staff_tg_id,
                   tgt.first_name AS staff_first_name,
                   tgt.last_name  AS staff_last_name
            FROM lead_reminders r
            JOIN leads l ON l.id = r.lead_id
            -- joined на фактического получателя
            JOIN staff tgt ON tgt.id = COALESCE(l.assigned_to, r.staff_id)
            WHERE r.remind_at <= NOW() AND r.sent_tg = FALSE
        """)

# ── agent_notifications ───────────────────────────────────────────────

async def ensure_agent_notifications_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_notifications (
                id         SERIAL PRIMARY KEY,
                agent_id   INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                lead_id    INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
                action     VARCHAR(50) NOT NULL,
                message    TEXT,
                is_read    BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_agent_notif_agent ON agent_notifications(agent_id, is_read);
        """)

async def create_agent_notification(agent_id: int, lead_id: int, action: str, message: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO agent_notifications (agent_id, lead_id, action, message)
            VALUES ($1,$2,$3,$4)
        """, agent_id, lead_id, action, message)

async def get_agent_notifications(agent_id: int, limit: int = 50):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch("""
            SELECT n.*, l.client_name, l.client_phone, l.lead_code
            FROM agent_notifications n
            JOIN leads l ON l.id = n.lead_id
            WHERE n.agent_id = $1
            ORDER BY n.created_at DESC LIMIT $2
        """, agent_id, limit)

async def count_unread_agent_notifications(agent_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM agent_notifications WHERE agent_id=$1 AND is_read=FALSE",
            agent_id) or 0

async def mark_agent_notifications_read(agent_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_notifications SET is_read=TRUE WHERE agent_id=$1 AND is_read=FALSE",
            agent_id)

async def mark_agent_notification_read_by_id(notif_id: int, agent_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE agent_notifications SET is_read=TRUE WHERE id=$1 AND agent_id=$2",
            notif_id, agent_id)


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

async def create_empty_items(order_id: int, count: int) -> list:
    if not pool: return []
    result = []
    async with pool.acquire() as conn:
        for _ in range(count):
            row = await conn.fetchrow("""
                INSERT INTO order_items (order_id, service, sqm, price_per_sqm)
                VALUES ($1, '', 0, 0) RETURNING *
            """, order_id)
            if row:
                result.append(dict(row))
    return result

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

async def delete_order(order_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        # Блокируем удаление если есть платежи — деньги должны быть сняты вручную
        payment_count = await conn.fetchval(
            "SELECT COUNT(*) FROM order_payments WHERE order_id=$1", order_id)
        if payment_count:
            raise ValueError(f"has_payments:{payment_count}")
        # Получаем order_num до удаления (нужен для history)
        row = await conn.fetchrow("SELECT order_num FROM orders WHERE id=$1", order_id)
        order_num = dict(row).get("order_num") if row else None
        # Удаляем медиафайлы позиций
        await conn.execute("DELETE FROM order_item_media WHERE order_id=$1", order_id)
        # Удаляем позиции
        await conn.execute("DELETE FROM order_items WHERE order_id=$1", order_id)
        # Удаляем фото заказа
        await conn.execute("DELETE FROM order_photos WHERE order_id=$1", order_id)
        # Удаляем из маршрутов
        await conn.execute("DELETE FROM route_orders WHERE order_id=$1", order_id)
        # Удаляем историю статусов
        if order_num:
            await conn.execute("DELETE FROM order_status_history WHERE order_num=$1", order_num)
        # Удаляем сам заказ
        res = await conn.execute("DELETE FROM orders WHERE id=$1", order_id)
        return res == "DELETE 1"

async def get_order_by_id(order_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM orders WHERE id=$1", order_id)
        return dict(row) if row else {}

async def update_order(order_id: int, **kwargs) -> dict:
    if not pool: return {}
    allowed = {"client_first_name", "client_last_name", "client_phone",
               "branch", "address", "short_address", "location", "location_address", "note", "deadline",
               "service_type", "pickup_type", "self_pickup_discount",
               "discount_sum", "manual_discount",
               "delivery_type", "delivery_discount", "delivery_discount_pct"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields: return {}
    set_parts = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE orders SET {set_parts} WHERE id=$1 RETURNING *",
            order_id, *list(fields.values()))
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

async def save_measure_dims(item_id: int, width_cm: float, length_cm: float) -> dict:
    if not pool: return {}
    sqm = round(width_cm * length_cm / 10000, 3)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET width_cm=$2, length_cm=$3, sqm=$4
            WHERE id=$1 AND measure_status != 'approved'
            RETURNING *
        """, item_id, width_cm, length_cm, sqm)
        return dict(row) if row else {}

async def update_item_washer(item_id: int, washer_login: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_items SET washer_login=$2 WHERE id=$1 RETURNING *", item_id, washer_login or None)
        return dict(row) if row else {}


# ══════════════════════════════════════
#  ИСТОРИЯ СТАТУСОВ ЗАКАЗА
# ══════════════════════════════════════
async def get_order_status_history(order_num: str) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_status_history WHERE order_num=$1 ORDER BY created_at",
            order_num)
        return [dict(r) for r in rows]

# ══════════════════════════════════════
#  ДУБЛИКАТЫ ТЕЛЕФОНА
# ══════════════════════════════════════
async def check_phone_duplicate(phone: str) -> dict:
    if not pool: return {}
    clean = ''.join(c for c in phone if c.isdigit() or c == '+')
    if not clean: return {}
    async with pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT id, order_num, client_first_name, client_last_name, status FROM orders "
            "WHERE REGEXP_REPLACE(client_phone,'[^0-9]','','g') = REGEXP_REPLACE($1,'[^0-9]','','g') "
            "ORDER BY created_at DESC LIMIT 1", clean)
        lead = await conn.fetchrow(
            "SELECT id, name, status FROM leads "
            "WHERE REGEXP_REPLACE(phone,'[^0-9]','','g') = REGEXP_REPLACE($1,'[^0-9]','','g') "
            "ORDER BY created_at DESC LIMIT 1", clean)
        return {
            "order": dict(order) if order else None,
            "lead":  dict(lead)  if lead  else None,
        }

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

async def upsert_push_subscription(staff_id: int, endpoint: str, p256dh: str, auth: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO push_subscriptions (staff_id, endpoint, p256dh, auth)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (endpoint) DO UPDATE SET staff_id=$1, p256dh=$3, auth=$4
        """, staff_id, endpoint, p256dh, auth)

async def get_push_subscriptions(staff_id: int):
    if not pool: return []
    async with pool.acquire() as conn:
        return await conn.fetch(
            "SELECT * FROM push_subscriptions WHERE staff_id=$1", staff_id)

async def delete_push_subscription(endpoint: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM push_subscriptions WHERE endpoint=$1", endpoint)

# ── order_photos ──────────────────────────────────────────────────────────────

async def get_order_photos(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_photos WHERE order_id=$1 ORDER BY created_at", order_id)
        return [dict(r) for r in rows]

async def save_order_photo(order_id: int, tg_file_id: str, tg_file_type: str,
                           photo_type: str, note: str, uploaded_by: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_photos (order_id, tg_file_id, tg_file_type, photo_type, note, uploaded_by)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, order_id, tg_file_id, tg_file_type, photo_type, note, uploaded_by)
        return dict(row) if row else {}

async def delete_order_photo(photo_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM order_photos WHERE id=$1", photo_id)
        return res == "DELETE 1"

async def get_photo_by_id(photo_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM order_photos WHERE id=$1", photo_id)
        return dict(row) if row else {}

# ── Оплата заказов ────────────────────────────────────────────────────────────

async def update_order_payment(order_id: int, payment_method: str, payment_status: str,
                                prepaid_amount: float) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        paid_at = "NOW()" if payment_status == "paid" else "NULL"
        row = await conn.fetchrow(f"""
            UPDATE orders SET
                payment_method = $1,
                payment_status = $2,
                prepaid_amount = $3,
                paid_at        = {paid_at}
            WHERE id = $4 RETURNING *
        """, payment_method, payment_status, prepaid_amount, order_id)
        return dict(row) if row else {}

# ── Касса ─────────────────────────────────────────────────────────────────────

async def get_cash_summary(date_from: str, date_to: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                payment_method,
                payment_status,
                COALESCE(SUM(
                    CASE WHEN payment_status='paid' THEN COALESCE(total_price,0) - COALESCE(discount_sum,0)
                         WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                         ELSE 0 END
                ), 0) AS amount,
                COUNT(*) AS cnt
            FROM orders
            WHERE created_at::date BETWEEN $1 AND $2
              AND payment_status IN ('paid','partial')
            GROUP BY payment_method, payment_status
        """, date_from, date_to)
        orders = await conn.fetch("""
            SELECT id, order_num, created_at, total_price, discount_sum,
                   payment_method, payment_status, prepaid_amount, paid_at
            FROM orders
            WHERE created_at::date BETWEEN $1 AND $2
              AND payment_status IS DISTINCT FROM 'unpaid'
            ORDER BY created_at DESC
        """, date_from, date_to)
        return {
            "summary": [dict(r) for r in rows],
            "orders": [dict(r) for r in orders],
        }

async def close_cash_shift(shift_date: str, closed_by: str, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            WITH totals AS (
                SELECT
                    COALESCE(SUM(CASE WHEN payment_method='cash' THEN
                        CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                             WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                             ELSE 0 END ELSE 0 END),0) AS cash_total,
                    COALESCE(SUM(CASE WHEN payment_method='card' THEN
                        CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                             WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                             ELSE 0 END ELSE 0 END),0) AS card_total,
                    COALESCE(SUM(CASE WHEN payment_method='transfer' THEN
                        CASE WHEN payment_status='paid' THEN COALESCE(total_price,0)-COALESCE(discount_sum,0)
                             WHEN payment_status='partial' THEN COALESCE(prepaid_amount,0)
                             ELSE 0 END ELSE 0 END),0) AS transfer_total,
                    COUNT(*) FILTER (WHERE payment_status IN ('paid','partial')) AS orders_count
                FROM orders
                WHERE created_at::date = $1
            )
            INSERT INTO cash_shifts (shift_date, closed_by, cash_total, card_total, transfer_total, grand_total, orders_count, note)
            SELECT $1, $2, cash_total, card_total, transfer_total,
                   cash_total+card_total+transfer_total, orders_count, $3
            FROM totals
            RETURNING *
        """, shift_date, closed_by, note)
        return dict(row) if row else {}

async def get_cash_shifts(limit: int = 30) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM cash_shifts ORDER BY shift_date DESC, closed_at DESC LIMIT $1", limit)
        return [dict(r) for r in rows]

# ── order_payments ────────────────────────────────────────────────────────────

async def get_order_payments(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   o.client_first_name, o.client_last_name, o.short_address,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_full_name,
                   s.phone AS staff_phone
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            LEFT JOIN staff s ON s.id = p.created_by_staff_id
            WHERE p.order_id=$1
            ORDER BY p.created_at
        """, order_id)
        return [dict(r) for r in rows]

async def add_order_payment(order_id: int, amount: float, method: str, purpose: str, note: str, created_by: str, handed_to_staff_id: int = None, created_by_staff_id: int = None) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_payments (order_id, amount, method, purpose, note, created_by, handed_to_staff_id, created_by_staff_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, order_id, amount, method, purpose, note, created_by, handed_to_staff_id, created_by_staff_id)
        # Пересчитать payment_status на orders (не считаем отклонённые)
        total_row = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS paid FROM order_payments WHERE order_id=$1 AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)", order_id)
        paid = float(total_row['paid'])
        order = await conn.fetchrow("SELECT total_price, discount_sum FROM orders WHERE id=$1", order_id)
        if order:
            net = float(order['total_price'] or 0) - float(order['discount_sum'] or 0)
            status = 'paid' if paid >= net and net > 0 else ('partial' if paid > 0 else 'unpaid')
            await conn.execute(
                "UPDATE orders SET payment_status=$1 WHERE id=$2", status, order_id)
        return dict(row) if row else {}

async def _recalc_payment_status(conn, order_id: int):
    total_row = await conn.fetchrow(
        "SELECT COALESCE(SUM(amount),0) AS paid FROM order_payments WHERE order_id=$1 AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)", order_id)
    paid = float(total_row['paid'])
    order = await conn.fetchrow("SELECT total_price, discount_sum, delivery_discount, manual_discount FROM orders WHERE id=$1", order_id)
    if order:
        net = float(order['total_price'] or 0) - float(order['discount_sum'] or 0) - float(order['delivery_discount'] or 0) - float(order['manual_discount'] or 0)
        status = 'paid' if paid >= net and net > 0 else ('partial' if paid > 0 else 'unpaid')
        await conn.execute("UPDATE orders SET payment_status=$1 WHERE id=$2", status, order_id)

async def add_order_activity(order_id: int, staff_id: int, staff_name: str, action: str, details: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff_id, staff_name, action, details)

async def get_order_activity(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_activity WHERE order_id=$1 ORDER BY created_at DESC LIMIT 100", order_id)
        return [dict(r) for r in rows]

async def delete_order_payment(payment_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM order_payments WHERE id=$1", payment_id)
        if not row: return {}
        order_id = row['order_id']
        await conn.execute("DELETE FROM order_payments WHERE id=$1", payment_id)
        await _recalc_payment_status(conn, order_id)
        return dict(row)

async def edit_order_payment(payment_id: int, amount: float, method: str, purpose: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_payments SET amount=$2, method=$3, purpose=$4 WHERE id=$1 RETURNING *",
            payment_id, amount, method, purpose)
        if row:
            await _recalc_payment_status(conn, row['order_id'])
        return dict(row) if row else {}

# ── order_item_media (замеры) ─────────────────────────────────────────────────

async def get_item_media(item_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM order_item_media WHERE item_id=$1 ORDER BY created_at", item_id)
        return [dict(r) for r in rows]

async def add_item_media(item_id: int, order_id: int, tg_file_id: str, tg_file_type: str, created_by: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO order_item_media (item_id, order_id, tg_file_id, tg_file_type, created_by)
            VALUES ($1,$2,$3,$4,$5) RETURNING *
        """, item_id, order_id, tg_file_id, tg_file_type, created_by)
        return dict(row) if row else {}

async def delete_item_media(media_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM order_item_media WHERE id=$1", media_id)
        return True

async def claim_measure_review(item_id: int, staff_id: int) -> dict:
    """Пометить замер как «принят на проверку» конкретным сотрудником."""
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
            SET review_claimed_by=$2, review_claimed_at=NOW()
            WHERE id=$1 AND measure_status='submitted'
            RETURNING *
        """, item_id, staff_id)
        return dict(row) if row else {}

async def get_pending_measure_reviews() -> list:
    """Все замеры со статусом 'submitted' с информацией о заказе и кто принял."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT oi.id AS item_id, oi.order_id, oi.service, oi.review_claimed_by,
                   oi.review_claimed_at,
                   o.order_num, o.status AS order_status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   s.first_name AS claimer_first, s.last_name AS claimer_last
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            LEFT JOIN staff s ON s.id = oi.review_claimed_by
            WHERE oi.measure_status = 'submitted'
            ORDER BY oi.id ASC
        """)
        return [dict(r) for r in rows]

async def get_all_approvers() -> list:
    """Все сотрудники у которых can_approve_measure = true."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE can_approve_measure=TRUE AND active=TRUE"
        )
        return [dict(r) for r in rows]

async def get_all_cashiers_for_push() -> list:
    """Все сотрудники у которых can_manage_cash = true."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE can_manage_cash=TRUE AND active=TRUE"
        )
        return [dict(r) for r in rows]

async def reject_payment(payment_id: int, rejected_by: int, note: str = "") -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_payments
               SET confirmed=FALSE, confirmed_by=$2, confirmed_at=NOW(), reject_note=$3
             WHERE id=$1
             RETURNING *
        """, payment_id, rejected_by, note or None)
        if row:
            await _recalc_payment_status(conn, row["order_id"])
        return dict(row) if row else {}

async def submit_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='submitted', reject_note=NULL
            WHERE id=$1 AND width_cm IS NOT NULL AND length_cm IS NOT NULL
            RETURNING *
        """, item_id)
        return dict(row) if row else {}

async def approve_item_measure(item_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='approved', reject_note=NULL
            WHERE id=$1 RETURNING *
        """, item_id)
        return dict(row) if row else {}

async def reject_item_measure(item_id: int, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items SET measure_status='rejected', reject_note=$2
            WHERE id=$1 RETURNING *
        """, item_id, note or '')
        return dict(row) if row else {}

async def get_item_media_by_id(media_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM order_item_media WHERE id=$1", media_id)
        return dict(row) if row else {}

# ── МАРШРУТЫ (routes) ────────────────────────────────────────────────────────

async def get_routes(date: str | None = None, driver_id: int | None = None,
                     branch: str | None = None, status: str | None = None) -> list:
    if not pool: return []
    filters, vals, i = [], [], 1
    if date:      filters.append(f"r.date=${ i}"); vals.append(date); i+=1
    if driver_id: filters.append(f"r.driver_id=${i}"); vals.append(driver_id); i+=1
    if branch:    filters.append(f"r.branch=${i}"); vals.append(branch); i+=1
    if status:    filters.append(f"r.status=${i}"); vals.append(status); i+=1
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT r.*,
                   s.first_name || ' ' || s.last_name AS driver_name,
                   COUNT(ro.id) AS order_count
            FROM routes r
            LEFT JOIN staff s ON s.id = r.driver_id
            LEFT JOIN route_orders ro ON ro.route_id = r.id
            {where}
            GROUP BY r.id, s.first_name, s.last_name
            ORDER BY r.date DESC, r.id DESC
        """, *vals)
        return [dict(r) for r in rows]

async def create_route(data: dict) -> dict:
    if not pool: return {}
    from datetime import date as _date
    d = _date.fromisoformat(data["date"]) if isinstance(data.get("date"), str) else data.get("date")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO routes (name, date, driver_id, branch, type, status, note)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, data.get("name",""), d, data.get("driver_id"),
             data.get("branch"), data.get("type","mixed"),
             data.get("status","planned"), data.get("note"))
        return dict(row) if row else {}

async def get_route(route_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.*, s.first_name || ' ' || s.last_name AS driver_name
            FROM routes r LEFT JOIN staff s ON s.id=r.driver_id WHERE r.id=$1
        """, route_id)
        if not row: return None
        route = dict(row)
        stops = await conn.fetch("""
            SELECT ro.*, o.order_num, o.client_first_name, o.client_last_name,
                   o.client_phone, o.address, o.short_address,
                   o.location, o.location_address, o.status AS order_status,
                   o.service, o.branch
            FROM route_orders ro
            JOIN orders o ON o.id=ro.order_id
            WHERE ro.route_id=$1
            ORDER BY ro.sort_order, ro.id
        """, route_id)
        route["stops"] = [dict(s) for s in stops]
        return route

async def update_route(route_id: int, data: dict) -> dict:
    if not pool: return {}
    allowed = {"name","date","driver_id","branch","type","status","note"}
    fields = {k: v for k, v in data.items() if k in allowed and v is not None}
    if "date" in fields and isinstance(fields["date"], str):
        from datetime import date as _date
        fields["date"] = _date.fromisoformat(fields["date"])
    if not fields: return {}
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE routes SET {sets}, updated_at=NOW() WHERE id=$1 RETURNING *",
            route_id, *fields.values())
        return dict(row) if row else {}

async def delete_route(route_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM routes WHERE id=$1", route_id)
        return True

async def add_orders_to_route(route_id: int, order_ids: list[int]) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        cur_max = await conn.fetchval(
            "SELECT COALESCE(MAX(sort_order),0) FROM route_orders WHERE route_id=$1", route_id)
        count = 0
        for i, oid in enumerate(order_ids):
            try:
                await conn.execute("""
                    INSERT INTO route_orders (route_id, order_id, sort_order)
                    VALUES ($1,$2,$3) ON CONFLICT DO NOTHING
                """, route_id, oid, (cur_max or 0) + i + 1)
                count += 1
            except Exception:
                pass
        return count

async def remove_order_from_route(route_id: int, order_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM route_orders WHERE route_id=$1 AND order_id=$2", route_id, order_id)
        return True

async def update_route_stop(route_id: int, order_id: int, data: dict) -> bool:
    if not pool: return False
    allowed = {"sort_order","stop_status","note"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields: return False
    sets = ", ".join(f"{k}=${i+3}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE route_orders SET {sets} WHERE route_id=$1 AND order_id=$2",
            route_id, order_id, *fields.values())
        return True

# ── Касса / наличные ──────────────────────────────────────────────────────────

async def get_cashiers() -> list:
    """Ответственные за кассу (can_manage_cash)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, first_name, last_name FROM staff WHERE can_manage_cash=TRUE AND active=TRUE ORDER BY last_name, first_name")
        return [dict(r) for r in rows]

async def get_my_cash_balance(staff_id: int) -> dict:
    """Баланс конкретного сотрудника: принял / сдал / на руках."""
    if not pool: return {}
    async with pool.acquire() as conn:
        # Имя сотрудника для совпадения со старыми записями (до created_by_staff_id)
        staff_row = await conn.fetchrow("SELECT first_name, last_name, login FROM staff WHERE id=$1", staff_id)
        staff_name = ""
        if staff_row:
            staff_name = " ".join(filter(None, [staff_row['last_name'], staff_row['first_name']])) or staff_row['login'] or ""
        # Принял от клиентов (я записал платёж — по id или по имени для старых)
        r1 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE method='cash' AND (created_by_staff_id=$1 OR (created_by_staff_id IS NULL AND created_by=$2))",
            staff_id, staff_name)
        # Сдал сразу при записи (handed_to != me)
        r2 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE method='cash' AND handed_to_staff_id IS NOT NULL AND handed_to_staff_id!=$1 AND (created_by_staff_id=$1 OR (created_by_staff_id IS NULL AND created_by=$2))",
            staff_id, staff_name)
        # Получил от других сотрудников через платёж (они сдали мне)
        r3 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE handed_to_staff_id=$1 AND method='cash' AND (created_by_staff_id IS NULL OR created_by_staff_id<>$1)",
            staff_id)
        # Получил через ручную передачу (cash_handovers to me)
        r4 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE to_staff_id=$1", staff_id)
        # Сдал через ручную передачу (cash_handovers from me)
        r5 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE from_staff_id=$1", staff_id)
        collected   = float(r1)
        given_imm   = float(r2)
        recv_others = float(r3)
        recv_hand   = float(r4)
        given_hand  = float(r5)
        on_hand = collected - given_imm + recv_others + recv_hand - given_hand
        return {
            "collected":         collected,
            "given_immediately": given_imm,
            "received_from_others": recv_others + recv_hand,
            "handed_over":       given_imm + given_hand,
            "on_hand":           on_hand,
        }

async def get_cash_balance() -> list:
    """Баланс наличных по всем сотрудникам (два уровня: исполнители + ответственные)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, first_name, last_name, can_manage_cash FROM staff WHERE active=TRUE ORDER BY can_manage_cash DESC, last_name, first_name")
        result = []
        for s in rows:
            bal = await get_my_cash_balance(s['id'])
            if bal['collected'] == 0 and bal['received_from_others'] == 0:
                continue  # пропускаем тех у кого нет движения
            result.append({
                "id": s['id'],
                "first_name": s['first_name'],
                "last_name":  s['last_name'],
                "can_manage_cash": s['can_manage_cash'],
                **bal,
            })
        return result

async def add_cash_handover(from_staff_id: int, to_staff_id: int, amount: float, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note)
            VALUES ($1,$2,$3,$4) RETURNING *
        """, from_staff_id, to_staff_id, amount, note)
        return dict(row) if row else {}

async def get_cash_handovers(limit: int = 50) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name,
                   TRIM(COALESCE(st.last_name,'') || ' ' || COALESCE(st.first_name,'')) AS to_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            LEFT JOIN staff st ON st.id = ch.to_staff_id
            ORDER BY ch.created_at DESC LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

async def confirm_cash_handover(handover_id: int, confirmed_by: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cash_handovers SET status='confirmed', confirmed_at=NOW(), confirmed_by=$2
            WHERE id=$1 RETURNING *
        """, handover_id, confirmed_by)
        return dict(row) if row else {}

async def get_pending_handovers_for(staff_id: int) -> list:
    """Входящие неподтверждённые передачи для данного сотрудника."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_staff_id=$1 AND ch.status='pending'
            ORDER BY ch.created_at DESC
        """, staff_id)
        return [dict(r) for r in rows]

async def confirm_payment(payment_id: int, confirmed_by: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_payments SET confirmed=TRUE, confirmed_by=$2, confirmed_at=NOW()
            WHERE id=$1 RETURNING *
        """, payment_id, confirmed_by)
        return dict(row) if row else {}

async def save_payment_receipt(payment_id: int, receipt_url: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE order_payments SET receipt_url=$2 WHERE id=$1 RETURNING *",
            payment_id, receipt_url)
        return dict(row) if row else {}

async def get_unconfirmed_payments() -> list:
    """Неподтверждённые платежи картой/переводом (только ожидающие, не отклонённые)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT p.*,
                   o.client_first_name, o.client_last_name, o.short_address,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_full_name,
                   s.phone AS staff_phone
            FROM order_payments p
            LEFT JOIN orders o ON o.id = p.order_id
            LEFT JOIN staff s ON s.id = p.created_by_staff_id
            WHERE p.method IN ('card','transfer') AND p.confirmed=FALSE AND p.confirmed_at IS NULL
            ORDER BY p.created_at DESC
        """)
        return [dict(r) for r in rows]

async def get_cash_tg_channel() -> str:
    if not pool: return ""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT cash_tg_channel_id FROM settings LIMIT 1")
            return (row['cash_tg_channel_id'] or "") if row else ""
    except Exception:
        return ""

async def get_media_channel_id() -> str:
    if not pool: return ""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT media_channel_id FROM settings LIMIT 1")
            return (row['media_channel_id'] or "") if row else ""
    except Exception:
        return ""

# ── plans (roadmap) ──────────────────────────────────────────────────────────

async def ensure_plans_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS plans (
                id          SERIAL PRIMARY KEY,
                title       VARCHAR(500) NOT NULL,
                description TEXT         DEFAULT '',
                status      VARCHAR(20)  DEFAULT 'pending',
                priority    VARCHAR(20)  DEFAULT 'normal',
                created_at  TIMESTAMPTZ  DEFAULT NOW(),
                done_at     TIMESTAMPTZ  DEFAULT NULL
            )
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM plans")
        if count == 0:
            seed = [
                ("Архивация заказов",
                 "Каждые 60 дней переносить доставленные и отменённые заказы в отдельную таблицу orders_archive, чтобы снизить нагрузку на основную таблицу orders. Добавить кнопку ручной архивации в «Обслуживание БД» и автоматический cron.",
                 "high"),
                ("Флоу мастерской (мойщик)",
                 "Реализовать полный цикл работы мойщика: список позиций с названиями услуг и размерами, замеры (ширина/длина/площадь), подтверждение приёмки, смена статусов Получен → Мойка → Сушка → Готов. Мойщик работает только со своими назначенными заказами.",
                 "high"),
                ("Аналитика и отчёты",
                 "Страница отчётов в admin: выручка за период (день/неделя/месяц), количество заказов по статусам, топ услуг по площади и сумме, загруженность мастерской. Экспорт в Excel.",
                 "normal"),
                ("История изменений заказа",
                 "Полный лог всех действий по заказу: кто и когда изменил статус, добавил позицию, добавил/отклонил оплату, изменил адрес. Уже частично есть order_activity — расширить и красиво отобразить в карточке заказа.",
                 "normal"),
                ("SMS / TG уведомления клиенту по статусам",
                 "Автоматически отправлять клиенту сообщение в Telegram при каждой смене статуса заказа (шаблоны уже есть в tg_status_messages). Проверить и доработать: кнопки «Тест отправки», статистика доставки.",
                 "normal"),
                ("Мобильная версия staff (PWA)",
                 "Улучшить работу staff.html на мобильном: добавить иконку на рабочий стол (PWA manifest), офлайн-заглушку, оптимизировать таблицы и модалки для маленьких экранов.",
                 "low"),
            ]
            await conn.executemany(
                "INSERT INTO plans(title, description, priority) VALUES($1, $2, $3)",
                seed
            )

async def get_plans():
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM plans ORDER BY status, priority DESC, created_at DESC")
    return [dict(r) for r in rows]

async def create_plan(title: str, description: str, priority: str):
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO plans(title,description,priority) VALUES($1,$2,$3) RETURNING *",
            title, description, priority
        )
    return dict(row)

async def update_plan(plan_id: int, **kwargs):
    if not pool: return None
    fields = {k: v for k, v in kwargs.items() if k in ('title','description','status','priority','done_at')}
    if not fields: return None
    sets   = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    values = list(fields.values())
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE plans SET {sets} WHERE id=$1 RETURNING *", plan_id, *values
        )
    return dict(row) if row else None

async def delete_plan(plan_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM plans WHERE id=$1", plan_id)


# ── Chat ──────────────────────────────────────────────────────────────────────

async def ensure_chat_tables():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_sessions (
            id           SERIAL PRIMARY KEY,
            code         VARCHAR(12) UNIQUE NOT NULL,
            client_phone VARCHAR(20) DEFAULT '',
            client_name  VARCHAR(100) DEFAULT '',
            branch       VARCHAR(50) DEFAULT '',
            status       VARCHAR(20) DEFAULT 'pending',
            claimed_by   INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            claimed_name VARCHAR(100) DEFAULT '',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            updated_at   TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_status ON chat_sessions(status);
        CREATE INDEX IF NOT EXISTS idx_chat_sessions_code   ON chat_sessions(code);

        CREATE TABLE IF NOT EXISTS chat_messages (
            id          SERIAL PRIMARY KEY,
            session_id  INTEGER REFERENCES chat_sessions(id) ON DELETE CASCADE,
            sender_type VARCHAR(10) NOT NULL,
            sender_name VARCHAR(100) DEFAULT '',
            text        TEXT NOT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON chat_messages(session_id);
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS last_client_msg_at TIMESTAMPTZ;
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS warned_at TIMESTAMPTZ;
        ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS lang VARCHAR(5) DEFAULT 'uz';
        """)

async def create_chat_session(code: str, client_phone: str = '', client_name: str = '', branch: str = '', lang: str = 'uz') -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO chat_sessions (code, client_phone, client_name, branch, lang) VALUES ($1,$2,$3,$4,$5) RETURNING *",
            code, client_phone, client_name, branch, lang
        )
        return dict(row) if row else None

async def get_chat_session(code: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM chat_sessions WHERE code=$1", code)
        return dict(row) if row else None

async def get_active_chat_sessions() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM chat_sessions WHERE status IN ('pending','active') ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]

async def is_first_client_message(session_id: int) -> bool:
    """True если клиент ещё не писал ни одного сообщения в этой сессии."""
    if not pool: return False
    async with pool.acquire() as conn:
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM chat_messages WHERE session_id=$1 AND sender_type='client'",
            session_id
        )
        return count == 0

async def get_active_chat_by_phone(phone: str) -> dict:
    """Найти активный/pending чат клиента по номеру телефона."""
    if not pool or not phone: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM chat_sessions WHERE client_phone=$1 AND status IN ('pending','active') ORDER BY created_at DESC LIMIT 1",
            phone.strip()
        )
        return dict(row) if row else None

async def get_closed_chat_sessions(limit: int = 50, offset: int = 0,
                                    staff_id: int = None, own_only: bool = False) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        if own_only and staff_id:
            rows = await conn.fetch(
                "SELECT * FROM chat_sessions WHERE status='closed' AND claimed_by=$1 ORDER BY updated_at DESC LIMIT $2 OFFSET $3",
                staff_id, limit, offset
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM chat_sessions WHERE status='closed' ORDER BY updated_at DESC LIMIT $1 OFFSET $2",
                limit, offset
            )
        return [dict(r) for r in rows]

async def claim_chat_session(code: str, staff_id: int, staff_name: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """UPDATE chat_sessions SET status='active', claimed_by=$2, claimed_name=$3, updated_at=NOW()
               WHERE code=$1 AND (claimed_by IS NULL OR claimed_by=$2) RETURNING *""",
            code, staff_id, staff_name
        )
        return dict(row) if row else None

async def close_chat_session(code: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE chat_sessions SET status='closed', updated_at=NOW() WHERE code=$1 RETURNING *", code
        )
        return dict(row) if row else None

async def add_chat_message(session_id: int, sender_type: str, sender_name: str, text: str) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO chat_messages (session_id, sender_type, sender_name, text) VALUES ($1,$2,$3,$4) RETURNING *",
            session_id, sender_type, sender_name, text
        )
        return dict(row) if row else None

async def get_chat_messages(session_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM chat_messages WHERE session_id=$1 ORDER BY created_at ASC", session_id
        )
        return [dict(r) for r in rows]

async def get_staff_for_chat_push() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id FROM staff WHERE active=TRUE AND role IN ('admin','manager','callcenter')"
        )
        return [r['id'] for r in rows]

async def ensure_chat_templates():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS chat_templates (
            id         SERIAL PRIMARY KEY,
            key        VARCHAR(30) DEFAULT 'quick',
            lang       VARCHAR(5) NOT NULL DEFAULT 'uz',
            text       TEXT NOT NULL,
            sort_order INT DEFAULT 0,
            active     BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_templates_lang ON chat_templates(lang, key);
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM chat_templates")
        if count == 0:
            await conn.executemany(
                "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                _CHAT_TEMPLATE_SEED
            )

async def get_chat_templates(lang: str = None, key: str = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        conditions, args = ["active=TRUE"], []
        if lang:  args.append(lang);  conditions.append(f"lang=${len(args)}")
        if key:   args.append(key);   conditions.append(f"key=${len(args)}")
        rows = await conn.fetch(
            f"SELECT * FROM chat_templates WHERE {' AND '.join(conditions)} ORDER BY lang, sort_order, id",
            *args
        )
        return [dict(r) for r in rows]

async def get_all_chat_templates() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM chat_templates ORDER BY lang, key, sort_order, id")
        return [dict(r) for r in rows]

async def upsert_chat_template(data: dict) -> dict:
    if not pool: return None
    async with pool.acquire() as conn:
        tid = data.get('id')
        if tid:
            row = await conn.fetchrow(
                "UPDATE chat_templates SET key=$2,lang=$3,text=$4,sort_order=$5,active=$6 WHERE id=$1 RETURNING *",
                tid, data['key'], data['lang'], data['text'],
                data.get('sort_order', 0), data.get('active', True)
            )
        else:
            row = await conn.fetchrow(
                "INSERT INTO chat_templates (key,lang,text,sort_order,active) VALUES ($1,$2,$3,$4,$5) RETURNING *",
                data['key'], data['lang'], data['text'], data.get('sort_order', 0), data.get('active', True)
            )
        return dict(row) if row else None

async def delete_chat_template(tid: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM chat_templates WHERE id=$1", tid)

_CHAT_TEMPLATE_SEED = [
    # ── Авто-сообщения RU ──
    ('welcome',      'ru', "Здравствуйте, {name}! 👋 Рады видеть вас в ARTEZ. Напишите ваш вопрос — оператор ответит в ближайшее время.", 0),
    ('auto_reply',   'ru', "✅ {name}, ваше сообщение принято! Оператор ответит в течение 1–3 минут. Спасибо за ожидание 🙏", 1),
    ('warn_timeout', 'ru', "⏰ {name}, вы давно не отвечаете. Чат будет автоматически закрыт через 2 минуты.", 2),
    ('bye_m',        'ru', "Спасибо за обращение, {name}! Рад был помочь 😊 Если появятся вопросы — мы всегда здесь. Хорошего дня!", 3),
    ('bye_f',        'ru', "Спасибо за обращение, {name}! Рада была помочь 😊 Если появятся вопросы — мы всегда здесь. Хорошего дня!", 4),
    # ── Авто-сообщения UZ ──
    ('welcome',      'uz', "Salom, {name}! 👋 Sizni ARTEZ'da ko'rganimizdan xursandmiz. Savolingizni yozing — operator tez orada javob beradi.", 0),
    ('auto_reply',   'uz', "✅ {name}, xabaringiz qabul qilindi! Operator 1–3 daqiqa ichida javob beradi. Kutganingiz uchun rahmat 🙏", 1),
    ('warn_timeout', 'uz', "⏰ {name}, siz uzoq vaqtdan beri javob bermadingiz. Chat 2 daqiqadan so'ng avtomatik yopiladi.", 2),
    ('bye_m',        'uz', "Murojaat qilganingiz uchun rahmat, {name}! Yordam bera olganim uchun xursandman 😊 Savol bo'lsa — biz doim shu yerdamiz. Yaxshi kun!", 3),
    ('bye_f',        'uz', "Murojaat qilganingiz uchun rahmat, {name}! Yordam bera olganim uchun xursandman 😊 Savol bo'lsa — biz doim shu yerdamiz. Yaxshi kun!", 4),
    # ── Быстрые ответы RU ──
    ('quick', 'ru', "Здравствуйте, {name}! Чем могу помочь? 😊", 10),
    ('quick', 'ru', "Какое изделие нужно почистить? (ковёр, диван, матрас, шторы...)", 11),
    ('quick', 'ru', "Стоимость зависит от размера и состояния. Пришлите фото или назовите размеры? 📐", 12),
    ('quick', 'ru', "Выезд мастера для замера и забора — бесплатно 🚗", 13),
    ('quick', 'ru', "Срок чистки — 1–3 дня. Вернём чистым и свежим 🧹", 14),
    ('quick', 'ru', "Работаем ежедневно с 9:00 до 20:00 🕐", 15),
    ('quick', 'ru', "Оплата при получении — наличными или картой 💳", 16),
    ('quick', 'ru', "Используем профессиональную химию — безопасно для детей и аллергиков ✅", 17),
    ('quick', 'ru', "Уточните адрес, {name}? Выедем в удобное для вас время 📍", 18),
    ('quick', 'ru', "Записываем вас! Мастер свяжется для подтверждения времени ✅", 19),
    ('quick', 'ru', "Если есть ещё вопросы — спрашивайте, с удовольствием помогу 😊", 20),
    ('quick', 'ru', "Спасибо, {name}! Ждём ваше изделие 🙏", 21),
    # ── Быстрые ответы UZ ──
    ('quick', 'uz', "Salom, {name}! Qanday yordam bera olaman? 😊", 10),
    ('quick', 'uz', "Qaysi mahsulotni tozalash kerak? (gilam, divan, matras, parda...)", 11),
    ('quick', 'uz', "Narx o'lcham va holatiga qarab. Rasm yuboring yoki o'lchamlarini ayting? 📐", 12),
    ('quick', 'uz', "Usta o'lchov va olib ketish uchun chiqishi bepul 🚗", 13),
    ('quick', 'uz', "Tozalash muddati — 1–3 kun. Toza va yangi holda qaytaramiz 🧹", 14),
    ('quick', 'uz', "Har kuni soat 9:00 dan 20:00 gacha ishlaymiz 🕐", 15),
    ('quick', 'uz', "To'lov qabul qilishda — naqd yoki karta orqali 💳", 16),
    ('quick', 'uz', "Professional kimyo ishlatamiz — bolalar va allergiklar uchun xavfsiz ✅", 17),
    ('quick', 'uz', "Manzilni ayta olasizmi, {name}? Qulay vaqtingizda chiqamiz 📍", 18),
    ('quick', 'uz', "Yozib olyapmiz! Usta vaqtni tasdiqlash uchun bog'lanadi ✅", 19),
    ('quick', 'uz', "Yana savollar bo'lsa — so'rang, mamnuniyat bilan yordam beraman 😊", 20),
    ('quick', 'uz', "Rahmat, {name}! Mahsulotingizni kutamiz 🙏", 21),
]

async def seed_chat_templates_forced():
    """Обновить auto-шаблоны (welcome/auto_reply/warn/bye) по key+lang,
       добавить quick-шаблоны если текста ещё нет."""
    if not pool: return 0
    updated = 0
    inserted = 0
    async with pool.acquire() as conn:
        existing_texts = {r['text'] for r in await conn.fetch("SELECT text FROM chat_templates")}
        for key, lang, text, sort_order in _CHAT_TEMPLATE_SEED:
            if key == 'quick':
                if text not in existing_texts:
                    await conn.execute(
                        "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                        key, lang, text, sort_order
                    )
                    inserted += 1
            else:
                row = await conn.fetchrow(
                    "SELECT id FROM chat_templates WHERE key=$1 AND lang=$2 LIMIT 1", key, lang
                )
                if row:
                    await conn.execute(
                        "UPDATE chat_templates SET text=$1, sort_order=$2 WHERE id=$3",
                        text, sort_order, row['id']
                    )
                    updated += 1
                else:
                    await conn.execute(
                        "INSERT INTO chat_templates (key, lang, text, sort_order) VALUES ($1,$2,$3,$4)",
                        key, lang, text, sort_order
                    )
                    inserted += 1
    return updated + inserted

async def get_chat_template_text(key: str, lang: str) -> str:
    """Получить текст шаблона по ключу и языку, fallback на uz."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT text FROM chat_templates WHERE key=$1 AND lang=$2 AND active=TRUE LIMIT 1",
            key, lang
        )
        if not row:
            row = await conn.fetchrow(
                "SELECT text FROM chat_templates WHERE key=$1 AND active=TRUE LIMIT 1", key
            )
        return row['text'] if row else None

async def touch_chat_client_activity(code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_sessions SET last_client_msg_at=NOW(), warned_at=NULL, updated_at=NOW() WHERE code=$1",
            code
        )

async def get_sessions_to_warn() -> list:
    """Активные сессии, где клиент молчит 10+ мин и предупреждение ещё не отправлено."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*, s.gender AS staff_gender, s.first_name AS staff_first_name
            FROM chat_sessions cs
            LEFT JOIN staff s ON s.id = cs.claimed_by
            WHERE cs.status = 'active'
              AND cs.last_client_msg_at IS NOT NULL
              AND cs.last_client_msg_at < NOW() - INTERVAL '10 minutes'
              AND cs.warned_at IS NULL
        """)
        return [dict(r) for r in rows]

async def get_sessions_to_close() -> list:
    """Активные сессии, где предупреждение было >2 мин назад."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*, s.gender AS staff_gender, s.first_name AS staff_first_name
            FROM chat_sessions cs
            LEFT JOIN staff s ON s.id = cs.claimed_by
            WHERE cs.status = 'active'
              AND cs.warned_at IS NOT NULL
              AND cs.warned_at < NOW() - INTERVAL '2 minutes'
        """)
        return [dict(r) for r in rows]

async def set_chat_warned(code: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE chat_sessions SET warned_at=NOW(), updated_at=NOW() WHERE code=$1", code
        )
