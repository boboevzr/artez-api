import os
import asyncpg
import logging
from datetime import datetime, timezone, timedelta

_TASHKENT = timezone(timedelta(hours=5))

def _tz_range(date_from: str, date_to: str):
    """Преобразует строки дат (Ташкент) в UTC границы для TIMESTAMPTZ-сравнения."""
    df = datetime.fromisoformat(date_from).replace(tzinfo=_TASHKENT)
    dt = datetime.fromisoformat(date_to).replace(tzinfo=_TASHKENT) + timedelta(days=1)
    return df, dt

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
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS address    VARCHAR(200)  DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS car_plate  VARCHAR(20)   DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS osago_expiry DATE        DEFAULT NULL",
        "ALTER TABLE users   ADD COLUMN IF NOT EXISTS last_login  TIMESTAMPTZ  DEFAULT NULL",
        "UPDATE users SET last_login = updated_at WHERE last_login IS NULL AND updated_at IS NOT NULL",
        """CREATE TABLE IF NOT EXISTS tg_phone_links (
            phone      VARCHAR(20) PRIMARY KEY,
            tg_id      BIGINT      NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS total_price   INT          DEFAULT NULL",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE orders      ADD COLUMN IF NOT EXISTS discount_sum  NUMERIC(12,2) DEFAULT 0",
        "ALTER TABLE leads       ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS address       TEXT         DEFAULT ''",
        "ALTER TABLE crm_clients ADD COLUMN IF NOT EXISTS short_address VARCHAR(200) DEFAULT ''",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_edit_items      BOOLEAN DEFAULT TRUE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_measure         BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_approve_measure BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff       ADD COLUMN IF NOT EXISTS can_override_measure BOOLEAN DEFAULT FALSE",
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
        # Маршруты: хранить TG message_id отправленных сообщений водителям
        "ALTER TABLE routes ADD COLUMN IF NOT EXISTS tg_delivery_msg_ids JSONB DEFAULT NULL",
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
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS to_type VARCHAR(20) DEFAULT 'staff'",
        # Платежи: подтверждение и фото чека
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed BOOLEAN DEFAULT FALSE",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS receipt_url TEXT DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS reject_note TEXT DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS receipt_file_id TEXT DEFAULT NULL",
        # Таблица настроек (создаём если не существует + гарантируем одну строку)
        "CREATE TABLE IF NOT EXISTS settings (id SERIAL PRIMARY KEY)",
        "INSERT INTO settings DEFAULT VALUES ON CONFLICT DO NOTHING",
        # Настройки: ТГ канал кассы
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS cash_tg_channel_id VARCHAR(50) DEFAULT NULL",
        # Настройки: канал медиафайлов (замеры, чеки и т.д.)
        "ALTER TABLE settings ADD COLUMN IF NOT EXISTS media_channel_id VARCHAR(50) DEFAULT NULL",
        # Уведомления о новых пользователях сайта
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS notify_new_users BOOLEAN DEFAULT FALSE",
        "ALTER TABLE clients ADD COLUMN IF NOT EXISTS blocked BOOLEAN DEFAULT FALSE",
        # Автодозвон: кампании
        """CREATE TABLE IF NOT EXISTS autodial_campaigns (
            id             SERIAL PRIMARY KEY,
            name           VARCHAR(200) NOT NULL,
            status         VARCHAR(20) DEFAULT 'draft',
            ivr_exten      VARCHAR(20) DEFAULT '1000',
            max_parallel   INT DEFAULT 3,
            source_type    VARCHAR(20) DEFAULT 'both',
            manual_phones  TEXT DEFAULT '',
            created_at     TIMESTAMPTZ DEFAULT NOW(),
            started_at     TIMESTAMPTZ,
            finished_at    TIMESTAMPTZ,
            total_count    INT DEFAULT 0,
            dialed_count   INT DEFAULT 0,
            answered_count INT DEFAULT 0,
            failed_count   INT DEFAULT 0
        )""",
        # Автодозвон: записи звонков
        """CREATE TABLE IF NOT EXISTS autodial_calls (
            id            SERIAL PRIMARY KEY,
            campaign_id   INT REFERENCES autodial_campaigns(id) ON DELETE CASCADE,
            source_type   VARCHAR(20) DEFAULT 'manual',
            source_id     INT DEFAULT NULL,
            phone         VARCHAR(20) NOT NULL,
            name          VARCHAR(200) DEFAULT '',
            status        VARCHAR(30) DEFAULT 'pending',
            ami_action_id VARCHAR(100),
            started_at    TIMESTAMPTZ,
            answered_at   TIMESTAMPTZ,
            hangup_at     TIMESTAMPTZ,
            hangup_cause  VARCHAR(50),
            pressed_key   VARCHAR(5),
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_autodial_calls_campaign ON autodial_calls(campaign_id)",
        # Автодозвон: группы контактов
        """CREATE TABLE IF NOT EXISTS autodial_groups (
            id         SERIAL PRIMARY KEY,
            name       VARCHAR(200) NOT NULL,
            notes      TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS autodial_group_members (
            id          SERIAL PRIMARY KEY,
            group_id    INT REFERENCES autodial_groups(id) ON DELETE CASCADE,
            phone       VARCHAR(20) NOT NULL,
            name        VARCHAR(200) DEFAULT '',
            source_type VARCHAR(20) DEFAULT 'manual',
            source_id   INT DEFAULT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, phone)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_agm_group ON autodial_group_members(group_id)",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS group_ids JSONB DEFAULT '[]'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS caller_id VARCHAR(20) DEFAULT '1000'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_time_from TIME DEFAULT '09:00'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_time_to   TIME DEFAULT '21:00'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_days SMALLINT[] DEFAULT '{0,1,2,3,4,5,6}'",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_date_from DATE",
        "ALTER TABLE autodial_campaigns ADD COLUMN IF NOT EXISTS sched_date_to   DATE",
        """CREATE TABLE IF NOT EXISTS autodial_callerids (
            id         SERIAL PRIMARY KEY,
            number     VARCHAR(20) NOT NULL,
            label      VARCHAR(100) DEFAULT '',
            sort_order INT DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS autodial_ivrs (
            id         SERIAL PRIMARY KEY,
            exten      VARCHAR(20) NOT NULL,
            label      VARCHAR(100) DEFAULT '',
            sort_order INT DEFAULT 0
        )""",
        "ALTER TABLE autodial_ivrs ADD COLUMN IF NOT EXISTS ivr_group VARCHAR(30) DEFAULT 'promo'",
        # SMS группы и контакты (отдельно от автодозвона)
        """CREATE TABLE IF NOT EXISTS sms_groups (
            id          SERIAL PRIMARY KEY,
            name        VARCHAR(200) NOT NULL,
            description TEXT DEFAULT '',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS sms_contacts (
            id               SERIAL PRIMARY KEY,
            group_id         INT REFERENCES sms_groups(id) ON DELETE CASCADE,
            phone            VARCHAR(20) NOT NULL,
            name             VARCHAR(200) DEFAULT '',
            status           VARCHAR(20) DEFAULT 'active',
            last_sms_at      TIMESTAMPTZ,
            last_sms_status  VARCHAR(20),
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(group_id, phone)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_sms_contacts_group ON sms_contacts(group_id)",
        "CREATE INDEX IF NOT EXISTS idx_sms_contacts_status ON sms_contacts(status)",
        # Смены: поддержка открытия смены и привязки операций
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ DEFAULT NULL",
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'closed'",
        "ALTER TABLE cash_shifts ADD COLUMN IF NOT EXISTS opened_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS shift_id INTEGER REFERENCES cash_shifts(id) ON DELETE SET NULL DEFAULT NULL",
        # Расходы: источник выплаты (сейф / банк / наличные)
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_from VARCHAR(20) DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_by INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ DEFAULT NULL",
        # Передачи наличных: TG-сообщение для редактирования после confirm/reject
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS tg_chat_id BIGINT DEFAULT NULL",
        "ALTER TABLE cash_handovers ADD COLUMN IF NOT EXISTS tg_msg_id BIGINT DEFAULT NULL",
        # Расходы: получатель (сотрудник) — для зарплаты/аванса
        "ALTER TABLE expenses ADD COLUMN IF NOT EXISTS for_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL DEFAULT NULL",
        # Категории расходов: флаг «требует указать сотрудника»
        "ALTER TABLE expense_categories ADD COLUMN IF NOT EXISTS for_staff BOOLEAN DEFAULT FALSE",
        # Зарплата сотрудников — рабочих дней в месяц
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS salary_work_days INTEGER DEFAULT 26",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS fired BOOLEAN DEFAULT FALSE",
        "ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_view_timesheet BOOLEAN DEFAULT FALSE",
        # Процентные ставки по типам работ
        """CREATE TABLE IF NOT EXISTS staff_salary_percents (
            id       SERIAL PRIMARY KEY,
            staff_id INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            role     VARCHAR(20) NOT NULL,
            percent  NUMERIC(5,2) NOT NULL DEFAULT 0,
            UNIQUE(staff_id, role)
        )""",
        # Ставки за единицу измерения по услугам
        """CREATE TABLE IF NOT EXISTS staff_salary_per_unit (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            service_key VARCHAR(30) NOT NULL,
            type_key    VARCHAR(20) NOT NULL,
            total_rate  NUMERIC(10,2) DEFAULT 0,
            unit_rate   NUMERIC(10,2) DEFAULT 0,
            rate_type   VARCHAR(10) DEFAULT 'sum',
            UNIQUE(staff_id, service_key, type_key)
        )""",
        "ALTER TABLE staff_salary_per_unit ADD COLUMN IF NOT EXISTS rate_type VARCHAR(10) DEFAULT 'sum'",
        # KPI правила
        """CREATE TABLE IF NOT EXISTS staff_salary_kpi (
            id           SERIAL PRIMARY KEY,
            staff_id     INTEGER REFERENCES staff(id) ON DELETE CASCADE,
            metric       VARCHAR(30) NOT NULL,
            target_value NUMERIC(10,2) NOT NULL,
            bonus_type   VARCHAR(10) DEFAULT 'fixed',
            bonus_value  NUMERIC(10,2) NOT NULL DEFAULT 0
        )""",
        # Начисления агентам за лиды (комиссия)
        """CREATE TABLE IF NOT EXISTS staff_commissions (
            id           SERIAL PRIMARY KEY,
            staff_id     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            order_id     INTEGER REFERENCES orders(id) ON DELETE SET NULL,
            order_num    VARCHAR(20) DEFAULT '',
            lead_id      INTEGER REFERENCES leads(id) ON DELETE SET NULL,
            amount       NUMERIC(12,2) NOT NULL DEFAULT 0,
            percent      NUMERIC(5,2)  NOT NULL DEFAULT 0,
            order_total  NUMERIC(12,2) NOT NULL DEFAULT 0,
            note         TEXT DEFAULT '',
            status       VARCHAR(20) DEFAULT 'pending',
            paid_at      TIMESTAMPTZ DEFAULT NULL,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_staff_commissions_staff ON staff_commissions(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_staff_commissions_order ON staff_commissions(order_id)",
        # Табель рабочего времени
        """CREATE TABLE IF NOT EXISTS timesheet (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
            date        DATE NOT NULL,
            hours       NUMERIC(4,1) DEFAULT 8,
            type        VARCHAR(20) DEFAULT 'work'
                        CHECK (type IN ('work','overtime','sick','vacation','dayoff')),
            note        TEXT DEFAULT '',
            created_at  TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(staff_id, date)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_timesheet_staff  ON timesheet(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_timesheet_date   ON timesheet(date)",
        # Отметки прихода/ухода (для почасовых сотрудников) — журнал событий (несколько пар в день)
        "DROP TABLE IF EXISTS staff_attendance",
        """CREATE TABLE IF NOT EXISTS staff_attendance_events (
            id          SERIAL PRIMARY KEY,
            staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
            event_type  VARCHAR(10) NOT NULL CHECK (event_type IN ('in','out')),
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_attendance_events_staff ON staff_attendance_events(staff_id)",
        "CREATE INDEX IF NOT EXISTS idx_attendance_events_created ON staff_attendance_events(created_at)",
        # Журнал отправок чека клиенту — для предупреждения о повторной отправке
        """CREATE TABLE IF NOT EXISTS order_receipt_log (
            id          SERIAL PRIMARY KEY,
            order_id    INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
            staff_id    INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            staff_name  VARCHAR(100) DEFAULT '',
            note        TEXT DEFAULT '',
            tg_chat_id    BIGINT DEFAULT NULL,
            tg_message_id BIGINT DEFAULT NULL,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )""",
        "CREATE INDEX IF NOT EXISTS idx_receipt_log_order ON order_receipt_log(order_id)",
        "ALTER TABLE order_receipt_log ADD COLUMN IF NOT EXISTS tg_chat_id    BIGINT DEFAULT NULL",
        "ALTER TABLE order_receipt_log ADD COLUMN IF NOT EXISTS tg_message_id BIGINT DEFAULT NULL",
    ]
    async with pool.acquire() as c:
        for sql in other_migrations:
            try:
                await c.execute(sql)
            except Exception:
                pass
        # IVR 7000 — основной, всегда должен быть в списке
        await c.execute(
            "INSERT INTO autodial_ivrs (exten,label,ivr_group) "
            "SELECT '7000','Общее приветствие','promo' WHERE NOT EXISTS "
            "(SELECT 1 FROM autodial_ivrs WHERE exten='7000')"
        )
        # IVR: если нет записей с группами — чистим старые и засеваем новые
        cnt_grouped = await c.fetchval("SELECT COUNT(*) FROM autodial_ivrs WHERE ivr_group IS NOT NULL AND ivr_group != ''")
        if cnt_grouped == 0:
            await c.execute("DELETE FROM autodial_ivrs")
            ivr_seeds = [
                # 📢 Рекламные (7001–7005)
                ("7001","Реклама 1","promo"),("7002","Реклама 2","promo"),
                ("7003","Реклама 3","promo"),("7004","Реклама 4","promo"),("7005","Реклама 5","promo"),
                # 🎉 Поздравления (7011–7015)
                ("7011","Поздравление 1","greetings"),("7012","Поздравление 2","greetings"),
                ("7013","Поздравление 3","greetings"),("7014","Поздравление 4","greetings"),("7015","Поздравление 5","greetings"),
                # 🔔 Напоминания (7021–7025)
                ("7021","Напоминание 1","reminders"),("7022","Напоминание 2","reminders"),
                ("7023","Напоминание 3","reminders"),("7024","Напоминание 4","reminders"),("7025","Напоминание 5","reminders"),
            ]
            await c.executemany(
                "INSERT INTO autodial_ivrs (exten,label,ivr_group) VALUES ($1,$2,$3)",
                ivr_seeds
            )

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
    await ensure_washer_notifications_table()

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

    # ── Шаг 13: источник и tg_id клиента в лидах ───────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS source       VARCHAR(20) DEFAULT 'staff';
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS client_tg_id BIGINT DEFAULT NULL;
        """)

    # ── Шаг 14: дата и время вывоза в лидах ─────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS pickup_date VARCHAR(20)  DEFAULT '';
        ALTER TABLE leads ADD COLUMN IF NOT EXISTS pickup_time VARCHAR(100) DEFAULT '';
        """)

    # ── Шаг 15b: tg_id водителя в платежах ──────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE order_payments ADD COLUMN IF NOT EXISTS driver_tg_id BIGINT;
        """)

    # ── Шаг 15: таблица услуг с именами RU/UZ ────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS services (
            key       VARCHAR(30) PRIMARY KEY,
            name_ru   VARCHAR(100) NOT NULL DEFAULT '',
            name_uz   VARCHAR(100) NOT NULL DEFAULT '',
            emoji     VARCHAR(10)  NOT NULL DEFAULT '',
            order_idx INTEGER      NOT NULL DEFAULT 0
        );
        INSERT INTO services (key, name_ru, name_uz, emoji, order_idx) VALUES
            ('carpet',      'Чистка ковра',         'Gilam tozalash',      '🧺', 1),
            ('carpet_home', 'Чистка ковра на дому', 'Uyda gilam tozalash', '🏠', 2),
            ('sofa',        'Диван, кресло',         'Divan, kreslo',       '🛋', 3),
            ('mattress',    'Матрас, одеяло',        'Matras, ko''rpa',     '🛏', 4),
            ('curtains',    'Шторы',                 'Pardalar',            '🪟', 5)
        ON CONFLICT (key) DO NOTHING;
        """)

    # ── Шаг 16: учёт долгов по заказам ──────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_approve_debt BOOLEAN DEFAULT FALSE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_responsible_id INTEGER REFERENCES staff(id) ON DELETE SET NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_due_date DATE;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS debt_approved_at TIMESTAMPTZ;
        ALTER TABLE route_orders ADD COLUMN IF NOT EXISTS driver_confirmed BOOLEAN DEFAULT FALSE;
        """)

    # ── Шаг 17: запросы скидок от водителей ──────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS discount_requests (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER REFERENCES orders(id) ON DELETE CASCADE,
            order_num       VARCHAR(50),
            driver_tg_id    BIGINT,
            requested_amount NUMERIC(12,2) NOT NULL,
            status          VARCHAR(20) DEFAULT 'pending',
            approved_amount NUMERIC(12,2),
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            resolved_by     INTEGER REFERENCES staff(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_discount_requests_status ON discount_requests(status);
        """)

    # ── Шаг 18: запросы долгового одобрения ──────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS debt_approval_requests (
            id              SERIAL PRIMARY KEY,
            order_id        INTEGER REFERENCES orders(id) ON DELETE CASCADE,
            order_num       VARCHAR(50),
            driver_tg_id    BIGINT,
            debt_amount     NUMERIC(12,2),
            mgr_msgs        JSONB DEFAULT '{}',
            status          VARCHAR(20) DEFAULT 'pending',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            resolved_at     TIMESTAMPTZ,
            resolved_by     INTEGER REFERENCES staff(id) ON DELETE SET NULL,
            resolution      VARCHAR(20),
            responsible_id  INTEGER REFERENCES staff(id) ON DELETE SET NULL
        );
        CREATE INDEX IF NOT EXISTS idx_debt_approval_status ON debt_approval_requests(status);
        """)

    # ── Шаг 19: флаг водителя ────────────────────────────────────────────
    async with pool.acquire() as c:
        await c.execute("ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_drive BOOLEAN DEFAULT FALSE;")

    # ── Шаг 20: промо-акции (generic-модель, одна кампания засеяна) ──────
    async with pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS promotions (
            id              SERIAL PRIMARY KEY,
            code            VARCHAR(50) UNIQUE NOT NULL,
            title_ru        VARCHAR(200) NOT NULL,
            title_uz        VARCHAR(200) NOT NULL,
            text_ru         TEXT DEFAULT '',
            text_uz         TEXT DEFAULT '',
            discount_pct    NUMERIC(5,2) NOT NULL DEFAULT 0,
            starts_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            ends_at         TIMESTAMPTZ NOT NULL,
            window_hours    INTEGER NOT NULL DEFAULT 48,
            sound_enabled   BOOLEAN DEFAULT TRUE,
            target_new_only BOOLEAN DEFAULT TRUE,
            is_active       BOOLEAN DEFAULT TRUE,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS promo_user_state (
            id              SERIAL PRIMARY KEY,
            promotion_id    INTEGER NOT NULL REFERENCES promotions(id) ON DELETE CASCADE,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            shown_at        TIMESTAMPTZ,
            expires_at      TIMESTAMPTZ,
            channel         VARCHAR(10),
            used_order_id   INTEGER REFERENCES orders(id) ON DELETE SET NULL,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(promotion_id, user_id)
        );
        ALTER TABLE leads  ADD COLUMN IF NOT EXISTS promo_id INTEGER REFERENCES promotions(id) ON DELETE SET NULL;
        ALTER TABLE orders ADD COLUMN IF NOT EXISTS promo_id INTEGER REFERENCES promotions(id) ON DELETE SET NULL;
        """)
        # Единственная засеянная кампания: -20% до 31.08.2026 23:59 (Ташкент, UTC+5 → 18:59 UTC)
        await c.execute("""
        INSERT INTO promotions (code, title_ru, title_uz, text_ru, text_uz,
                                 discount_pct, ends_at, window_hours,
                                 sound_enabled, target_new_only, is_active)
        VALUES (
            'aug2026_20pct',
            'Акция: скидка 20%!',
            'Aksiya: 20% chegirma!',
            'Успей заказать до 31 августа и получи скидку 20% на первый заказ! Действует 48 часов.',
            '31 avgustgacha ulgurib buyurtma bering va birinchi buyurtmangizga 20% chegirma oling! 48 soat davomida amal qiladi.',
            20,
            '2026-08-31 18:59:00+00',
            48,
            TRUE, TRUE, TRUE
        )
        ON CONFLICT (code) DO NOTHING;
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

async def save_tg_phone_link(phone: str, tg_id: int):
    """Сохраняет связку телефон→tg_id от бота (до регистрации на сайте)."""
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO tg_phone_links (phone, tg_id, created_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (phone) DO UPDATE SET tg_id=$2, created_at=NOW()
        """, phone, tg_id)
        # Если пользователь уже зарегистрирован — сразу линкуем
        await conn.execute("""
            UPDATE users SET tg_id=$2, updated_at=NOW()
            WHERE phone=$1 AND tg_id IS NULL
        """, phone, tg_id)

async def get_tg_id_by_phone(phone: str):
    """Возвращает tg_id для телефона: сначала из users, потом из tg_phone_links."""
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT tg_id FROM users WHERE phone=$1 AND tg_id IS NOT NULL", phone)
        if row:
            return row["tg_id"]
        row = await conn.fetchrow(
            "SELECT tg_id FROM tg_phone_links WHERE phone=$1", phone)
        return row["tg_id"] if row else None


async def get_receipt_tg_id(order: dict) -> int | None:
    """Определяет tg_id клиента для отправки чека: сначала client_tg_id заказа, потом по телефону."""
    tg_id = order.get('client_tg_id')
    if tg_id:
        return tg_id
    phone = order.get('client_phone')
    if phone:
        return await get_tg_id_by_phone(phone)
    return None

async def log_receipt_send(order_id: int, staff_id: int, staff_name: str, note: str = "",
                            tg_chat_id: int | None = None, tg_message_id: int | None = None) -> None:
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO order_receipt_log (order_id, staff_id, staff_name, note, tg_chat_id, tg_message_id)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, order_id, staff_id, staff_name, note or "", tg_chat_id, tg_message_id)

async def get_last_receipt_send(order_id: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT staff_name, note, created_at FROM order_receipt_log
            WHERE order_id=$1 ORDER BY created_at DESC LIMIT 1
        """, order_id)
        return dict(row) if row else None

async def get_prior_receipt_messages(order_id: int) -> list[dict]:
    """Ранее отправленные в Telegram чеки этого заказа (для удаления при повторной отправке)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tg_chat_id, tg_message_id FROM order_receipt_log
            WHERE order_id=$1 AND tg_message_id IS NOT NULL
        """, order_id)
        return [dict(r) for r in rows]


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

async def get_all_site_users(search: str = "", limit: int = 500):
    if not pool: return []
    base = """
        SELECT u.id, u.phone, u.first_name, u.is_verified, u.tg_id,
               u.address, u.car_plate, u.osago_expiry,
               u.created_at, u.updated_at, u.last_login,
               EXISTS(SELECT 1 FROM staff s WHERE s.site_user_id = u.id AND s.active = TRUE) AS is_agent
        FROM users u
    """
    async with pool.acquire() as conn:
        if search:
            return await conn.fetch(
                base + "WHERE u.phone ILIKE $1 OR u.first_name ILIKE $1 ORDER BY u.created_at DESC LIMIT $2",
                f"%{search}%", limit)
        return await conn.fetch(base + "ORDER BY u.created_at DESC LIMIT $1", limit)


async def update_user_last_login(user_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_login=NOW() WHERE id=$1", user_id)


async def update_user_profile(user_id: int, first_name: str, address: str = None,
                               car_plate: str = None, osago_expiry=None):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET first_name=$2, address=$3, car_plate=$4, osago_expiry=$5, updated_at=NOW() WHERE id=$1
        """, user_id, first_name, address, car_plate, osago_expiry)


async def get_staff_notify_new_users():
    """Возвращает tg_id сотрудников с включённым notify_new_users."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tg_id FROM staff WHERE notify_new_users=TRUE AND tg_id IS NOT NULL AND active=TRUE")
    return [r["tg_id"] for r in rows]


async def update_tg_client(tg_id: int, data: dict):
    if not pool: return
    allowed = {"first_name", "last_name", "phone"}
    fields = {k: v for k, v in data.items() if k in allowed}
    if not fields: return
    sets = ", ".join(f"{k}=${i+2}" for i, k in enumerate(fields))
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE clients SET {sets}, updated_at=NOW() WHERE tg_id=$1",
            tg_id, *fields.values())

async def block_tg_client(tg_id: int, blocked: bool):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE clients SET blocked=$1 WHERE tg_id=$2", blocked, tg_id)

async def delete_tg_client(tg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM clients WHERE tg_id=$1", tg_id)

async def get_all_bot_client_tg_ids() -> list:
    """Все tg_id клиентов бота (таблица clients)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT tg_id FROM clients WHERE tg_id IS NOT NULL")
    return [r["tg_id"] for r in rows]

async def set_user_tg_id(phone: str, tg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET tg_id=$2, updated_at=NOW() WHERE phone=$1", phone, tg_id)


async def delete_site_user(user_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM users WHERE id=$1", user_id)
    return result != "DELETE 0"


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
#  ПРОМО-АКЦИИ
# ══════════════════════════════════════
async def _get_active_promotion(conn):
    """Текущая активная кампания (is_active=TRUE и сейчас между starts_at и ends_at)."""
    return await conn.fetchrow("""
        SELECT * FROM promotions
        WHERE is_active = TRUE AND NOW() BETWEEN starts_at AND ends_at
        ORDER BY id DESC LIMIT 1
    """)


def _promo_public_fields(promo, mode: str, expires_at) -> dict:
    return {
        "id":            promo["id"],
        "code":          promo["code"],
        "title_ru":      promo["title_ru"],
        "title_uz":      promo["title_uz"],
        "text_ru":       promo["text_ru"],
        "text_uz":       promo["text_uz"],
        "discount_pct":  float(promo["discount_pct"]) if promo["discount_pct"] is not None else 0,
        "sound_enabled": promo["sound_enabled"],
        "mode":          mode,
        "expires_at":    expires_at.isoformat() if expires_at else None,
    }


async def check_promo_eligibility(user_id: int, phone: str, channel: str) -> dict | None:
    """Единый источник правды по эквайру акции для сайта/бота (см. GET /api/promo/status).
    Возвращает None если акции нет/клиент не подходит, иначе dict с mode: full|silent|none."""
    if not pool:
        return None
    async with pool.acquire() as conn:
        promo = await _get_active_promotion(conn)
        if not promo:
            return None

        if promo["target_new_only"]:
            has_order = await conn.fetchval(
                "SELECT 1 FROM orders WHERE client_phone=$1 LIMIT 1", phone
            )
            if has_order:
                return None

        state = await conn.fetchrow(
            "SELECT * FROM promo_user_state WHERE promotion_id=$1 AND user_id=$2",
            promo["id"], user_id
        )
        if not state:
            state = await conn.fetchrow("""
                INSERT INTO promo_user_state (promotion_id, user_id, shown_at, expires_at, channel)
                VALUES ($1, $2, NOW(), NOW() + ($3 * INTERVAL '1 hour'), $4)
                ON CONFLICT (promotion_id, user_id) DO NOTHING
                RETURNING *
            """, promo["id"], user_id, promo["window_hours"], channel)
            if state:
                mode = "full"
            else:
                # Гонка: параллельный запрос уже вставил строку — перечитываем
                state = await conn.fetchrow(
                    "SELECT * FROM promo_user_state WHERE promotion_id=$1 AND user_id=$2",
                    promo["id"], user_id
                )
                if not state:
                    return None
                mode = "silent" if (state["used_order_id"] is None and state["expires_at"]
                                     and state["expires_at"] > datetime.now(timezone.utc)) else "none"
        else:
            if state["used_order_id"] is None and state["expires_at"] and state["expires_at"] > datetime.now(timezone.utc):
                mode = "silent"
            else:
                mode = "none"

        return _promo_public_fields(promo, mode, state["expires_at"])


async def get_active_promotion_public() -> dict | None:
    """Для незарегистрированных посетителей сайта/бота: общая информация об активной
    акции без персонального трекинга (нет user_id — нечего писать в promo_user_state).
    Личное окно 48ч и mode full/silent считаются только после регистрации, см.
    check_promo_eligibility()."""
    if not pool:
        return None
    async with pool.acquire() as conn:
        promo = await _get_active_promotion(conn)
        if not promo:
            return None
        return _promo_public_fields(promo, "public", promo["ends_at"])


async def apply_promo_to_order(order_num: str, user_id: int) -> int | None:
    """Если у пользователя есть живое (не истёкшее, не использованное) окно акции —
    привязывает заказ к акции (orders.promo_id) и закрывает окно (used_order_id).
    Возвращает id акции если применена, иначе None."""
    if not pool or not user_id:
        return None
    async with pool.acquire() as conn:
        async with conn.transaction():
            state = await conn.fetchrow("""
                SELECT pus.promotion_id
                FROM promo_user_state pus
                JOIN promotions p ON p.id = pus.promotion_id
                WHERE pus.user_id = $1 AND pus.used_order_id IS NULL
                  AND pus.expires_at > NOW() AND p.is_active = TRUE
                ORDER BY pus.created_at DESC LIMIT 1
            """, user_id)
            if not state:
                return None
            promo_id = state["promotion_id"]
            order_row = await conn.fetchrow(
                "UPDATE orders SET promo_id=$1 WHERE order_num=$2 RETURNING id",
                promo_id, order_num
            )
            if not order_row:
                return None
            await conn.execute("""
                UPDATE promo_user_state SET used_order_id=$1
                WHERE promotion_id=$2 AND user_id=$3 AND used_order_id IS NULL
            """, order_row["id"], promo_id, user_id)
            return promo_id


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
#  УСЛУГИ (названия RU/UZ)
# ══════════════════════════════════════
async def get_services():
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM services ORDER BY order_idx, key")
        return [dict(r) for r in rows]

async def upsert_service(key: str, name_ru: str, name_uz: str, emoji: str = '', order_idx: int = 0):
    if not pool:
        return False
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO services (key, name_ru, name_uz, emoji, order_idx)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (key) DO UPDATE SET
                name_ru   = EXCLUDED.name_ru,
                name_uz   = EXCLUDED.name_uz,
                emoji     = EXCLUDED.emoji,
                order_idx = EXCLUDED.order_idx
        """, key, name_ru, name_uz, emoji, order_idx)
        return True

async def delete_service(key: str):
    if not pool:
        return False
    async with pool.acquire() as conn:
        r = await conn.execute("DELETE FROM services WHERE key=$1", key)
        return r == "DELETE 1"

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
                   COALESCE(i.corr, 0)::int AS corrected_count,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount
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

async def get_staff_roles_by_logins(logins: list) -> dict:
    """Возвращает {login: role} для списка логинов (для проверки, что позицию замерил мойщик)."""
    if not pool or not logins: return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT login, role FROM staff WHERE login = ANY($1)", list(logins))
        return {r["login"]: r["role"] for r in rows}

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

async def create_agent_from_user(user: dict, password_hash: str, branch: str = "") -> int:
    """Создаёт staff-аккаунт агента из пользователя сайта, авто-назначает зарплату 'leads'."""
    if not pool: return None
    # Получить глобальный процент комиссии за лид
    pct_str = await get_config("agent_commission_percent") or "0"
    try:
        lead_pct = float(pct_str)
    except ValueError:
        lead_pct = 0.0
    async with pool.acquire() as conn:
        staff_id = await conn.fetchval("""
            INSERT INTO staff (first_name, phone, login, password_hash, role,
                               tg_id, site_user_id, active, branch,
                               salary_type, salary_work_days)
            VALUES ($1,$2,$3,$4,'agent',$5,$6,TRUE,$7,'leads',26)
            ON CONFLICT (login) DO NOTHING
            RETURNING id
        """, user["first_name"], user["phone"], user["phone"],
            password_hash, user.get("tg_id"), user["id"], branch or None)
        if staff_id and lead_pct > 0:
            await conn.execute(
                "INSERT INTO staff_salary_percents (staff_id, role, percent)"
                " VALUES ($1,'lead',$2) ON CONFLICT (staff_id, role) DO NOTHING",
                staff_id, lead_pct)
        return staff_id

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

async def get_first_admin_staff():
    if not pool: return None
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM staff WHERE role='admin' AND active=TRUE ORDER BY id LIMIT 1")

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

async def get_staff_salary(staff_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT salary_type, salary_rate, salary_work_days, advance_percent FROM staff WHERE id=$1", staff_id)
        if not row:
            return {}
        result = {
            "salary_type":     row["salary_type"] or "fixed",
            "base_amount":     float(row["salary_rate"] or 0),
            "work_days":       row["salary_work_days"] or 26,
            "advance_percent": float(row["advance_percent"]) if row["advance_percent"] else None,
        }
        prows = await conn.fetch(
            "SELECT role, percent FROM staff_salary_percents WHERE staff_id=$1 ORDER BY id", staff_id)
        result["percents"] = [{"role": r["role"], "percent": float(r["percent"])} for r in prows]
        urows = await conn.fetch(
            "SELECT service_key, type_key, total_rate, unit_rate, rate_type "
            "FROM staff_salary_per_unit WHERE staff_id=$1 ORDER BY id", staff_id)
        result["per_unit"] = [{"service_key": r["service_key"], "type_key": r["type_key"],
                                "total_rate": float(r["total_rate"] or 0),
                                "unit_rate":  float(r["unit_rate"]  or 0),
                                "rate_type":  r["rate_type"] or "sum"} for r in urows]
        krows = await conn.fetch(
            "SELECT metric, target_value, bonus_type, bonus_value "
            "FROM staff_salary_kpi WHERE staff_id=$1 ORDER BY id", staff_id)
        result["kpi"] = [{"metric": r["metric"], "target_value": float(r["target_value"]),
                           "bonus_type": r["bonus_type"], "bonus_value": float(r["bonus_value"])} for r in krows]
        return result

async def save_staff_salary(staff_id: int, data: dict) -> None:
    if not pool: return
    async with pool.acquire() as conn:
        adv_pct = data.get("advance_percent")
        await conn.execute(
            "UPDATE staff SET salary_type=$2, salary_rate=$3, salary_work_days=$4, advance_percent=$5, updated_at=NOW() WHERE id=$1",
            staff_id, data.get("salary_type", "fixed"),
            data.get("base_amount") or None, data.get("work_days", 26),
            float(adv_pct) if adv_pct is not None else None)
        await conn.execute("DELETE FROM staff_salary_percents WHERE staff_id=$1", staff_id)
        for p in (data.get("percents") or []):
            if p.get("role") and p.get("percent") is not None:
                await conn.execute(
                    "INSERT INTO staff_salary_percents (staff_id, role, percent) VALUES ($1,$2,$3)"
                    " ON CONFLICT (staff_id, role) DO UPDATE SET percent=$3",
                    staff_id, p["role"], float(p["percent"]))
        await conn.execute("DELETE FROM staff_salary_per_unit WHERE staff_id=$1", staff_id)
        for u in (data.get("per_unit") or []):
            if u.get("service_key") and u.get("type_key"):
                await conn.execute(
                    "INSERT INTO staff_salary_per_unit (staff_id, service_key, type_key, total_rate, unit_rate, rate_type)"
                    " VALUES ($1,$2,$3,$4,$5,$6)"
                    " ON CONFLICT (staff_id, service_key, type_key) DO UPDATE SET total_rate=$4, unit_rate=$5, rate_type=$6",
                    staff_id, u["service_key"], u["type_key"],
                    float(u.get("total_rate") or 0), float(u.get("unit_rate") or 0),
                    u.get("rate_type") or "sum")
        await conn.execute("DELETE FROM staff_salary_kpi WHERE staff_id=$1", staff_id)
        for k in (data.get("kpi") or []):
            if k.get("metric") and k.get("target_value") is not None:
                await conn.execute(
                    "INSERT INTO staff_salary_kpi (staff_id, metric, target_value, bonus_type, bonus_value)"
                    " VALUES ($1,$2,$3,$4,$5)",
                    staff_id, k["metric"], float(k["target_value"]),
                    k.get("bonus_type", "fixed"), float(k.get("bonus_value") or 0))

async def get_monthly_salary_calc(year: int, month: int) -> list:
    """Расчёт зарплаты всех активных сотрудников за месяц."""
    if not pool: return []
    import calendar as _cal
    from datetime import date as _date
    start = _date(year, month, 1)
    end   = _date(year, month, _cal.monthrange(year, month)[1])

    async with pool.acquire() as conn:
        staff_rows = await conn.fetch("""
            SELECT s.id, s.first_name, s.last_name, s.role, s.branch, s.login,
                   s.salary_type, s.salary_rate, s.salary_work_days,
                   COALESCE(s.active, TRUE) AS active
            FROM staff s
            WHERE s.role <> 'agent'
              AND (
                s.active = TRUE OR s.active IS NULL
                OR EXISTS (
                  SELECT 1 FROM timesheet t
                  WHERE t.staff_id = s.id AND t.date >= $1 AND t.date <= $2
                )
              )
            ORDER BY s.branch NULLS LAST, s.last_name, s.first_name
        """, start, end)

        # Часы из табеля за период (все типы записей)
        ts_rows = await conn.fetch("""
            SELECT staff_id,
                   COALESCE(SUM(hours), 0)  AS total_hours,
                   COUNT(*)                  AS days_count
            FROM timesheet
            WHERE date >= $1 AND date <= $2
            GROUP BY staff_id
        """, start, end)
        ts_map = {r['staff_id']: r for r in ts_rows}

        # Забор/доставка водителей за период одним запросом
        route_rows = await conn.fetch("""
            SELECT r.driver_id,
                COUNT(CASE WHEN r.type = 'pickup' AND ro.stop_status = 'done' THEN 1 END) AS pickup_done,
                COUNT(CASE WHEN r.type IN ('delivery','mixed') AND ro.stop_status = 'done' THEN 1 END) AS delivery_done
            FROM routes r
            JOIN route_orders ro ON ro.route_id = r.id
            WHERE r.date >= $1 AND r.date <= $2 AND r.driver_id IS NOT NULL
            GROUP BY r.driver_id
        """, start, end)
        route_map = {r['driver_id']: r for r in route_rows}

        # Суммы заказов водителей за период (для percent pickup/delivery)
        route_sum_rows = await conn.fetch("""
            SELECT r.driver_id,
                COALESCE(SUM(CASE WHEN r.type = 'pickup'
                    THEN COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                       oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                                   FROM order_items oi WHERE oi.order_id = o.id), 0) END), 0) AS pickup_sum,
                COUNT(DISTINCT CASE WHEN r.type = 'pickup'
                    THEN ro.order_id END) AS pickup_count,
                COALESCE(SUM(CASE WHEN r.type IN ('delivery','mixed')
                    THEN COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                       oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                                   FROM order_items oi WHERE oi.order_id = o.id), 0) END), 0) AS delivery_sum,
                COUNT(DISTINCT CASE WHEN r.type IN ('delivery','mixed')
                    THEN ro.order_id END) AS delivery_count
            FROM routes r
            JOIN route_orders ro ON ro.route_id = r.id AND ro.stop_status = 'done'
            JOIN orders o ON o.id = ro.order_id
            WHERE r.date >= $1 AND r.date <= $2 AND r.driver_id IS NOT NULL
            GROUP BY r.driver_id
        """, start, end)
        route_sum_map = {r['driver_id']: dict(r) for r in route_sum_rows}

        # Ставки за точку
        pu_rows = await conn.fetch("""
            SELECT staff_id, service_key, unit_rate, rate_type
            FROM staff_salary_per_unit
            WHERE service_key IN ('__pickup__','__delivery__')
        """)
        pu_map = {}
        for r in pu_rows:
            pu_map.setdefault(r['staff_id'], {})[r['service_key']] = r

        # Процентные ставки сотрудников (тип percent / fixed_percent)
        sp_rows = await conn.fetch("SELECT staff_id, role, percent FROM staff_salary_percents")
        sp_map: dict = {}  # staff_id -> {role: percent}
        for r in sp_rows:
            sp_map.setdefault(r['staff_id'], {})[r['role']] = float(r['percent'])

        # Объём позиций по washer_login за период (для role='washing')
        washing_rows = await conn.fetch("""
            SELECT s.id AS staff_id,
                   COALESCE(SUM(
                       COALESCE(oi.actual_total_sum,
                                oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0)
                   ), 0) AS total_sum,
                   COUNT(DISTINCT oi.id) AS item_count
            FROM staff s
            JOIN order_items oi ON oi.washer_login = s.login
            JOIN orders o ON o.id = oi.order_id
            WHERE s.login IS NOT NULL
              AND o.status != 'cancelled'
              AND COALESCE(o.washed_at, o.created_at) >= $1::timestamptz
              AND COALESCE(o.washed_at, o.created_at) <  $2::timestamptz
            GROUP BY s.id
        """, start, end)
        washing_map = {r['staff_id']: {'total': float(r['total_sum']), 'count': int(r['item_count'])}
                       for r in washing_rows}

        # Конвертированные лиды за период (для percent lead)
        lead_sum_rows = await conn.fetch("""
            SELECT l.assigned_to AS staff_id,
                COUNT(l.id) AS count,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                  oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                              FROM order_items oi WHERE oi.order_id = o.id), 0)
                ), 0) AS total
            FROM leads l
            LEFT JOIN orders o ON o.order_num = l.converted_order
            WHERE l.status = 'converted'
              AND l.updated_at >= $1::timestamptz
              AND l.updated_at <  $2::timestamptz
              AND l.assigned_to IS NOT NULL
            GROUP BY l.assigned_to
        """, start, end)
        lead_sum_map = {r['staff_id']: {'total': float(r['total']), 'count': int(r['count'])}
                        for r in lead_sum_rows}

        # Упакованные заказы за период (для percent packing)
        packing_rows = await conn.fetch("""
            SELECT s.id AS staff_id,
                COALESCE(SUM(
                    COALESCE((SELECT SUM(COALESCE(oi.actual_total_sum,
                                                  oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0))
                              FROM order_items oi WHERE oi.order_id = o.id), 0)
                ), 0) AS total_sum,
                COUNT(DISTINCT o.id) AS order_count
            FROM staff s
            JOIN orders o ON o.packer_login = s.login
            WHERE s.login IS NOT NULL
              AND o.status NOT IN ('cancelled')
              AND COALESCE(o.packed_at, o.created_at) >= $1::timestamptz
              AND COALESCE(o.packed_at, o.created_at) <  $2::timestamptz
            GROUP BY s.id
        """, start, end)
        packing_map = {r['staff_id']: {'total': float(r['total_sum']), 'count': int(r['order_count'])}
                       for r in packing_rows}

        results = []
        for s in staff_rows:
            sid      = s['id']
            sal_type = s['salary_type'] or 'fixed'
            base     = float(s['salary_rate'] or 0)
            norm_days = int(s['salary_work_days'] or 26)
            entry = {
                'staff_id':    sid,
                'name':        f"{s['last_name'] or ''} {s['first_name'] or ''}".strip(),
                'role':        s['role'],
                'branch':      s['branch'],
                'active':      bool(s['active']),
                'salary_type': sal_type,
                'base_amount': base,
                'calc':        {},
                'total':       None,
            }

            # ── A: Фиксированная основа (fixed и fixed_percent) ──
            fixed_earn = 0.0
            if sal_type in ('fixed', 'fixed_percent'):
                ts = ts_map.get(sid)
                norm_hours = norm_days * 8.0
                if ts:
                    work_hours = float(ts['total_hours'])
                    work_days  = int(ts['days_count'])
                    if base > 0 and norm_hours > 0:
                        fixed_earn = round(base / norm_hours * work_hours, 2)
                    if sal_type == 'fixed':
                        entry['calc']  = {
                            'work_hours': work_hours, 'norm_hours': norm_hours,
                            'work_days':  work_days,  'norm_days':  norm_days,
                        }
                        entry['total'] = fixed_earn
                else:
                    entry['calc']  = {'no_timesheet': True, 'norm_days': norm_days}
                    entry['total'] = None
                    if sal_type == 'fixed':
                        results.append(entry)
                        continue

            # ── B: Зарплата за точку ──
            if sal_type == 'per_point':
                rd = route_map.get(sid, {})
                pickup_cnt   = int(rd.get('pickup_done')   or 0)
                delivery_cnt = int(rd.get('delivery_done') or 0)
                rates = pu_map.get(sid, {})
                p_row = rates.get('__pickup__')
                d_row = rates.get('__delivery__')
                p_rate = float(p_row['unit_rate'] if p_row else 0)
                d_rate = float(d_row['unit_rate'] if d_row else 0)
                entry['calc']  = {
                    'pickup_count':   pickup_cnt,  'pickup_rate':    p_rate,
                    'delivery_count': delivery_cnt, 'delivery_rate':  d_rate,
                }
                entry['total'] = pickup_cnt * p_rate + delivery_cnt * d_rate

            # ── C: Процентные составляющие (percent и fixed_percent) ──
            elif sal_type in ('percent', 'fixed_percent'):
                rates = sp_map.get(sid, {})
                total_pct  = 0.0
                calc_lines = {}

                washing_pct = rates.get('washing', 0)
                if washing_pct:
                    wd = washing_map.get(sid, {})
                    w_earn = round(wd.get('total', 0) * washing_pct / 100, 2)
                    total_pct += w_earn
                    calc_lines['washing'] = {
                        'pct': washing_pct, 'base': wd.get('total', 0),
                        'earn': w_earn, 'count': wd.get('count', 0),
                    }

                pickup_pct = rates.get('pickup', 0)
                if pickup_pct:
                    rd = route_sum_map.get(sid, {})
                    p_sum  = float(rd.get('pickup_sum',   0) or 0)
                    p_cnt  = int  (rd.get('pickup_count', 0) or 0)
                    p_earn = round(p_sum * pickup_pct / 100, 2)
                    total_pct += p_earn
                    calc_lines['pickup'] = {
                        'pct': pickup_pct, 'base': p_sum,
                        'earn': p_earn, 'count': p_cnt,
                    }

                delivery_pct = rates.get('delivery', 0)
                if delivery_pct:
                    rd = route_sum_map.get(sid, {})
                    d_sum  = float(rd.get('delivery_sum',   0) or 0)
                    d_cnt  = int  (rd.get('delivery_count', 0) or 0)
                    d_earn = round(d_sum * delivery_pct / 100, 2)
                    total_pct += d_earn
                    calc_lines['delivery'] = {
                        'pct': delivery_pct, 'base': d_sum,
                        'earn': d_earn, 'count': d_cnt,
                    }

                lead_pct = rates.get('lead', 0)
                if lead_pct:
                    ld = lead_sum_map.get(sid, {})
                    l_sum  = float(ld.get('total', 0) or 0)
                    l_cnt  = int  (ld.get('count', 0) or 0)
                    l_earn = round(l_sum * lead_pct / 100, 2)
                    total_pct += l_earn
                    calc_lines['lead'] = {
                        'pct': lead_pct, 'base': l_sum,
                        'earn': l_earn, 'count': l_cnt,
                    }

                packing_pct = rates.get('packing', 0)
                if packing_pct:
                    pd = packing_map.get(sid, {})
                    pk_sum  = float(pd.get('total', 0) or 0)
                    pk_cnt  = int  (pd.get('count', 0) or 0)
                    pk_earn = round(pk_sum * packing_pct / 100, 2)
                    total_pct += pk_earn
                    calc_lines['packing'] = {
                        'pct': packing_pct, 'base': pk_sum,
                        'earn': pk_earn, 'count': pk_cnt,
                    }

                if sal_type == 'fixed_percent' and fixed_earn:
                    calc_lines['fixed'] = {'rate': base, 'earn': fixed_earn}

                entry['calc']  = {'lines': calc_lines, 'norm_days': norm_days}
                entry['total'] = round(fixed_earn + total_pct, 2)

            # ── D: Прочие типы (не реализованы) ──
            elif sal_type not in ('fixed',):
                entry['total'] = None

            # ── E: Часы из табеля для не-фиксированных типов ──
            if sal_type not in ('fixed', 'fixed_percent'):
                ts = ts_map.get(sid)
                if ts:
                    norm_hours = norm_days * 8.0
                    entry['calc']['work_hours'] = float(ts['total_hours'])
                    entry['calc']['norm_hours'] = norm_hours
                    entry['calc']['work_days']  = int(ts['days_count'])
                    entry['calc']['norm_days']  = norm_days

            results.append(entry)

        return results


async def trigger_order_agent_commission(order_id: int, order_num: str, total_price: float) -> None:
    """При доставке заказа начисляет комиссию агенту, который создал лид."""
    if not pool or not total_price: return
    async with pool.acquire() as conn:
        # Уже начислено?
        exists = await conn.fetchval(
            "SELECT 1 FROM staff_commissions WHERE order_id=$1", order_id)
        if exists: return
        # Найти лид, из которого создан этот заказ
        lead = await conn.fetchrow(
            "SELECT id, created_by FROM leads WHERE converted_order=$1 AND created_by IS NOT NULL",
            order_num)
        if not lead: return
        agent_id = lead["created_by"]
        lead_id  = lead["id"]
        # Проверить что это агент
        role = await conn.fetchval("SELECT role FROM staff WHERE id=$1", agent_id)
        if role != "agent": return
        # Тип начисления из настроек
        comm_type = await conn.fetchval(
            "SELECT value FROM config WHERE key='agent_commission_type'") or "percent"
        # Индивидуальное значение агента (переопределяет глобальное)
        pct_row = await conn.fetchrow(
            "SELECT percent FROM staff_salary_percents WHERE staff_id=$1 AND role='lead'", agent_id)
        if comm_type == "fixed":
            if pct_row and float(pct_row["percent"]) > 0:
                amount = float(pct_row["percent"])
            else:
                fixed_str = await conn.fetchval(
                    "SELECT value FROM config WHERE key='agent_commission_fixed'") or "0"
                try: amount = float(fixed_str)
                except ValueError: amount = 0.0
            pct = 0  # не используется в fixed-режиме
            if amount <= 0: return
        else:
            if pct_row and float(pct_row["percent"]) > 0:
                pct = float(pct_row["percent"])
            else:
                pct_str = await conn.fetchval(
                    "SELECT value FROM config WHERE key='agent_commission_percent'") or "5.0"
                try: pct = float(pct_str)
                except ValueError: pct = 0.0
            if pct <= 0: return
            amount = round(total_price * pct / 100, 2)
        await conn.execute("""
            INSERT INTO staff_commissions
                (staff_id, order_id, order_num, lead_id, amount, percent, order_total)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, agent_id, order_id, order_num, lead_id, amount, pct, total_price)

async def get_agent_commissions(staff_id: int, year: int = None, month: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """SELECT c.*, o.order_num as o_num
                 FROM staff_commissions c
                 LEFT JOIN orders o ON o.id = c.order_id
                 WHERE c.staff_id=$1"""
        args = [staff_id]
        if year:
            args.append(year); sql += f" AND EXTRACT(YEAR FROM c.created_at)=${len(args)}"
        if month:
            args.append(month); sql += f" AND EXTRACT(MONTH FROM c.created_at)=${len(args)}"
        sql += " ORDER BY c.created_at DESC LIMIT 200"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def get_all_commissions(year: int = None, month: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """SELECT c.*, s.first_name, s.last_name
                 FROM staff_commissions c
                 LEFT JOIN staff s ON s.id = c.staff_id
                 WHERE 1=1"""
        args = []
        if year:
            args.append(year); sql += f" AND EXTRACT(YEAR FROM c.created_at)=${len(args)}"
        if month:
            args.append(month); sql += f" AND EXTRACT(MONTH FROM c.created_at)=${len(args)}"
        sql += " ORDER BY c.created_at DESC LIMIT 500"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

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
async def get_timesheet(year: int, month: int, staff_id: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """
            SELECT t.id, t.staff_id, t.date::text, t.hours, t.type, t.note,
                   s.first_name, s.last_name,
                   (s.first_name || ' ' || COALESCE(s.last_name,'')) AS staff_name
            FROM timesheet t
            JOIN staff s ON s.id = t.staff_id
            WHERE EXTRACT(YEAR FROM t.date)=$1 AND EXTRACT(MONTH FROM t.date)=$2
        """
        args = [year, month]
        if staff_id:
            args.append(staff_id); sql += f" AND t.staff_id=${len(args)}"
        sql += " ORDER BY t.date DESC, s.last_name, s.first_name"
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def save_timesheet(data: dict) -> dict:
    if not pool: return {}
    from datetime import date as _date
    d = data["date"]
    if isinstance(d, str): d = _date.fromisoformat(d)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO timesheet (staff_id, date, hours, type, note)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (staff_id, date)
            DO UPDATE SET hours=$3, type=$4, note=$5
            RETURNING id, staff_id, date::text, hours, type, note
        """, int(data["staff_id"]), d,
            float(data.get("hours") or 8),
            data.get("type", "work"),
            data.get("note", ""))
        return dict(row) if row else {}

async def update_timesheet(ts_id: int, data: dict) -> dict:
    if not pool: return {}
    from datetime import date as _date
    d = data["date"]
    if isinstance(d, str): d = _date.fromisoformat(d)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE timesheet SET staff_id=$2, date=$3, hours=$4, type=$5, note=$6
            WHERE id=$1
            RETURNING id, staff_id, date::text, hours, type, note
        """, ts_id, int(data["staff_id"]), d,
            float(data.get("hours") or 8),
            data.get("type", "work"),
            data.get("note", ""))
        return dict(row) if row else {}

async def delete_timesheet(ts_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM timesheet WHERE id=$1", ts_id)
        return result == "DELETE 1"

async def init_timesheet_month(year: int, month: int, until_today: bool = False) -> dict:
    """Создать записи 'work' для всех активных не-агентов на каждый будний день месяца.
    Пропускает сотрудников с salary_type='percent'.
    Учитывает hire_date — не создаёт записи раньше даты приёма.
    ON CONFLICT DO NOTHING — уже существующие записи не трогает."""
    if not pool: return {"created": 0}
    import calendar as _cal
    from datetime import date
    _, last_day = _cal.monthrange(year, month)
    today = date.today()
    if until_today and date(year, month, 1) <= today:
        last_day = min(last_day, today.day if (today.year == year and today.month == month) else last_day)
    month_start = date(year, month, 1)
    async with pool.acquire() as conn:
        staff_rows = await conn.fetch("""
            SELECT id, salary_type, hire_date FROM staff
            WHERE (active IS NULL OR active = TRUE)
              AND COALESCE(role,'') != 'agent'
              AND COALESCE(salary_type,'') NOT IN ('percent','per_unit','per_point')
        """)
        count = 0
        skipped = 0
        for s in staff_rows:
            # Дата приёма: если нанят позже начала месяца — начинаем с его даты
            hire = s["hire_date"]
            if hire and hire > date(year, month, last_day):
                skipped += 1
                continue  # ещё не работал в этом месяце
            start_day = max(1, hire.day if (hire and hire.year == year and hire.month == month) else 1)
            for day in range(start_day, last_day + 1):
                d = date(year, month, day)
                if d.weekday() == 6:  # воскресенье — пропуск
                    continue
                r = await conn.execute("""
                    INSERT INTO timesheet (staff_id, date, hours, type)
                    VALUES ($1, $2, 8, 'work')
                    ON CONFLICT (staff_id, date) DO NOTHING
                """, s["id"], d)
                if r.endswith("0 1"):
                    count += 1
    return {"created": count, "skipped": skipped}

async def reset_timesheet_month(year: int, month: int) -> dict:
    if not pool: return {"deleted": 0}
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM timesheet WHERE EXTRACT(YEAR FROM date)=$1 AND EXTRACT(MONTH FROM date)=$2",
            year, month)
        deleted = int(result.split()[-1]) if result else 0
    return {"deleted": deleted}

# ══════════════════════════════════════
#  ОТМЕТКИ ПРИХОДА/УХОДА (staff_attendance)
# ══════════════════════════════════════

async def attendance_check_in(staff_id: int) -> dict:
    if not pool: return {}
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        if events and events[-1]["event_type"] == "in":
            return {"error": "already_in"}
        row = await conn.fetchrow("""
            INSERT INTO staff_attendance_events (staff_id, event_type)
            VALUES ($1, 'in')
            RETURNING id, staff_id, event_type, created_at
        """, staff_id)
        return dict(row) if row else {}

async def attendance_check_out(staff_id: int) -> dict:
    if not pool: return {}
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        if not events or events[-1]["event_type"] == "out":
            return {"error": "not_checked_in"}
        row = await conn.fetchrow("""
            INSERT INTO staff_attendance_events (staff_id, event_type)
            VALUES ($1, 'out')
            RETURNING id, staff_id, event_type, created_at
        """, staff_id)
        return dict(row) if row else {}

def _pair_attendance_sessions(events: list) -> list:
    """Pairs consecutive in/out events (sorted ascending by created_at) into sessions."""
    sessions = []
    open_session = None
    for ev in events:
        if ev["event_type"] == "in":
            open_session = {"check_in_at": ev["created_at"], "check_out_at": None}
            sessions.append(open_session)
        elif ev["event_type"] == "out":
            if open_session is not None and open_session["check_out_at"] is None:
                open_session["check_out_at"] = ev["created_at"]
                open_session = None
            # else: stray "out" with no open session — defensively skip it
    return sessions

def _sessions_total_hours(sessions: list) -> float:
    total = timedelta()
    for s in sessions:
        if s["check_out_at"] is not None:
            total += (s["check_out_at"] - s["check_in_at"])
    return round(total.total_seconds() / 3600, 1)

async def get_attendance_today(staff_id: int) -> dict:
    if not pool: return {"current_state": "out", "sessions": [], "total_hours": 0}
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        events = await conn.fetch("""
            SELECT id, staff_id, event_type, created_at
            FROM staff_attendance_events
            WHERE staff_id=$1 AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2
            ORDER BY created_at
        """, staff_id, today)
        events = [dict(e) for e in events]
        sessions = _pair_attendance_sessions(events)
        current_state = "in" if events and events[-1]["event_type"] == "in" else "out"
        total_hours = _sessions_total_hours(sessions)
        return {
            "current_state": current_state,
            "sessions": [
                {
                    "check_in_at": s["check_in_at"].isoformat() if s["check_in_at"] else None,
                    "check_out_at": s["check_out_at"].isoformat() if s["check_out_at"] else None,
                }
                for s in sessions
            ],
            "total_hours": total_hours,
        }

async def get_admin_attendance(year: int, month: int, staff_id: int = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        sql = """
            SELECT a.staff_id, a.event_type, a.created_at,
                   s.first_name, s.last_name, s.role, s.branch, s.salary_type
            FROM staff_attendance_events a
            JOIN staff s ON s.id = a.staff_id
            WHERE EXTRACT(YEAR FROM (a.created_at AT TIME ZONE 'Asia/Tashkent'))=$1
              AND EXTRACT(MONTH FROM (a.created_at AT TIME ZONE 'Asia/Tashkent'))=$2
        """
        args = [year, month]
        if staff_id:
            args.append(staff_id); sql += f" AND a.staff_id=${len(args)}"
        sql += " ORDER BY a.staff_id, a.created_at"
        rows = await conn.fetch(sql, *args)

        groups = {}
        meta = {}
        for r in rows:
            local_date = r["created_at"].astimezone(_TASHKENT).date()
            key = (r["staff_id"], local_date)
            groups.setdefault(key, []).append({
                "event_type": r["event_type"],
                "created_at": r["created_at"],
            })
            if key not in meta:
                meta[key] = {
                    "staff_id": r["staff_id"],
                    "name": f"{r['last_name'] or ''} {r['first_name'] or ''}".strip(),
                    "role": r["role"],
                    "branch": r["branch"],
                }

        result = []
        for key, events in groups.items():
            sessions = _pair_attendance_sessions(events)
            result.append({
                "staff_id": meta[key]["staff_id"],
                "name": meta[key]["name"],
                "role": meta[key]["role"],
                "branch": meta[key]["branch"],
                "date": key[1].strftime("%Y-%m-%d"),
                "sessions": [
                    {
                        "check_in_at": s["check_in_at"].isoformat() if s["check_in_at"] else None,
                        "check_out_at": s["check_out_at"].isoformat() if s["check_out_at"] else None,
                    }
                    for s in sessions
                ],
                "total_hours": _sessions_total_hours(sessions),
            })
        result.sort(key=lambda x: x["staff_id"])
        result.sort(key=lambda x: x["date"], reverse=True)
        return result

async def create_lead(data: dict) -> dict:
    if not pool: return None
    # Тег акции (только видимость для сотрудников, не расходует окно) — только для
    # лидов с сайта/бота, привязанных к зарегистрированному пользователю с живым окном
    promo_id = None
    source = data.get("source", "staff")
    if source in ("site", "bot") and data.get("client_phone"):
        try:
            async with pool.acquire() as pconn:
                user = await pconn.fetchrow("SELECT id FROM users WHERE phone=$1", data["client_phone"])
                if user:
                    promo_row = await pconn.fetchrow("""
                        SELECT pus.promotion_id
                        FROM promo_user_state pus
                        JOIN promotions p ON p.id = pus.promotion_id
                        WHERE pus.user_id=$1 AND pus.used_order_id IS NULL
                          AND pus.expires_at > NOW() AND p.is_active = TRUE
                        ORDER BY pus.created_at DESC LIMIT 1
                    """, user["id"])
                    if promo_row:
                        promo_id = promo_row["promotion_id"]
        except Exception as e:
            logging.error(f"create_lead: promo tag failed: {e}")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO leads (client_name, client_phone, service, branch,
                               city, address, short_address, note, status, assigned_to,
                               created_by, volunteer_id, location, location_address,
                               source, client_tg_id, pickup_date, pickup_time, promo_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
            RETURNING *
        """, data.get("client_name"), data["client_phone"],
            data.get("service"), data.get("branch"), data.get("city"),
            data.get("address"), data.get("short_address", ""), data.get("note"),
            data.get("status","new"), data.get("assigned_to"), data.get("created_by"),
            data.get("volunteer_id"), data.get("location"), data.get("location_address"),
            source, data.get("client_tg_id"),
            data.get("pickup_date", ""), data.get("pickup_time", ""), promo_id)
        rid      = row["id"]
        lead_num = f"LEAD-{rid:04d}"
        lead_code = f"L-{rid:04d}"
        await conn.execute(
            "UPDATE leads SET lead_num=$1, lead_code=$2 WHERE id=$3",
            lead_num, lead_code, rid
        )
        return dict(row) | {"lead_num": lead_num, "lead_code": lead_code}

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
    allowed = {"client_name","client_phone","service","branch","city","address","short_address","note","status","location","location_address","volunteer_id","pickup_type","delivery_type","pickup_date","pickup_time"}
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


# ── washer_notifications ─────────────────────────────────────────────────────

async def ensure_washer_notifications_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS washer_notifications (
                id         SERIAL PRIMARY KEY,
                staff_id   INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                order_id   INTEGER NOT NULL,
                order_num  VARCHAR(50) DEFAULT '',
                message    TEXT DEFAULT '',
                is_read    BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_washer_notif_staff ON washer_notifications(staff_id, is_read);
        """)
        # Idempotent columns added after initial release
        await conn.execute("ALTER TABLE washer_notifications ADD COLUMN IF NOT EXISTS item_id INTEGER")
        await conn.execute("ALTER TABLE washer_notifications ADD COLUMN IF NOT EXISTS notification_type VARCHAR(30) DEFAULT 'order_item'")

async def create_washer_notification(staff_id: int, order_id: int, order_num: str, message: str,
                                      item_id: int = None, notification_type: str = 'order_item'):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO washer_notifications (staff_id, order_id, order_num, message, item_id, notification_type) "
            "VALUES ($1,$2,$3,$4,$5,$6)",
            staff_id, order_id, order_num, message, item_id, notification_type)

async def get_washer_notifications(staff_id: int, limit: int = 50) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM washer_notifications WHERE staff_id=$1 ORDER BY created_at DESC LIMIT $2",
            staff_id, limit)
        return [dict(r) for r in rows]

async def count_unread_washer_notifications(staff_id: int) -> int:
    if not pool: return 0
    async with pool.acquire() as conn:
        return int(await conn.fetchval(
            "SELECT COUNT(*) FROM washer_notifications WHERE staff_id=$1 AND is_read=FALSE", staff_id))

async def mark_washer_notifications_read(staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE washer_notifications SET is_read=TRUE WHERE staff_id=$1 AND is_read=FALSE", staff_id)

async def mark_washer_notification_read(notif_id: int, staff_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE washer_notifications SET is_read=TRUE WHERE id=$1 AND staff_id=$2", notif_id, staff_id)


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
        if status == 'washing':
            sql = "UPDATE orders SET status=$2, washed_at=NOW() WHERE id=$1 RETURNING *"
        elif status == 'packing':
            sql = "UPDATE orders SET status=$2, packed_at=NOW() WHERE id=$1 RETURNING *"
        else:
            sql = "UPDATE orders SET status=$2 WHERE id=$1 RETURNING *"
        row = await conn.fetchrow(sql, order_id, status)
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

async def get_order_debt_history(order_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT h.new_status, h.note, h.created_at
              FROM order_status_history h
              JOIN orders o ON o.order_num = h.order_num
             WHERE o.id = $1
               AND h.new_status IN ('delivered','debt_extended','debt_paid')
             ORDER BY h.created_at
        """, order_id)
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
        blocked_col = "blocked" if await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name='clients' AND column_name='blocked'"
        ) else "FALSE::boolean AS blocked"
        if search:
            rows = await conn.fetch(f"""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at, {blocked_col}
                FROM clients
                WHERE phone ILIKE $1 OR first_name ILIKE $1
                   OR last_name ILIKE $1 OR tg_username ILIKE $1
                ORDER BY created_at DESC
                LIMIT $2
            """, f"%{search}%", limit)
        else:
            rows = await conn.fetch(f"""
                SELECT id, tg_id, tg_username, first_name, last_name, phone,
                       lang, total_orders AS orders_count,
                       NULL::numeric AS total_spent,
                       NULL::timestamptz AS last_order_at,
                       created_at, {blocked_col}
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
    ts_from, ts_to = _tz_range(date_from, date_to)
    async with pool.acquire() as conn:
        # Суммы по методу оплаты (из order_payments)
        rows = await conn.fetch("""
            SELECT
                method AS payment_method,
                COALESCE(SUM(amount), 0) AS amount,
                COUNT(DISTINCT order_id) AS cnt
            FROM order_payments
            WHERE created_at >= $1 AND created_at < $2
              AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
            GROUP BY method
        """, ts_from, ts_to)
        # Заказы с оплатами за период
        orders = await conn.fetch("""
            SELECT
                o.id, o.order_num, o.created_at, o.total_price, o.discount_sum,
                o.payment_method, o.payment_status, o.prepaid_amount, o.paid_at,
                sub.paid_total,
                sub.last_payment_at AS payment_at
            FROM orders o
            JOIN (
                SELECT
                    order_id,
                    SUM(amount) AS paid_total,
                    MAX(created_at) AS last_payment_at
                FROM order_payments
                WHERE created_at >= $1 AND created_at < $2
                  AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
                GROUP BY order_id
            ) sub ON sub.order_id = o.id
            ORDER BY sub.last_payment_at DESC
        """, ts_from, ts_to)
        return {
            "summary": [dict(r) for r in rows],
            "orders": [dict(r) for r in orders],
        }

async def get_payments_log(date_from: str, date_to: str) -> list:
    if not pool: return []
    ts_from, ts_to = _tz_range(date_from, date_to)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                op.id,
                op.order_id,
                o.order_num,
                op.amount,
                op.method,
                op.purpose,
                op.note,
                op.confirmed,
                op.confirmed_at,
                op.reject_note,
                op.created_at,
                op.created_by,
                TRIM(COALESCE(cs.last_name,'') || ' ' || COALESCE(cs.first_name,'')) AS created_by_name,
                TRIM(COALESCE(cv.last_name,'') || ' ' || COALESCE(cv.first_name,'')) AS confirmed_by_name,
                op.driver_tg_id
            FROM order_payments op
            LEFT JOIN orders o ON o.id = op.order_id
            LEFT JOIN staff cs ON cs.id = op.created_by_staff_id
            LEFT JOIN staff cv ON cv.id = op.confirmed_by
            WHERE op.created_at >= $1 AND op.created_at < $2
            ORDER BY op.created_at DESC
        """, ts_from, ts_to)
        return [dict(r) for r in rows]

async def close_cash_shift(shift_date, closed_by: str, note: str) -> dict:
    if not pool: return {}
    from datetime import date as _date
    if isinstance(shift_date, str):
        shift_date = _date.fromisoformat(shift_date)
    async with pool.acquire() as conn:
        totals = await conn.fetchrow("""
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
            FROM orders WHERE created_at::date = $1
        """, shift_date)
        ct = float(totals['cash_total']); kt = float(totals['card_total'])
        tt = float(totals['transfer_total']); oc = int(totals['orders_count'])
        open_shift = await conn.fetchrow(
            "SELECT id FROM cash_shifts WHERE status='open' ORDER BY opened_at DESC LIMIT 1")
        if open_shift:
            row = await conn.fetchrow("""
                UPDATE cash_shifts SET
                    shift_date=$1, closed_by=$2, note=$3, status='closed', closed_at=NOW(),
                    cash_total=$4, card_total=$5, transfer_total=$6, grand_total=$7, orders_count=$8
                WHERE id=$9 RETURNING *
            """, shift_date, closed_by, note, ct, kt, tt, ct+kt+tt, oc, open_shift['id'])
        else:
            row = await conn.fetchrow("""
                INSERT INTO cash_shifts (shift_date, closed_by, cash_total, card_total, transfer_total, grand_total, orders_count, note, status, closed_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'closed',NOW()) RETURNING *
            """, shift_date, closed_by, ct, kt, tt, ct+kt+tt, oc, note)
        return dict(row) if row else {}

async def open_cash_shift(opened_by_id: int) -> dict | None:
    """None — если уже есть открытая смена."""
    if not pool: return {}
    async with pool.acquire() as conn:
        existing = await conn.fetchval(
            "SELECT id FROM cash_shifts WHERE status='open' LIMIT 1")
        if existing:
            return None
        row = await conn.fetchrow("""
            INSERT INTO cash_shifts (opened_at, opened_by, status, shift_date)
            VALUES (NOW(), $1, 'open', NOW()::date) RETURNING *
        """, opened_by_id)
        return dict(row) if row else {}

async def delete_cash_shift(shift_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM cash_shifts WHERE id=$1", shift_id)
        return res == "DELETE 1"

async def get_current_shift() -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT cs.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS opener_name
            FROM cash_shifts cs
            LEFT JOIN staff s ON s.id = cs.opened_by
            WHERE cs.status = 'open'
            ORDER BY cs.opened_at DESC LIMIT 1
        """)
        return dict(row) if row else {}

async def _get_current_shift_id(conn) -> int:
    row = await conn.fetchrow(
        "SELECT id FROM cash_shifts WHERE status='open' ORDER BY opened_at DESC LIMIT 1")
    return row['id'] if row else None

async def get_cash_shifts(limit: int = 50) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT cs.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS opener_name
            FROM cash_shifts cs
            LEFT JOIN staff s ON s.id = cs.opened_by
            ORDER BY COALESCE(cs.opened_at, cs.closed_at) DESC LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

async def get_cash_daily_total(date_str: str) -> dict:
    if not pool: return {}
    ts_from, ts_to = _tz_range(date_str, date_str)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COALESCE(SUM(CASE WHEN method='cash'     THEN amount ELSE 0 END), 0) AS cash,
                COALESCE(SUM(CASE WHEN method='card'     THEN amount ELSE 0 END), 0) AS card,
                COALESCE(SUM(CASE WHEN method='transfer' THEN amount ELSE 0 END), 0) AS transfer,
                COALESCE(SUM(amount), 0) AS total
            FROM order_payments
            WHERE created_at >= $1 AND created_at < $2
              AND NOT (confirmed = FALSE AND confirmed_at IS NOT NULL)
        """, ts_from, ts_to)
        return dict(row) if row else {}

async def get_cash_payment_history(year: int, month: int, branch: str = '', day: int = 0) -> list:
    if not pool: return []
    from datetime import date as _date
    import calendar
    if day > 0:
        target = _date(year, month, day)
        ts_from, ts_to = _tz_range(str(target), str(target))
    else:
        first_day = _date(year, month, 1)
        last_day  = _date(year, month, calendar.monthrange(year, month)[1])
        ts_from, ts_to = _tz_range(str(first_day), str(last_day))
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                (op.created_at AT TIME ZONE 'Asia/Tashkent')::date AS pay_date,
                s.id AS staff_id,
                TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS staff_name,
                s.branch,
                COALESCE(SUM(CASE WHEN op.method='cash'     THEN op.amount ELSE 0 END), 0) AS cash,
                COALESCE(SUM(CASE WHEN op.method='card'     THEN op.amount ELSE 0 END), 0) AS card,
                COALESCE(SUM(CASE WHEN op.method='transfer' THEN op.amount ELSE 0 END), 0) AS transfer,
                COALESCE(SUM(op.amount), 0) AS total
            FROM order_payments op
            JOIN staff s ON s.id = op.created_by_staff_id
            WHERE op.created_at >= $1 AND op.created_at < $2
              AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)
              AND ($3 = '' OR s.branch = $3)
            GROUP BY pay_date, s.id, s.last_name, s.first_name, s.branch
            ORDER BY pay_date DESC, staff_name
        """, ts_from, ts_to, branch)
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
    order = await conn.fetchrow("""
        SELECT total_price, discount_sum, delivery_discount, manual_discount,
               COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                         FROM order_items WHERE order_id=$1), 0) AS items_total
        FROM orders WHERE id=$1
    """, order_id)
    if order:
        base = float(order['total_price'] or 0) or float(order['items_total'] or 0)
        net = base - float(order['discount_sum'] or 0) - float(order['delivery_discount'] or 0) - float(order['manual_discount'] or 0)
        status = 'paid' if paid >= net and net > 0 else ('partial' if paid > 0 else 'unpaid')
        await conn.execute("UPDATE orders SET payment_status=$1 WHERE id=$2", status, order_id)

async def add_order_activity(order_id: int, staff_id: int, staff_name: str, action: str, details: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff_id or None, staff_name, action, details)

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
        """, payment_id, rejected_by or None, note or None)
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

async def direct_approve_measure(item_id: int, width_cm: float, length_cm: float) -> dict:
    if not pool: return {}
    sqm = round(width_cm * length_cm / 10000, 3)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_items
               SET width_cm=$2, length_cm=$3, sqm=$4,
                   measure_status='approved', reject_note=NULL
             WHERE id=$1 RETURNING *
        """, item_id, width_cm, length_cm, sqm)
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
    from datetime import date as _date
    if isinstance(date, str): date = _date.fromisoformat(date)
    filters, vals, i = [], [], 1
    if date:      filters.append(f"r.date=${i}::date"); vals.append(date); i+=1
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

async def roll_forward_stale_routes() -> int:
    """Переносит planned/active маршруты с прошедшей датой на сегодня (Asia/Tashkent)."""
    if not pool: return 0
    today = datetime.now(_TASHKENT).date()
    async with pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE routes SET date = $1, updated_at = NOW()
            WHERE status IN ('planned','active') AND date < $1
        """, today)
        return int(result.split()[-1]) if result else 0

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

async def get_active_route_orders() -> list:
    """Все заказы в активных маршрутах (не done/cancelled) → [{order_id, route_id, route_name, route_date, route_type}]"""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ro.order_id, r.id AS route_id, r.name AS route_name,
                   r.date AS route_date, r.type AS route_type
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            WHERE r.status NOT IN ('done','cancelled')
        """)
        return [dict(r) for r in rows]

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
                   ro.driver_confirmed,
                   o.service, o.branch,
                   o.pickup_date, o.deadline,
                   o.total_price, o.prepaid_amount, o.payment_status,
                   o.discount_sum, o.delivery_discount, o.manual_discount,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id), 0)::int AS item_count,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount
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

async def get_channel_msg_for_order(order_id: int):
    """Возвращает (branch, channel_msg_id) для обновления кнопок в канале."""
    if not pool: return None, None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.branch, r.tg_delivery_msg_ids
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            WHERE ro.order_id = $1
            ORDER BY ro.id DESC LIMIT 1
        """, order_id)
    if not row: return None, None
    raw = row["tg_delivery_msg_ids"]
    if not raw: return row["branch"], None
    import json as _j
    try:
        msg_ids = _j.loads(raw) if isinstance(raw, str) else raw
        return row["branch"], msg_ids.get(str(order_id))
    except Exception:
        return row["branch"], None


async def get_channel_stop_full(order_id: int) -> dict | None:
    """Полные данные стопа для перестройки текста сообщения в канале после изменения оплаты."""
    if not pool: return None
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.branch, r.tg_delivery_msg_ids, ro.sort_order,
                   o.id AS order_id, o.order_num, o.status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE(o.total_price, 0) AS total_price,
                   COALESCE(o.discount_sum, 0) AS discount_sum,
                   COALESCE(o.delivery_discount, 0) AS delivery_discount,
                   COALESCE(o.manual_discount, 0) AS manual_discount,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id), 0) AS items_total,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)), 0) AS paid_amount
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            JOIN orders o ON o.id = ro.order_id
            WHERE ro.order_id = $1
            ORDER BY ro.id DESC LIMIT 1
        """, order_id)
    if not row: return None
    d = dict(row)
    raw = d.get("tg_delivery_msg_ids") or "{}"
    try: msg_ids = _j.loads(raw) if isinstance(raw, str) else (raw or {})
    except: msg_ids = {}
    d["msg_id"] = msg_ids.get(str(order_id))
    stored_ch = msg_ids.get("__channel__")
    d["channel_id"] = int(stored_ch) if stored_ch else None
    return d


# ── Касса / наличные ──────────────────────────────────────────────────────────

async def get_cashiers() -> list:
    """Ответственные за кассу (can_manage_cash). Admins первыми."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, first_name, last_name, role, position
               FROM staff
               WHERE can_manage_cash=TRUE AND active=TRUE
               ORDER BY (role='admin') DESC, last_name, first_name""")
        return [dict(r) for r in rows]

async def get_my_cash_balance(staff_id: int) -> dict:
    """Баланс конкретного сотрудника: принял / сдал / на руках."""
    if not pool: return {}
    async with pool.acquire() as conn:
        # Принял от клиентов (только по staff_id)
        r1 = await conn.fetchval(
            """SELECT COALESCE(SUM(amount),0) FROM order_payments
               WHERE method='cash' AND created_by_staff_id=$1""",
            staff_id)
        # Сдал сразу при записи (handed_to != me)
        r2 = await conn.fetchval(
            """SELECT COALESCE(SUM(amount),0) FROM order_payments
               WHERE method='cash' AND handed_to_staff_id IS NOT NULL AND handed_to_staff_id!=$1
               AND created_by_staff_id=$1""",
            staff_id)
        # Получил от других сотрудников через платёж (они сдали мне)
        r3 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE handed_to_staff_id=$1 AND method='cash' AND (created_by_staff_id IS NULL OR created_by_staff_id<>$1)",
            staff_id)
        # Получил через ручную передачу (cash_handovers to me, только подтверждённые)
        r4 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE to_staff_id=$1 AND status='confirmed'", staff_id)
        # Сдал через ручную передачу (cash_handovers from me, только подтверждённые)
        r5 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE from_staff_id=$1 AND status='confirmed'", staff_id)
        # Ожидают подтверждения (cash_handovers from me, status='pending')
        r6 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM cash_handovers WHERE from_staff_id=$1 AND status='pending'", staff_id)
        # Расходы утверждённые — вычитаются из наличных на руках
        r7 = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE created_by_staff_id=$1 AND status IN ('approved','paid')",
            staff_id)
        collected         = float(r1)
        given_imm         = float(r2)
        recv_others       = float(r3)
        recv_hand         = float(r4)
        given_hand        = float(r5)
        pending_sent      = float(r6)
        expenses_approved = float(r7)
        on_hand = collected - given_imm + recv_others + recv_hand - given_hand - expenses_approved
        return {
            "collected":            collected,
            "given_immediately":    given_imm,
            "received_from_others": recv_others + recv_hand,
            "handed_over":          given_imm + given_hand,
            "pending_sent":         pending_sent,
            "expenses_approved":    expenses_approved,
            "on_hand":              on_hand,
        }

async def get_cash_balance() -> list:
    """Баланс наличных по всем сотрудникам (два уровня: исполнители + ответственные)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, first_name, last_name, can_manage_cash, role FROM staff WHERE active=TRUE ORDER BY (role='admin') DESC, can_manage_cash DESC, last_name, first_name")
        result = []
        for s in rows:
            bal = await get_my_cash_balance(s['id'])
            is_admin = s['role'] == 'admin'
            if not is_admin and not s['can_manage_cash'] and bal['collected'] == 0 and bal['received_from_others'] == 0:
                continue
            result.append({
                "id": s['id'],
                "first_name": s['first_name'],
                "last_name":  s['last_name'],
                "can_manage_cash": s['can_manage_cash'],
                "role": s['role'],
                **bal,
            })
        return result

async def add_cash_handover(from_staff_id: int, to_staff_id: int, amount: float, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, shift_id)
            VALUES ($1,$2,$3,$4,$5) RETURNING *
        """, from_staff_id, to_staff_id, amount, note, shift_id)
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
            WHERE ch.to_type='staff' OR ch.to_type IS NULL
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

async def reject_cash_handover(handover_id: int, rejected_by: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cash_handovers SET status='rejected', confirmed_at=NOW(), confirmed_by=$2
            WHERE id=$1 AND status='pending' RETURNING *
        """, handover_id, rejected_by)
        return dict(row) if row else {}

async def cancel_cash_handover(handover_id: int, cancelled_by: int, is_admin: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        # Admin can cancel any, staff only pending own
        if is_admin:
            row = await conn.fetchrow(
                "UPDATE cash_handovers SET status='cancelled', confirmed_at=NOW(), confirmed_by=$2 WHERE id=$1 RETURNING *",
                handover_id, cancelled_by)
        else:
            row = await conn.fetchrow(
                "UPDATE cash_handovers SET status='cancelled', confirmed_at=NOW(), confirmed_by=$2 WHERE id=$1 AND from_staff_id=$2 AND status='pending' RETURNING *",
                handover_id, cancelled_by)
        return dict(row) if row else {}

async def update_handover_tg_msg(handover_id: int, tg_chat_id: int, tg_msg_id: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE cash_handovers SET tg_chat_id=$2, tg_msg_id=$3 WHERE id=$1",
            handover_id, tg_chat_id, tg_msg_id)

async def mark_expense_paid(expense_id: int, paid_by: int, paid_from: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='paid', paid_by=$2, paid_from=$3, paid_at=NOW()
            WHERE id=$1 AND status='approved' RETURNING *
        """, expense_id, paid_by, paid_from)
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

async def get_my_sent_handovers(staff_id: int) -> list:
    """Исходящие передачи наличных от данного сотрудника (все типы: сотрудник / банк / сейф)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(st.last_name,'') || ' ' || COALESCE(st.first_name,'')) AS to_name
            FROM cash_handovers ch
            LEFT JOIN staff st ON st.id = ch.to_staff_id
            WHERE ch.from_staff_id = $1
            ORDER BY ch.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def get_my_received_handovers(staff_id: int) -> list:
    """Входящие подтверждённые передачи наличных для данного сотрудника."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_staff_id = $1 AND ch.status = 'confirmed'
            ORDER BY ch.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def add_safe_deposit(from_staff_id: int, amount: float, note: str = '') -> dict:
    """Сдача в сейф от администратора: создаёт pending запись для подтверждения."""
    if not pool: return {}
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, to_type, status, shift_id)
            VALUES ($1, NULL, $2, $3, 'safe', 'pending', $4) RETURNING *
        """, from_staff_id, amount, note, shift_id)
        return dict(row) if row else {}

async def get_pending_safe_deposits() -> list:
    """Pending сдачи в сейф для отображения в admin.html."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff sf ON sf.id = ch.from_staff_id
            WHERE ch.to_type='safe' AND ch.status='pending'
            ORDER BY ch.created_at DESC
        """)
        return [dict(r) for r in rows]

async def create_bank_deposit(from_staff_id: int, amount: float, to_type: str, note: str = '') -> dict:
    """Инкассация: наличные сданы в банк/сейф. Сразу подтверждена."""
    if not pool: return {}
    async with pool.acquire() as conn:
        shift_id = await _get_current_shift_id(conn)
        row = await conn.fetchrow("""
            INSERT INTO cash_handovers (from_staff_id, to_staff_id, amount, note, to_type, status, confirmed_at, shift_id)
            VALUES ($1, NULL, $2, $3, $4, 'confirmed', NOW(), $5) RETURNING *
        """, from_staff_id, amount, note, to_type, shift_id)
        return dict(row) if row else {}

async def get_bank_deposits(limit: int = 100) -> list:
    """История инкассаций (bank/safe)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ch.*,
                   TRIM(COALESCE(s.last_name,'') || ' ' || COALESCE(s.first_name,'')) AS from_name
            FROM cash_handovers ch
            LEFT JOIN staff s ON s.id = ch.from_staff_id
            WHERE ch.to_type IN ('bank','safe')
            ORDER BY ch.created_at DESC LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

async def get_cash_dashboard() -> dict:
    """Сводные метрики для дашборда кассы."""
    if not pool: return {}
    balances = await get_cash_balance()
    staff_on_hand   = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') != 'admin' and not b.get('can_manage_cash'))
    manager_on_hand = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') != 'admin' and b.get('can_manage_cash'))
    admin_on_hand   = sum(float(b.get('on_hand', 0)) for b in balances if b.get('role') == 'admin')
    from datetime import date as _date
    today = _date.today()
    async with pool.acquire() as conn:
        r1 = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS total FROM cash_handovers WHERE to_type IN ('bank','safe') AND created_at::date=$1", today)
        r3 = await conn.fetchrow(
            "SELECT COALESCE(SUM(amount),0) AS total, COUNT(*) AS cnt FROM expenses WHERE status IN ('pending','mgr_approved')")
        # К оплате: все заказы в работе (не доставлены), у которых есть остаток
        r_pending = await conn.fetchrow("""
            SELECT COALESCE(SUM(GREATEST(0,
                COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                  FROM order_items WHERE order_id=o.id), 0),
                         COALESCE(o.total_price,0), 0)
                - COALESCE(o.discount_sum,0)
                - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                - COALESCE((SELECT SUM(op.amount) FROM order_payments op
                             WHERE op.order_id = o.id
                               AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)), 0)
            )), 0) AS total
            FROM orders o
            WHERE o.payment_status IN ('unpaid','partial')
              AND o.status IN ('ready','delivery')
        """)
        # Долги: доставленные с debt_responsible_id, ещё не оплачены полностью
        r_debt = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total_cnt,
                COUNT(CASE WHEN o.debt_due_date IS NOT NULL AND o.debt_due_date < CURRENT_DATE THEN 1 END) AS overdue_cnt,
                COALESCE(SUM(GREATEST(0,
                    COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                      FROM order_items WHERE order_id=o.id), 0),
                             COALESCE(o.total_price,0), 0)
                    - COALESCE(o.discount_sum,0)
                    - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                    - COALESCE((SELECT SUM(op.amount) FROM order_payments op
                                 WHERE op.order_id = o.id
                                   AND NOT (op.confirmed = FALSE AND op.confirmed_at IS NOT NULL)), 0)
                )), 0) AS total_debt
            FROM orders o
            WHERE o.debt_responsible_id IS NOT NULL
              AND o.payment_status IN ('unpaid','partial')
        """)
    return {
        'staff_on_hand':          staff_on_hand,
        'manager_on_hand':        manager_on_hand,
        'admin_on_hand':          admin_on_hand,
        'banked_today':           float(r1['total']),
        'pending_client_cash':    float(r_pending['total']),
        'expenses_pending_sum':   float(r3['total']),
        'expenses_pending_count': int(r3['cnt']),
        'debt_total':             float(r_debt['total_debt']),
        'debt_count':             int(r_debt['total_cnt']),
        'debt_overdue_count':     int(r_debt['overdue_cnt']),
    }

async def confirm_payment(payment_id: int, confirmed_by: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE order_payments SET confirmed=TRUE, confirmed_by=$2, confirmed_at=NOW()
            WHERE id=$1 RETURNING *
        """, payment_id, confirmed_by or None)
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
                status      VARCHAR(20)  DEFAULT 'new',
                priority    VARCHAR(20)  DEFAULT 'normal',
                created_at  TIMESTAMPTZ  DEFAULT NOW(),
                done_at     TIMESTAMPTZ  DEFAULT NULL
            )
        """)
        await conn.execute("ALTER TABLE plans ALTER COLUMN status SET DEFAULT 'new'")
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
              AND COALESCE(cs.last_client_msg_at, cs.created_at) < NOW() - INTERVAL '10 minutes'
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

# ══════════════════════════════════════
#  ДОЛГИ ПО ЗАКАЗАМ
# ══════════════════════════════════════

async def get_order_debt_amount(order_id: int) -> float:
    if not pool: return 0.0
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT GREATEST(0,
                COALESCE(o.total_price,0) - COALESCE(o.discount_sum,0)
                - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                - COALESCE((SELECT SUM(amount) FROM order_payments
                             WHERE order_id=o.id AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)),0)
            ) AS debt FROM orders o WHERE o.id=$1
        """, order_id)
        return float(row["debt"]) if row else 0.0

async def get_debt_approvers() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, first_name, last_name, tg_id FROM staff "
            "WHERE can_approve_debt=TRUE AND active=TRUE AND tg_id IS NOT NULL")
        return [dict(r) for r in rows]

async def mark_order_delivered_with_debt(order_id: int, responsible_id: int,
                                         due_date_str: str | None, by_name: str) -> bool:
    if not pool: return False
    from datetime import date, timedelta
    due = None
    if due_date_str:
        try: due = date.fromisoformat(due_date_str)
        except Exception: pass
    if due is None:
        due = date.today() + timedelta(days=7)
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE orders SET status='delivered', debt_responsible_id=$2,
                   debt_due_date=$3, debt_approved_at=NOW() WHERE id=$1
        """, order_id, responsible_id, due)
        await conn.execute(
            "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status='pending'",
            order_id)
        resp_name = await conn.fetchval(
            "SELECT COALESCE(last_name||' '||first_name, login) FROM staff WHERE id=$1", responsible_id)
        await conn.execute(
            "INSERT INTO order_status_history(order_num, new_status, note) "
            "SELECT order_num,'delivered','Закрыт с долгом · '||$2||' (отв: '||$3||')' FROM orders WHERE id=$1",
            order_id, by_name, resp_name or "")
    return True

async def get_orders_with_debt() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT o.id, o.order_num, o.status, o.debt_due_date, o.debt_approved_at,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   TRIM(COALESCE(sr.last_name,'') || ' ' || COALESCE(sr.first_name,'')) AS responsible_name,
                   sr.id AS responsible_id,
                   GREATEST(0,
                     COALESCE(NULLIF((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                                       FROM order_items WHERE order_id=o.id), 0),
                              COALESCE(o.total_price,0), 0)
                     - COALESCE(o.discount_sum,0)
                     - COALESCE(o.delivery_discount,0) - COALESCE(o.manual_discount,0)
                     - COALESCE((SELECT SUM(amount) FROM order_payments
                                  WHERE order_id=o.id
                                    AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)),0)
                   ) AS debt_amount
              FROM orders o
              LEFT JOIN staff sr ON sr.id = o.debt_responsible_id
             WHERE o.debt_responsible_id IS NOT NULL
             ORDER BY o.debt_due_date ASC NULLS LAST, o.id DESC
        """)
        return [r for r in [dict(r) for r in rows] if r["debt_amount"] > 0]

async def extend_debt_due_date(order_id: int, new_due_date_str: str, note: str = '') -> bool:
    if not pool: return False
    from datetime import date as _date
    try:
        new_due = _date.fromisoformat(new_due_date_str)
    except Exception:
        return False
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE orders SET debt_due_date=$2 WHERE id=$1 AND debt_responsible_id IS NOT NULL",
            order_id, new_due
        )
        if res == "UPDATE 1":
            suffix = f": {note}" if note else ""
            await conn.execute(
                "INSERT INTO order_status_history(order_num, new_status, note) "
                "SELECT order_num, 'debt_extended', $2 FROM orders WHERE id=$1",
                order_id, f"Срок долга продлён до {new_due_date_str}{suffix}"
            )
        return res == "UPDATE 1"

# ── discount_requests ─────────────────────────────────────────────────────────

async def create_discount_request(order_id: int, order_num: str, driver_tg_id: int, requested_amount: float) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO discount_requests(order_id, order_num, driver_tg_id, requested_amount)
            VALUES($1,$2,$3,$4) RETURNING *
        """, order_id, order_num, driver_tg_id, requested_amount)
        return dict(row) if row else None

async def get_pending_discount_requests() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dr.*, o.client_first_name, o.client_last_name,
                   COALESCE(o.total_price,0) AS order_total
            FROM discount_requests dr
            LEFT JOIN orders o ON o.id = dr.order_id
            WHERE dr.status='pending'
            ORDER BY dr.created_at ASC
        """)
        return [dict(r) for r in rows]

async def resolve_discount_request(request_id: int, approved_amount: float, resolved_by: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE discount_requests
               SET status='approved', approved_amount=$2, resolved_by=$3, resolved_at=NOW()
             WHERE id=$1 AND status='pending'
            RETURNING *
        """, request_id, approved_amount, resolved_by)
        if not row:
            return None
        r = dict(row)
        await conn.execute("""
            UPDATE orders SET manual_discount = COALESCE(manual_discount,0) + $2 WHERE id=$1
        """, r["order_id"], approved_amount)
        return r

async def reject_discount_request(request_id: int, resolved_by: int) -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE discount_requests
               SET status='rejected', resolved_by=$2, resolved_at=NOW()
             WHERE id=$1 AND status='pending'
            RETURNING *
        """, request_id, resolved_by)
        return dict(row) if row else None


# ── Долговые одобрения ────────────────────────────────────────────────────────

async def create_debt_approval_request(order_id: int, order_num: str, driver_tg_id: int,
                                        debt_amount: float, mgr_msgs_json: str = '{}') -> dict | None:
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO debt_approval_requests(order_id, order_num, driver_tg_id, debt_amount, mgr_msgs)
            VALUES($1,$2,$3,$4,$5::jsonb)
            RETURNING id, order_id, order_num, debt_amount, status
        """, order_id, order_num, driver_tg_id, debt_amount, mgr_msgs_json)
        return dict(row) if row else None

async def get_pending_debt_approvals() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT dar.id, dar.order_id, dar.order_num, dar.driver_tg_id,
                   dar.debt_amount, dar.mgr_msgs, dar.created_at,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS order_total,
                   COALESCE(o.discount_sum, 0) + COALESCE(o.delivery_discount, 0)
                       + COALESCE(o.manual_discount, 0) AS total_discount,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id = o.id
                                AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                     OR (method<>'cash' AND confirmed=TRUE))), 0) AS paid_amount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id = o.id), 0)::int AS item_count
            FROM debt_approval_requests dar
            LEFT JOIN orders o ON o.id = dar.order_id
            WHERE dar.status = 'pending'
            ORDER BY dar.created_at ASC
        """)
        return [dict(r) for r in rows]

async def get_order_channel_info(order_id: int) -> dict | None:
    """Возвращает channel_id, msg_id и данные стопа для обновления канального сообщения."""
    import json as _j
    if not pool: return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT r.id AS route_id, r.branch, r.tg_delivery_msg_ids,
                   ro.sort_order,
                   o.order_num, o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address,
                   COALESCE((SELECT SUM(COALESCE(price_per_sqm,0)*COALESCE(sqm,0))
                              FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS items_total,
                   COALESCE(o.discount_sum,0) AS discount_sum,
                   COALESCE(o.delivery_discount,0) AS delivery_discount,
                   COALESCE(o.manual_discount,0) AS manual_discount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id),0)::int AS item_count,
                   COALESCE((SELECT SUM(amount) FROM order_payments
                              WHERE order_id=o.id
                                AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                     OR (method<>'cash' AND confirmed=TRUE))),0) AS paid_amount
            FROM route_orders ro
            JOIN routes r ON r.id = ro.route_id
            JOIN orders o ON o.id = ro.order_id
            WHERE ro.order_id = $1
            ORDER BY r.created_at DESC LIMIT 1
        """, order_id)
        if not row: return None
        d = dict(row)
        raw = d.get("tg_delivery_msg_ids") or "{}"
        try: msg_ids = _j.loads(raw) if isinstance(raw, str) else (raw or {})
        except: msg_ids = {}
        d["msg_id"] = msg_ids.get(str(order_id))
        stored_ch = msg_ids.get("__channel__")
        if stored_ch:
            d["channel_id"] = int(stored_ch)
        else:
            branch = d.get("branch", "")
            key_ch = "delivery_channel_navoi_id" if branch == "navoi" else "delivery_channel_zarafshan_id"
            key_gr = "delivery_group_navoi_id"   if branch == "navoi" else "delivery_group_zarafshan_id"
            d["channel_id"] = 0
            for key in (key_ch, key_gr, "delivery_group_id"):
                cfg = await conn.fetchrow("SELECT value FROM config WHERE key=$1", key)
                if cfg and cfg["value"]:
                    d["channel_id"] = int(cfg["value"])
                    break
        num_row = await conn.fetchrow(
            "SELECT COUNT(*)+1 AS num FROM route_orders WHERE route_id=$1 AND sort_order < $2",
            d["route_id"], d["sort_order"])
        d["stop_num"] = int(num_row["num"]) if num_row else 1
        return d

async def resolve_debt_approval(request_id: int, resolution: str, resolved_by: int,
                                 responsible_id: int | None = None) -> dict | None:
    if not pool: return None
    from datetime import date, timedelta
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE debt_approval_requests
            SET status=$2, resolution=$3, resolved_by=$4, responsible_id=$5, resolved_at=NOW()
            WHERE id=$1 AND status='pending'
            RETURNING *
        """, request_id, resolution, resolution, resolved_by, responsible_id)
        if not row: return None
        r = dict(row)
        if resolution == 'approved' and responsible_id:
            due = date.today() + timedelta(days=7)
            await conn.execute("""
                UPDATE orders SET status='delivered', debt_responsible_id=$2,
                       debt_due_date=$3, debt_approved_at=NOW() WHERE id=$1
            """, r['order_id'], responsible_id, due)
            await conn.execute(
                "UPDATE route_orders SET stop_status='done' WHERE order_id=$1 AND stop_status='pending'",
                r['order_id'])
        return r

async def get_routes_today(branch: str | None = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        where_clause = "WHERE r.date = CURRENT_DATE AND r.status != 'cancelled'"
        vals = []
        if branch:
            where_clause += " AND r.branch = $1"
            vals = [branch]
        rows = await conn.fetch(f"""
            SELECT r.id AS route_id, r.name, r.date::text, r.type, r.status AS route_status, r.branch,
                   TRIM(COALESCE(s.first_name,'') || ' ' || COALESCE(s.last_name,'')) AS driver_name,
                   ro.sort_order, ro.stop_status, ro.driver_confirmed,
                   o.id AS order_id, o.order_num, o.status AS order_status,
                   o.client_first_name, o.client_last_name, o.client_phone,
                   o.address, o.short_address, o.location, o.location_address,
                   COALESCE((SELECT SUM(COALESCE(sqm*price_per_sqm,0)) FROM order_items WHERE order_id=o.id),
                            COALESCE(o.total_price,0)) AS items_total,
                   COALESCE(o.discount_sum,0)+COALESCE(o.delivery_discount,0)+COALESCE(o.manual_discount,0) AS total_discount,
                   COALESCE((SELECT SUM(amount) FROM order_payments WHERE order_id=o.id
                              AND ((method='cash' AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL))
                                   OR (method<>'cash' AND confirmed=TRUE))), 0) AS paid_amount,
                   COALESCE((SELECT COUNT(*) FROM order_items WHERE order_id=o.id),0)::int AS item_count
            FROM routes r
            LEFT JOIN staff s ON s.id = r.driver_id
            JOIN route_orders ro ON ro.route_id = r.id
            JOIN orders o ON o.id = ro.order_id
            {where_clause}
            ORDER BY r.id, ro.sort_order
        """, *vals)
        routes: dict = {}
        for row in rows:
            rid = row["route_id"]
            if rid not in routes:
                routes[rid] = {"id": rid, "name": row["name"], "date": row["date"],
                               "type": row["type"], "status": row["route_status"],
                               "branch": row["branch"],
                               "driver_name": row["driver_name"] or None,
                               "stops": []}
            routes[rid]["stops"].append({
                "order_id":       row["order_id"],
                "order_num":      row["order_num"],
                "sort_order":     row["sort_order"],
                "stop_status":    row["stop_status"],
                "driver_confirmed": bool(row["driver_confirmed"]),
                "order_status":   row["order_status"],
                "client_first_name": row["client_first_name"],
                "client_last_name":  row.get("client_last_name"),
                "client_phone":   row.get("client_phone"),
                "address":        row.get("address"),
                "short_address":  row.get("short_address"),
                "location":       row.get("location"),
                "location_address": row.get("location_address"),
                "items_total":    float(row["items_total"]),
                "total_discount": float(row["total_discount"]),
                "paid_amount":    float(row["paid_amount"]),
                "item_count":     row["item_count"],
            })
        return list(routes.values())

async def driver_set_stop_status(order_id: int, status: str):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE route_orders SET stop_status=$2 WHERE order_id=$1 AND stop_status!='done'",
            order_id, status)

async def driver_set_confirmed(order_id: int, confirmed: bool = True):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE route_orders SET driver_confirmed=$2 WHERE order_id=$1", order_id, confirmed)

async def driver_update_order_status(order_id: int, new_status: str, staff_id: int, staff_name: str, note: str = ""):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE orders SET status=$2, updated_at=NOW() WHERE id=$1", order_id, new_status)
        await conn.execute(
            "INSERT INTO order_activity (order_id, staff_id, staff_name, action, details) VALUES ($1,$2,$3,$4,$5)",
            order_id, staff_id, staff_name, f"status_{new_status}", note or f"Статус → {new_status}")


# ── Расходы ───────────────────────────────────────────────────────────────────

# ── Seed данные: 2-уровневая структура ───────────────────────────────────────
_EXPENSE_CAT_PARENTS = [
    # (name_ru, name_uz, icon, sort_order)  — имя БЕЗ эмодзи, эмодзи только в icon
    ("Транспорт",        "Transport",          "🚗", 10),
    ("Коммунальные",     "Kommunal",           "💡", 20),
    ("Персонал",         "Xodimlar",           "👷", 30),
    ("Химия/Материалы",  "Kimyo/Materiallar",  "🧴", 40),
    ("Закупки/Склад",    "Xarid/Ombor",        "📦", 50),
    ("Офис",             "Ofis",               "🏢", 60),
    ("Обслуживание",     "Texnik xizmat",      "🔧", 70),
    ("Маркетинг",        "Marketing",          "📣", 80),
    ("Финансы",          "Moliya",             "🏦", 90),
    ("Прочее",           "Boshqalar",          "❓", 100),
]

# (parent_name_ru, [(name_ru, name_uz, icon, approve_level, receipt_required, amount_threshold, sort_order)])
_EXPENSE_CAT_CHILDREN = [
    ("Транспорт", [
        ("Топливо",         "Yoqilg'i",       "⛽", "manager", True,  None, 1),
        ("Ремонт авто",     "Avto ta'miri",   "🔧", "both",    True,  None, 2),
        ("Парковка",        "Parkovka",       "🅿️","manager", False, None, 3),
    ]),
    ("Коммунальные", [
        ("Электричество",   "Elektr",         "💡", "admin",   True,  None, 1),
        ("Вода",            "Suv",            "💧", "admin",   True,  None, 2),
        ("Интернет",        "Internet",       "🌐", "admin",   True,  None, 3),
        ("Газ",             "Gaz",            "🔥", "admin",   True,  None, 4),
    ]),
    ("Персонал", [
        ("Зарплата",        "Maosh",          "💰", "admin",   False, None, 1),
        ("Аванс",           "Avans",          "💸", "both",    False, None, 2),
        ("Питание",         "Ovqat",          "🍽", "manager", False, None, 3),
        ("Медицина",        "Tibbiyot",       "🏥", "both",    True,  None, 4),
    ]),
    ("Химия/Материалы", [
        ("Бытовая химия",   "Kimyo",          "🧴", "manager", True,  None, 1),
        ("Инвентарь",       "Inventar",       "🪣", "manager", False, None, 2),
    ]),
    ("Закупки/Склад", [
        ("Упаковка",        "Qadoqlash",      "📦", "manager", False, None, 1),
        ("Прочие закупки",  "Boshqa xaridlar","🛒", "both",    True,  None, 2),
    ]),
    ("Офис", [
        ("Канцтовары",      "Kantselyariya",  "📎", "manager", False, None, 1),
        ("Продукты",        "Oziq-ovqat",     "☕", "manager", False, None, 2),
        ("Связь/SIM",       "Aloqa/SIM",      "📱", "manager", True,  None, 3),
        ("Аренда",          "Ijara",          "🏢", "admin",   True,  None, 4),
    ]),
    ("Обслуживание", [
        ("Ремонт обор-я",   "Jihoz ta'miri",  "🔩", "both",    True,  None, 1),
        ("Уборка помещ.",   "Xona tozalash",  "🧹", "manager", False, None, 2),
    ]),
    ("Маркетинг", [
        ("Реклама",         "Reklama",        "📣", "both",    True,  None, 1),
        ("Представит.",     "Vakillik",       "🎁", "admin",   True,  None, 2),
    ]),
    ("Финансы", [
        ("Инкассация",      "Inkassatsiya",   "🏦", "admin",   True,  None, 1),
    ]),
    ("Прочее", [
        ("Прочее",             "Boshqalar",             "❓", "admin",   True,  None, 1),
    ]),
]

async def ensure_expense_tables():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expense_categories (
                id               SERIAL PRIMARY KEY,
                name_ru          TEXT NOT NULL,
                name_uz          TEXT NOT NULL,
                icon             TEXT DEFAULT '',
                parent_id        INT,
                approve_level    TEXT NOT NULL DEFAULT 'manager',
                receipt_required BOOLEAN NOT NULL DEFAULT FALSE,
                amount_threshold NUMERIC,
                sort_order       INT DEFAULT 0,
                active           BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)
        # Миграция: добавить parent_id если таблица уже существует без него
        await conn.execute("""
            ALTER TABLE expense_categories ADD COLUMN IF NOT EXISTS parent_id INT
        """)
        try:
            await conn.execute("""
                ALTER TABLE expense_categories ADD CONSTRAINT fk_exp_cat_parent
                FOREIGN KEY (parent_id) REFERENCES expense_categories(id) ON DELETE SET NULL
            """)
        except Exception:
            pass  # constraint уже есть

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id                   SERIAL PRIMARY KEY,
                category_id          INT REFERENCES expense_categories(id),
                amount               NUMERIC NOT NULL,
                description          TEXT DEFAULT '',
                created_by_staff_id  INT REFERENCES staff(id),
                branch               TEXT DEFAULT '',
                status               TEXT NOT NULL DEFAULT 'pending',
                manager_id           INT REFERENCES staff(id),
                manager_at           TIMESTAMPTZ,
                admin_id             INT REFERENCES staff(id),
                admin_at             TIMESTAMPTZ,
                reject_reason        TEXT DEFAULT '',
                receipt_url          TEXT DEFAULT '',
                paid_from            TEXT DEFAULT 'cash',
                created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Сеем 2-уровневую структуру если подкатегорий ещё нет
        has_children = await conn.fetchval(
            "SELECT COUNT(*) FROM expense_categories WHERE parent_id IS NOT NULL")
        if has_children == 0:
            # Очищаем старые плоские категории (расходов в production пока нет)
            await conn.execute("TRUNCATE expenses RESTART IDENTITY")
            await conn.execute("TRUNCATE expense_categories RESTART IDENTITY CASCADE")
            # Сеем родителей
            for (name_ru, name_uz, icon, sort_order) in _EXPENSE_CAT_PARENTS:
                await conn.execute(
                    "INSERT INTO expense_categories (name_ru, name_uz, icon, sort_order) VALUES ($1,$2,$3,$4)",
                    name_ru, name_uz, icon, sort_order)
            # Сеем детей
            for (parent_name, children) in _EXPENSE_CAT_CHILDREN:
                pid = await conn.fetchval(
                    "SELECT id FROM expense_categories WHERE name_ru=$1", parent_name)
                if not pid:
                    continue
                for (nm_ru, nm_uz, icon, approve_level, receipt_req, threshold, sord) in children:
                    await conn.execute("""
                        INSERT INTO expense_categories
                            (name_ru, name_uz, icon, parent_id, approve_level, receipt_required, amount_threshold, sort_order)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    """, nm_ru, nm_uz, icon, pid, approve_level, receipt_req, threshold, sord)
        else:
            # Миграция: убираем дублирующий эмодзи из начала name_ru/name_uz
            # если name_ru начинается с icon + пробел (например "⛽ Топливо" → "Топливо")
            await conn.execute("""
                UPDATE expense_categories
                SET name_ru = TRIM(SUBSTRING(name_ru FROM LENGTH(icon) + 2)),
                    name_uz = TRIM(SUBSTRING(name_uz FROM LENGTH(icon) + 2))
                WHERE icon != ''
                  AND LENGTH(name_ru) > LENGTH(icon) + 1
                  AND SUBSTRING(name_ru FROM 1 FOR LENGTH(icon)) = icon
                  AND SUBSTRING(name_ru FROM LENGTH(icon) + 1 FOR 1) = ' '
            """)

async def get_expense_categories_tree() -> list:
    """Возвращает дерево: родители со списком children."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM expense_categories WHERE active=TRUE ORDER BY sort_order, id")
        cats = [dict(r) for r in rows]
    parents = [c for c in cats if not c['parent_id']]
    ch_map: dict = {}
    for c in cats:
        if c['parent_id']:
            ch_map.setdefault(c['parent_id'], []).append(c)
    for p in parents:
        p['children'] = ch_map.get(p['id'], [])
    return parents

async def get_expense_categories() -> list:
    """Плоский список всех активных категорий (для обратной совместимости)."""
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM expense_categories WHERE active=TRUE ORDER BY sort_order, id")
        return [dict(r) for r in rows]

async def create_expense_category(name_ru: str, name_uz: str, icon: str,
                                   parent_id, approve_level: str,
                                   receipt_required: bool, amount_threshold,
                                   sort_order: int, for_staff: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO expense_categories
                (name_ru, name_uz, icon, parent_id, approve_level, receipt_required, amount_threshold, sort_order, for_staff)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *
        """, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, for_staff)
        return dict(row) if row else {}

async def update_expense_category(cat_id: int, name_ru: str, name_uz: str, icon: str,
                                   parent_id, approve_level: str,
                                   receipt_required: bool, amount_threshold,
                                   sort_order: int, active: bool, for_staff: bool = False) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expense_categories
            SET name_ru=$2, name_uz=$3, icon=$4, parent_id=$5,
                approve_level=$6, receipt_required=$7, amount_threshold=$8,
                sort_order=$9, active=$10, for_staff=$11
            WHERE id=$1 RETURNING *
        """, cat_id, name_ru, name_uz, icon, parent_id, approve_level,
             receipt_required, amount_threshold, sort_order, active, for_staff)
        return dict(row) if row else {}

async def delete_expense_category(cat_id: int) -> dict:
    if not pool: return {"ok": False, "error": "no pool"}
    async with pool.acquire() as conn:
        has_expenses = await conn.fetchval(
            "SELECT COUNT(*) FROM expenses WHERE category_id=$1", cat_id)
        if has_expenses:
            return {"ok": False, "error": "has_expenses"}
        has_children = await conn.fetchval(
            "SELECT COUNT(*) FROM expense_categories WHERE parent_id=$1", cat_id)
        if has_children:
            return {"ok": False, "error": "has_children"}
        await conn.execute("DELETE FROM expense_categories WHERE id=$1", cat_id)
        return {"ok": True}

async def create_expense(category_id: int, amount: float, description: str,
                         staff_id: int, branch: str, for_staff_id: int = None) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO expenses (category_id, amount, description, created_by_staff_id, branch, for_staff_id)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, category_id, amount, description, staff_id, branch, for_staff_id)
        return dict(row) if row else {}

async def get_expenses(branch: str = None, status: str = None,
                       category_id: int = None, limit: int = 100) -> list:
    if not pool: return []
    filters, params = [], []
    if branch:      filters.append(f"e.branch=${len(params)+1}");           params.append(branch)
    if status == 'paid':
        filters.append("e.status IN ('paid','approved')")
    elif status:
        filters.append(f"e.status=${len(params)+1}"); params.append(status)
    if category_id: filters.append(f"e.category_id=${len(params)+1}"); params.append(category_id)
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz, ep.icon AS parent_icon,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   TRIM(COALESCE(sm.last_name,'') || ' ' || COALESCE(sm.first_name,'')) AS manager_name,
                   TRIM(COALESCE(sa.last_name,'') || ' ' || COALESCE(sa.first_name,'')) AS admin_name,
                   TRIM(COALESCE(sf.last_name,'') || ' ' || COALESCE(sf.first_name,'')) AS for_staff_name
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            LEFT JOIN staff sm ON sm.id = e.manager_id
            LEFT JOIN staff sa ON sa.id = e.admin_id
            LEFT JOIN staff sf ON sf.id = e.for_staff_id
            {where}
            ORDER BY e.created_at DESC LIMIT {limit}
        """, *params)
        return [dict(r) for r in rows]

async def get_my_expenses(staff_id: int) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            WHERE e.created_by_staff_id = $1
            ORDER BY e.created_at DESC LIMIT 50
        """, staff_id)
        return [dict(r) for r in rows]

async def get_pending_expenses_for_manager(branch: str = None) -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        cond = "AND e.branch=$1" if branch else ""
        params = [branch] if branch else []
        rows = await conn.fetch(f"""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   sc.phone AS creator_phone
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            WHERE e.status='pending' AND ec.approve_level IN ('manager','both')
            {cond}
            ORDER BY e.created_at DESC
        """, *params)
        return [dict(r) for r in rows]

async def get_pending_expenses_for_admin() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT e.*,
                   ec.name_ru AS category_name_ru, ec.name_uz AS category_name_uz,
                   ec.icon AS category_icon, ec.approve_level, ec.receipt_required,
                   ep.name_ru AS parent_name_ru, ep.name_uz AS parent_name_uz,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name,
                   sc.phone AS creator_phone,
                   TRIM(COALESCE(sm.last_name,'') || ' ' || COALESCE(sm.first_name,'')) AS manager_name
            FROM expenses e
            LEFT JOIN expense_categories ec ON ec.id = e.category_id
            LEFT JOIN expense_categories ep ON ep.id = ec.parent_id
            LEFT JOIN staff sc ON sc.id = e.created_by_staff_id
            LEFT JOIN staff sm ON sm.id = e.manager_id
            WHERE (e.status='pending' AND ec.approve_level IN ('admin','both'))
               OR e.status='mgr_approved'
            ORDER BY e.created_at DESC
        """)
        return [dict(r) for r in rows]

async def approve_expense_manager(expense_id: int, manager_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        cat = await conn.fetchrow("""
            SELECT ec.approve_level, ec.name_ru AS category_name_ru FROM expenses e
            JOIN expense_categories ec ON ec.id=e.category_id
            WHERE e.id=$1
        """, expense_id)
        new_status = 'paid' if cat and cat['approve_level'] == 'manager' else 'mgr_approved'
        row = await conn.fetchrow("""
            UPDATE expenses SET status=$2, manager_id=$3, manager_at=NOW()
            WHERE id=$1 AND status='pending' RETURNING *
        """, expense_id, new_status, manager_id)
        if row and new_status == 'paid':
            exp = dict(row)
            exp['category_name_ru'] = cat['category_name_ru'] if cat else ''
            await _create_ledger_for_expense(conn, exp)
        return dict(row) if row else {}

async def approve_expense_admin(expense_id: int, admin_id: int) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='paid', admin_id=$2, admin_at=NOW()
            WHERE id=$1 AND status IN ('pending','mgr_approved') RETURNING *
        """, expense_id, admin_id)
        if row:
            cat = await conn.fetchval(
                "SELECT name_ru FROM expense_categories WHERE id=$1", row['category_id'])
            exp = dict(row)
            exp['category_name_ru'] = cat or ''
            await _create_ledger_for_expense(conn, exp)
        return dict(row) if row else {}

async def reject_expense(expense_id: int, staff_id: int, reason: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE expenses SET status='rejected', reject_reason=$2,
                manager_id=CASE WHEN manager_id IS NULL THEN $3 ELSE manager_id END,
                admin_id=CASE WHEN status IN ('pending','mgr_approved') THEN $3 ELSE admin_id END
            WHERE id=$1 AND status NOT IN ('rejected','paid') RETURNING *
        """, expense_id, reason, staff_id)
        return dict(row) if row else {}

async def save_expense_receipt(expense_id: int, receipt_url: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE expenses SET receipt_url=$2 WHERE id=$1 RETURNING *",
            expense_id, receipt_url)
        return dict(row) if row else {}


# ── SMS рассылки по расписанию ───────────────────────────────────────────────

async def ensure_salary_ledger_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS salary_ledger (
                id          SERIAL PRIMARY KEY,
                staff_id    INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
                period      DATE    NOT NULL,
                type        VARCHAR(30) NOT NULL DEFAULT 'accrual',
                amount      NUMERIC(12,2) NOT NULL,
                note        TEXT DEFAULT '',
                expense_id  INTEGER REFERENCES expenses(id) ON DELETE SET NULL,
                created_by  INTEGER REFERENCES staff(id) ON DELETE SET NULL,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_salary_ledger_staff_period ON salary_ledger(staff_id, period)")
        await conn.execute(
            "ALTER TABLE staff ADD COLUMN IF NOT EXISTS advance_percent NUMERIC(5,2) DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE settings ADD COLUMN IF NOT EXISTS advance_max_percent NUMERIC(5,2) DEFAULT 50")
        await conn.execute(
            "ALTER TABLE salary_ledger ADD COLUMN IF NOT EXISTS fine_reason TEXT DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS packer_login VARCHAR(100) DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS washed_at TIMESTAMPTZ DEFAULT NULL")
        await conn.execute(
            "ALTER TABLE orders ADD COLUMN IF NOT EXISTS packed_at TIMESTAMPTZ DEFAULT NULL")

async def get_advance_max_percent() -> float:
    if not pool: return 50.0
    async with pool.acquire() as conn:
        v = await conn.fetchval("SELECT COALESCE(advance_max_percent, 50) FROM settings LIMIT 1")
        return float(v or 50)

async def save_advance_max_percent(pct: float) -> None:
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("UPDATE settings SET advance_max_percent=$1", pct)

async def get_salary_balance(staff_id: int) -> dict:
    """Баланс сотрудника: общий остаток, разбивка за текущий месяц, лимит аванса."""
    if not pool: return {}
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    async with pool.acquire() as conn:
        # Общий накопленный баланс
        total = await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1", staff_id)
        # За текущий месяц
        month_rows = await conn.fetch("""
            SELECT type, COALESCE(SUM(amount),0) AS s
            FROM salary_ledger WHERE staff_id=$1 AND period=$2
            GROUP BY type
        """, staff_id, period)
        month_map = {r['type']: float(r['s']) for r in month_rows}
        accrual_month   = month_map.get('accrual', 0)
        advances_month  = abs(month_map.get('advance', 0))
        # advance_percent: личный или глобальный
        pct_row = await conn.fetchrow("""
            SELECT s.advance_percent, st.advance_max_percent
            FROM staff s, settings st
            WHERE s.id=$1
            LIMIT 1
        """, staff_id)
        pct = float(pct_row['advance_percent'] or pct_row['advance_max_percent'] or 50)
        advance_limit = max(0.0, accrual_month * pct / 100 - advances_month)
        return {
            'balance':        float(total or 0),
            'accrual_month':  accrual_month,
            'advances_month': advances_month,
            'advance_pct':    pct,
            'advance_limit':  advance_limit,
        }

async def get_salary_ledger(staff_id: int, year: int, month: int) -> list:
    if not pool: return []
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT sl.*, TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name
            FROM salary_ledger sl
            LEFT JOIN staff sc ON sc.id = sl.created_by
            WHERE sl.staff_id=$1 AND sl.period=$2
            ORDER BY sl.created_at
        """, staff_id, period)
        return [dict(r) for r in rows]

async def add_salary_ledger_entry(staff_id: int, period_str: str, type_: str,
                                   amount: float, note: str = '',
                                   expense_id: int = None, created_by: int = None,
                                   fine_reason: str = None) -> dict:
    if not pool: return {}
    from datetime import date as _date
    period = _date.fromisoformat(period_str)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO salary_ledger (staff_id, period, type, amount, note, expense_id, created_by, fine_reason)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, staff_id, period, type_, amount, note, expense_id, created_by, fine_reason)
        return dict(row) if row else {}

async def get_salary_daily_breakdown(staff_id: int, year: int, month: int) -> dict:
    """Посуточный регистр: начисление считается из табеля, авансы/штрафы из ledger."""
    if not pool: return {}
    from datetime import date as _date
    import calendar as _cal
    period = _date(year, month, 1)
    next_m = month + 1 if month < 12 else 1
    next_y = year if month < 12 else year + 1
    period_end = _date(next_y, next_m, 1)

    async with pool.acquire() as conn:
        # Накопленный баланс ДО этого месяца (все типы, все периоды < текущего)
        opening_balance = float(await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1 AND period < $2",
            staff_id, period) or 0)

        # Ставка и норма из staff
        st = await conn.fetchrow(
            "SELECT salary_type, salary_rate, salary_work_days, login FROM staff WHERE id=$1", staff_id)
        sal_type    = (st['salary_type'] or 'fixed') if st else 'fixed'
        sal_rate    = float((st['salary_rate'] or 0)) if st else 0.0
        norm_days   = int((st['salary_work_days'] or 26)) if st else 26
        staff_login = (st['login'] or '') if st else ''
        norm_hours  = norm_days * 8.0
        hourly_rate = sal_rate / norm_hours if (sal_type in ('fixed','fixed_percent') and norm_hours > 0) else 0.0

        # Табель за месяц
        ts_rows = await conn.fetch(
            "SELECT date, hours, type FROM timesheet WHERE staff_id=$1 AND date>=$2 AND date<$3 ORDER BY date",
            staff_id, period, period_end)

        # Ledger — только НЕ начисления (авансы, штрафы, выплаты) за этот период
        ledger_rows = await conn.fetch("""
            SELECT sl.*, DATE(sl.created_at AT TIME ZONE 'Asia/Tashkent') AS entry_date,
                   TRIM(COALESCE(sc.last_name,'') || ' ' || COALESCE(sc.first_name,'')) AS creator_name
            FROM salary_ledger sl
            LEFT JOIN staff sc ON sc.id = sl.created_by
            WHERE sl.staff_id=$1 AND sl.period=$2 AND sl.type != 'accrual'
            ORDER BY sl.created_at
        """, staff_id, period)

        # Для percent/fixed_percent: посуточный объём мойки
        washing_daily: dict = {}
        if sal_type in ('percent', 'fixed_percent') and staff_login:
            washing_pct_val = await conn.fetchval(
                "SELECT COALESCE(percent,0) FROM staff_salary_percents WHERE staff_id=$1 AND role='washing'",
                staff_id)
            washing_pct = float(washing_pct_val or 0)
            if washing_pct:
                day_rows = await conn.fetch("""
                    SELECT DATE(o.created_at AT TIME ZONE 'Asia/Tashkent') AS day,
                        COALESCE(SUM(COALESCE(oi.actual_total_sum,
                            oi.price_per_sqm * COALESCE(oi.actual_sqm, oi.sqm), 0)), 0) AS day_sum
                    FROM order_items oi
                    JOIN orders o ON o.id = oi.order_id
                    WHERE oi.washer_login = $1
                      AND o.status != 'cancelled'
                      AND o.created_at >= $2::timestamptz
                      AND o.created_at <  $3::timestamptz
                    GROUP BY DATE(o.created_at AT TIME ZONE 'Asia/Tashkent')
                """, staff_login, period, period_end)
                washing_daily = {
                    str(r['day']): round(float(r['day_sum']) * washing_pct / 100, 2)
                    for r in day_rows
                }

    # Карта табеля {date_str: hours}
    ts_map = {str(r['date']): float(r['hours'] or 0) for r in ts_rows
              if r['type'] in ('work', 'overtime')}

    # Карта ledger по дате транзакции
    from collections import defaultdict
    ledger_by_date = defaultdict(list)
    for r in ledger_rows:
        rd = dict(r)
        rd['amount'] = float(r['amount'])
        rd['entry_date'] = str(r['entry_date'])
        ledger_by_date[rd['entry_date']].append(rd)

    # Строим посуточную таблицу
    from datetime import date as _date2
    days_in_month = _cal.monthrange(year, month)[1]
    today = _date2.today()
    last_day = days_in_month if (year < today.year or month < today.month) else min(days_in_month, today.day)

    result_days = []
    running = 0.0  # таблица показывает движения текущего месяца; opening_balance — в шапке
    for d in range(1, last_day + 1):
        date_str = f"{year}-{month:02d}-{d:02d}"
        entries  = ledger_by_date.get(date_str, [])
        op = running

        # Начисление: из табеля (fixed) + мойка по дням (percent)
        hours_today = ts_map.get(date_str, 0.0)
        accrual = round(hourly_rate * hours_today, 2) if hourly_rate > 0 else 0.0
        accrual = round(accrual + washing_daily.get(date_str, 0.0), 2)

        advance = 0.0; salary_payment = 0.0; fine = 0.0; other = 0.0
        for e in entries:
            amt = e['amount']
            t   = e['type']
            if t == 'advance': advance += amt
            elif t == 'salary_payment': salary_payment += amt
            elif t == 'fine': fine += amt
            else: other += amt

        # running — только движения внутри месяца (без opening_balance)
        running = round(op + accrual + advance + salary_payment + fine + other, 2)
        result_days.append({
            'date': date_str,
            'opening': op,
            'accrual': accrual,
            'hours': hours_today,
            'advance': advance,
            'salary_payment': salary_payment,
            'fine': fine,
            'other': other,
            'closing': running,
            'entries': entries,
        })

    # Итоги за месяц
    total_accrual = sum(d['accrual'] for d in result_days)
    total_advance = sum(d['advance'] for d in result_days)
    total_fine    = sum(d['fine']    for d in result_days)
    total_payment = sum(d['salary_payment'] for d in result_days)
    total_other   = sum(d['other']   for d in result_days)
    month_balance = round(total_accrual + total_advance + total_fine + total_payment + total_other, 2)

    return {
        'days': result_days,
        'opening_balance': opening_balance,
        'salary_type': sal_type,
        'hourly_rate': hourly_rate,
        'total_accrual': total_accrual,
        'total_advance': abs(total_advance),
        'total_fine': abs(total_fine),
        'total_payment': abs(total_payment),
        'month_balance': month_balance,
        'final_balance': round(opening_balance + month_balance, 2),
    }

async def get_agent_monitoring_stats() -> dict:
    """Мониторинг активности персонала: order_activity / lead_calls / chat_messages /
    staff_attendance_events / cash_shifts + orders_in_work + presence_status."""
    if not pool: return {"agents": [], "order_status_breakdown": {}, "activity_trend_7d": []}

    today_tk = datetime.now(_TASHKENT).date()
    yesterday_tk = today_tk - timedelta(days=1)
    week_start_tk = today_tk - timedelta(days=6)
    ts_from, ts_to = _tz_range(str(week_start_tk), str(today_tk))
    now_utc = datetime.now(timezone.utc)

    def _secs_ago(dt):
        """Секунд назад от now_utc; поддерживает aware и naive (UTC) datetime."""
        if dt is None:
            return float('inf')
        if dt.tzinfo is None:
            return (datetime.utcnow() - dt).total_seconds()
        return (now_utc - dt).total_seconds()

    async with pool.acquire() as conn:
        staff_rows = await conn.fetch(
            "SELECT id, login, first_name, last_name, role, branch FROM staff WHERE active = true")

        oa_rows = await conn.fetch("""
            SELECT staff_id,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM order_activity
            WHERE staff_id IS NOT NULL
            GROUP BY staff_id
        """, today_tk, yesterday_tk)

        lc_rows = await conn.fetch("""
            SELECT operator_id AS staff_id,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM lead_calls
            WHERE operator_id IS NOT NULL
            GROUP BY operator_id
        """, today_tk, yesterday_tk)

        cm_rows = await conn.fetch("""
            SELECT sender_name,
                   MAX(created_at) AS last_active,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS actions_today,
                   COUNT(*) FILTER (WHERE (created_at AT TIME ZONE 'Asia/Tashkent')::date = $2) AS actions_yesterday
            FROM chat_messages
            WHERE sender_type = 'staff'
            GROUP BY sender_name
        """, today_tk, yesterday_tk)

        # attendance events: приход/уход
        try:
            ae_rows = await conn.fetch("""
                SELECT staff_id,
                       MAX(created_at) AS last_active,
                       COUNT(*) FILTER (WHERE event_type = 'in'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS in_today,
                       MIN(created_at) FILTER (WHERE event_type = 'in'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS first_in_today,
                       COUNT(*) FILTER (WHERE event_type = 'out'
                           AND (created_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS out_today
                FROM staff_attendance_events
                GROUP BY staff_id
            """, today_tk)
        except Exception:
            ae_rows = []

        # смены кассы
        try:
            cs_rows = await conn.fetch("""
                SELECT opened_by AS staff_id,
                       MAX(opened_at) AS last_active,
                       COUNT(*) FILTER (WHERE status = 'open'
                           AND (opened_at AT TIME ZONE 'Asia/Tashkent')::date = $1) AS shift_open_today
                FROM cash_shifts
                WHERE opened_by IS NOT NULL
                GROUP BY opened_by
            """, today_tk)
        except Exception:
            cs_rows = []

        # заказы в работе (linked via login)
        try:
            oiw_rows = await conn.fetch("""
                SELECT s.id AS staff_id, COUNT(*) AS cnt
                FROM orders o
                JOIN staff s ON s.login = o.assigned_to
                WHERE o.status NOT IN ('delivered','cancelled','new')
                  AND o.assigned_to IS NOT NULL
                GROUP BY s.id
            """)
        except Exception:
            oiw_rows = []

        status_row = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE status NOT IN ('delivered','cancelled')) AS in_progress,
                COUNT(*) FILTER (
                    WHERE status = 'delivered'
                      AND (updated_at AT TIME ZONE 'Asia/Tashkent')::date = $1
                ) AS delivered_today,
                COUNT(*) FILTER (
                    WHERE status = 'cancelled'
                      AND (updated_at AT TIME ZONE 'Asia/Tashkent')::date = $1
                ) AS cancelled_today
            FROM orders
        """, today_tk)

        trend_oa_rows = await conn.fetch("""
            SELECT (created_at AT TIME ZONE 'Asia/Tashkent')::date AS day, COUNT(*) AS cnt
            FROM order_activity
            WHERE created_at >= $1 AND created_at < $2
            GROUP BY day
        """, ts_from, ts_to)

        trend_lc_rows = await conn.fetch("""
            SELECT (created_at AT TIME ZONE 'Asia/Tashkent')::date AS day, COUNT(*) AS cnt
            FROM lead_calls
            WHERE created_at >= $1 AND created_at < $2
            GROUP BY day
        """, ts_from, ts_to)

    # ── вспомогательная merge-функция ─────────────────────────────────────
    def _merge_id(dest: dict, sid, last_active, at=0, ay=0):
        cur = dest.get(sid)
        if cur is None:
            dest[sid] = {'last_active': last_active, 'actions_today': at, 'actions_yesterday': ay}
        else:
            if last_active and (cur['last_active'] is None or last_active > cur['last_active']):
                cur['last_active'] = last_active
            cur['actions_today'] += at
            cur['actions_yesterday'] += ay

    # ── merge order_activity + lead_calls по staff.id ────────────────────
    by_id: dict = {}
    for r in oa_rows:
        _merge_id(by_id, r['staff_id'], r['last_active'],
                  r['actions_today'] or 0, r['actions_yesterday'] or 0)
    for r in lc_rows:
        _merge_id(by_id, r['staff_id'], r['last_active'],
                  r['actions_today'] or 0, r['actions_yesterday'] or 0)

    # ── merge chat_messages по имени ──────────────────────────────────────
    by_name: dict = {}
    for r in cm_rows:
        name = (r['sender_name'] or '').strip()
        if not name:
            continue
        by_name[name] = {
            'last_active': r['last_active'],
            'actions_today': r['actions_today'] or 0,
            'actions_yesterday': r['actions_yesterday'] or 0,
        }

    # ── индексы дополнительных источников ────────────────────────────────
    ae_by_id: dict = {r['staff_id']: r for r in ae_rows}
    cs_by_id: dict = {r['staff_id']: r for r in cs_rows}
    oiw_by_id: dict = {r['staff_id']: int(r['cnt'] or 0) for r in oiw_rows}

    # ── сборка agents[] ───────────────────────────────────────────────────
    agents = []
    for s in staff_rows:
        sid = s['id']
        full_name = f"{(s['last_name'] or '').strip()} {(s['first_name'] or '').strip()}".strip()
        name_key = ((s['last_name'] or '') + ' ' + (s['first_name'] or '')).strip()

        last_active = None
        actions_today = 0
        actions_yesterday = 0

        m1 = by_id.get(sid)
        if m1:
            last_active = m1['last_active']
            actions_today += m1['actions_today']
            actions_yesterday += m1['actions_yesterday']

        m2 = by_name.get(name_key)
        if m2:
            if m2['last_active'] and (last_active is None or m2['last_active'] > last_active):
                last_active = m2['last_active']
            actions_today += m2['actions_today']
            actions_yesterday += m2['actions_yesterday']

        # attendance
        ae = ae_by_id.get(sid)
        checked_in_today = bool(ae and (ae['in_today'] or 0) > 0)
        checked_out_today = bool(ae and (ae['out_today'] or 0) > 0)
        checked_in_time = None
        if ae and ae['first_in_today']:
            t = ae['first_in_today']
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
            checked_in_time = t.astimezone(_TASHKENT).strftime('%H:%M')
        if ae and ae['last_active']:
            if last_active is None or ae['last_active'] > last_active:
                last_active = ae['last_active']

        # cash_shifts
        cs = cs_by_id.get(sid)
        shift_open_today = bool(cs and (cs['shift_open_today'] or 0) > 0)
        if cs and cs['last_active']:
            if last_active is None or cs['last_active'] > last_active:
                last_active = cs['last_active']

        # orders in work
        orders_in_work = oiw_by_id.get(sid, 0)

        # presence_status
        secs = _secs_ago(last_active)
        if secs <= 300:
            presence_status = 'online'
        elif checked_in_today and checked_out_today:
            presence_status = 'left'
        elif checked_in_today:
            presence_status = 'today'
        else:
            presence_status = 'offline'

        agents.append({
            'id': sid,
            'name': full_name,
            'role': s['role'],
            'branch': s['branch'],
            'last_active': last_active.isoformat() if last_active else None,
            'actions_today': int(actions_today),
            'actions_yesterday': int(actions_yesterday),
            'checked_in_today': checked_in_today,
            'checked_in_time': checked_in_time,
            'checked_out_today': checked_out_today,
            'shift_open_today': shift_open_today,
            'orders_in_work': orders_in_work,
            'presence_status': presence_status,
        })

    order_status_breakdown = {
        'in_progress': int(status_row['in_progress']) if status_row else 0,
        'delivered_today': int(status_row['delivered_today']) if status_row else 0,
        'cancelled_today': int(status_row['cancelled_today']) if status_row else 0,
    }

    trend_map: dict = {}
    for r in trend_oa_rows:
        trend_map[r['day']] = trend_map.get(r['day'], 0) + (r['cnt'] or 0)
    for r in trend_lc_rows:
        trend_map[r['day']] = trend_map.get(r['day'], 0) + (r['cnt'] or 0)

    activity_trend_7d = []
    for i in range(7):
        d = week_start_tk + timedelta(days=i)
        activity_trend_7d.append({
            'date': str(d),
            'actions': int(trend_map.get(d, 0)),
        })

    return {
        'agents': agents,
        'order_status_breakdown': order_status_breakdown,
        'activity_trend_7d': activity_trend_7d,
    }

async def set_opening_balance(staff_id: int, year: int, month: int,
                               target: float, created_by: int) -> dict:
    """Устанавливает остаток на начало месяца через корректирующую запись в предыдущем периоде."""
    if not pool: return {}
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        current = float(await conn.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM salary_ledger WHERE staff_id=$1 AND period < $2",
            staff_id, period) or 0)
        diff = round(target - current, 2)
        if diff == 0:
            return {'ok': True, 'diff': 0}
        # Корректировка записывается в предыдущий период
        prev_month = month - 1 if month > 1 else 12
        prev_year  = year if month > 1 else year - 1
        prev_period = _date(prev_year, prev_month, 1)
        row = await conn.fetchrow("""
            INSERT INTO salary_ledger (staff_id, period, type, amount, note, created_by)
            VALUES ($1,$2,'adjustment',$3,'Ручная корректировка входящего остатка',$4)
            RETURNING id
        """, staff_id, prev_period, diff, created_by)
        return {'ok': True, 'diff': diff, 'entry_id': row['id'] if row else None}

async def delete_month_accruals(staff_id: int, year: int, month: int) -> int:
    """Удаляет все записи типа 'accrual' за указанный период для сотрудника."""
    if not pool: return 0
    from datetime import date as _date
    period = _date(year, month, 1)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM salary_ledger WHERE staff_id=$1 AND period=$2 AND type='accrual'",
            staff_id, period)
        return int(result.split()[-1])

async def update_salary_ledger_entry(entry_id: int, amount: float, note: str) -> dict:
    if not pool: return {}
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE salary_ledger SET amount=$2, note=$3 WHERE id=$1 RETURNING *
        """, entry_id, amount, note)
        return dict(row) if row else {}

async def delete_salary_ledger_entry(entry_id: int) -> bool:
    if not pool: return False
    async with pool.acquire() as conn:
        r = await conn.execute("DELETE FROM salary_ledger WHERE id=$1", entry_id)
        return r == "DELETE 1"

async def auto_accrue_monthly_salaries() -> int:
    """Начисляет оклады 1-го числа месяца. Пропускает если уже есть accrual за период."""
    if not pool: return 0
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    async with pool.acquire() as conn:
        staff_rows = await conn.fetch("""
            SELECT id, salary_type, salary_rate
            FROM staff
            WHERE (active = TRUE OR active IS NULL)
              AND role <> 'agent'
              AND salary_type IS NOT NULL AND salary_rate > 0
        """)
        count = 0
        for s in staff_rows:
            exists = await conn.fetchval(
                "SELECT 1 FROM salary_ledger WHERE staff_id=$1 AND period=$2 AND type='accrual'",
                s['id'], period)
            if exists:
                continue
            await conn.execute("""
                INSERT INTO salary_ledger (staff_id, period, type, amount, note)
                VALUES ($1, $2, 'accrual', $3, 'Автоначисление оклада')
            """, s['id'], period, float(s['salary_rate'] or 0))
            count += 1
        return count

async def _create_ledger_for_expense(conn, expense: dict) -> None:
    """Создаёт запись salary_ledger при утверждении расхода на сотрудника."""
    if not expense.get('for_staff_id'):
        return
    cat_name = (expense.get('category_name_ru') or '').lower()
    if 'аванс' in cat_name:
        ltype = 'advance'
    elif 'зарплат' in cat_name:
        ltype = 'salary_payment'
    elif 'бонус' in cat_name or 'премия' in cat_name:
        ltype = 'bonus'
    else:
        ltype = 'deduction'
    from datetime import date as _date
    today = _date.today()
    period = _date(today.year, today.month, 1)
    await conn.execute("""
        INSERT INTO salary_ledger (staff_id, period, type, amount, note, expense_id, created_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
    """, expense['for_staff_id'], period, ltype, -float(expense['amount']),
        expense.get('description') or '', expense.get('id'), expense.get('admin_id') or expense.get('manager_id'))

async def ensure_sms_dispatch_table():
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_dispatches (
                id           SERIAL PRIMARY KEY,
                name         TEXT NOT NULL DEFAULT 'Рассылка',
                message      TEXT NOT NULL,
                from_nick    TEXT NOT NULL DEFAULT 'ARTEZ',
                phones       JSONB NOT NULL,
                scheduled_at TIMESTAMPTZ NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                sent_at      TIMESTAMPTZ,
                sent_count   INT DEFAULT 0,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)

async def create_sms_dispatch(name: str, message: str, from_nick: str,
                               phones: list, scheduled_at) -> int:
    if not pool: return 0
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO sms_dispatches (name, message, from_nick, phones, scheduled_at)
            VALUES ($1, $2, $3, $4, $5) RETURNING id
        """, name, message, from_nick, _j.dumps(phones), scheduled_at)
        return row["id"] if row else 0

async def get_pending_sms_dispatches() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM sms_dispatches
            WHERE status='pending' AND scheduled_at <= NOW()
            ORDER BY scheduled_at ASC LIMIT 20
        """)
        return [dict(r) for r in rows]

async def mark_sms_dispatch_sent(dispatch_id: int, sent_count: int):
    if not pool: return
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE sms_dispatches SET status='sent', sent_at=NOW(), sent_count=$2
            WHERE id=$1
        """, dispatch_id, sent_count)

_SMS_OPERATOR_DEFAULTS = [
    {"operator": "beeline",   "display_name": "Beeline",   "prefixes": [90, 91, 92],     "price_service": 115, "price_ad": 300},
    {"operator": "uzmobile",  "display_name": "Uzmobile",  "prefixes": [99, 77, 70, 95], "price_service": 145, "price_ad": 350},
    {"operator": "mobiuz",    "display_name": "MobiUz",    "prefixes": [97, 88, 87],     "price_service": 110, "price_ad": 290},
    {"operator": "ucell",     "display_name": "Ucell",     "prefixes": [93, 94, 50],     "price_service": 160, "price_ad": 340},
    {"operator": "humans",    "display_name": "Humans",    "prefixes": [33],              "price_service": 95,  "price_ad": 95},
    {"operator": "oq",        "display_name": "OQ",        "prefixes": [20],              "price_service": 0,   "price_ad": 0},
    {"operator": "perfectum", "display_name": "Perfectum", "prefixes": [98, 80],         "price_service": 95,  "price_ad": 95},
]

async def ensure_sms_operator_prices():
    if not pool: return
    import json as _j
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sms_operator_prices (
                id           SERIAL PRIMARY KEY,
                operator     TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                prefixes     JSONB NOT NULL DEFAULT '[]',
                price_service INT NOT NULL DEFAULT 0,
                price_ad      INT NOT NULL DEFAULT 0,
                updated_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Вставить дефолты если таблица пуста
        count = await conn.fetchval("SELECT COUNT(*) FROM sms_operator_prices")
        if count == 0:
            for op in _SMS_OPERATOR_DEFAULTS:
                await conn.execute("""
                    INSERT INTO sms_operator_prices (operator, display_name, prefixes, price_service, price_ad)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
                """, op["operator"], op["display_name"], _j.dumps(op["prefixes"]),
                    op["price_service"], op["price_ad"])

async def get_sms_operator_prices() -> list:
    if not pool: return []
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM sms_operator_prices ORDER BY id")
        return [dict(r) for r in rows]

async def update_sms_operator_price(op_id: int, display_name: str, prefixes: list,
                                     price_service: int, price_ad: int) -> dict:
    if not pool: return {}
    import json as _j
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE sms_operator_prices
            SET display_name=$2, prefixes=$3, price_service=$4, price_ad=$5, updated_at=NOW()
            WHERE id=$1 RETURNING *
        """, op_id, display_name, _j.dumps(prefixes), price_service, price_ad)
        return dict(row) if row else {}

def _sms_date_range(start_date: str, end_date: str):
    from datetime import datetime, timezone, timedelta
    tz5 = timezone(timedelta(hours=5))
    s = datetime.fromisoformat(start_date).replace(hour=0,  minute=0,  second=0,  tzinfo=tz5)
    e = datetime.fromisoformat(end_date).replace(  hour=23, minute=59, second=59, tzinfo=tz5)
    return s, e

async def get_sms_stats_by_month(start_date: str, end_date: str) -> list:
    if not pool: return []
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                TO_CHAR(COALESCE(sent_at, scheduled_at) AT TIME ZONE 'Asia/Tashkent', 'YYYY-MM') AS month,
                COUNT(*)::int AS dispatches,
                COALESCE(SUM(sent_count),0)::int AS sent_sms,
                COALESCE(SUM(jsonb_array_length(phones)),0)::int AS total_phones
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
            GROUP BY 1 ORDER BY 1 DESC
        """, s, e)
        return [dict(r) for r in rows]

async def get_sms_stats_by_date(start_date: str, end_date: str) -> list:
    if not pool: return []
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT
                TO_CHAR(COALESCE(sent_at, scheduled_at) AT TIME ZONE 'Asia/Tashkent', 'YYYY-MM-DD') AS date,
                COUNT(*)::int AS dispatches,
                COALESCE(SUM(sent_count),0)::int AS sent_sms,
                COALESCE(SUM(jsonb_array_length(phones)),0)::int AS total_phones
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
            GROUP BY 1 ORDER BY 1 DESC
        """, s, e)
        return [dict(r) for r in rows]

async def get_sms_dispatches_report(start_date: str, end_date: str) -> list:
    if not pool: return []
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, from_nick,
                   LEFT(message, 60) AS message_preview,
                   jsonb_array_length(phones)::int AS total_phones,
                   COALESCE(sent_count,0)::int AS sent_count, status,
                   TO_CHAR(scheduled_at AT TIME ZONE 'Asia/Tashkent', 'DD.MM.YYYY HH24:MI') AS scheduled,
                   TO_CHAR(sent_at     AT TIME ZONE 'Asia/Tashkent', 'DD.MM.YYYY HH24:MI') AS sent_at
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
            ORDER BY id DESC LIMIT 200
        """, s, e)
        return [dict(r) for r in rows]

async def get_sms_dispatches_for_export(start_date: str, end_date: str) -> list:
    if not pool: return []
    s, e = _sms_date_range(start_date, end_date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, from_nick, message,
                   jsonb_array_length(phones)::int AS total_phones,
                   COALESCE(sent_count,0)::int AS sent_count, status,
                   scheduled_at AT TIME ZONE 'Asia/Tashkent' AS scheduled_at,
                   sent_at      AT TIME ZONE 'Asia/Tashkent' AS sent_at,
                   created_at   AT TIME ZONE 'Asia/Tashkent' AS created_at
            FROM sms_dispatches
            WHERE COALESCE(sent_at, scheduled_at) BETWEEN $1 AND $2
            ORDER BY id DESC
        """, s, e)
        return [dict(r) for r in rows]
