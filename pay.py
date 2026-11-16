"""Онлайн-оплата заказов картой (PLUM, Uzcard/Humo) и заявки на рассрочку (SmartBill) — Э3, 2026-11-15.

Решения владельца (память project_mobile_app_pin_plum_plan_2026-11-15):
- деньги — ООО ARTEZ, ключи PLUM общие с ZarCity, посредник на artez.uz, префикс userId/extraId «art-»;
- оплата остатка — после замера (push «К оплате X сум»), до доставки (и долг после доставки);
- предоплата — по желанию клиента, фиксированная сумма (pay_prepay_amount, по умолчанию 100 000) —
  на заявке с сайта/приложения (leads) или на заказе до замера; заявка → заказ: предоплата сама
  переходит в оплаты заказа (attach_lead_prepays);
- итог меньше предоплаты — возврат вручную (кнопка «Вернули вручную» в админке); полный возврат
  в тот же день — paymentReverse («Вернуть на карту»);
- режим pay_mode: off | testers | all. Тестировщики (pay_testers) платят подставным банком (plum_fake),
  настоящие деньги не списываются; их платежи помечены is_test и идут способом 'online_test'.

Железные правила (C:\\Projects\\_shared\\plum.md):
- номер карты / срок / код не сохраняем и не пишем в логи — только маска;
- строка pay_tx (status=pending) КОММИТИТСЯ до запроса в PLUM, который может списать деньги;
- одна незавершённая оплата на заказ/заявку (частичные уникальные индексы + блокировка строки заказа);
- смена статусов — только compare-and-set (UPDATE … WHERE status IN (…));
- неясный исход (PlumPending) оставляет pending — решает сверка по getTransactionByExt (воркер раз в минуту).
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from jose import JWTError, jwt

import database as db
import plum_client as plum

log = logging.getLogger("pay")
router = APIRouter()

OWNER_CHAT = 624826036          # отчёты/уведомления на время тестов — только владельцу
PAY_LIMIT_PER_HOUR = 10
CARD_BIND_LIMIT_PER_HOUR = 5
STALE_PENDING = timedelta(minutes=30)
OTP_WINDOW = timedelta(minutes=20)
REVIEW_AFTER = timedelta(hours=24)
REVIEW_GIVE_UP = timedelta(days=7)
CARD_BRANDS = {"8600": "uzcard", "5614": "uzcard", "9860": "humo"}
PREPAY_STATUSES = ("new", "confirmed", "pickup", "received")
BALANCE_OPEN_STATUSES = ("received", "washing", "drying", "packing", "ready", "delivery", "delivered")
MEASURED_OK = ("approved", "confirmed")
DEFAULTS = {"pay_mode": "testers", "pay_prepay_amount": "100000", "pay_otpless_limit": "500000",
            "pay_prepay_min": "50000", "pay_prepay_max": "1000000",
            "pay_notify": "owner"}

MSG: dict[str, tuple[str, str]] = {
    "payments_disabled": ("Онлайн-оплата пока недоступна", "Onlayn to'lov hozircha mavjud emas"),
    "not_found": ("Не найдено", "Topilmadi"),
    "nothing_to_pay": ("Сейчас нечего оплачивать", "Hozircha to'lanadigan summa yo'q"),
    "not_measured": ("Сумма к оплате появится после замера", "To'lov summasi o'lchovdan keyin chiqadi"),
    "payment_in_progress": ("Оплата уже идёт — дождитесь результата", "To'lov jarayonda — natijani kuting"),
    "prepay_unavailable": ("Предоплата для этого заказа недоступна", "Bu buyurtma uchun oldindan to'lov mavjud emas"),
    "card_invalid": ("Неверный номер карты", "Karta raqami noto'g'ri"),
    "card_not_supported": ("Принимаем только Uzcard и Humo", "Faqat Uzcard va Humo qabul qilinadi"),
    "card_expire_invalid": ("Неверный срок карты (ММ/ГГ)", "Karta muddati noto'g'ri (OO/YY)"),
    "card_not_found": ("Карта не найдена — добавьте её заново", "Karta topilmadi — qaytadan qo'shing"),
    "payment_not_waiting": ("Код уже не нужен — обновите экран", "Kod endi kerak emas — sahifani yangilang"),
    "too_many": ("Слишком часто — попробуйте позже", "Juda tez-tez — keyinroq urinib ko'ring"),
    "resend_wait": ("Новый код можно запросить через 2 минуты", "Yangi kodni 2 daqiqadan keyin so'rash mumkin"),
    "prepay_amount_invalid": ("Сумма предоплаты вне допустимых пределов", "Oldindan to'lov summasi ruxsat etilgan chegaradan tashqarida"),
    "installment_exists": ("Заявка на рассрочку уже отправлена", "Muddatli to'lov arizasi allaqachon yuborilgan"),
    "installment_closed": ("Для этого заказа рассрочка недоступна", "Bu buyurtma uchun muddatli to'lov mavjud emas"),
    "plum_otp_invalid": ("Неверный код из SMS", "SMS kodi noto'g'ri"),
    "plum_otp_expired": ("Код устарел — запросите новый", "Kod eskirgan — yangisini so'rang"),
    "plum_otp_limit": ("Слишком много попыток ввода кода", "Kod kiritishga urinishlar juda ko'p"),
    "plum_no_money": ("Недостаточно средств на карте", "Kartada mablag' yetarli emas"),
    "plum_card_invalid": ("Банк не нашёл карту — проверьте номер", "Bank kartani topmadi — raqamni tekshiring"),
    "plum_card_expired": ("Срок карты истёк или указан неверно", "Karta muddati tugagan yoki noto'g'ri"),
    "plum_card_blocked": ("Карта заблокирована", "Karta bloklangan"),
    "plum_no_sms": ("К карте не подключено SMS-информирование", "Kartaga SMS-xabarnoma ulanmagan"),
    "plum_corporate": ("Корпоративные карты не принимаются", "Korporativ kartalar qabul qilinmaydi"),
    "plum_limit": ("Превышен лимит по карте", "Karta limiti oshib ketdi"),
    "plum_min_amount": ("Сумма слишком мала для оплаты картой", "Karta bilan to'lash uchun summa juda kichik"),
    "plum_card_exists": ("Эта карта уже сохранена", "Bu karta allaqachon saqlangan"),
    "plum_card_other": ("Карта привязана к другому пользователю", "Karta boshqa foydalanuvchiga bog'langan"),
    "plum_unavailable": ("Банк временно недоступен — попробуйте позже", "Bank vaqtincha ishlamayapti — keyinroq urinib ko'ring"),
    "plum_failed": ("Платёж не прошёл", "To'lov o'tmadi"),
    "plum_disabled": ("Онлайн-оплата пока недоступна", "Onlayn to'lov hozircha mavjud emas"),
    "plum_not_found": ("Платёж не найден в банке", "To'lov bankda topilmadi"),
}


class PayError(Exception):
    def __init__(self, code: str, status: int = 400):
        super().__init__(code)
        self.code, self.status = code, status


def _http(e: PayError) -> HTTPException:
    ru, uz = MSG.get(e.code, MSG["plum_failed"])
    return HTTPException(status_code=e.status, detail={"code": e.code, "message": f"{ru} / {uz}", "ru": ru, "uz": uz})


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fmt(v) -> str:
    return f"{int(v or 0):,}".replace(",", " ")


# ══════════════════════════════════════
#  Таблицы (аддитивно, при старте API)
# ══════════════════════════════════════
SCHEMA = """
CREATE TABLE IF NOT EXISTS pay_testers (
    id          SERIAL PRIMARY KEY,
    phone       VARCHAR(20) UNIQUE NOT NULL,
    name        VARCHAR(120) DEFAULT '',
    real_card   BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS pay_cards (
    id              SERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    plum_user_id    VARCHAR(40) NOT NULL,
    plum_card_id    BIGINT NOT NULL,
    plum_binding_id BIGINT,
    mask            VARCHAR(32) DEFAULT '****',
    pc_type         SMALLINT DEFAULT 0,
    owner           VARCHAR(120),
    expire          VARCHAR(8),
    is_test         BOOLEAN DEFAULT FALSE,
    used_count      INTEGER DEFAULT 0,
    last_otpless_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    deleted_at      TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_cards ON pay_cards(user_id, is_test, plum_card_id);
CREATE TABLE IF NOT EXISTS pay_card_binds (
    session     BIGINT PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    is_test     BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS pay_tx (
    id               SERIAL PRIMARY KEY,
    kind             VARCHAR(10) NOT NULL DEFAULT 'pay',
    purpose          VARCHAR(12) NOT NULL DEFAULT 'balance',
    order_id         INTEGER,
    lead_id          INTEGER,
    user_id          INTEGER,
    phone            VARCHAR(20),
    amount           INTEGER NOT NULL,
    status           VARCHAR(12) NOT NULL DEFAULT 'pending',
    card_id          INTEGER,
    card_mask        VARCHAR(32),
    otpless          BOOLEAN DEFAULT FALSE,
    is_test          BOOLEAN DEFAULT FALSE,
    extra_id         VARCHAR(40) UNIQUE,
    plum_tx_id       BIGINT,
    plum_session     BIGINT,
    otp_phone        VARCHAR(40),
    otp_sent_at      TIMESTAMPTZ,
    error_code       VARCHAR(40),
    lang             VARCHAR(5) DEFAULT 'ru',
    refund_of        INTEGER,
    order_payment_id INTEGER,
    note             TEXT,
    done_by          VARCHAR(120),
    checked_at       TIMESTAMPTZ,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pay_tx_order ON pay_tx(order_id);
CREATE INDEX IF NOT EXISTS idx_pay_tx_lead ON pay_tx(lead_id);
CREATE INDEX IF NOT EXISTS idx_pay_tx_status ON pay_tx(status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_tx_open_order ON pay_tx(order_id)
    WHERE kind='pay' AND status IN ('pending','otp') AND order_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_tx_open_lead ON pay_tx(lead_id)
    WHERE kind='pay' AND status IN ('pending','otp') AND lead_id IS NOT NULL AND order_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_tx_auto_refund ON pay_tx(refund_of)
    WHERE kind='refund' AND status IN ('pending','paid');
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_tx_order_payment ON pay_tx(order_payment_id)
    WHERE order_payment_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS pay_installments (
    id           SERIAL PRIMARY KEY,
    order_id     INTEGER NOT NULL,
    user_id      INTEGER,
    phone        VARCHAR(20),
    client_name  VARCHAR(200),
    amount       INTEGER,
    status       VARCHAR(12) NOT NULL DEFAULT 'new',
    client_note  TEXT,
    manager_note TEXT,
    handled_by   VARCHAR(120),
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_pay_installments_open ON pay_installments(order_id)
    WHERE status IN ('new','in_work');
CREATE TABLE IF NOT EXISTS pay_due_push (
    order_id  INTEGER PRIMARY KEY,
    amount    INTEGER NOT NULL,
    sent_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS pay_log (
    id         SERIAL PRIMARY KEY,
    actor      VARCHAR(120),
    action     VARCHAR(40),
    details    TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE staff ADD COLUMN IF NOT EXISTS can_refund_prepay BOOLEAN DEFAULT FALSE;
CREATE TABLE IF NOT EXISTS pay_fake_state (
    id         INTEGER PRIMARY KEY,
    state      JSONB,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""


async def init() -> None:
    if not db.pool:
        return
    async with db.pool.acquire() as c:
        await c.execute(SCHEMA)


# ══════════════════════════════════════
#  Настройки, тестировщики, режим банка
# ══════════════════════════════════════
async def setting(key: str) -> str:
    v = await db.get_config(key)
    return v if v not in (None, "") else DEFAULTS[key]


async def prepay_amount() -> int:
    try:
        return max(1000, int(float(await setting("pay_prepay_amount"))))
    except ValueError:
        return int(DEFAULTS["pay_prepay_amount"])


async def prepay_limits() -> tuple[int, int, int]:
    """(мин, макс, по умолчанию) предоплаты — клиент выбирает сумму сам в этих пределах (16.11, просьба владельца)."""
    def num(v, d):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return int(d)
    lo = max(1000, num(await setting("pay_prepay_min"), DEFAULTS["pay_prepay_min"]))
    hi = max(lo, num(await setting("pay_prepay_max"), DEFAULTS["pay_prepay_max"]))
    return lo, hi, min(max(await prepay_amount(), lo), hi)


async def prepay_pick(requested) -> int:
    """Сумма предоплаты клиента: пусто — по умолчанию; иначе в пределах, кратно 1000 (округляем вниз)."""
    lo, hi, default = await prepay_limits()
    if requested in (None, "", 0, "0"):
        return default
    try:
        v = int(float(str(requested).replace(" ", "")))
    except ValueError:
        raise PayError("prepay_amount_invalid", 422)
    v -= v % 1000
    if v < lo or v > hi:
        raise PayError("prepay_amount_invalid", 422)
    return v


async def tester(phone: str) -> dict | None:
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM pay_testers WHERE phone=$1", phone)
    return dict(r) if r else None


async def bank_for(user) -> str | None:
    """'test' — подставной банк, 'real' — настоящий PLUM, None — оплата этому человеку недоступна."""
    mode = await setting("pay_mode")
    if mode == "off":
        return None
    t = await tester(user["phone"])
    if t:
        if t["real_card"]:
            return "real" if plum.real_configured() else None
        return "test"
    if mode == "all" and plum.real_configured():
        return "real"
    return None


def plum_user(user_id: int) -> str:
    return f"{plum.prefix()}u{user_id}"


def luhn_ok(n: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(n)):
        d = int(ch)
        if i % 2:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def parse_card(number: str, expire: str) -> tuple[str, str]:
    """Номер — 16 цифр Uzcard/Humo; срок MM/YY → формат PLUM ГГММ. Ничего не логирует."""
    n = re.sub(r"\D", "", str(number or ""))
    if len(n) != 16 or n[:4] not in CARD_BRANDS:
        raise PayError("card_not_supported" if len(n) == 16 else "card_invalid", 422)
    if not luhn_ok(n):
        raise PayError("card_invalid", 422)
    m = re.fullmatch(r"\s*(\d{2})\s*/?\s*(\d{2})\s*", str(expire or ""))
    if not m or not 1 <= int(m.group(1)) <= 12:
        raise PayError("card_expire_invalid", 422)
    return n, m.group(2) + m.group(1)


async def _log(actor: str, action: str, details: str) -> None:
    try:
        async with db.pool.acquire() as c:
            await c.execute("INSERT INTO pay_log (actor, action, details) VALUES ($1,$2,$3)", actor, action, details)
    except Exception as e:  # журнал не должен ломать оплату
        log.warning("pay_log: %s", type(e).__name__)


async def notify(text: str) -> None:
    """Уведомление сотрудникам о платежах/рассрочке. На время тестов (pay_notify=owner) — только владельцу."""
    import main
    target = OWNER_CHAT if await setting("pay_notify") != "group" else (main.LEADS_GROUP_ID or OWNER_CHAT)
    try:
        await main.send_tg(target, text)
    except Exception as e:
        log.warning("pay notify: %s", type(e).__name__)


# ══════════════════════════════════════
#  Сумма к оплате
# ══════════════════════════════════════
async def _attach_lead_prepays(order_num: str | None = None) -> int:
    """Предоплата, внесённая на заявке: заявку превратили в заказ (leads.converted_order) →
    добавляем её в оплаты заказа (order_payments). Одна транзакция, compare-and-set по order_payment_id."""
    q = """SELECT t.id, o.id AS oid FROM pay_tx t
             JOIN leads l ON l.id = t.lead_id
             JOIN orders o ON o.order_num = l.converted_order
            WHERE t.kind='pay' AND t.status='paid' AND t.order_id IS NULL AND l.status='converted'"""
    args = []
    if order_num:
        q += " AND o.order_num=$1"
        args.append(order_num)
    async with db.pool.acquire() as c:
        rows = await c.fetch(q + " LIMIT 50", *args)
    n = 0
    for r in rows:
        async with db.pool.acquire() as c:
            async with c.transaction():
                t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1 AND order_id IS NULL AND status='paid' FOR UPDATE", r["id"])
                if not t:
                    continue
                await c.execute("SELECT id FROM orders WHERE id=$1 FOR UPDATE", r["oid"])
                await c.execute("UPDATE pay_tx SET order_id=$2, updated_at=NOW() WHERE id=$1", t["id"], r["oid"])
                await _insert_order_payment(c, dict(t) | {"order_id": r["oid"]})
                n += 1
    return n


async def money(order: dict) -> dict:
    """Итог заказа как в карточке (минимумы — db.get_order_items_billing), минус скидки,
    округление вниз до 1000 (как _orderNet в staff/admin), минус внесённые оплаты (кроме отклонённых)."""
    billing = await db.get_order_items_billing(order["id"])
    items = billing.get("items") or []
    base = float(billing.get("order_total") or 0) if items else float(order.get("total_price") or 0)
    disc = sum(float(order.get(k) or 0) for k in ("discount_sum", "delivery_discount", "manual_discount", "client_discount"))
    net = max(0.0, base - disc)
    net -= net % 1000
    async with db.pool.acquire() as c:
        paid = float(await c.fetchval(
            "SELECT COALESCE(SUM(amount),0) FROM order_payments WHERE order_id=$1 "
            "AND NOT (confirmed=FALSE AND confirmed_at IS NOT NULL)", order["id"]) or 0)
        prepaid_online = await c.fetchval(
            "SELECT COUNT(*) FROM pay_tx WHERE order_id=$1 AND kind='pay' AND purpose='prepay' AND status IN ('paid','refunded')",
            order["id"])
        open_tx = await c.fetchrow(
            "SELECT id, status, purpose, amount FROM pay_tx WHERE order_id=$1 AND kind='pay' AND status IN ('pending','otp') LIMIT 1",
            order["id"])
    measured = bool(items) and all((it.get("measure_status") or "") in MEASURED_OK for it in items)
    due = max(0, int(net - paid))
    return {"net": int(net), "paid": int(paid), "due": due, "measured": measured, "items": len(items),
            "prepaid_online": bool(prepaid_online), "open_tx": dict(open_tx) if open_tx else None}


async def order_pay_info(order: dict, user) -> dict:
    await _attach_lead_prepays(order["order_num"])
    m = await money(order)
    bank = await bank_for(user)
    st = order.get("status") or ""
    can_balance = (st in BALANCE_OPEN_STATUSES and m["due"] >= 1000
                   and (m["measured"] or st in ("ready", "delivery", "delivered")))
    pre = await prepay_amount()
    can_prepay = (st in PREPAY_STATUSES and not m["measured"] and m["paid"] == 0 and not m["prepaid_online"])
    async with db.pool.acquire() as c:
        inst = await c.fetchrow("SELECT id, status, created_at FROM pay_installments WHERE order_id=$1 "
                                "ORDER BY id DESC LIMIT 1", order["id"])
    return {
        "ok": True, "order_num": order["order_num"], "status": st,
        "enabled": bank is not None, "test_bank": bank == "test",
        "net": m["net"], "paid": m["paid"], "due": m["due"], "measured": m["measured"],
        "can_pay": bool(bank) and can_balance, "can_prepay": bool(bank) and can_prepay, "prepay_amount": pre,
        "prepay_min": (await prepay_limits())[0], "prepay_max": (await prepay_limits())[1],
        "open_tx": m["open_tx"],
        "installment": {"id": inst["id"], "status": inst["status"]} if inst else None,
        "can_installment": st not in ("cancelled",) and not (inst and inst["status"] in ("new", "in_work"))
                           and (m["due"] >= 1000 or not m["measured"]),
    }


# ══════════════════════════════════════
#  Оплата: начало, код, повтор кода
# ══════════════════════════════════════
async def _rate(user_id: int) -> None:
    async with db.pool.acquire() as c:
        n = await c.fetchval("SELECT COUNT(*) FROM pay_tx WHERE user_id=$1 AND kind='pay' "
                             "AND created_at > NOW() - INTERVAL '1 hour'", user_id)
    if n >= PAY_LIMIT_PER_HOUR:
        raise PayError("too_many", 429)


async def _card_of(user, card_id: int, is_test: bool) -> dict:
    async with db.pool.acquire() as c:
        k = await c.fetchrow("SELECT * FROM pay_cards WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL AND is_test=$3",
                             card_id, user["id"], is_test)
    if not k:
        raise PayError("card_not_found", 404)
    return dict(k)


async def start(user, *, order: dict | None = None, lead: dict | None = None, purpose: str,
                card: dict | None = None, card_id: int | None = None, lang: str = "ru", amount_req=None) -> dict:
    bank = await bank_for(user)
    if not bank:
        raise PayError("payments_disabled", 403)
    with plum.test_mode(bank == "test"):
        return await _start(user, order=order, lead=lead, purpose=purpose, card=card, card_id=card_id,
                            lang=lang, is_test=bank == "test", amount_req=amount_req)


async def _start(user, *, order, lead, purpose, card, card_id, lang, is_test, amount_req=None) -> dict:
    await _rate(user["id"])
    saved = None
    number = expire = None
    if card_id:
        saved = await _card_of(user, int(card_id), is_test)
    else:
        number, expire = parse_card((card or {}).get("number"), (card or {}).get("expire"))
    lang = "uz" if str(lang or "").startswith("uz") else "ru"

    async with db.pool.acquire() as c:
        async with c.transaction():
            # блокируем заказ/заявку: две оплаты одновременно невозможны (плюс частичный уникальный индекс)
            if order:
                await c.execute("SELECT id FROM orders WHERE id=$1 FOR UPDATE", order["id"])
                old = await c.fetchrow("SELECT * FROM pay_tx WHERE order_id=$1 AND kind='pay' "
                                       "AND status IN ('pending','otp') FOR UPDATE", order["id"])
            else:
                await c.execute("SELECT id FROM leads WHERE id=$1 FOR UPDATE", lead["id"])
                old = await c.fetchrow("SELECT * FROM pay_tx WHERE lead_id=$1 AND order_id IS NULL AND kind='pay' "
                                       "AND status IN ('pending','otp') FOR UPDATE", lead["id"])
            if old and old["status"] == "pending":
                raise PayError("payment_in_progress", 409)
            if old:  # ждали код по прошлой попытке — она брошена (без кода деньги не списаны)
                await c.execute("UPDATE pay_tx SET status='cancelled', error_code='abandoned', updated_at=NOW() "
                                "WHERE id=$1 AND status='otp'", old["id"])
            # сумму считаем ПОД блокировкой
            if order:
                fresh = dict(await c.fetchrow("SELECT * FROM orders WHERE id=$1", order["id"]))
    if order:
        info = await order_pay_info(fresh, user)
        if purpose == "prepay":
            if not info["can_prepay"]:
                raise PayError("prepay_unavailable", 409)
            amount = await prepay_pick(amount_req)
        else:
            if not info["can_pay"]:
                raise PayError("nothing_to_pay" if info["measured"] or info["due"] < 1000 else "not_measured", 409)
            amount = info["due"]
    else:
        amount = await prepay_pick(amount_req)
        async with db.pool.acquire() as c:
            done = await c.fetchval("SELECT COUNT(*) FROM pay_tx WHERE lead_id=$1 AND kind='pay' AND purpose='prepay' "
                                    "AND status IN ('paid','refunded','review')", lead["id"])
        if done:
            raise PayError("prepay_unavailable", 409)

    otpless = False
    if saved:
        fresh_card = (saved["used_count"] or 0) == 0 and saved["created_at"] and _now() - saved["created_at"] < timedelta(minutes=10)
        limit = int(float(await setting("pay_otpless_limit")))
        recent = saved["last_otpless_at"] and _now() - saved["last_otpless_at"] < timedelta(hours=24)
        otpless = bool(fresh_card or (amount <= limit and not recent))
    mask = saved["mask"] if saved else plum.mask(number or "")
    try:
        async with db.pool.acquire() as c:
            async with c.transaction():
                tx = await c.fetchrow(
                    """INSERT INTO pay_tx (kind, purpose, order_id, lead_id, user_id, phone, amount, status, card_id,
                                           card_mask, otpless, is_test, lang)
                       VALUES ('pay',$1,$2,$3,$4,$5,$6,'pending',$7,$8,$9,$10,$11) RETURNING *""",
                    purpose, order["id"] if order else None, None if order else lead["id"], user["id"], user["phone"],
                    amount, saved["id"] if saved else None, mask, otpless, is_test, lang)
                extra = f"{plum.prefix()}pay-{tx['id']}"
                await c.execute("UPDATE pay_tx SET extra_id=$2 WHERE id=$1", tx["id"], extra)
        # ← запись закоммичена ДО запроса, который может списать деньги
    except Exception as e:
        if "uq_pay_tx_open" in str(e):
            raise PayError("payment_in_progress", 409) from None
        raise
    tx = dict(tx) | {"extra_id": extra}
    data = f"order:{order['order_num']}" if order else f"lead:{lead.get('lead_code') or lead['id']}"
    try:
        if saved:
            res = await plum.pay_card(saved["plum_user_id"], saved["plum_card_id"], amount, extra,
                                      send_otp=not otpless, data=data, lang=lang)
        else:
            res = await plum.pay_new(amount, number, expire, extra, data, lang=lang)
    except plum.PlumPending:
        return await tx_public(tx["id"])
    except plum.PlumError as e:
        code = e.code
        async with db.pool.acquire() as c:
            await c.execute("UPDATE pay_tx SET status='failed', error_code=$2, updated_at=NOW() WHERE id=$1 AND status='pending'",
                            tx["id"], code)
            if saved and e.plum_code == -114:  # банк не знает привязку — убираем карту, человек добавит заново
                await c.execute("UPDATE pay_cards SET deleted_at=NOW() WHERE id=$1", saved["id"])
                code = "card_not_found"
        raise PayError(code, 402) from None
    finally:
        number = expire = None  # noqa: F841 — не держим номер дольше нужного
    async with db.pool.acquire() as c:
        if res.get("transactionId"):
            await c.execute("UPDATE pay_tx SET plum_tx_id=$2 WHERE id=$1", tx["id"], int(res["transactionId"]))
        if res.get("session"):
            await c.execute("UPDATE pay_tx SET status='otp', plum_session=$2, otp_phone=$3, otp_sent_at=NOW(), otpless=FALSE, "
                            "updated_at=NOW() WHERE id=$1 AND status='pending'",
                            tx["id"], int(res["session"]), res.get("otpSentPhone"))
            return await tx_public(tx["id"])
    if res.get("status") not in (None, plum.ST_PAID):
        return await tx_public(tx["id"])  # «проведено», но статус не «успешно» — решит сверка
    await mark_paid(tx["id"])
    return await tx_public(tx["id"])


async def _insert_order_payment(c, t: dict) -> None:
    """Онлайн-оплата → строка order_payments (сразу подтверждена) + пересчёт статуса оплаты заказа."""
    method = "online_test" if t["is_test"] else "online"
    purpose = "prepayment" if t["purpose"] == "prepay" else "final"
    note = f"Онлайн PLUM · {t.get('card_mask') or ''} · {t['extra_id']}" + (" · ТЕСТ (подставной банк)" if t["is_test"] else "")
    op = await c.fetchval(
        """INSERT INTO order_payments (order_id, amount, method, purpose, note, created_by, confirmed, confirmed_at)
           VALUES ($1,$2,$3,$4,$5,'Клиент · онлайн',TRUE,NOW()) RETURNING id""",
        t["order_id"], t["amount"], method, purpose, note)
    await c.execute("UPDATE pay_tx SET order_payment_id=$2 WHERE id=$1", t["id"], op)
    await db._recalc_payment_status(c, t["order_id"])


async def mark_paid(tx_id: int) -> bool:
    """pending/otp → paid (compare-and-set). Для заказа — строка в order_payments в той же транзакции."""
    lost_dup = None
    async with db.pool.acquire() as c:
        async with c.transaction():
            t = await c.fetchrow("UPDATE pay_tx SET status='paid', error_code=NULL, updated_at=NOW() "
                                 "WHERE id=$1 AND status IN ('pending','otp','review') RETURNING *", tx_id)
            if not t:
                return False
            t = dict(t)
            if t["card_id"]:
                await c.execute("UPDATE pay_cards SET used_count=used_count+1"
                                + (", last_otpless_at=NOW()" if t["otpless"] else "") + " WHERE id=$1", t["card_id"])
            if t["purpose"] == "prepay":
                # вторая оплаченная предоплата той же заявки/заказа — лишняя, вернём её на карту
                dup = await c.fetchval(
                    "SELECT id FROM pay_tx WHERE kind='pay' AND purpose='prepay' AND status='paid' AND id<>$1 "
                    "AND ((order_id IS NOT NULL AND order_id=$2) OR (lead_id IS NOT NULL AND lead_id=$3))",
                    t["id"], t["order_id"], t["lead_id"])
                if dup:
                    lost_dup = t
            if t["order_id"] and not lost_dup:
                await c.execute("SELECT id FROM orders WHERE id=$1 FOR UPDATE", t["order_id"])
                await _insert_order_payment(c, t)
            if t["lead_id"] and not t["order_id"] and not lost_dup:
                await c.execute("UPDATE leads SET note = COALESCE(note,'') || $2, updated_at=NOW() WHERE id=$1",
                                t["lead_id"], f"\n💳 Предоплата {_fmt(t['amount'])} сум внесена картой онлайн"
                                + (" (ТЕСТ)" if t["is_test"] else ""))
    if lost_dup:
        log.error("pay_tx %s: duplicate prepay → auto reverse", tx_id)
        await reverse_tx(tx_id, actor="система", reason="duplicate")
        return True
    where = await _where(t)
    await notify(f"💳 Оплата картой онлайн{' (ТЕСТ, подставной банк)' if t['is_test'] else ''}\n"
                 f"{where}\nСумма: {_fmt(t['amount'])} сум · {'предоплата' if t['purpose'] == 'prepay' else 'оплата'}\n"
                 f"Карта: {t.get('card_mask') or '—'}")
    return True


async def _where(t: dict) -> str:
    async with db.pool.acquire() as c:
        if t.get("order_id"):
            n = await c.fetchval("SELECT order_num FROM orders WHERE id=$1", t["order_id"])
            return f"Заказ: {n}"
        if t.get("lead_id"):
            n = await c.fetchval("SELECT COALESCE(lead_code, id::text) FROM leads WHERE id=$1", t["lead_id"])
            return f"Заявка: {n}"
    return "—"


async def _own_tx(user, tx_id: int) -> dict:
    async with db.pool.acquire() as c:
        t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1 AND user_id=$2 AND kind='pay'", tx_id, user["id"])
    if not t:
        raise PayError("not_found", 404)
    return dict(t)


async def confirm(user, tx_id: int, otp: str) -> dict:
    t = await _own_tx(user, tx_id)
    with plum.test_mode(t["is_test"]):
        if t["status"] != "otp" or not t["plum_session"]:
            raise PayError("payment_not_waiting", 409)
        if not re.fullmatch(r"\d{4,8}", str(otp or "")):
            raise PayError("plum_otp_invalid", 422)
        async with db.pool.acquire() as c:
            moved = await c.fetchval("UPDATE pay_tx SET status='pending', updated_at=NOW() WHERE id=$1 AND status='otp' RETURNING id", tx_id)
        if not moved:
            raise PayError("payment_not_waiting", 409)
        # ← статус pending закоммичен до запроса, который списывает деньги
        try:
            res = await plum.confirm_pay(t["plum_session"], str(otp), lang=t["lang"] or "ru")
        except plum.PlumPending:
            return await tx_public(tx_id)
        except plum.PlumError as e:
            async with db.pool.acquire() as c:
                if e.code == "plum_otp_invalid":  # можно ввести код ещё раз
                    await c.execute("UPDATE pay_tx SET status='otp', updated_at=NOW() WHERE id=$1 AND status='pending'", tx_id)
                else:
                    await c.execute("UPDATE pay_tx SET status='failed', error_code=$2, updated_at=NOW() "
                                    "WHERE id=$1 AND status='pending'", tx_id, e.code)
            raise PayError(e.code, 402) from None
        if res.get("transactionId"):
            async with db.pool.acquire() as c:
                await c.execute("UPDATE pay_tx SET plum_tx_id=$2 WHERE id=$1", tx_id, int(res["transactionId"]))
        if res.get("status") not in (None, plum.ST_PAID):
            return await tx_public(tx_id)
        await mark_paid(tx_id)
        return await tx_public(tx_id)


async def resend(user, tx_id: int) -> dict:
    t = await _own_tx(user, tx_id)
    if t["status"] != "otp" or not t["plum_session"]:
        raise PayError("payment_not_waiting", 409)
    if t["otp_sent_at"] and _now() - t["otp_sent_at"] < timedelta(seconds=120):  # PLUM: не раньше 2 минут
        raise PayError("resend_wait", 429)
    with plum.test_mode(t["is_test"]):
        try:
            res = await plum.resend_otp(t["plum_session"], lang=t["lang"] or "ru")
        except plum.PlumError as e:
            raise PayError(e.code, 402) from None
    async with db.pool.acquire() as c:
        await c.execute("UPDATE pay_tx SET plum_session=$2, otp_phone=COALESCE($3, otp_phone), otp_sent_at=NOW(), updated_at=NOW() "
                        "WHERE id=$1 AND status='otp'", tx_id, int(res.get("session") or t["plum_session"]), res.get("otpSentPhone"))
    return await tx_public(tx_id)


async def tx_public(tx_id: int) -> dict:
    async with db.pool.acquire() as c:
        t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1", tx_id)
    if not t:
        raise PayError("not_found", 404)
    ru, uz = MSG.get(t["error_code"] or "", (None, None))
    return {"ok": True, "tx_id": t["id"], "status": t["status"], "amount": t["amount"], "purpose": t["purpose"],
            "card_mask": t["card_mask"], "otp_phone": t["otp_phone"], "test_bank": t["is_test"],
            "error_code": t["error_code"], "error_ru": ru, "error_uz": uz,
            "resend_in": max(0, 120 - int((_now() - t["otp_sent_at"]).total_seconds())) if t["otp_sent_at"] and t["status"] == "otp" else 0}


# ══════════════════════════════════════
#  Сверка и возвраты
# ══════════════════════════════════════
async def _set(tx_id: int, status: str, frm: tuple, error: str | None = None) -> bool:
    async with db.pool.acquire() as c:
        return bool(await c.fetchval(
            f"UPDATE pay_tx SET status=$2, error_code=COALESCE($3, error_code), updated_at=NOW(), checked_at=NOW() "
            f"WHERE id=$1 AND status = ANY($4::text[]) RETURNING id", tx_id, status, error, list(frm)))


async def reconcile(tx_id: int) -> str:
    async with db.pool.acquire() as c:
        t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1", tx_id)
    if not t or t["kind"] != "pay" or t["status"] not in ("pending", "otp", "review"):
        return t["status"] if t else "missing"
    t = dict(t)
    with plum.test_mode(t["is_test"]):
        async with db.pool.acquire() as c:
            await c.execute("UPDATE pay_tx SET checked_at=NOW() WHERE id=$1", tx_id)
        try:
            tx = await plum.transaction(t["extra_id"])
        except plum.PlumError:
            return t["status"]
        st = tx.get("status") if tx else None
        age = _now() - t["created_at"]
        open_ = ("pending", "otp", "review")
        if tx is None:
            if age > timedelta(minutes=5):  # PLUM о платеже не знает — денег не брали
                await _set(tx_id, "cancelled", open_, "not_in_bank")
        elif st == plum.ST_PAID:
            pid = tx.get("id") or tx.get("transactionId")
            if pid:
                async with db.pool.acquire() as c:
                    await c.execute("UPDATE pay_tx SET plum_tx_id=COALESCE(plum_tx_id,$2) WHERE id=$1", tx_id, int(pid))
            await mark_paid(tx_id)
        elif st in plum.ST_FINAL_FAIL:
            await _set(tx_id, "failed", open_, f"bank_status_{st}")
        elif st == plum.ST_NEEDS_REVERSE:
            try:
                await plum.reverse(tx.get("id") or tx.get("transactionId"))
                await _set(tx_id, "cancelled", open_, "reversed_9")
            except plum.PlumError:
                pass
        elif st == 0 and t["status"] in ("pending", "review") and age > STALE_PENDING:
            # «так и не провелась» — но не верим на слово: сначала отменяем у PLUM, чтобы не провелась позже
            try:
                await plum.reverse(tx.get("id") or tx.get("transactionId"))
                await _set(tx_id, "cancelled", open_, "stale")
            except plum.PlumError as e:
                if e.code == "plum_not_found":
                    await _set(tx_id, "cancelled", open_, "stale")
        elif t["status"] == "otp" and age > OTP_WINDOW:
            await _set(tx_id, "cancelled", ("otp",), "otp_timeout")  # код так и не ввели; статус 0 — не списано
    async with db.pool.acquire() as c:
        now_st = await c.fetchval("SELECT status FROM pay_tx WHERE id=$1", tx_id)
    if now_st in ("pending", "otp") and _now() - t["created_at"] > REVIEW_AFTER:
        if await _set(tx_id, "review", ("pending", "otp")):
            log.error("pay_tx %s: unresolved for 24h → review", tx_id)
            await notify(f"⚠️ Онлайн-оплата не прояснилась за 24 часа — проверьте вручную\n"
                         f"{await _where(t)} · {_fmt(t['amount'])} сум · {t['extra_id']}")
        now_st = "review"
    return now_st


async def reverse_tx(tx_id: int, actor: str, reason: str = "admin") -> dict:
    """Полный возврат оплаченной транзакции на карту (paymentReverse). Сначала запись refund (pending) —
    уникальный индекс не даст сделать второй автоворот по той же оплате; потом запрос в банк."""
    try:
        async with db.pool.acquire() as c, c.transaction():
            # блокируем оплату: параллельный ручной возврат ждёт; полный автовозврат — только если
            # по этой оплате ещё НИЧЕГО не возвращали (иначе банк вернул бы сумму второй раз)
            t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1 AND kind='pay' FOR UPDATE", tx_id)
            if not t or t["status"] != "paid" or not t["plum_tx_id"]:
                raise PayError("not_found", 404)
            t = dict(t)
            if await c.fetchval("SELECT COUNT(*) FROM pay_tx WHERE refund_of=$1 AND kind='refund' "
                                "AND status IN ('paid','manual','pending')", tx_id):
                raise PayError("payment_in_progress", 409)
            r = await c.fetchrow(
                """INSERT INTO pay_tx (kind, purpose, order_id, lead_id, user_id, phone, amount, status, card_mask, is_test,
                                       refund_of, plum_tx_id, note, done_by)
                   VALUES ('refund','refund',$1,$2,$3,$4,$5,'pending',$6,$7,$8,$9,$10,$11) RETURNING id""",
                t["order_id"], t["lead_id"], t["user_id"], t["phone"], t["amount"], t["card_mask"], t["is_test"],
                t["id"], t["plum_tx_id"], reason, actor)
            await c.execute("UPDATE pay_tx SET extra_id=$2 WHERE id=$1", r["id"], f"{plum.prefix()}ref-{r['id']}")
    except Exception as e:
        if "uq_pay_tx_auto_refund" in str(e):
            raise PayError("payment_in_progress", 409) from None
        raise
    await _log(actor, "reverse", f"pay_tx {tx_id} · {t['amount']} · {reason}")
    with plum.test_mode(t["is_test"]):
        try:
            await plum.reverse(t["plum_tx_id"])
        except plum.PlumError as e:
            if e.code == "plum_unavailable":  # ответ потерян — возврат мог пройти; доводит воркер по статусу транзакции
                raise PayError(e.code, 502) from None
            async with db.pool.acquire() as c:
                await c.execute("UPDATE pay_tx SET status='failed', error_code=$2, updated_at=NOW() WHERE id=$1",
                                r["id"], e.code)
            raise PayError(e.code, 402) from None
    await _finish_refund(r["id"], t)
    return await tx_public(r["id"])


async def _finish_refund(refund_id: int, t: dict) -> None:
    async with db.pool.acquire() as c:
        async with c.transaction():
            ok = await c.fetchval("UPDATE pay_tx SET status='paid', updated_at=NOW() WHERE id=$1 AND status='pending' RETURNING id",
                                  refund_id)
            if not ok:
                return
            await c.execute("UPDATE pay_tx SET status='refunded', updated_at=NOW() WHERE id=$1 AND status='paid'", t["id"])
            oid = await c.fetchval("SELECT order_id FROM pay_tx WHERE id=$1", t["id"])
            if oid:
                await _insert_refund_payment(c, oid, t["amount"], t["is_test"], f"Возврат на карту {t.get('card_mask') or ''} · {t['extra_id']}")


async def _insert_refund_payment(c, order_id: int, amount: int, is_test: bool, note: str) -> None:
    await c.execute("SELECT id FROM orders WHERE id=$1 FOR UPDATE", order_id)
    await c.execute(
        """INSERT INTO order_payments (order_id, amount, method, purpose, note, created_by, confirmed, confirmed_at)
           VALUES ($1,$2,$3,'refund',$4,'Возврат · онлайн',TRUE,NOW())""",
        order_id, -abs(int(amount)), "online_test" if is_test else "online", note)
    await db._recalc_payment_status(c, order_id)


async def manual_refund(tx_id: int, amount: int, note: str, actor: str) -> dict:
    """«Вернули вручную»: деньги вернули через кассу/перевод. Запись refund (manual) + минус в оплатах заказа."""
    async with db.pool.acquire() as c:
        async with c.transaction():
            t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1 AND kind='pay' FOR UPDATE", tx_id)
            if not t or t["status"] not in ("paid", "refunded"):
                raise PayError("not_found", 404)
            back = await c.fetchval("SELECT COALESCE(SUM(amount),0) FROM pay_tx WHERE refund_of=$1 AND kind='refund' "
                                    "AND status IN ('paid','manual','pending')", tx_id)
            left = int(t["amount"]) - int(back or 0)
            if amount <= 0 or amount > left:
                raise HTTPException(400, f"Сумма возврата должна быть от 1 до {left}")
            r = await c.fetchval(
                """INSERT INTO pay_tx (kind, purpose, order_id, lead_id, user_id, phone, amount, status, card_mask, is_test,
                                       refund_of, note, done_by)
                   VALUES ('refund','refund',$1,$2,$3,$4,$5,'manual',$6,$7,$8,$9,$10) RETURNING id""",
                t["order_id"], t["lead_id"], t["user_id"], t["phone"], amount, t["card_mask"], t["is_test"], t["id"],
                note or "вернули вручную", actor)
            await c.execute("UPDATE pay_tx SET extra_id=$2 WHERE id=$1", r, f"{plum.prefix()}man-{r}")
            if amount == left:
                await c.execute("UPDATE pay_tx SET status='refunded', updated_at=NOW() WHERE id=$1", tx_id)
            if t["order_id"]:
                await _insert_refund_payment(c, t["order_id"], amount, t["is_test"], f"Вернули вручную: {note or ''}".strip())
    await _log(actor, "manual_refund", f"pay_tx {tx_id} · {amount} · {note}")
    return {"ok": True, "refund_id": r}


async def _check_pending_refunds() -> None:
    """Возврат, на который банк не ответил: смотрим статус исходной транзакции (4 — отменена)."""
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT id, refund_of FROM pay_tx WHERE kind='refund' AND status='pending' AND refund_of IS NOT NULL "
                             "AND created_at < NOW() - INTERVAL '1 minute' LIMIT 50")
    for row in rows:
        refund_id = row["id"]
        async with db.pool.acquire() as c:
            t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1", row["refund_of"])
        if not t:
            continue
        t = dict(t)
        with plum.test_mode(t["is_test"]):
            try:
                tx = await plum.transaction(t["extra_id"])
            except plum.PlumError:
                continue
        if tx and tx.get("status") in plum.ST_FINAL_FAIL:
            await _finish_refund(refund_id, t)


# ══════════════════════════════════════
#  Сохранённые карты
# ══════════════════════════════════════
_bind_hits: dict[int, list[float]] = {}


def _bind_rate(user_id: int) -> None:
    now = _now().timestamp()
    hits = [h for h in _bind_hits.get(user_id, []) if now - h < 3600]
    if len(hits) >= CARD_BIND_LIMIT_PER_HOUR:
        raise PayError("too_many", 429)
    hits.append(now)
    _bind_hits[user_id] = hits


def card_out(k) -> dict:
    return {"id": k["id"], "mask": k["mask"], "type": "humo" if k["pc_type"] == 1 else "uzcard",
            "owner": k["owner"], "expire": k["expire"], "test": k["is_test"]}


async def list_cards(user) -> list[dict]:
    bank = await bank_for(user)
    if not bank:
        return []
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM pay_cards WHERE user_id=$1 AND deleted_at IS NULL AND is_test=$2 ORDER BY id",
                             user["id"], bank == "test")
    return [card_out(k) for k in rows]


async def _upsert_card(user, card: dict, is_test: bool) -> dict:
    num = card.get("number") or ""
    mask = f"{num[:4]} **** **** {num[-4:]}" if len(num) >= 8 else "****"
    async with db.pool.acquire() as c:
        k = await c.fetchrow(
            """INSERT INTO pay_cards (user_id, plum_user_id, plum_card_id, plum_binding_id, mask, pc_type, owner, expire, is_test,
                                      used_count, created_at, deleted_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,0,NOW(),NULL)
               ON CONFLICT (user_id, is_test, plum_card_id) DO UPDATE SET plum_binding_id=EXCLUDED.plum_binding_id,
                   mask=EXCLUDED.mask, pc_type=EXCLUDED.pc_type, owner=EXCLUDED.owner, expire=EXCLUDED.expire,
                   deleted_at=NULL, used_count=0, created_at=NOW()
               RETURNING *""",
            user["id"], plum_user(user["id"]), int(card["cardId"]), int(card["id"]) if card.get("id") else None, mask,
            int(card.get("pcType") or 0), card.get("owner"), card.get("expireDate"), is_test)
    return card_out(k)


async def add_card(user, number: str, expire: str, lang: str) -> dict:
    bank = await bank_for(user)
    if not bank:
        raise PayError("payments_disabled", 403)
    _bind_rate(user["id"])
    n, e = parse_card(number, expire)
    with plum.test_mode(bank == "test"):
        try:
            res = await plum.create_card(plum_user(user["id"]), n, e, re.sub(r"\D", "", user["phone"]), lang=lang)
        except plum.PlumError as err:
            if err.code == "plum_card_exists":  # уже привязана к этому человеку — подтянем ИМЕННО эту карту
                try:
                    cards = await plum.user_cards(plum_user(user["id"]), lang=lang)
                except plum.PlumError:
                    cards = []
                match = [k for k in cards if (k.get("number") or "")[:6] == n[:6] and (k.get("number") or "")[-4:] == n[-4:]]
                if len(match) == 1:
                    return {"ok": True, "already": True, "card": await _upsert_card(user, match[0], bank == "test")}
            raise PayError(err.code, 402) from None
        finally:
            n = e = None  # noqa: F841
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO pay_card_binds (session, user_id, is_test) VALUES ($1,$2,$3) ON CONFLICT (session) DO NOTHING",
                        int(res["session"]), user["id"], bank == "test")
    return {"ok": True, "session": int(res["session"]), "otp_phone": res.get("otpSentPhone")}


async def confirm_card(user, session: int, otp: str, lang: str) -> dict:
    async with db.pool.acquire() as c:
        b = await c.fetchrow("SELECT * FROM pay_card_binds WHERE session=$1 AND user_id=$2 "
                             "AND created_at > NOW() - INTERVAL '15 minutes'", session, user["id"])
    if not b:
        raise PayError("not_found", 404)
    if not re.fullmatch(r"\d{4,8}", str(otp or "")):
        raise PayError("plum_otp_invalid", 422)
    with plum.test_mode(b["is_test"]):
        try:
            card = await plum.confirm_card(session, str(otp), lang=lang)
        except plum.PlumError as e:
            raise PayError(e.code, 402) from None
    async with db.pool.acquire() as c:
        await c.execute("DELETE FROM pay_card_binds WHERE session=$1", session)
    return {"ok": True, "card": await _upsert_card(user, card, b["is_test"])}


async def delete_card(user, card_id: int) -> None:
    async with db.pool.acquire() as c:
        k = await c.fetchrow("SELECT * FROM pay_cards WHERE id=$1 AND user_id=$2 AND deleted_at IS NULL", card_id, user["id"])
        if not k:
            raise PayError("not_found", 404)
        await c.execute("UPDATE pay_cards SET deleted_at=NOW() WHERE id=$1", card_id)
    if k["plum_binding_id"]:
        with plum.test_mode(k["is_test"]):  # карта удаляется в том банке, где её привязали
            try:
                await plum.delete_card(k["plum_binding_id"])
            except plum.PlumError:
                pass


async def delete_all_cards(user_id: int) -> None:
    """Удаление аккаунта клиента — убрать его карты (и у нас, и в банке)."""
    async with db.pool.acquire() as c:
        rows = await c.fetch("UPDATE pay_cards SET deleted_at=NOW() WHERE user_id=$1 AND deleted_at IS NULL RETURNING *", user_id)
    for k in rows:
        if k["plum_binding_id"]:
            with plum.test_mode(k["is_test"]):
                try:
                    await plum.delete_card(k["plum_binding_id"])
                except plum.PlumError:
                    pass


# ══════════════════════════════════════
#  Рассрочка (SmartBill) — пока заявка менеджеру
# ══════════════════════════════════════
async def installment_request(user, order: dict, note: str) -> dict:
    info = await order_pay_info(order, user)
    if not info["can_installment"]:
        raise PayError("installment_exists" if info["installment"] and info["installment"]["status"] in ("new", "in_work")
                       else "installment_closed", 409)
    name = " ".join(filter(None, [order.get("client_first_name"), order.get("client_last_name")])) or (user.get("first_name") or "")
    amount = info["due"] if info["measured"] else None
    try:
        async with db.pool.acquire() as c:
            iid = await c.fetchval(
                "INSERT INTO pay_installments (order_id, user_id, phone, client_name, amount, client_note) "
                "VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
                order["id"], user["id"], user["phone"], name[:200], amount, (note or "")[:500])
    except Exception as e:
        if "uq_pay_installments_open" in str(e):
            raise PayError("installment_exists", 409) from None
        raise
    await notify(f"🧾 Заявка на рассрочку (SmartBill)\nЗаказ: {order['order_num']}\nКлиент: {name or '—'} · {user['phone']}\n"
                 f"Сумма: {_fmt(amount) + ' сум' if amount else 'после замера'}\n"
                 + (f"Комментарий клиента: {note[:300]}\n" if note else "")
                 + "Оформить в кабинете SmartBill и отметить в админке → 💳 Оплаты → Рассрочка")
    return {"ok": True, "id": iid}


# ══════════════════════════════════════
#  Push «К оплате X сум» (после замера)
# ══════════════════════════════════════
async def _due_pushes() -> None:
    import main
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            """SELECT DISTINCT o.* FROM orders o
                 JOIN users u ON u.phone = o.client_phone
                 JOIN app_push_tokens t ON t.kind='client' AND t.subject_id = u.id AND t.revoked_at IS NULL
                WHERE o.status = ANY($1::text[]) AND o.created_at > NOW() - INTERVAL '60 days'
                  AND EXISTS (SELECT 1 FROM order_items i WHERE i.order_id=o.id)
                  AND NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_id=o.id
                                   AND COALESCE(i.measure_status,'pending') <> ALL($2::text[]))
                LIMIT 200""",
            ["received", "washing", "drying", "packing", "ready", "delivery"], list(MEASURED_OK))
    for o in rows:
        o = dict(o)
        try:
            async with db.pool.acquire() as c:
                user = await c.fetchrow("SELECT * FROM users WHERE phone=$1", o["client_phone"])
                last = await c.fetchval("SELECT amount FROM pay_due_push WHERE order_id=$1", o["id"])
            if not user or not await bank_for(user):
                continue
            m = await money(o)
            if m["due"] < 1000 or last == m["due"] or m["open_tx"]:
                continue
            tokens = await db.app_push_tokens_for_phone(o["client_phone"])
            async with db.pool.acquire() as c:  # сначала отметка — дважды одно и то же не уйдёт
                await c.execute("INSERT INTO pay_due_push (order_id, amount, sent_at) VALUES ($1,$2,NOW()) "
                                "ON CONFLICT (order_id) DO UPDATE SET amount=EXCLUDED.amount, sent_at=NOW()", o["id"], m["due"])
            if not tokens:
                continue
            ru = f"К оплате {_fmt(m['due'])} сум — можно оплатить картой в приложении"
            uz = f"To'lov uchun {_fmt(m['due'])} so'm — ilovada karta bilan to'lash mumkin"
            await main._expo_send([{
                "to": t["token"], "sound": "default", "priority": "high", "channelId": "default",
                "title": main._push_lang_text(t["lang"], f"ARTEZ · Заказ №{o['order_num']}", f"ARTEZ · Buyurtma №{o['order_num']}"),
                "body": main._push_lang_text(t["lang"], ru, uz),
                "data": {"kind": "pay_due", "order_num": o["order_num"], "amount": m["due"]},
            } for t in tokens])
        except Exception as e:
            log.warning("due push %s: %s", o.get("id"), type(e).__name__)


async def worker() -> None:
    """Раз в минуту: сверка неясных оплат, предоплаты заявок → заказы, возвраты без ответа, push «К оплате»."""
    await asyncio.sleep(30)
    while True:
        try:
            async with db.pool.acquire() as c:
                ids = await c.fetch(
                    "SELECT id FROM pay_tx WHERE kind='pay' AND ("
                    " (status IN ('pending','otp') AND created_at < NOW() - INTERVAL '30 seconds')"
                    " OR (status='review' AND created_at > NOW() - INTERVAL '7 days'"
                    "     AND (checked_at IS NULL OR checked_at < NOW() - INTERVAL '1 hour'))"
                    ") ORDER BY id LIMIT 200")
            for r in ids:
                await reconcile(r["id"])
            await _attach_lead_prepays()
            await _check_pending_refunds()
            await _due_pushes()
            async with db.pool.acquire() as c:
                await c.execute("DELETE FROM pay_card_binds WHERE created_at < NOW() - INTERVAL '1 day'")
        except Exception as e:
            log.warning("pay worker: %s: %s", type(e).__name__, e)
        await asyncio.sleep(60)


# ══════════════════════════════════════
#  HTTP: клиент (сайт и приложение — один и тот же JWT клиента)
# ══════════════════════════════════════
async def _user(authorization: str = Header(None)):
    import main
    return await main.get_current_user(authorization)


async def _order(user, order_num: str) -> dict:
    o = await db.get_order_by_num_and_phone(order_num, user["phone"])
    if not o:
        raise _http(PayError("not_found", 404))
    return o


async def _lead(user, code: str) -> dict:
    async with db.pool.acquire() as c:
        r = await c.fetchrow("SELECT * FROM leads WHERE (lead_code=$1 OR ('#' || id::text)=$1) AND client_phone=$2 "
                             "ORDER BY id DESC LIMIT 1", code, user["phone"])
    if not r:
        raise _http(PayError("not_found", 404))
    return dict(r)


def _card_body(body: dict) -> tuple[dict | None, int | None]:
    card_id = body.get("card_id")
    card = body.get("card") if isinstance(body.get("card"), dict) else None
    return card, (int(card_id) if card_id else None)


@router.get("/api/pay/config")
async def api_config(user=Depends(_user)):
    bank = await bank_for(user)
    lo, hi, _d = await prepay_limits()
    return {"ok": True, "enabled": bank is not None, "test_bank": bank == "test", "prepay_amount": await prepay_amount(),
            "prepay_min": lo, "prepay_max": hi,
            "test_card": "8600 1234 5678 9012 · 12/30 · SMS 111111" if bank == "test" else None}


@router.get("/api/pay/orders/{order_num}")
async def api_order(order_num: str, user=Depends(_user)):
    return await order_pay_info(await _order(user, order_num), user)


@router.post("/api/pay/orders/{order_num}/pay")
async def api_order_pay(order_num: str, body: dict = Body({}), user=Depends(_user)):
    order = await _order(user, order_num)
    card, card_id = _card_body(body)
    purpose = "prepay" if body.get("purpose") == "prepay" else "balance"
    try:
        return await start(user, order=order, purpose=purpose, card=card, card_id=card_id, lang=body.get("lang") or "ru",
                           amount_req=body.get("amount") if purpose == "prepay" else None)
    except PayError as e:
        raise _http(e) from None


@router.get("/api/pay/lead/{code}")
async def api_lead(code: str, user=Depends(_user)):
    lead = await _lead(user, code)
    bank = await bank_for(user)
    async with db.pool.acquire() as c:
        done = await c.fetchrow("SELECT id, status, amount FROM pay_tx WHERE lead_id=$1 AND kind='pay' AND purpose='prepay' "
                                "AND status IN ('paid','refunded','review','pending','otp') ORDER BY id DESC LIMIT 1", lead["id"])
    return {"ok": True, "lead_code": lead.get("lead_code"), "enabled": bank is not None, "test_bank": bank == "test",
            "prepay_amount": await prepay_amount(), "prepay_min": (await prepay_limits())[0], "prepay_max": (await prepay_limits())[1],
            "prepay": dict(done) if done else None,
            "can_prepay": bank is not None and not done and lead["status"] not in ("converted", "lost")}


@router.post("/api/pay/lead/{code}/pay")
async def api_lead_pay(code: str, body: dict = Body({}), user=Depends(_user)):
    lead = await _lead(user, code)
    if lead["status"] in ("converted", "lost"):
        raise _http(PayError("prepay_unavailable", 409))
    card, card_id = _card_body(body)
    try:
        return await start(user, lead=lead, purpose="prepay", card=card, card_id=card_id, lang=body.get("lang") or "ru",
                           amount_req=body.get("amount"))
    except PayError as e:
        raise _http(e) from None


@router.get("/api/pay/tx/{tx_id}")
async def api_tx(tx_id: int, user=Depends(_user)):
    try:
        t = await _own_tx(user, tx_id)
        if t["status"] == "pending" and _now() - t["created_at"] > timedelta(seconds=10):
            await reconcile(tx_id)
        return await tx_public(tx_id)
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/tx/{tx_id}/confirm")
async def api_tx_confirm(tx_id: int, body: dict = Body({}), user=Depends(_user)):
    try:
        return await confirm(user, tx_id, str(body.get("otp") or ""))
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/tx/{tx_id}/resend")
async def api_tx_resend(tx_id: int, user=Depends(_user)):
    try:
        return await resend(user, tx_id)
    except PayError as e:
        raise _http(e) from None


@router.get("/api/pay/cards")
async def api_cards(user=Depends(_user)):
    return {"ok": True, "cards": await list_cards(user)}


@router.post("/api/pay/cards")
async def api_card_add(body: dict = Body({}), user=Depends(_user)):
    try:
        return await add_card(user, body.get("number"), body.get("expire"), body.get("lang") or "ru")
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/cards/confirm")
async def api_card_confirm(body: dict = Body({}), user=Depends(_user)):
    try:
        return await confirm_card(user, int(body.get("session") or 0), str(body.get("otp") or ""), body.get("lang") or "ru")
    except PayError as e:
        raise _http(e) from None


@router.delete("/api/pay/cards/{card_id}")
async def api_card_delete(card_id: int, user=Depends(_user)):
    try:
        await delete_card(user, card_id)
        return {"ok": True}
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/orders/{order_num}/installment")
async def api_installment(order_num: str, body: dict = Body({}), user=Depends(_user)):
    order = await _order(user, order_num)
    try:
        return await installment_request(user, order, str(body.get("note") or ""))
    except PayError as e:
        raise _http(e) from None


# ══════════════════════════════════════
#  HTTP: админка (admin JWT или сотрудник admin/manager)
# ══════════════════════════════════════
async def _admin(authorization: str = Header(None)) -> dict:
    """{'name', 'is_admin'}: admin.html (sub=admin) — полный доступ; staff admin — полный; manager — без настроек."""
    import main
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Не авторизован")
    try:
        payload = jwt.decode(authorization.removeprefix("Bearer ").strip(), main.JWT_SECRET, algorithms=[main.JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(401, "Недействительный токен")
    if payload.get("sub") == "admin":
        return {"name": "Администратор", "is_admin": True}
    staff = await main._staff_from_payload(payload)
    if not staff or staff.get("role") not in ("admin", "manager"):
        raise HTTPException(403, "Нет доступа")
    name = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login") or "сотрудник"
    return {"name": name, "is_admin": staff.get("role") == "admin"}


def _need_admin(a: dict) -> None:
    if not a["is_admin"]:
        raise HTTPException(403, "Только администратор")


@router.get("/api/pay/admin/settings")
async def adm_settings(a=Depends(_admin)):
    lo, hi, _d = await prepay_limits()
    return {"ok": True, "mode": await setting("pay_mode"), "prepay_amount": await prepay_amount(),
            "prepay_min": lo, "prepay_max": hi,
            "otpless_limit": int(float(await setting("pay_otpless_limit"))), "notify": await setting("pay_notify"),
            "real_configured": plum.real_configured(), "prefix": plum.prefix(), "can_edit": a["is_admin"]}


@router.put("/api/pay/admin/settings")
async def adm_settings_put(body: dict = Body({}), a=Depends(_admin)):
    _need_admin(a)
    changes = []
    if "mode" in body:
        if body["mode"] not in ("off", "testers", "all"):
            raise HTTPException(400, "mode: off | testers | all")
        if body["mode"] == "all" and not plum.real_configured():
            raise HTTPException(400, "Ключи PLUM не настроены — «для всех» включить нельзя")
        await db.set_config("pay_mode", body["mode"]); changes.append(f"mode={body['mode']}")
    for key, cfg, lo in (("prepay_amount", "pay_prepay_amount", 1000), ("otpless_limit", "pay_otpless_limit", 0),
                         ("prepay_min", "pay_prepay_min", 1000), ("prepay_max", "pay_prepay_max", 1000)):
        if key in body:
            try:
                v = int(float(body[key]))
            except (TypeError, ValueError):
                raise HTTPException(400, f"{key}: число")
            if v < lo or v > 100_000_000:
                raise HTTPException(400, f"{key}: от {lo}")
            await db.set_config(cfg, str(v)); changes.append(f"{key}={v}")
    pmin, pmax, _pd = await prepay_limits()
    if pmin > pmax or not (pmin <= await prepay_amount() <= pmax):
        changes.append("⚠️ предоплата по умолчанию вне пределов — клиенту будет предложена ближайшая граница")
    if "notify" in body:
        if body["notify"] not in ("owner", "group"):
            raise HTTPException(400, "notify: owner | group")
        await db.set_config("pay_notify", body["notify"]); changes.append(f"notify={body['notify']}")
    await _log(a["name"], "settings", ", ".join(changes))
    return await adm_settings(a)


@router.get("/api/pay/admin/testers")
async def adm_testers(a=Depends(_admin)):
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM pay_testers ORDER BY id")
    return {"ok": True, "testers": [dict(r) for r in rows]}


@router.post("/api/pay/admin/testers")
async def adm_tester_add(body: dict = Body({}), a=Depends(_admin)):
    _need_admin(a)
    ph = re.sub(r"[^\d+]", "", str(body.get("phone") or ""))
    if re.fullmatch(r"\d{9}", ph):
        ph = "+998" + ph
    elif re.fullmatch(r"998\d{9}", ph):
        ph = "+" + ph
    if not re.fullmatch(r"\+998\d{9}", ph):
        raise HTTPException(400, "Телефон в формате +998XXXXXXXXX")
    async with db.pool.acquire() as c:
        r = await c.fetchrow("INSERT INTO pay_testers (phone, name, real_card) VALUES ($1,$2,$3) "
                             "ON CONFLICT (phone) DO UPDATE SET name=EXCLUDED.name, real_card=EXCLUDED.real_card RETURNING *",
                             ph, str(body.get("name") or "")[:120], bool(body.get("real_card")))
    await _log(a["name"], "tester_add", f"{ph} real_card={bool(body.get('real_card'))}")
    return {"ok": True, "tester": dict(r)}


@router.delete("/api/pay/admin/testers/{tid}")
async def adm_tester_del(tid: int, a=Depends(_admin)):
    _need_admin(a)
    async with db.pool.acquire() as c:
        ph = await c.fetchval("DELETE FROM pay_testers WHERE id=$1 RETURNING phone", tid)
    await _log(a["name"], "tester_del", str(ph))
    return {"ok": True}


@router.get("/api/pay/admin/tx")
async def adm_tx(status: str = "", limit: int = 200, a=Depends(_admin)):
    q = """SELECT t.id, t.kind, t.purpose, t.status, t.amount, t.card_mask, t.is_test, t.extra_id, t.error_code, t.refund_of,
                  t.note, t.done_by, t.created_at, t.updated_at, t.phone, t.order_id, t.lead_id,
                  o.order_num, o.status AS order_status,
                  TRIM(COALESCE(o.client_first_name,'') || ' ' || COALESCE(o.client_last_name,'')) AS client_name,
                  l.lead_code, l.status AS lead_status,
                  (SELECT COALESCE(SUM(r.amount),0) FROM pay_tx r WHERE r.refund_of=t.id AND r.kind='refund'
                     AND r.status IN ('paid','manual')) AS refunded
             FROM pay_tx t LEFT JOIN orders o ON o.id=t.order_id LEFT JOIN leads l ON l.id=t.lead_id"""
    args: list = []
    if status:
        q += " WHERE t.status = ANY($1::text[])"
        args.append(status.split(","))
    q += f" ORDER BY t.id DESC LIMIT {max(1, min(int(limit), 500))}"
    async with db.pool.acquire() as c:
        rows = await c.fetch(q, *args)
    return {"ok": True, "tx": [dict(r) for r in rows]}


@router.get("/api/pay/admin/refunds-needed")
async def adm_refunds_needed(a=Depends(_admin)):
    """Кому вернуть деньги: отменённые заказы и заявки с онлайн-оплатой; заказы, где внесено больше итога."""
    out = []
    async with db.pool.acquire() as c:
        rows = await c.fetch(
            """SELECT DISTINCT o.* FROM pay_tx t JOIN orders o ON o.id=t.order_id
                WHERE t.kind='pay' AND t.status='paid' AND t.created_at > NOW() - INTERVAL '120 days'""")
        leads = await c.fetch(
            """SELECT t.id AS tx_id, t.amount, t.is_test, l.lead_code, l.client_name, l.client_phone, l.status
                 FROM pay_tx t JOIN leads l ON l.id=t.lead_id
                WHERE t.kind='pay' AND t.status='paid' AND t.order_id IS NULL AND l.status='lost'""")
    for o in rows:
        o = dict(o)
        m = await money(o)
        over = (m["paid"] if o["status"] == "cancelled" else max(0, m["paid"] - m["net"]))
        if over > 0 and (o["status"] == "cancelled" or m["measured"] or o["status"] == "delivered"):
            async with db.pool.acquire() as c:
                txs = await c.fetch("SELECT id, amount, created_at, is_test FROM pay_tx WHERE order_id=$1 AND kind='pay' "
                                    "AND status='paid' ORDER BY id", o["id"])
            out.append({"order_num": o["order_num"], "order_status": o["status"], "net": m["net"], "paid": m["paid"],
                        "to_return": over, "client_phone": o["client_phone"],
                        "client_name": " ".join(filter(None, [o.get("client_first_name"), o.get("client_last_name")])),
                        "tx": [dict(t) for t in txs]})
    for l in leads:
        out.append({"lead_code": l["lead_code"], "order_status": "lead_lost", "net": 0, "paid": l["amount"],
                    "to_return": l["amount"], "client_phone": l["client_phone"], "client_name": l["client_name"],
                    "tx": [{"id": l["tx_id"], "amount": l["amount"], "is_test": l["is_test"]}]})
    return {"ok": True, "items": out}


@router.post("/api/pay/admin/tx/{tx_id}/reverse")
async def adm_reverse(tx_id: int, a=Depends(_admin)):
    _need_admin(a)
    try:
        return await reverse_tx(tx_id, actor=a["name"])
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/admin/tx/{tx_id}/manual-refund")
async def adm_manual_refund(tx_id: int, body: dict = Body({}), a=Depends(_admin)):
    _need_admin(a)
    try:
        amount = int(float(body.get("amount") or 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "Сумма — число")
    try:
        return await manual_refund(tx_id, amount, str(body.get("note") or "")[:300], a["name"])
    except PayError as e:
        raise _http(e) from None


@router.post("/api/pay/admin/tx/{tx_id}/recheck")
async def adm_recheck(tx_id: int, a=Depends(_admin)):
    return {"ok": True, "status": await reconcile(tx_id)}


@router.get("/api/pay/admin/installments")
async def adm_installments(a=Depends(_admin)):
    async with db.pool.acquire() as c:
        rows = await c.fetch("""SELECT i.*, o.order_num, o.status AS order_status FROM pay_installments i
                                  LEFT JOIN orders o ON o.id=i.order_id ORDER BY i.id DESC LIMIT 300""")
    return {"ok": True, "items": [dict(r) for r in rows]}


@router.patch("/api/pay/admin/installments/{iid}")
async def adm_installment_patch(iid: int, body: dict = Body({}), a=Depends(_admin)):
    st = body.get("status")
    if st not in (None, "new", "in_work", "done", "rejected"):
        raise HTTPException(400, "status: new | in_work | done | rejected")
    async with db.pool.acquire() as c:
        r = await c.fetchrow("UPDATE pay_installments SET status=COALESCE($2,status), manager_note=COALESCE($3,manager_note), "
                             "handled_by=$4, updated_at=NOW() WHERE id=$1 RETURNING *",
                             iid, st, (str(body["manager_note"])[:1000] if "manager_note" in body else None), a["name"])
    if not r:
        raise HTTPException(404, "Не найдено")
    await _log(a["name"], "installment", f"#{iid} → {st}")
    return {"ok": True, "item": dict(r)}


@router.get("/api/pay/admin/log")
async def adm_log(a=Depends(_admin)):
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM pay_log ORDER BY id DESC LIMIT 200")
    return {"ok": True, "log": [dict(r) for r in rows]}


# ══════════════════════════════════════
#  Предоплата по заявкам — для списков лидов (staff/admin) и кабинета клиента (16.11)
# ══════════════════════════════════════
async def prepaid_by_lead_ids(ids: list[int]) -> dict[int, int]:
    """{lead_id: сумма оплаченных онлайн-предоплат} (возвращённые не считаются)."""
    ids = [int(i) for i in ids if i]
    if not ids or not db.pool:
        return {}
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT lead_id, SUM(amount)::int AS s FROM pay_tx WHERE kind='pay' AND purpose='prepay' "
                             "AND status='paid' AND lead_id = ANY($1::int[]) GROUP BY lead_id", ids)
    return {r["lead_id"]: r["s"] for r in rows}


async def add_prepaid(leads: list[dict], phone: str | None = None) -> list[dict]:
    """Дописывает в каждую заявку поле prepaid (сум). Без id (кабинет клиента) — по lead_code и телефону."""
    try:
        if leads and all(l.get("id") for l in leads):
            m = await prepaid_by_lead_ids([l["id"] for l in leads])
            for l in leads:
                l["prepaid"] = m.get(l["id"], 0)
        elif leads and phone:
            codes = [l.get("lead_code") for l in leads if l.get("lead_code")]
            async with db.pool.acquire() as c:
                rows = await c.fetch("SELECT l.lead_code, SUM(t.amount)::int AS s FROM pay_tx t JOIN leads l ON l.id=t.lead_id "
                                     "WHERE t.kind='pay' AND t.purpose='prepay' AND t.status='paid' AND l.client_phone=$1 "
                                     "AND l.lead_code = ANY($2::text[]) GROUP BY l.lead_code", phone, codes)
            m = {r["lead_code"]: r["s"] for r in rows}
            for l in leads:
                l["prepaid"] = m.get(l.get("lead_code"), 0)
    except Exception as e:  # метка не должна ломать список заявок
        log.warning("add_prepaid: %s", type(e).__name__)
    return leads


# ══════════════════════════════════════
#  Возврат предоплаты из staff.html (право can_refund_prepay, 16.11)
# ══════════════════════════════════════
def _tash_day(dt: datetime) -> str:
    return (dt.astimezone(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%d")


async def _refund_staff(authorization: str = Header(None)) -> dict:
    """Сотрудник с правом возврата предоплаты (или админ)."""
    import main
    staff = await main.get_current_staff(authorization)
    if not (staff.get("role") == "admin" or staff.get("sub") == "admin" or staff.get("can_refund_prepay")):
        raise HTTPException(403, "Нет права на возврат предоплаты — включается в карточке сотрудника")
    name = " ".join(filter(None, [staff.get("first_name"), staff.get("last_name")])) or staff.get("login") or "сотрудник"
    return {"name": name, "id": staff.get("id")}


@router.get("/api/pay/staff/prepay")
async def staff_prepay(lead_id: int = 0, order_id: int = 0, s=Depends(_refund_staff)):
    """Оплаченные онлайн-предоплаты заявки/заказа: что и как можно вернуть."""
    if not (lead_id or order_id):
        raise HTTPException(400, "lead_id или order_id")
    q = ("SELECT * FROM pay_tx WHERE kind='pay' AND purpose='prepay' AND status IN ('paid','refunded') AND "
         + ("order_id=$1" if order_id else "lead_id=$1") + " ORDER BY id")
    async with db.pool.acquire() as c:
        rows = await c.fetch(q, order_id or lead_id)
        out = []
        for t in rows:
            back = await c.fetchval("SELECT COALESCE(SUM(amount),0) FROM pay_tx WHERE refund_of=$1 AND kind='refund' "
                                    "AND status IN ('paid','manual','pending')", t["id"])
            left = int(t["amount"]) - int(back or 0)
            out.append({"id": t["id"], "amount": t["amount"], "left": left, "card_mask": t["card_mask"], "is_test": t["is_test"],
                        "created_at": t["created_at"].isoformat(),
                        # на карту — всю сумму, только в день оплаты и если ещё ничего не возвращали
                        "can_reverse": bool(t["plum_tx_id"]) and t["status"] == "paid" and int(back or 0) == 0
                                       and _tash_day(t["created_at"]) == _tash_day(_now())})
    return {"ok": True, "tx": out}


async def _staff_tx(tx_id: int) -> dict:
    async with db.pool.acquire() as c:
        t = await c.fetchrow("SELECT * FROM pay_tx WHERE id=$1 AND kind='pay' AND purpose='prepay'", tx_id)
    if not t:
        raise HTTPException(404, "Предоплата не найдена")
    return dict(t)


@router.post("/api/pay/staff/tx/{tx_id}/reverse")
async def staff_reverse(tx_id: int, s=Depends(_refund_staff)):
    await _staff_tx(tx_id)
    try:
        r = await reverse_tx(tx_id, actor=s["name"], reason="staff")
    except PayError as e:
        raise _http(e) from None
    await notify(f"↩️ Предоплата возвращена на карту — {s['name']} · pay_tx {tx_id}")
    return r


@router.post("/api/pay/staff/tx/{tx_id}/manual-refund")
async def staff_manual_refund(tx_id: int, body: dict = Body({}), s=Depends(_refund_staff)):
    await _staff_tx(tx_id)
    try:
        amount = int(float(body.get("amount") or 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "Сумма — число")
    try:
        r = await manual_refund(tx_id, amount, str(body.get("note") or "")[:300], s["name"])
    except PayError as e:
        raise _http(e) from None
    await notify(f"↩️ Предоплата {amount:,} сум отмечена «вернули вручную» — {s['name']}".replace(",", " "))
    return r


@router.put("/api/pay/admin/staff/{staff_id}/refund-perm")
async def adm_refund_perm(staff_id: int, body: dict = Body({}), a=Depends(_admin)):
    """Право сотрудника «Возврат предоплаты» (переключатель в карточке сотрудника, admin.html)."""
    _need_admin(a)
    on = bool(body.get("value"))
    async with db.pool.acquire() as c:
        ok = await c.fetchval("UPDATE staff SET can_refund_prepay=$2 WHERE id=$1 RETURNING id", staff_id, on)
    if not ok:
        raise HTTPException(404, "Сотрудник не найден")
    await _log(a["name"], "refund_perm", f"staff {staff_id} → {on}")
    return {"ok": True, "value": on}
