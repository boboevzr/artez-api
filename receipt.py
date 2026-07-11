"""
Генерация JPEG-чека заказа (для отправки клиенту в Telegram).

Макет — под 80мм термопринтер (576px по ширине), чёрный текст на белом фоне
без градаций серого (термопринтеры печатают 1-bit чёрное/белое).
"""
import io
import os

from PIL import Image, ImageDraw, ImageFont

FONT_DIR = os.path.join(os.path.dirname(__file__), "assets", "fonts")
REGULAR = os.path.join(FONT_DIR, "DejaVuSans.ttf")
BOLD    = os.path.join(FONT_DIR, "DejaVuSans-Bold.ttf")
EMOJI_FONT_PATH = os.path.join(FONT_DIR, "OpenMoji-Black.ttf")

W = 576  # 80mm thermal printer standard raster width
PAD = 20
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)


def _fonts():
    return {
        "logo":      ImageFont.truetype(BOLD, 40),
        "h2":        ImageFont.truetype(REGULAR, 22),
        "item_name": ImageFont.truetype(BOLD, 22),
        "item_sub":  ImageFont.truetype(REGULAR, 19),
        "total":     ImageFont.truetype(BOLD, 28),
        "footer":    ImageFont.truetype(REGULAR, 18),
        "emoji":     ImageFont.truetype(EMOJI_FONT_PATH, 24),
    }


def fmt_n(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def _measure(draw, text, font):
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def _center_text(draw, y, text, font, fill=BLACK):
    w, h = _measure(draw, text, font)
    draw.text(((W - w) / 2, y), text, font=font, fill=fill)
    return h


def _dashed_line(draw, y):
    x = PAD
    while x < W - PAD:
        draw.line([(x, y), (x + 6, y)], fill=BLACK, width=2)
        x += 12


def _draw_mixed_line(draw, x, y, segments):
    """segments: list of (text, font) tuples, drawn left-to-right starting at x.
    Returns (total_width, max_height)."""
    cur_x = x
    max_h = 0
    for text, font in segments:
        draw.text((cur_x, y), text, font=font, fill=BLACK)
        w, h = _measure(draw, text, font)
        cur_x += w
        max_h = max(max_h, h)
    return cur_x - x, max_h


def generate_receipt_jpeg(order: dict, items: list[dict], branch_contacts: list[str],
                           header_text: str = "ARTEZ",
                           slogan: str = "Химчистка ковров, мебели, матрасов и штор",
                           footer_note: str = "",
                           service_emojis: dict | None = None,
                           type_label: str = "Стандарт") -> bytes:
    """
    Рисует JPEG-чек заказа и возвращает его байты.

    order: словарь заказа (как из db.get_order_by_id) — используются поля
           id/order_num, created_at (или уже отформатированная дата) и имя клиента.
    items: список позиций заказа (как из db.get_order_items) — service,
           width_cm, length_cm, sqm, price_per_sqm, total_sum.
    branch_contacts: список строк контактов филиала (уже отобранных вызывающей стороной).
    header_text: текст шапки-логотипа чека (по умолчанию "ARTEZ").
    slogan: слоган в подвале чека.
    footer_note: доп. строка в подвале чека (не выводится, если пустая).
    service_emojis: словарь {service_name: emoji}, уже разрешённый вызывающей стороной из БД.
    type_label: подпись типа услуги (Стандарт/Экспресс), общая для всех позиций чека.
    """
    service_emojis = service_emojis or {}
    f = _fonts()

    order_num = order.get("order_num") or order.get("id") or ""
    created_at = order.get("created_at")
    if hasattr(created_at, "strftime"):
        order_date_str = created_at.strftime("%d.%m.%Y %H:%M")
    else:
        order_date_str = str(created_at or "")

    client_name = " ".join(
        p for p in [order.get("client_first_name"), order.get("client_last_name")] if p
    ).strip() or order.get("client_name") or order.get("client_phone") or ""

    # Оверсайз-холст, обрезаем в конце по фактической высоте.
    # Щедрый фиксированный запас (не пропорциональный числу позиций) — при малом
    # количестве items с длинным header/footer_note/несколькими контактами
    # пропорциональная оценка недооценивала высоту, и crop() обрезал подвал чёрной полосой.
    img = Image.new("RGB", (W, max(1200, 400 + 200 * len(items))), WHITE)
    draw = ImageDraw.Draw(img)

    y = PAD
    y += _center_text(draw, y, header_text, f["logo"]) + 8
    y += _center_text(draw, y, f"Заказ №{order_num}  ·  {order_date_str}", f["h2"]) + 4
    if client_name:
        y += _center_text(draw, y, client_name, f["h2"]) + 10
    _dashed_line(draw, y); y += 16

    for i, it in enumerate(items, 1):
        name = it.get("service") or "—"
        emoji = service_emojis.get(it.get("service"), "🧺")
        segments = [
            (f"{i} ", f["item_name"]),
            (emoji, f["emoji"]),
            (f" {name} — {type_label}", f["item_name"]),
        ]
        _, line1_h = _draw_mixed_line(draw, PAD, y, segments)
        y += line1_h + 6

        w_cm, l_cm, sqm = it.get("width_cm"), it.get("length_cm"), it.get("sqm")
        price, total = it.get("price_per_sqm"), it.get("total_sum")

        if w_cm and l_cm:
            dim_line = f"{int(w_cm)}×{int(l_cm)} см · {float(sqm or 0):.2f} м² | {fmt_n(price)} сум/м²"
            total_line = f"{fmt_n(total)} сум"
            dim_w, dim_h = _measure(draw, dim_line, f["item_sub"])
            total_w, total_h = _measure(draw, total_line, f["total"])
            available = W - 2 * PAD
            min_gap = 12
            if dim_w + total_w + min_gap > available:
                # не помещаются рядом — переносим на отдельные строки
                draw.text((PAD + 14, y), dim_line, font=f["item_sub"], fill=BLACK)
                y += dim_h + 6
                draw.text((W - PAD - total_w, y), total_line, font=f["total"], fill=BLACK)
                y += total_h + 4
            else:
                bottom = y + dim_h
                draw.text((PAD + 14, y), dim_line, font=f["item_sub"], fill=BLACK)
                draw.text((W - PAD - total_w, bottom - total_h), total_line, font=f["total"], fill=BLACK)
                y += max(dim_h, total_h) + 4
        elif total:
            total_line = f"{fmt_n(total)} сум"
            total_w, total_h = _measure(draw, total_line, f["total"])
            draw.text((W - PAD - total_w, y), total_line, font=f["total"], fill=BLACK)
            y += total_h + 4

        y += 10
        _dashed_line(draw, y)
        y += 14

    grand = sum(float(it.get("total_sum") or 0) for it in items)
    y += 6
    y += _center_text(draw, y, f"ИТОГО: {fmt_n(grand)} сум", f["total"]) + 14
    _dashed_line(draw, y); y += 18

    for c in branch_contacts:
        if not c:
            continue
        y += _center_text(draw, y, c, f["footer"]) + 4
    y += 8
    y += _center_text(draw, y, slogan, f["footer"]) + 4
    if footer_note:
        y += _center_text(draw, y, footer_note, f["footer"]) + 4
    y += PAD

    final = img.crop((0, 0, W, int(y)))
    buf = io.BytesIO()
    final.save(buf, "JPEG", quality=95)
    return buf.getvalue()
