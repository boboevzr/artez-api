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


def generate_receipt_jpeg(order: dict, items: list[dict], branch_contacts: list[str],
                           header_text: str = "ARTEZ",
                           slogan: str = "Химчистка ковров, мебели, матрасов и штор",
                           footer_note: str = "") -> bytes:
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
    """
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

    # Оверсайз-холст, обрезаем в конце по фактической высоте
    img = Image.new("RGB", (W, 200 + 150 * max(len(items), 1)), WHITE)
    draw = ImageDraw.Draw(img)

    y = PAD
    y += _center_text(draw, y, header_text, f["logo"]) + 8
    y += _center_text(draw, y, f"Заказ №{order_num}  ·  {order_date_str}", f["h2"]) + 4
    if client_name:
        y += _center_text(draw, y, client_name, f["h2"]) + 10
    _dashed_line(draw, y); y += 16

    for i, it in enumerate(items, 1):
        name = it.get("service") or "—"
        line1 = f"{i}. {name}"
        draw.text((PAD, y), line1, font=f["item_name"], fill=BLACK)
        y += _measure(draw, line1, f["item_name"])[1] + 6

        w_cm, l_cm, sqm = it.get("width_cm"), it.get("length_cm"), it.get("sqm")
        if w_cm and l_cm:
            dim_line = f"{int(w_cm)}×{int(l_cm)} см · {float(sqm or 0):.2f} м²"
            draw.text((PAD + 14, y), dim_line, font=f["item_sub"], fill=BLACK)
            y += _measure(draw, dim_line, f["item_sub"])[1] + 4

        price, total = it.get("price_per_sqm"), it.get("total_sum")
        if price and total:
            price_line = f"{fmt_n(price)} с/м² · {fmt_n(total)} с"
        elif total:
            price_line = f"{fmt_n(total)} с"
        else:
            price_line = ""
        if price_line:
            draw.text((PAD + 14, y), price_line, font=f["item_sub"], fill=BLACK)
            y += _measure(draw, price_line, f["item_sub"])[1] + 4

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
