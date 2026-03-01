"""
Эмулирует историческую выгрузку B2B-заказов от торговых сетей,
HoReCa и оптовых покупателей. Пишет в PostgreSQL (схема source_kmk):
  - source_kmk.customers            — справочник клиентов (master data)
  - source_kmk.products             — справочник SKU (master data)
  - source_kmk.orders               — шапка заказа (snapshot)
  - source_kmk.order_items          — позиции заказа (snapshot)
  - source_kmk.shipments            — отгрузка/доставка (snapshot)
  - source_kmk.order_status_history — история статусов (append-only events)
  - source_kmk.payments             — оплаты/дебиторка (events)
  - source_kmk.returns              — возвраты (events)

Пайплайн:  Generator → PostgreSQL → Airflow → Spark → MinIO → ClickHouse → dbt → Metabase

Данные максимально приближены к реальным:
  - Реальные бренды КМК: «Село Зелёное», «Молочная речка», «Варвара Краса», «Топтыжка»
  - Реальные фасовки, ГОСТы, сроки годности
  - Реальный адрес производства: г. Казань, ул. Академика Арбузова, 7
  - Реальные торговые сети Татарстана
  - Мощность завода: ~850 тонн молока-сырья/сутки
  - Сертификация Халяль, система FSSC 22000

Использование:
  python kmk_batch_generator.py                       # 50k заказов, 90 дней
  python kmk_batch_generator.py --orders 100000 --days 180
  python kmk_batch_generator.py --mode incremental     # докинуть за сегодня
"""

import argparse
import os
import random
import uuid
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker

fake = Faker("ru_RU")
random.seed(42)

# ──────────────────────────────────────────────────────────────────
# СПРАВОЧНИК ПРОДУКЦИИ КМК
# ──────────────────────────────────────────────────────────────────

PRODUCTS = [
    # ═══════════ МОЛОКО ═══════════
    {"sku": "SZ-MLK-P-025-093", "name": "Молоко питьевое пастеризованное 2,5%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "пастеризованное",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 72.0, "shelf_life_days": 14, "gost": "ГОСТ 31450-2013", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-MLK-P-032-093", "name": "Молоко питьевое пастеризованное 3,2%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "пастеризованное",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 78.0, "shelf_life_days": 14, "gost": "ГОСТ 31450-2013", "fat_pct": 3.2, "halal": True},
    {"sku": "SZ-MLK-U-015-095", "name": "Молоко питьевое ультрапастеризованное 1,5%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "ультрапастеризованное",
     "volume": "950 мл", "weight_kg": 0.97, "unit": "ТБА", "pack_qty": 12, "base_price": 82.0, "shelf_life_days": 210, "gost": "ГОСТ 31450-2013", "fat_pct": 1.5, "halal": True},
    {"sku": "SZ-MLK-U-025-095", "name": "Молоко питьевое ультрапастеризованное 2,5%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "ультрапастеризованное",
     "volume": "950 мл", "weight_kg": 0.97, "unit": "ТБА", "pack_qty": 12, "base_price": 87.0, "shelf_life_days": 210, "gost": "ГОСТ 31450-2013", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-MLK-U-032-095", "name": "Молоко питьевое ультрапастеризованное 3,2%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "ультрапастеризованное",
     "volume": "950 мл", "weight_kg": 0.97, "unit": "ТБА", "pack_qty": 12, "base_price": 92.0, "shelf_life_days": 210, "gost": "ГОСТ 31450-2013", "fat_pct": 3.2, "halal": True},
    {"sku": "SZ-MLK-T-032-050", "name": "Молоко питьевое топлёное ультрапастеризованное 3,2%", "brand": "Село Зелёное", "category": "молоко", "subcategory": "топлёное",
     "volume": "500 г", "weight_kg": 0.50, "unit": "ТБА", "pack_qty": 12, "base_price": 65.0, "shelf_life_days": 120, "gost": "ГОСТ 31450-2013", "fat_pct": 3.2, "halal": True},
    {"sku": "MR-MLK-U-032-097", "name": "Молоко питьевое ультрапастеризованное 3,2%", "brand": "Молочная речка", "category": "молоко", "subcategory": "ультрапастеризованное",
     "volume": "970 г", "weight_kg": 0.97, "unit": "ТБА", "pack_qty": 12, "base_price": 68.0, "shelf_life_days": 210, "gost": "ГОСТ 31450-2013", "fat_pct": 3.2, "halal": True},
    {"sku": "MR-MLK-U-025-097", "name": "Молоко питьевое ультрапастеризованное 2,5%", "brand": "Молочная речка", "category": "молоко", "subcategory": "ультрапастеризованное",
     "volume": "970 г", "weight_kg": 0.97, "unit": "ТБА", "pack_qty": 12, "base_price": 64.0, "shelf_life_days": 210, "gost": "ГОСТ 31450-2013", "fat_pct": 2.5, "halal": True},

    # ═══════════ КЕФИР ═══════════
    {"sku": "SZ-KEF-010-093", "name": "Кефир 1%", "brand": "Село Зелёное", "category": "кефир", "subcategory": "классический",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 69.0, "shelf_life_days": 16, "gost": "ГОСТ 31454-2012", "fat_pct": 1.0, "halal": True},
    {"sku": "SZ-KEF-025-093", "name": "Кефир 2,5%", "brand": "Село Зелёное", "category": "кефир", "subcategory": "классический",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 74.0, "shelf_life_days": 16, "gost": "ГОСТ 31454-2012", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-KEF-032-093", "name": "Кефир 3,2%", "brand": "Село Зелёное", "category": "кефир", "subcategory": "классический",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 79.0, "shelf_life_days": 16, "gost": "ГОСТ 31454-2012", "fat_pct": 3.2, "halal": True},

    # ═══════════ РЯЖЕНКА ═══════════
    {"sku": "SZ-RZH-025-093", "name": "Ряженка 2,5%", "brand": "Село Зелёное", "category": "ряженка", "subcategory": "классическая",
     "volume": "930 г", "weight_kg": 0.93, "unit": "ПЭТ", "pack_qty": 6, "base_price": 75.0, "shelf_life_days": 14, "gost": "ГОСТ 31455-2012", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-RZH-032-043", "name": "Ряженка 3,2%", "brand": "Село Зелёное", "category": "ряженка", "subcategory": "классическая",
     "volume": "430 г", "weight_kg": 0.43, "unit": "ПЭТ", "pack_qty": 12, "base_price": 52.0, "shelf_life_days": 14, "gost": "ГОСТ 31455-2012", "fat_pct": 3.2, "halal": True},

    # ═══════════ СМЕТАНА ═══════════
    {"sku": "SZ-SMT-010-030", "name": "Сметана 10%", "brand": "Село Зелёное", "category": "сметана", "subcategory": "классическая",
     "volume": "300 г", "weight_kg": 0.30, "unit": "стакан", "pack_qty": 12, "base_price": 72.0, "shelf_life_days": 21, "gost": "ГОСТ 31452-2012", "fat_pct": 10.0, "halal": True},
    {"sku": "SZ-SMT-015-030", "name": "Сметана 15%", "brand": "Село Зелёное", "category": "сметана", "subcategory": "классическая",
     "volume": "300 г", "weight_kg": 0.30, "unit": "стакан", "pack_qty": 12, "base_price": 82.0, "shelf_life_days": 21, "gost": "ГОСТ 31452-2012", "fat_pct": 15.0, "halal": True},
    {"sku": "SZ-SMT-020-030", "name": "Сметана 20%", "brand": "Село Зелёное", "category": "сметана", "subcategory": "классическая",
     "volume": "300 г", "weight_kg": 0.30, "unit": "стакан", "pack_qty": 12, "base_price": 95.0, "shelf_life_days": 21, "gost": "ГОСТ 31452-2012", "fat_pct": 20.0, "halal": True},

    # ═══════════ ТВОРОГ ═══════════
    {"sku": "SZ-TVR-000-018", "name": "Творог обезжиренный", "brand": "Село Зелёное", "category": "творог", "subcategory": "классический",
     "volume": "180 г", "weight_kg": 0.18, "unit": "флоупак", "pack_qty": 16, "base_price": 52.0, "shelf_life_days": 14, "gost": "ГОСТ 31453-2013", "fat_pct": 0.2, "halal": True},
    {"sku": "SZ-TVR-018-020", "name": "Творог 1,8%", "brand": "Село Зелёное", "category": "творог", "subcategory": "классический",
     "volume": "200 г", "weight_kg": 0.20, "unit": "пакет", "pack_qty": 14, "base_price": 62.0, "shelf_life_days": 14, "gost": "ГОСТ 31453-2013", "fat_pct": 1.8, "halal": True},
    {"sku": "SZ-TVR-050-020", "name": "Творог 5%", "brand": "Село Зелёное", "category": "творог", "subcategory": "классический",
     "volume": "200 г", "weight_kg": 0.20, "unit": "пакет", "pack_qty": 14, "base_price": 72.0, "shelf_life_days": 14, "gost": "ГОСТ 31453-2013", "fat_pct": 5.0, "halal": True},
    {"sku": "SZ-TVR-090-030", "name": "Творог 9%", "brand": "Село Зелёное", "category": "творог", "subcategory": "классический",
     "volume": "300 г", "weight_kg": 0.30, "unit": "шайба", "pack_qty": 8, "base_price": 105.0, "shelf_life_days": 14, "gost": "ГОСТ 31453-2013", "fat_pct": 9.0, "halal": True},
    {"sku": "SZ-TVR-Z50-030", "name": "Творожное зерно 5%", "brand": "Село Зелёное", "category": "творог", "subcategory": "зернёный",
     "volume": "300 г", "weight_kg": 0.30, "unit": "стакан", "pack_qty": 8, "base_price": 115.0, "shelf_life_days": 14, "gost": "ТУ", "fat_pct": 5.0, "halal": True},
    {"sku": "MR-TVR-050-020", "name": "Творог 5%", "brand": "Молочная речка", "category": "творог", "subcategory": "классический",
     "volume": "200 г", "weight_kg": 0.20, "unit": "пакет", "pack_qty": 14, "base_price": 58.0, "shelf_life_days": 12, "gost": "ГОСТ 31453-2013", "fat_pct": 5.0, "halal": True},
    {"sku": "VK-TVR-050-020", "name": "Творог 5%", "brand": "Варвара Краса", "category": "творог", "subcategory": "классический",
     "volume": "200 г", "weight_kg": 0.20, "unit": "пакет", "pack_qty": 14, "base_price": 55.0, "shelf_life_days": 10, "gost": "ГОСТ 31453-2013", "fat_pct": 5.0, "halal": True},

    # ═══════════ СЛИВКИ ═══════════
    {"sku": "SZ-SLV-010-020", "name": "Сливки питьевые стерилизованные 10%", "brand": "Село Зелёное", "category": "сливки", "subcategory": "питьевые",
     "volume": "200 мл", "weight_kg": 0.21, "unit": "ТБА", "pack_qty": 24, "base_price": 68.0, "shelf_life_days": 180, "gost": "ГОСТ 34355-2017", "fat_pct": 10.0, "halal": True},
    {"sku": "SZ-SLV-010-050", "name": "Сливки питьевые стерилизованные 10%", "brand": "Село Зелёное", "category": "сливки", "subcategory": "питьевые",
     "volume": "500 мл", "weight_kg": 0.52, "unit": "ТБА", "pack_qty": 12, "base_price": 135.0, "shelf_life_days": 180, "gost": "ГОСТ 34355-2017", "fat_pct": 10.0, "halal": True},
    {"sku": "SZ-SLV-020-020", "name": "Сливки стерилизованные 20%", "brand": "Село Зелёное", "category": "сливки", "subcategory": "питьевые",
     "volume": "200 мл", "weight_kg": 0.21, "unit": "ТБА", "pack_qty": 24, "base_price": 82.0, "shelf_life_days": 180, "gost": "ГОСТ 34355-2017", "fat_pct": 20.0, "halal": True},
    {"sku": "MR-SLV-011-020", "name": "Сливки питьевые 11%", "brand": "Молочная речка", "category": "сливки", "subcategory": "питьевые",
     "volume": "200 мл", "weight_kg": 0.21, "unit": "ТБА", "pack_qty": 24, "base_price": 55.0, "shelf_life_days": 150, "gost": "ГОСТ 34355-2017", "fat_pct": 11.0, "halal": True},
    {"sku": "MR-SLV-022-100", "name": "Сливки питьевые 22%", "brand": "Молочная речка", "category": "сливки", "subcategory": "для кулинарии",
     "volume": "1000 мл", "weight_kg": 1.03, "unit": "ТБА", "pack_qty": 12, "base_price": 245.0, "shelf_life_days": 150, "gost": "ГОСТ 34355-2017", "fat_pct": 22.0, "halal": True},
    {"sku": "MR-SLV-033-100", "name": "Сливки для взбивания 33%", "brand": "Молочная речка", "category": "сливки", "subcategory": "для взбивания",
     "volume": "1000 мл", "weight_kg": 1.03, "unit": "ТБА", "pack_qty": 12, "base_price": 380.0, "shelf_life_days": 150, "gost": "ГОСТ 34355-2017", "fat_pct": 33.0, "halal": True},

    # ═══════════ КАТЫК ═══════════
    {"sku": "SZ-KTK-032-043", "name": "Катык 3,2%", "brand": "Село Зелёное", "category": "катык", "subcategory": "классический",
     "volume": "430 г", "weight_kg": 0.43, "unit": "ПЭТ", "pack_qty": 12, "base_price": 58.0, "shelf_life_days": 10, "gost": "ТУ", "fat_pct": 3.2, "halal": True},

    # ═══════════ СНЕЖОК ═══════════
    {"sku": "SZ-SNZ-032-043", "name": "Снежок 3,2%", "brand": "Село Зелёное", "category": "снежок", "subcategory": "сладкий",
     "volume": "430 г", "weight_kg": 0.43, "unit": "ПЭТ", "pack_qty": 12, "base_price": 54.0, "shelf_life_days": 10, "gost": "ТУ", "fat_pct": 3.2, "halal": True},

    # ═══════════ ЙОГУРТ ═══════════
    {"sku": "SZ-YGR-KLB-025", "name": "Йогурт питьевой клубника 2,5%", "brand": "Село Зелёное", "category": "йогурт", "subcategory": "питьевой фруктовый",
     "volume": "290 г", "weight_kg": 0.30, "unit": "ПЭТ", "pack_qty": 12, "base_price": 55.0, "shelf_life_days": 21, "gost": "ГОСТ 31981-2013", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-YGR-CHR-025", "name": "Йогурт питьевой черника 2,5%", "brand": "Село Зелёное", "category": "йогурт", "subcategory": "питьевой фруктовый",
     "volume": "290 г", "weight_kg": 0.30, "unit": "ПЭТ", "pack_qty": 12, "base_price": 55.0, "shelf_life_days": 21, "gost": "ГОСТ 31981-2013", "fat_pct": 2.5, "halal": True},
    {"sku": "SZ-YGR-ZLK-015", "name": "Йогурт питьевой злаки 1,5%", "brand": "Село Зелёное", "category": "йогурт", "subcategory": "питьевой фруктовый",
     "volume": "290 г", "weight_kg": 0.30, "unit": "ПЭТ", "pack_qty": 12, "base_price": 55.0, "shelf_life_days": 21, "gost": "ГОСТ 31981-2013", "fat_pct": 1.5, "halal": True},

    # ═══════════ МАСЛО ═══════════
    {"sku": "SZ-MSL-725-018", "name": "Масло сливочное Крестьянское 72,5%", "brand": "Село Зелёное", "category": "масло", "subcategory": "крестьянское",
     "volume": "180 г", "weight_kg": 0.18, "unit": "фольга", "pack_qty": 30, "base_price": 155.0, "shelf_life_days": 60, "gost": "ГОСТ 32261-2013", "fat_pct": 72.5, "halal": True},
    {"sku": "SZ-MSL-825-018", "name": "Масло сливочное Традиционное 82,5%", "brand": "Село Зелёное", "category": "масло", "subcategory": "традиционное",
     "volume": "180 г", "weight_kg": 0.18, "unit": "фольга", "pack_qty": 30, "base_price": 185.0, "shelf_life_days": 60, "gost": "ГОСТ 32261-2013", "fat_pct": 82.5, "halal": True},

    # ═══════════ СЫР ═══════════
    {"sku": "SZ-SYR-MOZ-030", "name": "Сыр рассольный Моцарелла для пиццы 40%", "brand": "Село Зелёное", "category": "сыр", "subcategory": "рассольный",
     "volume": "300 г", "weight_kg": 0.30, "unit": "картон", "pack_qty": 10, "base_price": 240.0, "shelf_life_days": 30, "gost": "ТУ", "fat_pct": 40.0, "halal": True},

    # ═══════════ КОКТЕЙЛИ ═══════════
    {"sku": "SZ-KKT-CAP-020", "name": "Коктейль молочный Капучино 3,2%", "brand": "Село Зелёное", "category": "коктейль", "subcategory": "молочный",
     "volume": "200 г", "weight_kg": 0.21, "unit": "ТБА", "pack_qty": 24, "base_price": 42.0, "shelf_life_days": 180, "gost": "ТУ", "fat_pct": 3.2, "halal": True},

    # ═══════════ СЫВОРОТКА ═══════════
    {"sku": "SZ-SYV-PMR-050", "name": "Напиток на основе молочной сыворотки персик-маракуйя", "brand": "Село Зелёное", "category": "сыворотка", "subcategory": "фруктовая",
     "volume": "500 мл", "weight_kg": 0.52, "unit": "ТБА", "pack_qty": 12, "base_price": 48.0, "shelf_life_days": 150, "gost": "ТУ", "fat_pct": 0.0, "halal": True},

    # ═══════════ ТОПТЫЖКА ═══════════
    {"sku": "TP-MLK-032-020", "name": "Молоко витаминизированное 3,2%", "brand": "Топтыжка", "category": "молоко", "subcategory": "детское",
     "volume": "200 мл", "weight_kg": 0.21, "unit": "ТБА с трубочкой", "pack_qty": 24, "base_price": 35.0, "shelf_life_days": 180, "gost": "ТУ", "fat_pct": 3.2, "halal": True},

    # ═══════════ ПРОСТОКВАША / НАРИНЭ ═══════════
    {"sku": "KMK-PRK-025-043", "name": "Простокваша Мечниковская 2,5%", "brand": "КМК", "category": "простокваша", "subcategory": "классическая",
     "volume": "430 г", "weight_kg": 0.43, "unit": "ПЭТ", "pack_qty": 12, "base_price": 52.0, "shelf_life_days": 7, "gost": "ТУ", "fat_pct": 2.5, "halal": True},
    {"sku": "KMK-NAR-025-043", "name": "Напиток кисломолочный Наринэ 2,5%", "brand": "КМК", "category": "наринэ", "subcategory": "функциональный",
     "volume": "430 г", "weight_kg": 0.43, "unit": "ПЭТ", "pack_qty": 12, "base_price": 56.0, "shelf_life_days": 10, "gost": "ТУ", "fat_pct": 2.5, "halal": True},

    # ═══════════ СУХАЯ ПРОДУКЦИЯ ═══════════
    {"sku": "KMK-SMS-025", "name": "Сыворотка молочная сухая", "brand": "КМК", "category": "сухие продукты", "subcategory": "промышленная",
     "volume": "25 кг", "weight_kg": 25.0, "unit": "мешок", "pack_qty": 1, "base_price": 4200.0, "shelf_life_days": 365, "gost": "ГОСТ 33958-2016", "fat_pct": 0.0, "halal": True},
    {"sku": "KMK-SOM-025", "name": "Сухое обезжиренное молоко (СОМ)", "brand": "КМК", "category": "сухие продукты", "subcategory": "промышленная",
     "volume": "25 кг", "weight_kg": 25.0, "unit": "мешок", "pack_qty": 1, "base_price": 6500.0, "shelf_life_days": 365, "gost": "ГОСТ 33629-2015", "fat_pct": 1.5, "halal": True},
]


# ──────────────────────────────────────────────────────────────────
# КЛИЕНТЫ
# ──────────────────────────────────────────────────────────────────

CUSTOMERS = [
    {"id": "CUST-001", "name": 'ООО "Бахетле"',                    "type": "retail_chain",  "inn": "1655065038", "contract": "РЦ-2024-001", "payment_terms_days": 30, "discount_pct": 12.0, "region": "Казань"},
    {"id": "CUST-002", "name": 'АО "Агроторг" (Пятёрочка)',        "type": "retail_chain",  "inn": "7825706086", "contract": "РЦ-2024-002", "payment_terms_days": 45, "discount_pct": 15.0, "region": "Казань"},
    {"id": "CUST-003", "name": 'ООО "Лента"',                      "type": "retail_chain",  "inn": "7814148471", "contract": "РЦ-2024-003", "payment_terms_days": 45, "discount_pct": 14.0, "region": "Казань"},
    {"id": "CUST-004", "name": 'АО "Тандер" (Магнит)',              "type": "retail_chain",  "inn": "2310031475", "contract": "РЦ-2024-004", "payment_terms_days": 60, "discount_pct": 16.0, "region": "Казань"},
    {"id": "CUST-005", "name": 'ООО "Эдельвейс"',                  "type": "retail_chain",  "inn": "1655123456", "contract": "РЦ-2024-005", "payment_terms_days": 30, "discount_pct": 10.0, "region": "Казань"},
    {"id": "CUST-006", "name": 'ООО "Перекрёсток" (X5)',            "type": "retail_chain",  "inn": "7728029110", "contract": "РЦ-2024-006", "payment_terms_days": 45, "discount_pct": 15.0, "region": "Казань"},
    {"id": "CUST-007", "name": 'ООО "Челны-Хлеб"',                 "type": "retail_chain",  "inn": "1650100001", "contract": "РЦ-2024-007", "payment_terms_days": 30, "discount_pct": 11.0, "region": "Набережные Челны"},
    {"id": "CUST-010", "name": 'ООО "Татнефть-АЗС-Центр"',         "type": "horeca", "inn": "1644012345", "contract": "HR-2024-010", "payment_terms_days": 14, "discount_pct": 5.0,  "region": "Казань"},
    {"id": "CUST-011", "name": 'ООО "Сеть кафе Добрая столовая"',   "type": "horeca", "inn": "1655098765", "contract": "HR-2024-011", "payment_terms_days": 14, "discount_pct": 7.0,  "region": "Казань"},
    {"id": "CUST-012", "name": 'ИП Хасанов Р.М. (кафе "Алан")',     "type": "horeca", "inn": "165500112233","contract": "HR-2024-012", "payment_terms_days": 7,  "discount_pct": 3.0,  "region": "Казань"},
    {"id": "CUST-013", "name": 'ООО "Пекарни Катюша"',              "type": "horeca", "inn": "1655667700", "contract": "HR-2024-014", "payment_terms_days": 7,  "discount_pct": 4.0,  "region": "Казань"},
    {"id": "CUST-020", "name": 'ООО "МилкТрейд"',                   "type": "distributor", "inn": "1655667788", "contract": "ДС-2024-020", "payment_terms_days": 30, "discount_pct": 18.0, "region": "Казань"},
    {"id": "CUST-021", "name": 'ООО "Продснаб Казань"',             "type": "distributor", "inn": "1655889900", "contract": "ДС-2024-021", "payment_terms_days": 30, "discount_pct": 17.0, "region": "Казань"},
    {"id": "CUST-022", "name": 'ООО "Волга-Опт"',                   "type": "distributor", "inn": "1655223344", "contract": "ДС-2024-022", "payment_terms_days": 45, "discount_pct": 20.0, "region": "Казань"},
    {"id": "CUST-030", "name": 'МУП "Департамент продовольствия г. Казани"', "type": "government", "inn": "1655001122", "contract": "ГК-2024-030", "payment_terms_days": 60, "discount_pct": 8.0, "region": "Казань"},
    {"id": "CUST-031", "name": 'ГБУ "Республиканская клиническая больница"', "type": "government", "inn": "1655002233", "contract": "ГК-2024-031", "payment_terms_days": 60, "discount_pct": 8.0, "region": "Казань"},
]


# ──────────────────────────────────────────────────────────────────
# СКЛАДЫ, ДОСТАВКА, МЕНЕДЖЕРЫ
# ──────────────────────────────────────────────────────────────────

WAREHOUSES = [
    {"id": "WH-MAIN",  "name": "Основной склад КМК",              "address": "г. Казань, ул. Академика Арбузова, 7"},
    {"id": "WH-UHT",   "name": "Склад УВТ-продукции",             "address": "г. Казань, ул. Академика Арбузова, 7, корп. 2"},
    {"id": "WH-DISTR", "name": "Распределительный центр «Восток»", "address": "г. Казань, ул. Тэцевская, 12"},
]

DELIVERY_POINTS = {
    "retail_chain": [
        "г. Казань, ул. Декабристов, 186 (Бахетле)", "г. Казань, пр. Ямашева, 71 (Пятёрочка)",
        "г. Казань, ул. Фучика, 90 (Лента)", "г. Казань, ул. Р. Зорге, 39 (Магнит)",
        "г. Казань, пр. Победы, 141 (Эдельвейс)", "г. Казань, ул. Петербургская, 1 (Перекрёсток)",
        "г. Набережные Челны, пр. Мира, 52Б (РЦ)", "г. Нижнекамск, пр. Химиков, 27 (РЦ)",
        "г. Альметьевск, ул. Ленина, 118 (РЦ)", "г. Зеленодольск, ул. Карла Маркса, 55",
    ],
    "horeca": [
        "г. Казань, ул. Баумана, 36", "г. Казань, ул. Кремлёвская, 17",
        "г. Казань, ул. Петербургская, 50", "г. Казань, ул. Пушкина, 19",
    ],
    "distributor": [
        "г. Казань, ул. Складская, 5 (МилкТрейд)", "г. Казань, ул. Тихорецкая, 11 (Продснаб)",
        "г. Казань, ул. Техническая, 22 (Волга-Опт)",
    ],
    "government": [
        "г. Казань, ул. Декабристов, 2 (ДП)", "г. Казань, ул. Оренбургский тракт, 138 (РКБ)",
    ],
}

SALES_MANAGERS = [
    {"id": "MGR-01", "name": "Фахрутдинова А.Р."}, {"id": "MGR-02", "name": "Галиуллин И.Ф."},
    {"id": "MGR-03", "name": "Сафиуллина Л.Н."},    {"id": "MGR-04", "name": "Зайцев Д.А."},
    {"id": "MGR-05", "name": "Нуриева Г.Т."},
]

CANCEL_REASONS = [
    "Клиент отказался от заказа", "Нет в наличии на складе",
    "Истёк срок подтверждения", "Ошибка оператора при оформлении",
    "Дубль заказа", "Не пройден входной контроль качества сырья",
]

PAYMENT_METHODS = ["bank_transfer", "factoring", "letter_of_credit"]

KMK_DETAILS = {
    "legal_name": 'ООО "Казанский молочный комбинат"',
    "inn": "1660297582", "kpp": "166001001", "ogrn": "1171690074126",
    "address": "420088, Республика Татарстан, г. Казань, ул. Академика Арбузова, д. 7",
    "holding": "КОМОС ГРУПП / АО МИЛКОМ",
}


# ══════════════════════════════════════════════════════════════════
# DDL — СОЗДАНИЕ СХЕМЫ И ТАБЛИЦ В POSTGRES
# ══════════════════════════════════════════════════════════════════

DDL = """
CREATE SCHEMA IF NOT EXISTS source_kmk;

-- ────────────────────────────────────────────────────────────────
-- Полная перезаливка (historical): чистим слой source_kmk
-- ────────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS source_kmk.returns              CASCADE;
DROP TABLE IF EXISTS source_kmk.payments             CASCADE;
DROP TABLE IF EXISTS source_kmk.order_status_history CASCADE;
DROP TABLE IF EXISTS source_kmk.shipments            CASCADE;
DROP TABLE IF EXISTS source_kmk.order_items          CASCADE;
DROP TABLE IF EXISTS source_kmk.orders               CASCADE;
DROP TABLE IF EXISTS source_kmk.customers            CASCADE;
DROP TABLE IF EXISTS source_kmk.products             CASCADE;

-- ────────────────────────────────────────────────────────────────
-- Reference / master data
-- ────────────────────────────────────────────────────────────────
CREATE TABLE source_kmk.customers (
    customer_id        VARCHAR(32)  PRIMARY KEY,
    customer_name      VARCHAR(256) NOT NULL,
    customer_type      VARCHAR(32)  NOT NULL,
    customer_inn       VARCHAR(20)  NOT NULL,
    contract_number    VARCHAR(64),
    payment_terms_days INTEGER,
    discount_pct       NUMERIC(5,2),
    customer_region    VARCHAR(128),
    created_at         TIMESTAMP    NOT NULL,
    updated_at         TIMESTAMP    NOT NULL
);

CREATE INDEX idx_customers_type   ON source_kmk.customers (customer_type);
CREATE INDEX idx_customers_region ON source_kmk.customers (customer_region);

CREATE TABLE source_kmk.products (
    sku             VARCHAR(32)  PRIMARY KEY,
    product_name    VARCHAR(256) NOT NULL,
    brand           VARCHAR(64)  NOT NULL,
    category        VARCHAR(64)  NOT NULL,
    subcategory     VARCHAR(64),
    volume          VARCHAR(32),
    packaging       VARCHAR(32),
    pack_qty        INTEGER,
    base_price      NUMERIC(10,2),
    weight_kg       NUMERIC(10,3),
    shelf_life_days INTEGER,
    gost            VARCHAR(32),
    fat_pct         NUMERIC(5,1),
    halal_certified BOOLEAN,
    created_at      TIMESTAMP    NOT NULL,
    updated_at      TIMESTAMP    NOT NULL
);

CREATE INDEX idx_products_brand    ON source_kmk.products (brand);
CREATE INDEX idx_products_category ON source_kmk.products (category);

-- ────────────────────────────────────────────────────────────────
-- Orders (header) + items + shipments (operational snapshots)
-- ────────────────────────────────────────────────────────────────
CREATE TABLE source_kmk.orders (
    order_id               VARCHAR(64)    PRIMARY KEY,
    supplier_name          VARCHAR(256)   NOT NULL,
    supplier_inn           VARCHAR(12)    NOT NULL,
    supplier_kpp           VARCHAR(9),
    customer_id            VARCHAR(32)    NOT NULL REFERENCES source_kmk.customers(customer_id),
    customer_name          VARCHAR(256)   NOT NULL,
    customer_type          VARCHAR(32)    NOT NULL,
    customer_inn           VARCHAR(20)    NOT NULL,
    customer_region        VARCHAR(128),
    contract_number        VARCHAR(64),
    warehouse_id           VARCHAR(32)    NOT NULL,
    warehouse_name         VARCHAR(256),
    warehouse_address      VARCHAR(512),
    delivery_address       VARCHAR(512),
    expected_delivery_date DATE,
    sales_manager_id       VARCHAR(16),
    sales_manager_name     VARCHAR(128),
    payment_method         VARCHAR(32),
    payment_terms_days     INTEGER,
    total_lines            INTEGER        NOT NULL,
    total_quantity         INTEGER        NOT NULL,
    total_weight_kg        NUMERIC(12,2)  NOT NULL,
    total_amount           NUMERIC(14,2)  NOT NULL,
    total_vat              NUMERIC(14,2),
    currency               VARCHAR(3)     DEFAULT 'RUB',
    status                 VARCHAR(32)    NOT NULL,
    cancel_reason          VARCHAR(256),
    created_at             TIMESTAMP      NOT NULL,
    updated_at             TIMESTAMP      NOT NULL,
    event_timestamp        TIMESTAMP      NOT NULL
);

CREATE INDEX idx_orders_event_ts ON source_kmk.orders (event_timestamp);
CREATE INDEX idx_orders_customer ON source_kmk.orders (customer_id);
CREATE INDEX idx_orders_status   ON source_kmk.orders (status);
CREATE INDEX idx_orders_created  ON source_kmk.orders (created_at);

CREATE TABLE source_kmk.order_items (
    order_id              VARCHAR(64)    NOT NULL REFERENCES source_kmk.orders(order_id),
    line_number           INTEGER        NOT NULL,
    sku                   VARCHAR(32)    NOT NULL REFERENCES source_kmk.products(sku),
    product_name          VARCHAR(256)   NOT NULL,
    brand                 VARCHAR(64)    NOT NULL,
    category              VARCHAR(64)    NOT NULL,
    subcategory           VARCHAR(64),
    volume                VARCHAR(32),
    packaging             VARCHAR(32),
    pack_qty              INTEGER,
    packs_ordered         INTEGER        NOT NULL,
    quantity              INTEGER        NOT NULL,
    unit_of_measure       VARCHAR(8)     DEFAULT 'шт',
    price_per_unit        NUMERIC(10,2)  NOT NULL,
    price_before_discount NUMERIC(10,2),
    discount_pct          NUMERIC(5,2),
    line_total            NUMERIC(14,2)  NOT NULL,
    vat_rate              NUMERIC(4,2),
    vat_amount            NUMERIC(14,2),
    weight_kg             NUMERIC(12,2),
    fat_pct               NUMERIC(5,1),
    shelf_life_days       INTEGER,
    gost                  VARCHAR(32),
    halal_certified       BOOLEAN,
    best_before           DATE,
    production_date       DATE,
    event_timestamp       TIMESTAMP      NOT NULL,
    PRIMARY KEY (order_id, line_number)
);

CREATE INDEX idx_items_event_ts ON source_kmk.order_items (event_timestamp);
CREATE INDEX idx_items_sku      ON source_kmk.order_items (sku);
CREATE INDEX idx_items_brand    ON source_kmk.order_items (brand);

CREATE TABLE source_kmk.shipments (
    shipment_id          VARCHAR(64)    PRIMARY KEY,
    order_id             VARCHAR(64)    NOT NULL REFERENCES source_kmk.orders(order_id),
    customer_id          VARCHAR(32)    NOT NULL,
    customer_name        VARCHAR(256),
    warehouse_id         VARCHAR(32),
    delivery_address     VARCHAR(512),
    status               VARCHAR(32)    NOT NULL,
    carrier              VARCHAR(256),
    vehicle_plate        VARCHAR(32),
    vehicle_type         VARCHAR(128),
    driver_name          VARCHAR(128),
    temperature_zone     VARCHAR(16),
    temperature_actual   NUMERIC(4,1),
    pallets_count        INTEGER,
    total_weight_kg      NUMERIC(12,2),
    shipped_at           TIMESTAMP,
    estimated_arrival    TIMESTAMP,
    actual_arrival       TIMESTAMP,
    waybill_number       VARCHAR(32),
    event_timestamp      TIMESTAMP      NOT NULL
);

CREATE INDEX idx_ship_event_ts ON source_kmk.shipments (event_timestamp);
CREATE INDEX idx_ship_order    ON source_kmk.shipments (order_id);
CREATE INDEX idx_ship_status   ON source_kmk.shipments (status);

-- ────────────────────────────────────────────────────────────────
-- Append-only "events": статусная история, оплаты, возвраты
-- ────────────────────────────────────────────────────────────────
CREATE TABLE source_kmk.order_status_history (
    event_id        VARCHAR(64)  PRIMARY KEY,
    order_id        VARCHAR(64)  NOT NULL REFERENCES source_kmk.orders(order_id),
    status          VARCHAR(32)  NOT NULL,
    status_ts       TIMESTAMP    NOT NULL,
    reason          VARCHAR(256),
    source_system   VARCHAR(32)  DEFAULT 'erp',
    event_timestamp TIMESTAMP    NOT NULL
);

CREATE INDEX idx_osh_order_ts ON source_kmk.order_status_history (order_id, status_ts);
CREATE INDEX idx_osh_status  ON source_kmk.order_status_history (status);

CREATE TABLE source_kmk.payments (
    payment_id      VARCHAR(64)   PRIMARY KEY,
    order_id        VARCHAR(64)   NOT NULL REFERENCES source_kmk.orders(order_id),
    customer_id     VARCHAR(32)   NOT NULL,
    method          VARCHAR(32),
    status          VARCHAR(32)   NOT NULL, -- planned/paid/partial/overdue
    amount_total    NUMERIC(14,2) NOT NULL,
    amount_paid     NUMERIC(14,2) NOT NULL,
    vat_amount      NUMERIC(14,2),
    currency        VARCHAR(3)    DEFAULT 'RUB',
    due_date        DATE,
    paid_at         TIMESTAMP,
    created_at      TIMESTAMP     NOT NULL,
    event_timestamp TIMESTAMP     NOT NULL
);

CREATE INDEX idx_pay_order   ON source_kmk.payments (order_id);
CREATE INDEX idx_pay_due     ON source_kmk.payments (due_date);
CREATE INDEX idx_pay_status  ON source_kmk.payments (status);

CREATE TABLE source_kmk.returns (
    return_id       VARCHAR(64)   PRIMARY KEY,
    order_id         VARCHAR(64)   NOT NULL REFERENCES source_kmk.orders(order_id),
    line_number      INTEGER       NOT NULL,
    sku              VARCHAR(32)   NOT NULL,
    quantity         INTEGER       NOT NULL,
    amount           NUMERIC(14,2) NOT NULL,
    reason           VARCHAR(256),
    created_at       TIMESTAMP     NOT NULL,
    event_timestamp  TIMESTAMP     NOT NULL
);

CREATE INDEX idx_ret_order ON source_kmk.returns (order_id);
"""


# ══════════════════════════════════════════════════════════════════
# БИЗНЕС-ЛОГИКА ГЕНЕРАЦИИ
# ══════════════════════════════════════════════════════════════════

def _order_profile(customer_type):
    profiles = {
        "retail_chain": {"min_items": 5,  "max_items": 18, "qty_packs_range": (4, 80)},
        "distributor":  {"min_items": 8,  "max_items": 25, "qty_packs_range": (20, 200)},
        "government":   {"min_items": 3,  "max_items": 10, "qty_packs_range": (10, 60)},
        "horeca":       {"min_items": 2,  "max_items": 6,  "qty_packs_range": (1, 8)},
    }
    return profiles.get(customer_type, profiles["horeca"])


def _select_products(customer_type, n_items):
    if customer_type == "horeca":
        preferred = [p for p in PRODUCTS if p["category"] in ("сливки", "молоко", "сметана", "творог", "масло")]
    elif customer_type == "government":
        preferred = [p for p in PRODUCTS if p["category"] in ("молоко", "кефир", "творог", "масло", "сметана")]
    else:
        preferred = PRODUCTS
    pool = preferred if len(preferred) >= n_items else PRODUCTS
    return random.sample(pool, min(n_items, len(pool)))


def _weekday_weight(dt):
    """Больше заказов в будни, меньше в выходные."""
    dow = dt.weekday()
    if dow < 5:
        return random.uniform(0.9, 1.2)
    elif dow == 5:
        return random.uniform(0.3, 0.5)
    else:
        return random.uniform(0.05, 0.15)


def _seasonality_factor(dt):
    """Летом молочка продаётся меньше, перед НГ — пик."""
    month = dt.month
    factors = {1: 1.15, 2: 1.0, 3: 1.05, 4: 1.0, 5: 0.95,
               6: 0.85, 7: 0.80, 8: 0.85, 9: 1.0, 10: 1.05,
               11: 1.10, 12: 1.25}
    return factors.get(month, 1.0)


def _new_event_id(prefix: str = "EVT") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12].upper()}"


def generate_order(order_dt):
    """
    Генерация одного заказа + связанных сущностей.

    Возвращает:
      order (snapshot), items (snapshot), shipment (snapshot|None),
      status_events (append-only), payments (append-only), returns (append-only)
    """
    customer = random.choice(CUSTOMERS)
    warehouse = random.choice(WAREHOUSES)
    manager = random.choice(SALES_MANAGERS)
    profile = _order_profile(customer["type"])

    hour = random.randint(6, 19)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    now = order_dt.replace(hour=hour, minute=minute, second=second)

    order_id = f"ORD-{now.strftime('%Y%m%d')}-{uuid.uuid4().hex[:8].upper()}"

    # бизнес-реальность: часть заказов отменяют
    is_cancelled = random.random() < 0.04
    cancel_reason = random.choice(CANCEL_REASONS) if is_cancelled else None

    n_items = random.randint(profile["min_items"], profile["max_items"])
    selected = _select_products(customer["type"], n_items)

    items = []
    total_amount = total_weight = total_qty = 0

    for idx, p in enumerate(selected, 1):
        n_packs = random.randint(*profile["qty_packs_range"])
        qty = n_packs * p["pack_qty"]
        price = round(p["base_price"] * (1 - customer["discount_pct"] / 100), 2)
        line_total = round(price * qty, 2)
        line_weight = round(p["weight_kg"] * qty, 2)
        vat_amount = round(line_total * 0.10 / 1.10, 2)

        total_amount += line_total
        total_weight += line_weight
        total_qty += qty

        items.append({
            "order_id": order_id, "line_number": idx,
            "sku": p["sku"], "product_name": p["name"], "brand": p["brand"],
            "category": p["category"], "subcategory": p["subcategory"],
            "volume": p["volume"], "packaging": p["unit"],
            "pack_qty": p["pack_qty"], "packs_ordered": n_packs,
            "quantity": qty, "unit_of_measure": "шт",
            "price_per_unit": price, "price_before_discount": p["base_price"],
            "discount_pct": customer["discount_pct"],
            "line_total": line_total, "vat_rate": 0.10, "vat_amount": vat_amount,
            "weight_kg": line_weight, "fat_pct": p["fat_pct"],
            "shelf_life_days": p["shelf_life_days"], "gost": p["gost"],
            "halal_certified": p["halal"],
            "best_before": (now + timedelta(days=p["shelf_life_days"])).strftime("%Y-%m-%d"),
            "production_date": now.strftime("%Y-%m-%d"),
            "event_timestamp": now,
        })

    delivery_points = DELIVERY_POINTS.get(customer["type"], DELIVERY_POINTS["horeca"])
    if customer["type"] == "horeca":
        delivery_date = (now + timedelta(hours=random.randint(4, 24))).date()
    elif customer["type"] == "government":
        delivery_date = (now + timedelta(days=random.randint(2, 5))).date()
    else:
        delivery_date = (now + timedelta(days=random.randint(1, 3))).date()

    # ────────────────────────────────────────────────────────────────
    # Shipment snapshot
    # ────────────────────────────────────────────────────────────────
    shipment = None
    shipped_at = None
    est_arrival = None
    act_arrival = None

    if not is_cancelled and random.random() < 0.70:
        ship_status = random.choices(
            ["shipped", "in_transit", "delivered", "partially_delivered", "delayed"],
            weights=[25, 25, 25, 10, 15], k=1
        )[0]

        letters = "АВЕКМНОРСТУХ"
        plate = (f"{random.choice(letters)}{random.randint(100,999)}"
                 f"{random.choice(letters)}{random.choice(letters)} "
                 f"{random.choice(['16','116','716'])}")

        shipped_at = now + timedelta(hours=random.randint(1, 6))
        est_arrival = shipped_at + timedelta(hours=random.randint(2, 12))

        # delivered может быть и с задержкой
        if ship_status in ("delivered", "partially_delivered"):
            act_arrival = shipped_at + timedelta(hours=random.randint(2, 16))
        elif ship_status == "delayed" and random.random() < 0.55:
            # часть "delayed" всё-таки доезжает в рамках горизонта данных
            act_arrival = shipped_at + timedelta(hours=random.randint(10, 30))

        shipment = {
            "shipment_id": f"SHP-{uuid.uuid4().hex[:8].upper()}",
            "order_id": order_id,
            "customer_id": customer["id"], "customer_name": customer["name"],
            "warehouse_id": warehouse["id"],
            "delivery_address": random.choice(delivery_points),
            "status": ship_status if act_arrival is None else ("delivered" if ship_status != "partially_delivered" else "partially_delivered"),
            "carrier": random.choice([
                "Собственный автопарк КМК", "ТК Деловые Линии",
                "ТК ПЭК", "ТК КИТ", "ТК Байкал-Сервис"
            ]),
            "vehicle_plate": plate,
            "vehicle_type": random.choice([
                "Рефрижератор ГАЗон NEXT", "Рефрижератор КАМАЗ 65207",
                "Рефрижератор ГАЗель NEXT"
            ]),
            "driver_name": fake.name(),
            "temperature_zone": random.choice(["2-6°C", "0-4°C", "2-4°C"]),
            "temperature_actual": round(random.uniform(1.5, 6.5), 1),
            "pallets_count": max(1, int(total_weight // 800)),
            "total_weight_kg": round(total_weight, 2),
            "shipped_at": shipped_at,
            "estimated_arrival": est_arrival,
            "actual_arrival": act_arrival,
            "waybill_number": f"ТН-{random.randint(100000, 999999)}",
            "event_timestamp": now,
        }

    # ────────────────────────────────────────────────────────────────
    # Status history (append-only)
    # ────────────────────────────────────────────────────────────────
    status_events = []
    def add_status(st, ts, reason=None, source="erp"):
        status_events.append({
            "event_id": _new_event_id("ST"),
            "order_id": order_id,
            "status": st,
            "status_ts": ts,
            "reason": reason,
            "source_system": source,
            "event_timestamp": ts,
        })

    add_status("new", now, source="erp")

    if is_cancelled:
        cancel_ts = now + timedelta(minutes=random.randint(10, 240))
        add_status("cancelled", cancel_ts, reason=cancel_reason, source="erp")
        final_status = "cancelled"
        last_ts = cancel_ts
    else:
        conf_ts = now + timedelta(minutes=random.randint(5, 90))
        add_status("confirmed", conf_ts, source="erp")
        asm_ts = conf_ts + timedelta(minutes=random.randint(30, 240))
        add_status("assembled", asm_ts, source="wms")

        last_ts = asm_ts
        final_status = "assembled"

        if shipment:
            add_status("shipped", shipped_at, source="tms")
            last_ts = max(last_ts, shipped_at)
            final_status = "shipped"

            # часть заказов попадает в in_transit
            if random.random() < 0.65:
                it_ts = shipped_at + timedelta(minutes=random.randint(10, 180))
                add_status("in_transit", it_ts, source="tms")
                last_ts = max(last_ts, it_ts)
                final_status = "in_transit"

            if shipment["status"] == "delayed":
                d_ts = est_arrival + timedelta(minutes=random.randint(30, 360))
                add_status("delayed", d_ts, reason="Ожидание окна разгрузки / пробки", source="tms")
                last_ts = max(last_ts, d_ts)
                final_status = "delayed"

            if act_arrival:
                add_status(
                    "delivered" if shipment["status"] != "partially_delivered" else "partially_delivered",
                    act_arrival,
                    source="tms"
                )
                last_ts = max(last_ts, act_arrival)
                final_status = "delivered" if shipment["status"] != "partially_delivered" else "partially_delivered"

    # ────────────────────────────────────────────────────────────────
    # Order snapshot (current state)
    # ────────────────────────────────────────────────────────────────
    order = {
        "order_id": order_id,
        "supplier_name": KMK_DETAILS["legal_name"],
        "supplier_inn": KMK_DETAILS["inn"], "supplier_kpp": KMK_DETAILS["kpp"],
        "customer_id": customer["id"], "customer_name": customer["name"],
        "customer_type": customer["type"], "customer_inn": customer["inn"],
        "customer_region": customer["region"], "contract_number": customer["contract"],
        "warehouse_id": warehouse["id"], "warehouse_name": warehouse["name"],
        "warehouse_address": warehouse["address"],
        "delivery_address": shipment["delivery_address"] if shipment else random.choice(delivery_points),
        "expected_delivery_date": delivery_date,
        "sales_manager_id": manager["id"], "sales_manager_name": manager["name"],
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_terms_days": customer["payment_terms_days"],
        "total_lines": len(items), "total_quantity": total_qty,
        "total_weight_kg": round(total_weight, 2),
        "total_amount": round(total_amount, 2),
        "total_vat": round(sum(i["vat_amount"] for i in items), 2),
        "currency": "RUB",
        "status": final_status,
        "cancel_reason": cancel_reason,
        "created_at": now,
        "updated_at": last_ts,
        "event_timestamp": last_ts,
    }

    # ────────────────────────────────────────────────────────────────
    # Payments (append-only)
    # ────────────────────────────────────────────────────────────────
    payments = []
    if not is_cancelled:
        today_d = datetime.utcnow().date()
        due_date = (now.date() + timedelta(days=int(customer["payment_terms_days"])))
        amount_total = float(order["total_amount"])
        vat_amount = float(order["total_vat"]) if order["total_vat"] is not None else None

        # часть клиентов платит авансом/по факту отгрузки
        prepay = (customer["type"] == "horeca" and random.random() < 0.25) or (random.random() < 0.05)

        if prepay:
            pay_status = "paid"
            paid_at = now + timedelta(hours=random.randint(1, 48))
            amount_paid = amount_total
        else:
            if due_date > today_d:
                pay_status = "planned"
                paid_at = None
                amount_paid = 0.0
            else:
                x = random.random()
                if x < 0.72:
                    pay_status = "paid"
                    paid_at = (act_arrival or (shipped_at or last_ts)) + timedelta(days=random.randint(0, 10))
                    amount_paid = amount_total
                elif x < 0.90:
                    pay_status = "partial"
                    paid_at = (act_arrival or (shipped_at or last_ts)) + timedelta(days=random.randint(0, 15))
                    amount_paid = round(amount_total * random.uniform(0.3, 0.9), 2)
                else:
                    pay_status = "overdue"
                    paid_at = None
                    amount_paid = 0.0

        payments.append({
            "payment_id": _new_event_id("PAY"),
            "order_id": order_id,
            "customer_id": customer["id"],
            "method": order["payment_method"],
            "status": pay_status,
            "amount_total": round(amount_total, 2),
            "amount_paid": round(amount_paid, 2),
            "vat_amount": round(vat_amount, 2) if vat_amount is not None else None,
            "currency": "RUB",
            "due_date": due_date,
            "paid_at": paid_at,
            "created_at": now,
            "event_timestamp": paid_at or last_ts,
        })

    # ────────────────────────────────────────────────────────────────
    # Returns (append-only)
    # ────────────────────────────────────────────────────────────────
    returns = []
    if (not is_cancelled) and act_arrival and random.random() < 0.03:
        n_ret_lines = 1 if random.random() < 0.75 else 2
        ret_lines = random.sample(items, min(n_ret_lines, len(items)))
        for li in ret_lines:
            max_qty = max(1, int(li["quantity"] * 0.1))
            ret_qty = random.randint(1, max_qty)
            ret_amount = round(float(li["price_per_unit"]) * ret_qty, 2)
            ret_ts = act_arrival + timedelta(days=random.randint(1, 10))
            returns.append({
                "return_id": _new_event_id("RET"),
                "order_id": order_id,
                "line_number": li["line_number"],
                "sku": li["sku"],
                "quantity": ret_qty,
                "amount": ret_amount,
                "reason": random.choice([
                    "Бой/повреждение при транспортировке",
                    "Несоответствие температуры хранения",
                    "Короткий срок годности при приемке",
                    "Пересорт",
                    "Возврат по качеству"
                ]),
                "created_at": ret_ts,
                "event_timestamp": ret_ts,
            })

    return order, items, shipment, status_events, payments, returns


# ══════════════════════════════════════════════════════════════════
# РАСПРЕДЕЛЕНИЕ ЗАКАЗОВ ПО ДНЯМ
# ══════════════════════════════════════════════════════════════════

def distribute_orders_over_days(total_orders, days_back):
    """Распределяет заказы по дням с учётом дня недели и сезонности."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start = today - timedelta(days=days_back)

    raw_weights = []
    dates = []
    for i in range(days_back):
        dt = start + timedelta(days=i)
        w = _weekday_weight(dt) * _seasonality_factor(dt)
        raw_weights.append(w)
        dates.append(dt)

    total_w = sum(raw_weights)
    counts = [max(1, int(round(total_orders * w / total_w))) for w in raw_weights]

    diff = total_orders - sum(counts)
    for i in range(abs(diff)):
        idx = random.randint(0, len(counts) - 1)
        counts[idx] += 1 if diff > 0 else -1
        counts[idx] = max(1, counts[idx])

    return list(zip(dates, counts))


# ══════════════════════════════════════════════════════════════════
# BULK INSERT В POSTGRES
# ══════════════════════════════════════════════════════════════════

ORDER_COLS = [
    "order_id", "supplier_name", "supplier_inn", "supplier_kpp",
    "customer_id", "customer_name", "customer_type", "customer_inn",
    "customer_region", "contract_number",
    "warehouse_id", "warehouse_name", "warehouse_address",
    "delivery_address", "expected_delivery_date",
    "sales_manager_id", "sales_manager_name",
    "payment_method", "payment_terms_days",
    "total_lines", "total_quantity", "total_weight_kg",
    "total_amount", "total_vat", "currency",
    "status", "cancel_reason",
    "created_at", "updated_at", "event_timestamp",
]

ITEM_COLS = [
    "order_id", "line_number", "sku", "product_name", "brand",
    "category", "subcategory", "volume", "packaging",
    "pack_qty", "packs_ordered", "quantity", "unit_of_measure",
    "price_per_unit", "price_before_discount", "discount_pct",
    "line_total", "vat_rate", "vat_amount",
    "weight_kg", "fat_pct", "shelf_life_days", "gost",
    "halal_certified", "best_before", "production_date",
    "event_timestamp",
]

SHIP_COLS = [
    "shipment_id", "order_id", "customer_id", "customer_name",
    "warehouse_id", "delivery_address", "status",
    "carrier", "vehicle_plate", "vehicle_type", "driver_name",
    "temperature_zone", "temperature_actual",
    "pallets_count", "total_weight_kg",
    "shipped_at", "estimated_arrival", "actual_arrival",
    "waybill_number", "event_timestamp",
]

CUSTOMER_COLS = [
    "customer_id", "customer_name", "customer_type", "customer_inn",
    "contract_number", "payment_terms_days", "discount_pct", "customer_region",
    "created_at", "updated_at",
]

PRODUCT_COLS = [
    "sku", "product_name", "brand", "category", "subcategory",
    "volume", "packaging", "pack_qty",
    "base_price", "weight_kg", "shelf_life_days", "gost", "fat_pct",
    "halal_certified",
    "created_at", "updated_at",
]

STATUS_COLS = [
    "event_id", "order_id", "status", "status_ts", "reason", "source_system", "event_timestamp",
]

PAYMENT_COLS = [
    "payment_id", "order_id", "customer_id", "method", "status",
    "amount_total", "amount_paid", "vat_amount", "currency",
    "due_date", "paid_at", "created_at", "event_timestamp",
]

RETURN_COLS = [
    "return_id", "order_id", "line_number", "sku", "quantity", "amount",
    "reason", "created_at", "event_timestamp",
]


def _to_tuple(d, cols):
    return tuple(d[c] for c in cols)


def insert_batch(conn, orders_buf, items_buf, ships_buf, status_buf=None, payments_buf=None, returns_buf=None):
    """
    Bulk-вставка через execute_values.

    Snapshot-таблицы (orders/shipments) — upsert (в реальности статусы меняются).
    Event-таблицы (status_history/payments/returns) — append-only.
    """
    status_buf = status_buf or []
    payments_buf = payments_buf or []
    returns_buf = returns_buf or []

    with conn.cursor() as cur:
        if orders_buf:
            cols = ", ".join(ORDER_COLS)
            execute_values(
                cur,
                f"""
                INSERT INTO source_kmk.orders ({cols}) VALUES %s
                ON CONFLICT (order_id) DO UPDATE SET
                    status=EXCLUDED.status,
                    cancel_reason=EXCLUDED.cancel_reason,
                    updated_at=EXCLUDED.updated_at,
                    event_timestamp=EXCLUDED.event_timestamp
                """,
                [_to_tuple(o, ORDER_COLS) for o in orders_buf],
                page_size=1000,
            )

        if items_buf:
            cols = ", ".join(ITEM_COLS)
            execute_values(
                cur,
                f"INSERT INTO source_kmk.order_items ({cols}) VALUES %s ON CONFLICT DO NOTHING",
                [_to_tuple(i, ITEM_COLS) for i in items_buf],
                page_size=5000,
            )

        if ships_buf:
            cols = ", ".join(SHIP_COLS)
            execute_values(
                cur,
                f"""
                INSERT INTO source_kmk.shipments ({cols}) VALUES %s
                ON CONFLICT (shipment_id) DO UPDATE SET
                    status=EXCLUDED.status,
                    temperature_actual=EXCLUDED.temperature_actual,
                    pallets_count=EXCLUDED.pallets_count,
                    total_weight_kg=EXCLUDED.total_weight_kg,
                    shipped_at=EXCLUDED.shipped_at,
                    estimated_arrival=EXCLUDED.estimated_arrival,
                    actual_arrival=EXCLUDED.actual_arrival,
                    event_timestamp=EXCLUDED.event_timestamp
                """,
                [_to_tuple(s, SHIP_COLS) for s in ships_buf],
                page_size=1000,
            )

        if status_buf:
            cols = ", ".join(STATUS_COLS)
            execute_values(
                cur,
                f"INSERT INTO source_kmk.order_status_history ({cols}) VALUES %s ON CONFLICT DO NOTHING",
                [_to_tuple(e, STATUS_COLS) for e in status_buf],
                page_size=5000,
            )

        if payments_buf:
            cols = ", ".join(PAYMENT_COLS)
            execute_values(
                cur,
                f"INSERT INTO source_kmk.payments ({cols}) VALUES %s ON CONFLICT DO NOTHING",
                [_to_tuple(p, PAYMENT_COLS) for p in payments_buf],
                page_size=2000,
            )

        if returns_buf:
            cols = ", ".join(RETURN_COLS)
            execute_values(
                cur,
                f"INSERT INTO source_kmk.returns ({cols}) VALUES %s ON CONFLICT DO NOTHING",
                [_to_tuple(r, RETURN_COLS) for r in returns_buf],
                page_size=2000,
            )

    conn.commit()


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "postgres"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "kmk"),
        user=os.getenv("PG_USER", "admin"),
        password=os.getenv("PG_PASSWORD", "admin"),
    )

def insert_reference_data(conn):
    """Заполняет справочники (customers/products)."""
    now = datetime.utcnow()

    customers_rows = []
    for c in CUSTOMERS:
        customers_rows.append({
            "customer_id": c["id"],
            "customer_name": c["name"],
            "customer_type": c["type"],
            "customer_inn": c["inn"],
            "contract_number": c.get("contract"),
            "payment_terms_days": int(c.get("payment_terms_days") or 0),
            "discount_pct": float(c.get("discount_pct") or 0.0),
            "customer_region": c.get("region"),
            "created_at": now,
            "updated_at": now,
        })

    products_rows = []
    for p in PRODUCTS:
        products_rows.append({
            "sku": p["sku"],
            "product_name": p["name"],
            "brand": p["brand"],
            "category": p["category"],
            "subcategory": p.get("subcategory"),
            "volume": p.get("volume"),
            "packaging": p.get("unit"),
            "pack_qty": int(p.get("pack_qty") or 0),
            "base_price": float(p.get("base_price") or 0.0),
            "weight_kg": float(p.get("weight_kg") or 0.0),
            "shelf_life_days": int(p.get("shelf_life_days") or 0),
            "gost": p.get("gost"),
            "fat_pct": float(p.get("fat_pct") or 0.0),
            "halal_certified": bool(p.get("halal")),
            "created_at": now,
            "updated_at": now,
        })

    with conn.cursor() as cur:
        cols = ", ".join(CUSTOMER_COLS)
        execute_values(
            cur,
            f"""
            INSERT INTO source_kmk.customers ({cols}) VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name=EXCLUDED.customer_name,
                customer_type=EXCLUDED.customer_type,
                customer_inn=EXCLUDED.customer_inn,
                contract_number=EXCLUDED.contract_number,
                payment_terms_days=EXCLUDED.payment_terms_days,
                discount_pct=EXCLUDED.discount_pct,
                customer_region=EXCLUDED.customer_region,
                updated_at=EXCLUDED.updated_at
            """,
            [_to_tuple(r, CUSTOMER_COLS) for r in customers_rows],
            page_size=500,
        )

        cols = ", ".join(PRODUCT_COLS)
        execute_values(
            cur,
            f"""
            INSERT INTO source_kmk.products ({cols}) VALUES %s
            ON CONFLICT (sku) DO UPDATE SET
                product_name=EXCLUDED.product_name,
                brand=EXCLUDED.brand,
                category=EXCLUDED.category,
                subcategory=EXCLUDED.subcategory,
                volume=EXCLUDED.volume,
                packaging=EXCLUDED.packaging,
                pack_qty=EXCLUDED.pack_qty,
                base_price=EXCLUDED.base_price,
                weight_kg=EXCLUDED.weight_kg,
                shelf_life_days=EXCLUDED.shelf_life_days,
                gost=EXCLUDED.gost,
                fat_pct=EXCLUDED.fat_pct,
                halal_certified=EXCLUDED.halal_certified,
                updated_at=EXCLUDED.updated_at
            """,
            [_to_tuple(r, PRODUCT_COLS) for r in products_rows],
            page_size=1000,
        )
    conn.commit()


def run_historical(total_orders, days_back):
    """Полная историческая генерация: DROP + CREATE + заполнение."""
    conn = get_connection()

    print("=" * 70)
    print(f"  КМК Batch Generator → PostgreSQL")
    print(f"  {KMK_DETAILS['legal_name']}")
    print(f"  Холдинг:   {KMK_DETAILS['holding']}")
    print(f"  Режим:     HISTORICAL (полная перезаливка)")
    print(f"  Заказов:   {total_orders:,}")
    print(f"  Период:    {days_back} дней назад → сегодня")
    print(f"  Каталог:   {len(PRODUCTS)} SKU, {len(set(p['brand'] for p in PRODUCTS))} брендов")
    print(f"  Клиенты:   {len(CUSTOMERS)}")
    print("=" * 70)

    print("\n[1/4] Создание схемы и таблиц...")
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    insert_reference_data(conn)
    print("      ✓ source_kmk.customers, products")
    print("      ✓ source_kmk.orders, order_items, shipments")
    print("      ✓ source_kmk.order_status_history, payments, returns")

    print(f"\n[2/4] Распределение {total_orders:,} заказов по {days_back} дням...")
    daily_plan = distribute_orders_over_days(total_orders, days_back)

    print(f"\n[3/4] Генерация и вставка...\n")

    BATCH_SIZE = 300
    orders_buf, items_buf, ships_buf = [], [], []
    status_buf, payments_buf, returns_buf = [], [], []

    total_generated = 0
    total_items = 0
    total_ships = 0
    total_status = 0
    total_payments = 0
    total_returns = 0

    for day_dt, day_count in daily_plan:
        for _ in range(day_count):
            order, items, shipment, status_events, payments, rets = generate_order(day_dt)

            orders_buf.append(order)
            items_buf.extend(items)
            status_buf.extend(status_events)
            payments_buf.extend(payments)
            returns_buf.extend(rets)

            total_items += len(items)
            total_status += len(status_events)
            total_payments += len(payments)
            total_returns += len(rets)

            if shipment:
                ships_buf.append(shipment)
                total_ships += 1

            total_generated += 1

            if len(orders_buf) >= BATCH_SIZE:
                insert_batch(conn, orders_buf, items_buf, ships_buf, status_buf, payments_buf, returns_buf)
                orders_buf, items_buf, ships_buf = [], [], []
                status_buf, payments_buf, returns_buf = [], [], []

        # прогресс каждые ~10 дней
        if (day_dt - daily_plan[0][0]).days % 10 == 0:
            pct = total_generated / total_orders * 100
            print(
                f"  {day_dt.strftime('%Y-%m-%d')} | "
                f"{total_generated:>7,}/{total_orders:,} ({pct:5.1f}%) | "
                f"{total_items:,} позиций | {total_ships:,} отгрузок | "
                f"{total_payments:,} оплат | {total_returns:,} возвратов"
            )

    if orders_buf:
        insert_batch(conn, orders_buf, items_buf, ships_buf, status_buf, payments_buf, returns_buf)

    conn.close()

    print(f"\n{'=' * 70}")
    print(f"  ГОТОВО!")
    print(f"  Заказов:      {total_generated:,}")
    print(f"  Позиций:      {total_items:,}")
    print(f"  Отгрузок:     {total_ships:,}")
    print(f"  Статус-событ.: {total_status:,}")
    print(f"  Оплат:        {total_payments:,}")
    print(f"  Возвратов:    {total_returns:,}")
    print(f"{'=' * 70}")

def run_incremental():
    """
    Инкрементальная генерация — новые заказы за сегодня + "late arriving" события
    (обновления статусов/доставки/оплат) по заказам последних дней.
    """
    conn = get_connection()
    insert_reference_data(conn)

    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    day_count = random.randint(400, 700)
    factor = _weekday_weight(today) * _seasonality_factor(today)
    day_count = max(50, int(day_count * factor))

    print(f"[incremental] Новые заказы: ~{day_count} за {today.strftime('%Y-%m-%d')}")

    orders_buf, items_buf, ships_buf = [], [], []
    status_buf, payments_buf, returns_buf = [], [], []

    total_items = 0
    total_ships = 0
    total_status = 0
    total_payments = 0
    total_returns = 0

    for _ in range(day_count):
        order, items, shipment, status_events, payments, rets = generate_order(today)
        orders_buf.append(order)
        items_buf.extend(items)
        status_buf.extend(status_events)
        payments_buf.extend(payments)
        returns_buf.extend(rets)

        total_items += len(items)
        total_status += len(status_events)
        total_payments += len(payments)
        total_returns += len(rets)

        if shipment:
            ships_buf.append(shipment)
            total_ships += 1

    insert_batch(conn, orders_buf, items_buf, ships_buf, status_buf, payments_buf, returns_buf)

    # ────────────────────────────────────────────────────────────────
    # Late arriving: обновляем часть заказов последних 7 дней
    # ────────────────────────────────────────────────────────────────
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT order_id, status, created_at
            FROM source_kmk.orders
            WHERE created_at >= %s
              AND status NOT IN ('delivered', 'cancelled')
            ORDER BY created_at DESC
            LIMIT 300
            """,
            (today - timedelta(days=7),),
        )
        recent = cur.fetchall()

    if recent:
        n = min(60, max(10, int(len(recent) * 0.2)))
        picked = random.sample(recent, n)

        late_ships, late_status, late_pay, late_ret = [], [], [], []

        def add_status(order_id, st, ts, reason=None, source="tms"):
            late_status.append({
                "event_id": _new_event_id("ST"),
                "order_id": order_id,
                "status": st,
                "status_ts": ts,
                "reason": reason,
                "source_system": source,
                "event_timestamp": ts,
            })

        with conn.cursor() as cur:
            for order_id, cur_status, created_at in picked:
                # подтягиваем минимум для отгрузки
                cur.execute(
                    """
                    SELECT order_id, customer_id, customer_name, warehouse_id, delivery_address,
                           total_weight_kg, status, updated_at, payment_method, payment_terms_days,
                           total_amount, total_vat
                    FROM source_kmk.orders
                    WHERE order_id = %s
                    """,
                    (order_id,),
                )
                row = cur.fetchone()
                if not row:
                    continue

                (oid, customer_id, customer_name, wh_id, addr,
                 total_weight, st, updated_at, pay_method, pay_terms, total_amount, total_vat) = row

                # текущая отгрузка (если есть)
                cur.execute(
                    "SELECT shipment_id, status, shipped_at, estimated_arrival, actual_arrival FROM source_kmk.shipments WHERE order_id=%s LIMIT 1",
                    (oid,),
                )
                ship_row = cur.fetchone()

                ts_base = max(updated_at, created_at) + timedelta(minutes=random.randint(30, 360))

                # простая "машина состояний"
                next_status = None
                if st in ("new",):
                    next_status = "confirmed"
                    add_status(oid, "confirmed", ts_base, source="erp")
                elif st in ("confirmed",):
                    next_status = "assembled"
                    add_status(oid, "assembled", ts_base, source="wms")
                elif st in ("assembled", "shipped", "in_transit", "delayed", "partially_delivered"):
                    # переводим к доставке
                    next_status = "delivered"
                    if not ship_row:
                        shipped_at = ts_base
                        est_arr = shipped_at + timedelta(hours=random.randint(2, 12))
                        act_arr = shipped_at + timedelta(hours=random.randint(3, 18))
                        late_ships.append({
                            "shipment_id": f"SHP-{uuid.uuid4().hex[:8].upper()}",
                            "order_id": oid,
                            "customer_id": customer_id,
                            "customer_name": customer_name,
                            "warehouse_id": wh_id,
                            "delivery_address": addr,
                            "status": "delivered",
                            "carrier": random.choice(["Собственный автопарк КМК", "ТК Деловые Линии", "ТК ПЭК"]),
                            "vehicle_plate": None,
                            "vehicle_type": None,
                            "driver_name": fake.name(),
                            "temperature_zone": random.choice(["2-6°C", "0-4°C", "2-4°C"]),
                            "temperature_actual": round(random.uniform(1.5, 6.5), 1),
                            "pallets_count": max(1, int(float(total_weight) // 800)),
                            "total_weight_kg": float(total_weight),
                            "shipped_at": shipped_at,
                            "estimated_arrival": est_arr,
                            "actual_arrival": act_arr,
                            "waybill_number": f"ТН-{random.randint(100000, 999999)}",
                            "event_timestamp": act_arr,
                        })
                        add_status(oid, "shipped", shipped_at, source="tms")
                        add_status(oid, "delivered", act_arr, source="tms")
                        ts_base = act_arr
                    else:
                        shipment_id, sst, shipped_at, est_arr, act_arr = ship_row
                        if not shipped_at:
                            shipped_at = ts_base
                        if not est_arr:
                            est_arr = shipped_at + timedelta(hours=random.randint(2, 12))
                        if not act_arr:
                            act_arr = shipped_at + timedelta(hours=random.randint(3, 18))
                        # апдейт snapshot-таблицы shipments через upsert
                        late_ships.append({
                            "shipment_id": shipment_id,
                            "order_id": oid,
                            "customer_id": customer_id,
                            "customer_name": customer_name,
                            "warehouse_id": wh_id,
                            "delivery_address": addr,
                            "status": "delivered",
                            "carrier": None,
                            "vehicle_plate": None,
                            "vehicle_type": None,
                            "driver_name": None,
                            "temperature_zone": None,
                            "temperature_actual": round(random.uniform(1.5, 6.5), 1),
                            "pallets_count": None,
                            "total_weight_kg": float(total_weight),
                            "shipped_at": shipped_at,
                            "estimated_arrival": est_arr,
                            "actual_arrival": act_arr,
                            "waybill_number": None,
                            "event_timestamp": act_arr,
                        })
                        add_status(oid, "delivered", act_arr, source="tms")
                        ts_base = act_arr

                    # оплата: создаём только если её ещё не было
                    cur.execute("SELECT 1 FROM source_kmk.payments WHERE order_id=%s LIMIT 1", (oid,))
                    if not cur.fetchone():
                        due_date = created_at.date() + timedelta(days=int(pay_terms or 0))
                        late_pay.append({
                            "payment_id": _new_event_id("PAY"),
                            "order_id": oid,
                            "customer_id": customer_id,
                            "method": pay_method,
                            "status": "paid" if random.random() < 0.75 else "planned",
                            "amount_total": float(total_amount),
                            "amount_paid": float(total_amount) if random.random() < 0.75 else 0.0,
                            "vat_amount": float(total_vat) if total_vat is not None else None,
                            "currency": "RUB",
                            "due_date": due_date,
                            "paid_at": ts_base + timedelta(days=random.randint(0, 7)) if random.random() < 0.75 else None,
                            "created_at": created_at,
                            "event_timestamp": ts_base,
                        })

                    # редкий возврат после доставки
                    if random.random() < 0.02:
                        # берём любую строку заказа
                        cur.execute(
                            "SELECT line_number, sku, price_per_unit, quantity FROM source_kmk.order_items WHERE order_id=%s ORDER BY random() LIMIT 1",
                            (oid,),
                        )
                        li = cur.fetchone()
                        if li:
                            line_number, sku, ppu, qty = li
                            ret_qty = random.randint(1, max(1, int(qty * 0.05)))
                            ret_ts = ts_base + timedelta(days=random.randint(1, 10))
                            late_ret.append({
                                "return_id": _new_event_id("RET"),
                                "order_id": oid,
                                "line_number": int(line_number),
                                "sku": sku,
                                "quantity": int(ret_qty),
                                "amount": round(float(ppu) * ret_qty, 2),
                                "reason": "Возврат по качеству (late event)",
                                "created_at": ret_ts,
                                "event_timestamp": ret_ts,
                            })

                if next_status:
                    cur.execute(
                        "UPDATE source_kmk.orders SET status=%s, updated_at=%s, event_timestamp=%s WHERE order_id=%s",
                        (next_status, ts_base, ts_base, oid),
                    )

            conn.commit()

        # записываем late события пачкой (orders уже обновили через UPDATE)
        if late_ships or late_status or late_pay or late_ret:
            insert_batch(conn, [], [], late_ships, late_status, late_pay, late_ret)
            print(f"[incremental] late events: {len(picked)} заказов обновлено, "
                  f"{len(late_ships)} отгрузок upsert, {len(late_status)} статус-событий, "
                  f"{len(late_pay)} оплат, {len(late_ret)} возвратов")

    conn.close()

    print(f"  ✓ {day_count} новых заказов, {total_items} позиций, {total_ships} отгрузок, "
          f"{total_payments} оплат, {total_returns} возвратов")

# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="КМК Batch Generator → PostgreSQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python kmk_batch_generator.py                          # 50k заказов, 90 дней
  python kmk_batch_generator.py --orders 100000 --days 180
  python kmk_batch_generator.py --mode incremental       # только за сегодня
        """)
    parser.add_argument("--mode", choices=["historical", "incremental"],
                        default="historical",
                        help="historical = полная перезаливка, incremental = только сегодня")
    parser.add_argument("--orders", type=int, default=50_000,
                        help="Кол-во заказов для historical (default: 50000)")
    parser.add_argument("--days", type=int, default=90,
                        help="За сколько дней назад (default: 90)")
    args = parser.parse_args()

    if args.mode == "historical":
        run_historical(args.orders, args.days)
    else:
        run_incremental()
