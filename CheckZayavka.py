import logging
import psycopg2
import re
import json
import decimal
from thefuzz import fuzz

# -----------------------------------------------------------------------------
# НАСТРОЙКА ЛОГИРОВАНИЯ
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Словари для нормализации форм и нумерации
# -----------------------------------------------------------------------------
FORMS = {
    'таблетка': 'таблетки',
    'таблетки': 'таблетки',
    'табл': 'таблетки',
    'tabl': 'таблетки',

    'капс': 'капсулы',
    'капсула': 'капсулы',
    'капсулы': 'капсулы',

    'капли': 'капли',
    'спрей': 'спрей',
    'аэрозоль': 'спрей',

    'раствор': 'раствор',
    'р-р': 'раствор',

    'инъекции': 'инъекции',
    'амп': 'инъекции',
    'амп.': 'инъекции',

    'свечи': 'свечи',
    'супп': 'свечи',
    'супп.': 'свечи'
}

NUMBERING = {
    'n1': '№1', '№1': '№1',
    'n10': '№10', '№10': '№10',
    'n20': '№20', '№20': '№20',
    'n30': '№30', '№30': '№30'
}

UNIT_CONVERSION = {
    'мг': 0.001, 'mg': 0.001,
    'г': 1.0, 'g': 1.0,
    'кг': 1000.0, 'kg': 1000.0,
    'мл': 1.0, 'л': 1000.0, 'ml': 1.0, 'l': 1000.0
}

# -----------------------------------------------------------------------------
# ФУНКЦИИ ДЛЯ ПАРСИНГА
# -----------------------------------------------------------------------------
def parse_dosages(text: str):
    """
    Ищем шаблон (число + единица измерения), например 500мг, 10мл.
    Возвращаем список (val, unit).
    """
    if not text:
        return []
    text = text.replace(',', '.')
    pattern = re.compile(r'(\d+(?:\.\d+)?)(\s?(?:мг|mg|г|g|кг|kg|мл|л|%|ml|l)?)', re.IGNORECASE)
    matches = pattern.findall(text)
    results = []
    for (num_str, unit_str) in matches:
        unit_str = unit_str.strip().lower()
        try:
            val = float(num_str)
        except:
            val = 0.0

        if unit_str == '%':
            val = val / 100.0
            unit_str = 'fraction'
        elif unit_str in UNIT_CONVERSION:
            factor = UNIT_CONVERSION[unit_str]
            val *= factor
            # Приведём разные единицы к общим обозначениям
            if unit_str in ['мг','mg','г','g','кг','kg']:
                unit_str = 'g'
            elif unit_str in ['мл','л','ml','l']:
                unit_str = 'ml'
        else:
            unit_str = ''
        results.append((val, unit_str))
    results.sort()
    return results

def detailed_parse_name(name: str):
    """
    Парсим название на brand1, brand2, brand3, form, numbering, dosage, tail
    Дозировки убираем из текста через parse_dosages.
    """
    original = name or ""
    dos_list = parse_dosages(original)

    # Убираем дозировки из строки
    tmp = original.replace(',', '.').lower()
    pat = re.compile(r'(\d+(?:\.\d+)?)(\s?(?:мг|mg|г|g|кг|kg|мл|л|%|ml|l)?)', re.IGNORECASE)
    cleaned = pat.sub(' ', tmp)
    cleaned = re.sub(r'[^\w\d\s]+', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    tokens = cleaned.split()
    brand1, brand2, brand3 = '', '', ''
    form_ = ''
    number_ = ''
    tail = []

    for t in tokens:
        if t in FORMS and not form_:
            form_ = FORMS[t]
        elif t in NUMBERING and not number_:
            number_ = NUMBERING[t]
        else:
            # Заполняем brand1/brand2/brand3, остальное в tail
            if not brand1:
                brand1 = t
            elif not brand2:
                brand2 = t
            elif not brand3:
                brand3 = t
            else:
                tail.append(t)

    tail_str = ' '.join(tail)
    return {
        'brand1': brand1,
        'brand2': brand2,
        'brand3': brand3,
        'form': form_,
        'numbering': number_,
        'dosage': dos_list,
        'tail': tail_str
    }

def partial_dosage_score(d1, d2, tolerance=0.05, strict_threshold=0.8):
    """Сопоставление списков дозировок (val, unit). Возвращает 0..100."""
    if not d1 and not d2:
        return 100
    if len(d1) != len(d2):
        return 0

    total_score = 0.0
    for (v1, u1), (v2, u2) in zip(d1, d2):
        if u1 != u2:
            return 0
        if abs(v1) < 1e-9 and abs(v2) < 1e-9:
            total_score += 100
            continue
        if (abs(v1) < 1e-9 and v2 > 0) or (abs(v2) < 1e-9 and v1 > 0):
            return 0
        bigger = max(v1, v2)
        ratio = min(v1, v2) / bigger if bigger > 0 else 1.0
        # жёсткий порог
        if ratio < strict_threshold:
            return 0
        if ratio >= (1 - tolerance):
            total_score += 100
        else:
            total_score += ratio * 100

    return int(round(total_score / len(d1)))

def price_score(p_price, z_price, tolerance=0.2):
    """Сопоставление цен, 0..100."""
    if not p_price or not z_price:
        return 0
    try:
        a = float(p_price)
        b = float(z_price)
    except:
        return 0
    if a <= 0 or b <= 0:
        return 0
    mx = max(a, b)
    ratio = min(a, b) / mx
    if ratio >= (1 - tolerance):
        return 100
    else:
        return int(round(ratio * 100))

def compare_parsed_structures(A, B):
    """
    Сравниваем brand1/brand2/brand3/форму/дозу и т.п.
    Итог 0..100.
    """
    sc_b1 = fuzz.ratio(A['brand1'], B['brand1'])
    if sc_b1 < 70:
        return 0

    sc_b2 = fuzz.ratio(A['brand2'], B['brand2']) if (A['brand2'] or B['brand2']) else 100
    sc_b3 = fuzz.ratio(A['brand3'], B['brand3']) if (A['brand3'] or B['brand3']) else 100

    # форма - точное совпадение -> 100, иначе 0 (если обе пустые - 100)
    sc_form = 100 if (A['form'] and B['form'] and A['form'] == B['form']) else 0
    if not A['form'] and not B['form']:
        sc_form = 100

    # номер упаковки
    sc_numb = 100 if (A['numbering'] and B['numbering'] and A['numbering'] == B['numbering']) else 0
    if not A['numbering'] and not B['numbering']:
        sc_numb = 100

    sc_dose = partial_dosage_score(A['dosage'], B['dosage'], 0.05)
    sc_tail = fuzz.WRatio(A['tail'], B['tail'])

    final_score = (sc_b1 * 0.2 + sc_b2 * 0.1 + sc_b3 * 0.05 +
                   sc_form * 0.1 + sc_numb * 0.1 +
                   sc_dose * 0.2 + sc_tail * 0.15)
    return int(round(final_score))

def compare_full(nameA, priceA, nameB, priceB):
    """
    Сравнение двух записей (название + цена).
    80% - текстовое сопоставление, 20% - сопоставление цены.
    """
    A = detailed_parse_name(nameA)
    B = detailed_parse_name(nameB)
    base = compare_parsed_structures(A, B)
    psc = price_score(priceB, priceA, 0.2)  # match цены
    final = (base * 0.8) + (psc * 0.2)
    return int(round(final))

# -----------------------------------------------------------------------------
# Кастомный сериализатор для Decimal
# -----------------------------------------------------------------------------
def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# -----------------------------------------------------------------------------
def extract_department_group(z_prim: str, allowed_groups: list) -> str:
    """
    Извлекает department_group из поля primechanie.
    Ожидает, что primechanie начинается с буквы и содержит одно из допустимых значений.
    Удаляет кавычки и нормализует пробелы перед сопоставлением.
    """
    z_prim = z_prim.strip() if z_prim else ""
    if not z_prim:
        logger.error("Поле primechanie пустое.")
        return None

    # Удаляем кавычки, если они присутствуют
    z_prim = z_prim.strip('\"\'')  # удаляет как " так и '

    # Проверяем, начинается ли primechanie с буквы
    if not re.match(r'^[А-Яа-яA-Za-z]', z_prim):
        logger.warning(f"primechanie не начинается с буквы: '{z_prim}'. Пропускаем.")
        return None

    # Нормализуем пробелы (заменяем несколько пробелов на один)
    z_prim_normalized = re.sub(r'\s+', ' ', z_prim.lower())

    for group in allowed_groups:
        # Нормализуем пробелы в group
        group_normalized = re.sub(r'\s+', ' ', group.lower())
        if group_normalized in z_prim_normalized:
            return group
    # Если не найдено, логируем ошибку и возвращаем None
    logger.error(f"Не удалось определить department_group из primechanie: '{z_prim}'")
    return None

def get_unique_department_groups(cursor):
    """
    Получает все уникальные значения department_group из таблицы products.
    Полезно для отладки и проверки наличия 'Асфендиярова 2'.
    """
    try:
        cursor.execute("""
            SELECT DISTINCT department_group
            FROM public.products
        """)
        groups = [row[0] for row in cursor.fetchall()]
        logger.info(f"Уникальные department_group в products: {groups}")
        return groups
    except Exception as e:
        logger.error("Ошибка при получении уникальных department_group из products:", exc_info=e)
        return []

# -----------------------------------------------------------------------------
# ГЛАВНАЯ ФУНКЦИЯ
# -----------------------------------------------------------------------------
def main():
    conn_params = {
        'host': 'localhost',
        'port': '5433',
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'Sputnik111'
    }
    conn = None

    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()

        # 1) Читаем заявки
        cursor.execute("""
            SELECT 
                z.id,
                z.naimenovanie,
                z.kolichestvo,
                z.stoim_so_sk,
                z.primechanie
            FROM public.zayavka z
            ORDER BY z.stoim_so_sk DESC
        """)
        zayavka_data = cursor.fetchall()

        if not zayavka_data:
            logger.info("Нет данных в таблице zayavka.")
            return

        # 2) Определяем допустимые department_group
        allowed_department_groups = [
            'Жумабаева 3',
            'Пушкина 1',
            'Асфендиярова 2',      # Добавлено новое значение
            'А. Бокейхана 32'      # Добавлено новое значение
        ]

        # 3) Для отладки: Получаем и логируем все уникальные department_group из products
        unique_groups = get_unique_department_groups(cursor)

        # 4) Извлекаем department_group из первого primechanie, начинающегося с буквы
        department_group = None
        for record in zayavka_data:
            z_prim = record[4]  # z.primechanie
            extracted_group = extract_department_group(z_prim, allowed_department_groups)
            if extracted_group:
                department_group = extracted_group
                break  # Найден первый подходящий department_group

        if not department_group:
            logger.error("Не удалось определить department_group из всех записей primechanie. Завершение работы.")
            return

        # Логируем извлеченное значение department_group
        logger.info(f"Определенный department_group: '{department_group}'")

        # 5) Читаем продукты с добавленными категориями и profit_sum для определенного department_group
        cursor.execute("""
            SELECT
                p.id,
                p.product_code,           -- Добавлено поле product_code
                p.product_name,
                p.current_stock,
                p.sales_rate,
                p.status,
                p.department_group,
                p.average_purchase_price,
                p.abc_category,           -- Добавлено поле abc_category
                p.xyz_category,           -- Добавлено поле xyz_category
                p.profit_sum              -- Новая колонка profit_sum
            FROM public.products p
            WHERE p.department_group = %s
        """, (department_group,))
        products_data = cursor.fetchall()

        if not products_data:
            logger.warning(f"Нет продуктов для department_group '{department_group}'.")
            # Дополнительно: Вы можете проверить, существуют ли похожие записи
            # или предложить возможные исправления.

        # 6) Читаем product_name из medpred_products
        cursor.execute("""
            SELECT DISTINCT product_name
            FROM public.medpred_products
        """)
        medpred_product_names = set([row[0].lower() for row in cursor.fetchall()])

        # 7) Используем enumerate для последовательного нумерования
        for idx, (z_id, z_name, z_kol, z_stoim, z_prim) in enumerate(zayavka_data, start=1):
            if not z_name:
                # Пустое название
                result = {
                    "Number": idx,
                    "NAIMENOVANIE": "(empty)",
                    "STOIM": z_stoim,
                    "COL": z_kol,
                    "PRIMECHANIE": z_prim or "-",
                    "BEST_MATCH": None,
                    "CUR_STK": "-",
                    "SALES": "-",
                    "STATUS": "-",
                    "ABC_CATEGORY": "-",   # Добавлено поле ABC_CATEGORY
                    "XYZ_CATEGORY": "-",   # Добавлено поле XYZ_CATEGORY
                    "PROFIT_SUM": "-",     # Новая колонка profit_sum
                    "RELATED_PRODUCTS": [],  # Пустой список связанных продуктов
                    "IN_MEDPRED": False      # Добавлено поле IN_MEDPRED
                }
                print(json.dumps(result, ensure_ascii=False, default=decimal_default))
                continue

            if not products_data:
                # Нет продуктов для данного department_group
                result = {
                    "Number": idx,
                    "NAIMENOVANIE": z_name,
                    "STOIM": z_stoim,
                    "COL": z_kol,
                    "PRIMECHANIE": z_prim or "-",
                    "BEST_MATCH": "(no products in department_group)",
                    "CUR_STK": "-",
                    "SALES": "-",
                    "STATUS": "-",
                    "ABC_CATEGORY": "-",
                    "XYZ_CATEGORY": "-",
                    "PROFIT_SUM": "-",     # Новая колонка profit_sum
                    "RELATED_PRODUCTS": [],
                    "IN_MEDPRED": False
                }
                print(json.dumps(result, ensure_ascii=False, default=decimal_default))
                continue

            best_score = -1
            best_p = None

            for (p_id, p_code, p_name, p_curstk, p_sales, p_stat, p_dept,
                 p_avgprice, p_abc_cat, p_xyz_cat, p_profit_sum) in products_data:
                if not p_name:
                    continue
                sc = compare_full(z_name, z_stoim, p_name, p_avgprice)
                if sc > best_score:
                    best_score = sc
                    best_p = (p_id, p_code, p_name, p_curstk, p_sales, p_stat, p_dept,
                              p_avgprice, p_abc_cat, p_xyz_cat, p_profit_sum)

            if best_score <= 0 or not best_p:
                # Не нашлось ничего подходящего
                result = {
                    "Number": idx,
                    "NAIMENOVANIE": z_name,
                    "STOIM": z_stoim,
                    "COL": z_kol,
                    "PRIMECHANIE": z_prim or "-",
                    "BEST_MATCH": "(no match)",
                    "CUR_STK": "-",
                    "SALES": "-",
                    "STATUS": "-",
                    "ABC_CATEGORY": "-",
                    "XYZ_CATEGORY": "-",
                    "PROFIT_SUM": "-",     # Новая колонка profit_sum
                    "IAotdel": department_group or "-",
                    "RELATED_PRODUCTS": [],
                    "IN_MEDPRED": False
                }
                print(json.dumps(result, ensure_ascii=False, default=decimal_default))
            else:
                (p_id, p_code, p_name, p_curstk, p_sales, p_stat, p_dept,
                 p_avgprice, p_abc_cat, p_xyz_cat, p_profit_sum) = best_p
                note = ""
                try:
                    if float(z_kol) > float(p_sales):
                        note = "!!!"
                except:
                    pass

                # Поиск связанных продуктов с тем же product_name, но другим product_code и тем же department_group
                try:
                    cursor.execute("""
                        SELECT
                            p.id,
                            p.product_code,
                            p.product_name,
                            p.current_stock,
                            p.sales_rate,
                            p.status,
                            p.department_group,
                            p.average_purchase_price,
                            p.abc_category,
                            p.xyz_category,
                            p.profit_sum
                        FROM public.products p
                        WHERE p.product_name = %s
                          AND p.product_code != %s
                          AND p.department_group = %s
                    """, (p_name, p_code, department_group))
                    related_products = cursor.fetchall()

                    related_list = []
                    for rel_p in related_products:
                        (rel_p_id, rel_p_code, rel_p_name, rel_p_curstk, rel_p_sales,
                         rel_p_stat, rel_p_dept, rel_p_avgprice, rel_p_abc_cat,
                         rel_p_xyz_cat, rel_p_profit_sum) = rel_p
                        related_list.append({
                            "id": rel_p_id,
                            "product_code": rel_p_code,
                            "product_name": rel_p_name,
                            "current_stock": rel_p_curstk,
                            "sales_rate": rel_p_sales,
                            "status": rel_p_stat,
                            "department_group": rel_p_dept,
                            "average_purchase_price": rel_p_avgprice,
                            "abc_category": rel_p_abc_cat,
                            "xyz_category": rel_p_xyz_cat,
                            "profit_sum": rel_p_profit_sum
                        })
                except Exception as e:
                    logger.error("Ошибка при поиске связанных продуктов:", exc_info=e)
                    related_list = []

                in_medpred = p_name.lower() in medpred_product_names

                result = {
                    "Number": idx,
                    "NAIMENOVANIE": z_name,
                    "STOIM": z_stoim,
                    "COL": z_kol,
                    "PRIMECHANIE": z_prim or "-",
                    "BEST_MATCH": p_name,
                    "CUR_STK": p_curstk or "-",
                    "SALES": p_sales or "-",
                    "STATUS": p_stat or "-",
                    "ABC_CATEGORY": p_abc_cat or "-",
                    "XYZ_CATEGORY": p_xyz_cat or "-",
                    "PROFIT_SUM": p_profit_sum or "-",  # Новая колонка profit_sum
                    "RELATED_PRODUCTS": related_list,
                    "IAotdel": department_group or "-",
                    "IN_MEDPRED": in_medpred
                }

                if in_medpred:
                    GREEN = '\033[92m'
                    RESET = '\033[0m'
                    colored_output = GREEN + json.dumps(result, ensure_ascii=False, default=decimal_default) + RESET
                    print(colored_output)
                else:
                    print(json.dumps(result, ensure_ascii=False, default=decimal_default))

    except Exception as e:
        logger.error("Ошибка в main:", exc_info=e)
        error_json = {
            "error": True,
            "message": str(e)
        }
        print(json.dumps(error_json, ensure_ascii=False, default=decimal_default))
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    main()
