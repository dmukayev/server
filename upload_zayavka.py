# upload_zayavka.py

import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Импортируем словарь сопоставления из mapping_zayavka.py
from mapping_zayavka import mapping_dict

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Изменено на DEBUG для более подробного логирования
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Логирование в stdout
        logging.FileHandler('upload_zayavka.log')  # Логирование в файл
    ]
)

def upload_zayavka(file_path):
    try:
        logging.info(f"Начало обработки файла: {file_path}")
        
        # Проверка существования файла
        if not os.path.exists(file_path):
            logging.error(f"Файл не найден: {file_path}")
            sys.exit(1)
        
        # Создаём движок SQLAlchemy для PostgreSQL
        engine = create_engine("postgresql://postgres:Sputnik111@localhost:5433/postgres")
        logging.info("SQLAlchemy engine создан.")
        
        # Читаем Excel-файл, пропуская первые 2 строки
        logging.debug(f"Чтение файла с пропуском первых 2 строк: {file_path}")
        df = pd.read_excel(file_path, skiprows=2, engine='openpyxl')
        logging.info("Excel-файл прочитан.")
        logging.debug(f"Данные из файла:\n{df.head()}")  # Логируем первые строки данных
        logging.info(f"Столбцы в файле: {df.columns.tolist()}")  # Логируем все столбцы
        
        # Стрипим пробелы и приводим названия столбцов к нижнему регистру
        df.columns = df.columns.str.strip().str.lower()
        logging.info(f"Столбцы после обработки: {df.columns.tolist()}")
        
        # Преобразуем ключи словаря сопоставления к нижнему регистру
        mapping_dict_lower = {k.lower(): v for k, v in mapping_dict.items()}
        
        # Проверяем наличие всех необходимых столбцов
        missing_columns = [col for col in mapping_dict_lower.keys() if col not in df.columns]
        if missing_columns:
            logging.error(f"Отсутствуют необходимые столбцы: {missing_columns}")
            sys.exit(1)
        
        # Переименовываем столбцы на английские
        df.rename(columns=mapping_dict_lower, inplace=True)
        logging.info("Столбцы переименованы.")
        
        # Выбираем только необходимые столбцы
        required_columns = list(mapping_dict_lower.values())
        df = df[required_columns]
        logging.info(f"Выбраны только необходимые столбцы: {required_columns}")
        logging.debug(f"Данные для загрузки:\n{df.head()}")  # Логируем первые строки данных
        
        # Валидация данных: проверка на полностью пустые столбцы
        if df.isnull().all().any():
            logging.error("Некоторые столбцы полностью пусты после выбора необходимых столбцов.")
            sys.exit(1)
        logging.info("Валидация данных пройдена.")
        
        with engine.connect() as connection:
            # Начинаем транзакцию
            with connection.begin():
                # Очистка таблицы 'zayavka'
                logging.debug("Очистка таблицы 'zayavka'")
                connection.execute(text("DELETE FROM public.zayavka;"))
                logging.info("Таблица 'zayavka' очищена.")
                
                # Загрузка данных в таблицу 'zayavka'
                logging.debug("Загрузка данных в таблицу 'zayavka'")
                df.to_sql("zayavka", connection, if_exists="append", index=False)
                logging.info("Новые данные успешно загружены в таблицу 'zayavka'.")
        
        logging.info("Обработка файла завершена успешно.")
    
    except SQLAlchemyError as e:
        logging.error(f"Произошла ошибка при работе с базой данных: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logging.error(f"Файл не найден: {file_path}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Использование: python3 upload_zayavka.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    upload_zayavka(file_path)
