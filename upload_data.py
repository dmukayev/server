# upload_data.py

import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Импортируем словарь сопоставления
from mapping_dict import mapping_dict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Логирование в stdout
        logging.FileHandler('upload_data.log')  # Логирование в файл
    ]
)

def upload_data(file_path, upload_type):
    try:
        logging.info(f"Начало обработки файла: {file_path} Тип загрузки: {upload_type}")
        
        # Создаём движок SQLAlchemy для PostgreSQL
        engine = create_engine("postgresql://postgres:Sputnik111@localhost:5433/postgres")
        logging.info("SQLAlchemy engine создан.")
        
        # Читаем Excel-файл
        df = pd.read_excel(file_path)
        logging.info("Excel-файл прочитан.")
        logging.info(f"Столбцы в файле: {df.columns.tolist()}")  # Логируем все столбцы
        
        # Стрипим пробелы и приводим названия столбцов к нижнему регистру
        df.columns = df.columns.str.strip().str.lower()
        logging.info(f"Столбцы после обработки: {df.columns.tolist()}")
        
        # Обновлённый словарь сопоставления с нижним регистром
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
        
        # Валидация данных
        if df.isnull().all().any():
            logging.error("Некоторые столбцы полностью пусты после выбора необходимых столбцов.")
            sys.exit(1)
        logging.info("Валидация данных пройдена.")
        
        with engine.connect() as connection:
            # Начинаем транзакцию
            with connection.begin():
                if upload_type == "assortment_plan":
                    # Обработка Ассортиментного плана
                    # Удаляем все записи из таблицы 'products'
                    connection.execute(text("DELETE FROM products;"))
                    logging.info("Все записи из таблицы 'products' удалены.")

                    # Загружаем DataFrame в PostgreSQL
                    df.to_sql("products", connection, if_exists="append", index=False)
                    logging.info("Новые данные успешно загружены в таблицу 'products'.")
                
                elif upload_type == "application":
                    # Обработка Заявки
                    # Пример: добавление новых записей в таблицу 'applications'
                    df.to_sql("applications", connection, if_exists="append", index=False)
                    logging.info("Новые данные успешно загружены в таблицу 'applications'.")
                
                else:
                    logging.error("Неверный тип загрузки.")
                    sys.exit(1)

        logging.info("Обработка файла завершена успешно.")

    except SQLAlchemyError as e:
        logging.error(f"Произошла ошибка при работе с базой данных: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Использование: python3 upload_data.py <file_path> <upload_type>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    upload_type = sys.argv[2]

    upload_data(file_path, upload_type)
