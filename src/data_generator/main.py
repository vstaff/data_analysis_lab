import os
import time
import random
import psycopg
from datetime import datetime
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker('ru_RU')

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

CONNECTION = f"host={DB_HOST} dbname={DB_NAME} user={DB_USER} password={DB_PASS} port={DB_PORT}"

CATALOG = {
    "Электроника": [("Смартфон Honor 70", 45000), ("Ноутбук ASUS Vivobook 17", 85000), ("Наушники Huawei Freebuds 5i", 7000)],
    "Одежда": [("Худи Number 9", 13500), ("Джинсы Amiri", 5000), ("Кепка Pussy Cat Lover Club", 4500)],
    "Дом": [("Чайник Aceline", 2500), ("Плед", 1800), ("Лампа Pixar", 1200)]
}
CITIES = ["Владивосток", "Артём", "Находка", "Южно-Сахалинск", "Нью-Йорк"]


def connect_db():
    print(f"Попытка подключения к {DB_HOST}...")
    while True:
        try:
            conn = psycopg.connect(CONNECTION)
            return conn
        except Exception as e:
            print(f"Ожидание базы... Ошибка: {e}")
            time.sleep(3)


def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                dt TIMESTAMP NOT NULL,
                category VARCHAR(50),
                item VARCHAR(100),
                price NUMERIC(10, 2),
                quantity INTEGER,
                city VARCHAR(50)
            );
        """)
        conn.commit()


def run_generator():
    conn = connect_db()
    init_db(conn)
    print("Генератор успешно запущен!")

    try:
        while True:
            category = random.choice(list(CATALOG.keys()))
            item, base_price = random.choice(CATALOG[category])

            data = (
                datetime.now(),
                category,
                item,
                float(base_price) * random.uniform(0.9, 1.1),
                random.randint(1, 5),
                random.choice(CITIES)
            )

            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (dt, category, item, price, quantity, city)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, data)
                conn.commit()

            print(f"Добавлен заказ: {item} ({category}) -> {data[5]}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Остановка...")
    finally:
        conn.close()


if __name__ == "__main__":
    run_generator()