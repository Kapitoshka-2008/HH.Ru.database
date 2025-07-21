import psycopg2
from psycopg2.extensions import connection
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.getenv('DB_NAME', 'hh_vacancies')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')


def create_database(db_name: str = DB_NAME) -> None:
    """
    Создаёт базу данных, если она не существует.
    """
    conn = psycopg2.connect(dbname='postgres', user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'CREATE DATABASE {db_name}')
    cur.close()
    conn.close()


def get_connection(db_name: str = DB_NAME) -> Optional[connection]:
    """
    Возвращает подключение к базе данных.
    """
    try:
        conn = psycopg2.connect(dbname=db_name, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к БД: {e}")
        return None


def create_tables(conn: connection) -> None:
    """
    Создаёт таблицы companies и vacancies, если их нет.
    """
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS companies (
                id SERIAL PRIMARY KEY,
                hh_id INTEGER UNIQUE NOT NULL,
                name TEXT NOT NULL,
                url TEXT,
                description TEXT
            );
        ''')
        cur.execute('''
            CREATE TABLE IF NOT EXISTS vacancies (
                id SERIAL PRIMARY KEY,
                hh_id INTEGER UNIQUE NOT NULL,
                company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                salary_from INTEGER,
                salary_to INTEGER,
                currency TEXT,
                url TEXT,
                requirement TEXT,
                responsibility TEXT
            );
        ''')
        conn.commit()


def insert_company(conn: connection, company: dict) -> int:
    """
    Вставляет компанию в таблицу companies. Возвращает id компании в таблице.
    """
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO companies (hh_id, name, url, description)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (hh_id) DO UPDATE SET name=EXCLUDED.name RETURNING id;
        ''', (
            company['id'],
            company['name'],
            company.get('alternate_url'),
            company.get('description')
        ))
        company_id = cur.fetchone()[0]
        conn.commit()
        return company_id


def insert_vacancy(conn: connection, vacancy: dict, company_id: int) -> None:
    """
    Вставляет вакансию в таблицу vacancies.
    """
    salary = vacancy.get('salary') or {}
    with conn.cursor() as cur:
        cur.execute('''
            INSERT INTO vacancies (hh_id, company_id, name, salary_from, salary_to, currency, url, requirement, responsibility)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hh_id) DO NOTHING;
        ''', (
            vacancy['id'],
            company_id,
            vacancy['name'],
            salary.get('from'),
            salary.get('to'),
            salary.get('currency'),
            vacancy.get('alternate_url'),
            (vacancy.get('snippet') or {}).get('requirement'),
            (vacancy.get('snippet') or {}).get('responsibility')
        ))
        conn.commit()
