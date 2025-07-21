from typing import List, Tuple, Optional
from psycopg2.extensions import connection
from db import get_connection


class DBManager:
    """
    Класс для работы с данными в БД PostgreSQL.
    """
    def __init__(self, conn: Optional[connection] = None):
        """
        :param conn: psycopg2 connection (если не передан, создаётся новый)
        """
        self.conn = conn or get_connection()

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список всех компаний и количество вакансий у каждой компании.
        :return: Список кортежей (название компании, количество вакансий)
        """
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT c.name, COUNT(v.id) as vacancies_count
                FROM companies c
                LEFT JOIN vacancies v ON c.id = v.company_id
                GROUP BY c.id
                ORDER BY vacancies_count DESC;
            ''')
            return cur.fetchall()

    def get_all_vacancies(self) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Получает список всех вакансий с указанием названия компании, названия вакансии, зарплаты и ссылки на вакансию.
        :return: Список кортежей (компания, вакансия, salary_from, salary_to, currency, url)
        """
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT c.name, v.name, v.salary_from, v.salary_to, v.currency, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                ORDER BY c.name;
            ''')
            return cur.fetchall()

    def get_avg_salary(self) -> Optional[float]:
        """
        Получает среднюю зарплату по вакансиям (по salary_from и salary_to).
        :return: Средняя зарплата или None
        """
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT AVG((COALESCE(salary_from,0) + COALESCE(salary_to,0))/2.0)
                FROM vacancies
                WHERE salary_from IS NOT NULL OR salary_to IS NOT NULL;
            ''')
            result = cur.fetchone()
            return result[0] if result else None

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :return: Список кортежей (компания, вакансия, salary_from, salary_to, currency, url)
        """
        avg_salary = self.get_avg_salary()
        if avg_salary is None:
            return []
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT c.name, v.name, v.salary_from, v.salary_to, v.currency, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                WHERE ((COALESCE(v.salary_from,0) + COALESCE(v.salary_to,0))/2.0) > %s
                ORDER BY ((COALESCE(v.salary_from,0) + COALESCE(v.salary_to,0))/2.0) DESC;
            ''', (avg_salary,))
            return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, Optional[int], Optional[int], Optional[str], str]]:
        """
        Получает список всех вакансий, в названии которых содержатся переданные в метод слова.
        :param keyword: Ключевое слово для поиска
        :return: Список кортежей (компания, вакансия, salary_from, salary_to, currency, url)
        """
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT c.name, v.name, v.salary_from, v.salary_to, v.currency, v.url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.id
                WHERE LOWER(v.name) LIKE %s
                ORDER BY c.name;
            ''', (f'%{keyword.lower()}%',))
            return cur.fetchall()

    def close(self):
        """
        Закрывает соединение с БД.
        """
        if self.conn:
            self.conn.close()
