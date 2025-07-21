from api_hh import HHApi
from db import create_database, get_connection, create_tables, insert_company, insert_vacancy
from typing import List

# Список id компаний
EMPLOYER_IDS = [1740, 3529, 15478, 2180, 78638, 3776, 80, 733, 84585, 3127]


def load_all_data(employer_ids: List[int] = EMPLOYER_IDS) -> None:
    """
    Загружает данные о компаниях и их вакансиях в БД.
    """
    create_database()
    conn = get_connection()
    if not conn:
        print('Не удалось подключиться к БД')
        return
    create_tables(conn)
    api = HHApi(employer_ids)
    employers = api.get_employers()
    for emp in employers:
        company_id = insert_company(conn, emp)
        vacancies = api.get_vacancies_for_employer(emp['id'])
        for vac in vacancies:
            insert_vacancy(conn, vac, company_id)
    conn.close()


if __name__ == "__main__":
    load_all_data()
