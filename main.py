from db_manager import DBManager
from load_data import load_all_data


def print_menu():
    print("""
    ========== Меню ==========
    1. Загрузить данные в БД (компании и вакансии)
    2. Показать компании и количество вакансий
    3. Показать все вакансии
    4. Показать среднюю зарплату по вакансиям
    5. Показать вакансии с зарплатой выше средней
    6. Поиск вакансий по ключевому слову
    0. Выйти
    """)


def main():
    dbm = DBManager()
    while True:
        print_menu()
        choice = input("Выберите пункт меню: ").strip()
        if choice == '1':
            print("Загрузка данных...")
            load_all_data()
            print("Данные успешно загружены!\n")
        elif choice == '2':
            print("\nКомпании и количество вакансий:")
            for name, count in dbm.get_companies_and_vacancies_count():
                print(f"- {name}: {count}")
            print()
        elif choice == '3':
            print("\nВсе вакансии:")
            for company, vacancy, salary_from, salary_to, currency, url in dbm.get_all_vacancies():
                salary = f"{salary_from or ''} - {salary_to or ''} {currency or ''}".strip()
                print(f"- {company}: {vacancy} | Зарплата: {salary} | {url}")
            print()
        elif choice == '4':
            avg = dbm.get_avg_salary()
            print(f"\nСредняя зарплата по вакансиям: {avg:.2f}" if avg else "Нет данных о зарплатах.\n")
        elif choice == '5':
            print("\nВакансии с зарплатой выше средней:")
            for company, vacancy, salary_from, salary_to, currency, url in dbm.get_vacancies_with_higher_salary():
                salary = f"{salary_from or ''} - {salary_to or ''} {currency or ''}".strip()
                print(f"- {company}: {vacancy} | Зарплата: {salary} | {url}")
            print()
        elif choice == '6':
            keyword = input("Введите ключевое слово для поиска: ").strip()
            print(f"\nВакансии по ключевому слову '{keyword}':")
            for company, vacancy, salary_from, salary_to, currency, url in dbm.get_vacancies_with_keyword(keyword):
                salary = f"{salary_from or ''} - {salary_to or ''} {currency or ''}".strip()
                print(f"- {company}: {vacancy} | Зарплата: {salary} | {url}")
            print()
        elif choice == '0':
            dbm.close()
            print("Выход.")
            break
        else:
            print("Некорректный ввод. Попробуйте снова.\n")


if __name__ == "__main__":
    main()
