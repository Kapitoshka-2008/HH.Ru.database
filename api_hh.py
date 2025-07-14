import requests
from typing import List, Dict, Any

class HHApi:
    """
    Класс для работы с API hh.ru: получение информации о компаниях и их вакансиях.
    """
    BASE_URL = 'https://api.hh.ru/'

    def __init__(self, employer_ids: List[int]):
        """
        :param employer_ids: Список id работодателей на hh.ru
        """
        self.employer_ids = employer_ids

    def get_employers(self) -> List[Dict[str, Any]]:
        """
        Получает информацию о работодателях по их id.
        :return: Список словарей с данными о работодателях
        """
        employers = []
        for emp_id in self.employer_ids:
            resp = requests.get(f'{self.BASE_URL}employers/{emp_id}')
            if resp.status_code == 200:
                employers.append(resp.json())
        return employers

    def get_vacancies_for_employer(self, employer_id: int, per_page: int = 100) -> List[Dict[str, Any]]:
        """
        Получает список вакансий для одного работодателя.
        :param employer_id: id работодателя
        :param per_page: количество вакансий на страницу (максимум 100)
        :return: Список словарей с вакансиями
        """
        vacancies = []
        page = 0
        while True:
            resp = requests.get(
                f'{self.BASE_URL}vacancies',
                params={
                    'employer_id': employer_id,
                    'per_page': per_page,
                    'page': page
                }
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            vacancies.extend(data.get('items', []))
            if page >= data.get('pages', 0) - 1:
                break
            page += 1
        return vacancies

    def get_all_vacancies(self) -> List[Dict[str, Any]]:
        """
        Получает вакансии для всех работодателей из списка.
        :return: Список всех вакансий
        """
        all_vacancies = []
        for emp_id in self.employer_ids:
            all_vacancies.extend(self.get_vacancies_for_employer(emp_id))
        return all_vacancies 