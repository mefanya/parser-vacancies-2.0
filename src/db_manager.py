import psycopg2
from config import config


class DBManager:
    """
    Класс для взаимодействия с БД
    """
    def __init__(self, conn):
        self.conn = conn
        self.cursor = self.conn.cursor()

    def get_companies_and_vacancies_count(self):
        """
        Получает список всех компаний и количество вакансий у каждой компании
        """
        self.cursor.execute("""
            SELECT companies.company_name, COUNT(vacancies.vacancy_id) AS vacancy_counter 
            FROM companies
            LEFT JOIN vacancies USING(company_id)
            GROUP BY companies.company_name;
        """)
        return self.cursor.fetchall()

    def get_all_vacancies(self):
        """
        Получает список всех вакансий с указанием названия компании,
        названия вакансии, зарплаты и ссылки на вакансию
        """
        self.cursor.execute("""
            SELECT c.company_name, v.vacancy_name, v.salary_min, v.salary_max, v.vacancy_url
            FROM companies c
            JOIN vacancies v USING(company_id);
        """)
        return self.cursor.fetchall()

    def get_avg_salary(self):
        """
        Получает среднюю зарплату по вакансиям
        """
        self.cursor.execute("""
            SELECT ROUND(AVG((salary_min + salary_max) / 2)) AS avg_salary
            FROM vacancies;
        """)
        return self.cursor.fetchone()[0]

    def get_vacancies_with_higher_salary(self):
        """
        Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям
        """
        self.cursor.execute("""
            SELECT * FROM vacancies
            WHERE (salary_min + salary_max) > 
            (SELECT AVG(salary_min + salary_max) FROM vacancies);
        """)
        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """
        Получает список всех вакансий,
        в названии которых содержатся переданные в метод слова
        """
        words_list = keyword.split()

        query = """
                SELECT 
                c.company_name, 
                v.vacancy_name, 
                v.salary_min, 
                v.salary_max, 
                v.vacancy_url
                FROM vacancies v
                JOIN companies c ON v.company_id = c.company_id
                WHERE """ + " AND ".join(["v.vacancy_name "
                                          "ILIKE %s"] * len(words_list))

        params = ['%' + word + '%' for word in words_list]

        self.cursor.execute(query, params)
        return self.cursor.fetchall()

    def __del__(self):
        self.cursor.close()
