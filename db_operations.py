import logging
import psycopg2
import argparse

from psycopg2 import sql
from dotenv import load_dotenv
from os import environ
from datetime import datetime


logger = logging.getLogger('db_operations')


class DatabaseConnection:
    __instance = None

    @staticmethod
    def get_instance():
        if not DatabaseConnection.__instance:
            DatabaseConnection()
        return DatabaseConnection.__instance

    def __init__(self, user, password, host, port, database=None):
        if DatabaseConnection.__instance:
            raise Exception("Этот класс является Singleton.")
        else:
            self.database = database
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.connection = None
            DatabaseConnection.__instance = self

    def connect(self):
        """
        Установка соединения с базой данных
        """
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                # Включаем autocommit для операций DDL
                # self.connection.autocommit = True
                logger.info('Соединение установлено.')
            except psycopg2.ConnectionError as e:
                logger.error(f"Ошибка подключения: {repr(e)}")
                raise
        return self.connection

    def disconnect(self):
        """
        Закрытие соединения
        """
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("Соединение закрыто")
            self.connection = None

    def get_connection(self):
        return self.connection

    def __del__(self):
        """
        Деструктор для автоматического закрытия соединения
        """
        self.disconnect()


class SimulativeDB:
    def __init__(self, db_connection, db_name) -> None:
        self.__db_connection = db_connection
        self.db_name = db_name
        self.__db_connection.connect()

    def create_database(self):
        """
        Функция для создания базы данных PostgreSQL
        """
        # Включение автокоммита
        connection = self.__db_connection.get_connection()
        connection.autocommit = True
        # Курсор для выполнения операций с базой данных
        try:
            cursor = connection.cursor()
            # Проверка существования базы данных
            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.db_name,))
            exists = cursor.fetchone()

            if not exists:
                # Создание базы данных
                sql_create_database = f'create database {self.db_name}'
                cursor.execute(sql_create_database)
                logger.info(f"База данных '{self.db_name}' успешно создана.")
            else:
                logger.info(f"База данных '{self.db_name}' уже существует.")
        except psycopg2.Error as e:
            logger.error(f"Ошибка подключения: {repr(e)}")
        finally:
            if cursor:
                cursor.close()

    def drop_database(self):
        self.__db_connection.database = 'postgres'
        connection = self.__db_connection.get_connection()
        connection.autocommit = True
        try:
            cursor = connection.cursor()
            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.db_name,))
            exists = cursor.fetchone()
            if exists:
                drop_query = sql.SQL("DROP DATABASE {}").format(
                    sql.Identifier(self.db_name)
                )
                cursor.execute(drop_query)
                logger.info(f"База данных '{self.db_name}' успешно удалена.")
            else:
                logger.info(f"База данных '{self.db_name}' не существует.")
        except psycopg2.Error as e:
            logger.error(f"Ошибка при удалении базы данных: {repr(e)}")
        finally:
            if cursor:
                cursor.close()

    def create_table(self):
        self.__db_connection.database = self.db_name
        connection = self.__db_connection.connect()
        if connection is None:
            raise ConnectionError(
                "Не удалось получить соединение с базой данных.")
        try:
            with connection.cursor() as cursor:
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS students_grade (
                        id SERIAL PRIMARY KEY,
                        user_id VARCHAR(100),
                        oauth_consumer_key VARCHAR(255),
                        lis_result_sourcedid VARCHAR(255),
                        lis_outcome_service_url VARCHAR(255),
                        is_correct INTEGER,
                        attempt_type VARCHAR(25),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                cursor.execute(create_table_query)
                connection.commit()
            logger.info('Таблица students_grade создана.')
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            logger.error(
                f'Ошибка при создании таблицы students_grade: {repr(e)}.')

    def drop_table(self):
        self.__db_connection.database = self.db_name
        connection = self.__db_connection.connect()
        try:
            with connection.cursor() as cursor:
                drop_table_query = 'DROP TABLE IF EXISTS students_grade'
                cursor.execute(drop_table_query)
                connection.commit()
            logger.info('Таблица students_grade удалена.')
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            logger.error(
                f'Ошибка при удалении таблицы students_grade: {repr(e)}.')


class StudentDAO:
    def __init__(self, db_connection) -> None:
        self.__db_connection = db_connection
        self.__db_connection.connect()

    def insert_students_data(self, students_data):
        connection = self.__db_connection.get_connection()
        try:
            with connection.cursor() as cursor:
                for student_data in students_data:
                    insert_student_data_query = '''
                        INSERT INTO students_grade (user_id, 
                            oauth_consumer_key,
                            lis_result_sourcedid,
                            lis_outcome_service_url,
                            is_correct,
                            attempt_type,
                            created_at)
                        VALUES (%(user_id)s,
                            %(oauth_consumer_key)s,
                            %(lis_result_sourcedid)s,
                            %(lis_outcome_service_url)s,
                            %(is_correct)s,
                            %(attempt_type)s,
                            %(created_at)s)
                    '''
                    cursor.execute(
                        insert_student_data_query, student_data
                    )
                connection.commit()
            logger.info(f"Успешно добавлено {len(students_data)} записей.")
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            logger.error(f'Ошибка при записи данных в таблицу: {repr(e)}.')

    def clear_students_data(self):
        connection = self.__db_connection.get_connection()
        try:
            with connection.cursor() as cursor:
                clear_query = 'TRUNCATE TABLE students_grade RESTART IDENTITY'
                cursor.execute(clear_query)
                connection.commit()
            logger.info('Таблица students_grade очищена, счётчики сброшены.')
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            logger.error(f'Ошибка при удалении данных из таблицы: {repr(e)}.')

    def fetch_students_data(self, num_query=0, date=None):
        connection = self.__db_connection.get_connection()
        queries = {
            0:  '''SELECT * 
                   FROM students_grade                   
                   LIMIT 10''',
            1:  '''SELECT count(DISTINCT user_id) 
                   FROM students_grade 
                   WHERE DATE(created_at) = %s''',
            2:  '''SELECT count(*) 
                   FROM students_grade 
                   WHERE DATE(created_at) = %s''',
            3:  '''SELECT count(*) 
                   FROM students_grade 
                   WHERE DATE(created_at) = %s and attempt_type = 'submit'
                ''',
        }
        fetch_query = queries[num_query]
        if not date:
            logger.info('ВЫполняется запрос по умолчанию')
            fetch_query = queries[0]
        else:
            logger.info(f'Выполняется запрос {fetch_query}')
        try:
            with connection.cursor() as cursor:
                cursor.execute(fetch_query, (date,))
                raws = cursor.fetchall()
            logger.info('Данные из таблицы извлечены')
            if date:
                return raws[0]
            for raw in raws:
                print(raw)
        except psycopg2.Error as e:
            if connection:
                connection.rollback()
            logger.error(
                f'Ошибка при извлечении данных из таблицы: {repr(e)}.')


def create_parser():
    parser = argparse.ArgumentParser(
        description='Работа с базой данных'
    )
    parser.add_argument(
        '-c',
        '--create',
        choices=['database', 'table'],
        nargs='+',
        help='Создание объекта',
        required=False
    )
    parser.add_argument(
        '-d',
        '--delete',
        choices=['database', 'table', 'data'],
        help='Удаление объекта',
        required=False,
    )
    parser.add_argument(
        '-f',
        '--fetch',
        help='Извлечение данных',
        required=False,
        action='store_true'
    )
    try:
        args = parser.parse_args()
        return args
    except SystemExit:
        parser.print_help()
        raise


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(
                f'logs/simulative_{datetime.now().strftime("%Y%m%d")}.log',
                encoding='utf-8'
            ),
            logging.StreamHandler()
        ]
    )


def main():
    setup_logging()
    logger = logging.getLogger('db_operations')
    load_dotenv()
    user = environ['USER']
    password = environ['PASSWORD']
    host = environ['HOST']
    port = int(environ['PORT'])
    db_name = 'simulative'
    args = create_parser()
    db_connection = None
    try:
        if args.create or args.delete in ('database', 'table'):
            db_connection = DatabaseConnection(user=user,
                                               password=password,
                                               host=host,
                                               port=port,
                                               )
            simulative = SimulativeDB(db_connection, db_name)
        elif args.delete == 'data' or args.fetch:
            db_connection = DatabaseConnection(user=user,
                                               password=password,
                                               host=host,
                                               port=port,
                                               database=db_name)
            students_grade = StudentDAO(db_connection)
        match args.create:
            case ['database']:
                simulative.create_database()
            case ['table']:
                simulative.create_table()
            case ['database', 'table']:
                simulative.create_database()
                simulative.create_table()

        match args.delete:
            case 'database':
                simulative.drop_database()
            case 'table':
                simulative.drop_table()
            case 'data':
                students_grade.clear_students_data()
        if args.fetch:
            students_grade.fetch_students_data()
    except Exception as e:
        logger.error(f'Ошибка {repr(e)}')
    finally:
        if db_connection:
            db_connection.disconnect()


if __name__ == '__main__':
    main()
