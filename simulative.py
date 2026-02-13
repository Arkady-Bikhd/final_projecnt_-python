import logging
import re
import argparse

import requests_to_simulative as rs
import db_operations as db

from dotenv import load_dotenv
from os import environ, listdir
from datetime import datetime
from pathlib import Path
from mail import send_email
from google_api import write_to_sheet


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


def del_log_files(logger):

    def del_files(log_files):
        for log_file in log_files:
            delta_time = datetime.now() - datetime.fromtimestamp(log_file.stat().st_ctime)
            if delta_time.days > 3:
                log_file.unlink(missing_ok=True)
                logger.info(f'Файл {log_file} удалён!')

    try:
        log_dir = Path.cwd() / "logs"
        if not log_dir.exists():
            raise FileNotFoundError(f"Папка '{log_dir}' не найдена")

        if not log_dir.is_dir():
            raise NotADirectoryError(f"'{log_dir}' — это не папка")
        try:
            listdir(log_dir)
        except PermissionError:
            raise PermissionError(f"Нет прав доступа к папке '{log_dir}'")
        log_files = list(log_dir.rglob("*.log"))
        if log_files:
            del_files(log_files)
        else:
            print(f"В папке '{log_dir}' нет файлов с расширением .log")

    except FileNotFoundError as e:
        print(f"Ошибка (файл/папка не найден): {repr(e)}")
    except NotADirectoryError as e:
        print(f"Ошибка (не папка): {repr(e)}")
    except PermissionError as e:
        print(f"Ошибка прав доступа: {repr(e)}")
    except Exception as e:
        print(f"Неожиданная ошибка: {repr(e)}")


def create_date_pattern():
    day_part = r'(?:0[1-9]|[12]\d|3[01])'
    month_part = r'(?:0[1-9]|1[0-2])'
    year_part = r'(?:\d{4})'
    return re.compile(
        r'\b' + year_part + r'-' + month_part + r'-' + day_part + r'\b')


def input_dates():
    start_date = None
    end_date = None
    date_pattern = create_date_pattern()
    while True:
        start_date = input('Введите начальную дату (или Enter для выхода)в формате YYYY-MM-DD: ')
        if not start_date:
            print('Выход из программы!')
            return None, None
        start_date = re.match(date_pattern, start_date)
        if not start_date:
            print('Начальная дата введена неверно!')
            continue
        end_date = input('Введите конечную дату (или Enter для выхода)в формате YYYY-MM-DD: ')
        if not end_date:
            print('Выход из программы!')
            return None, None
        end_date = re.match(date_pattern, end_date)
        if not end_date:
            print('Конечная дата введена неверно!')
            continue
        if datetime.strptime(start_date.string, "%Y-%m-%d") > datetime.strptime(end_date.string, "%Y-%m-%d"):
            print('Ошибка! Начальная дата больше конечной даты!')
            continue
        return start_date.string, end_date.string


def input_get_date():
    get_date = None
    date_pattern = create_date_pattern()
    while True:        
        get_date = input('Введите дату (или Enter для выхода) в формате YYYY-MM-DD: ')
        if not get_date:
            print('Выход из программы!')
            return None
        get_date = re.match(date_pattern, get_date)
        if not get_date:
            print('Дата введена неверно!')
            continue
        return get_date.string


def get_students_data(students_grade):
    num_queries = 3
    get_date = input_get_date()
    if not get_date:
        return
    students_data = list()
    for num_query in range(1, num_queries + 1):
        students_data.append(
            students_grade.fetch_students_data(num_query, get_date))        
    return students_data


def get_mail_address():
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    while True:
        mail_address = input('Введите адрес электронной почты (или Enter для выхода): ')
        if not mail_address:
            print('Выход из программы!')
            return None
        if not re.match(pattern, mail_address):
            print('Адрес введён неверно!')
            continue
        return mail_address


def create_parser():
    parser = argparse.ArgumentParser(
        description='Работа данными'
    )
    parser.add_argument(
        '-l',
        '--load',
        help='Загрузка данных',
        required=False,
        action='store_true'
    )
    parser.add_argument(
        '-f',
        '--fetch',
        choices=['sheet', 'mail'],
        nargs='+',
        help='Извлечение и передача данных',
        required=False,
    )
    try:
        args = parser.parse_args()
        return args
    except SystemExit:
        parser.print_help()
        raise


def main():
    setup_logging()
    logger = logging.getLogger(__name__)
    del_log_files(logger)
    load_dotenv()
    user = environ['USER']
    password = environ['PASSWORD']
    host = environ['HOST']
    port = int(environ['PORT'])
    client = environ['CLIENT']
    client_key = environ['CLIENT_KEY']
    db_name = 'simulative'
    db_connection = None
    try:
        db_connection = db.DatabaseConnection(user,
                                              password,
                                              host,
                                              port,
                                              db_name)
        students_grade = db.StudentDAO(db_connection)
        args = create_parser()
        start_date = None
        if args.load:
            start_date, end_date = input_dates()
            if not start_date:
                return
            fetching_data = rs.fetch_students_data(
                client, client_key, start_date, end_date)
            students_data = rs.format_for_db(fetching_data)
            students_grade.insert_students_data(students_data)
        students_data = get_students_data(students_grade)
        msg_txt = f'''Количество уникальных пользователей {students_data[0][0]};
                      Количество совершённых попыток {students_data[1][0]};
                      Количество учпешных попыток {students_data[2][0]}''' 
        sheet_data = [
            ['Количество уникальных пользователей', students_data[0][0]],
            ['Количество совершённых попыток', students_data[1][0]],
            ['Количество учпешных попыток', students_data[2][0]],
        ]
        match args.fetch:            
            case ['sheet']:
                write_to_sheet(sheet_data)
                logger.info("Данные успешно записаны в таблицу!")
            case ['mail']:
                msg_to = get_mail_address()
                send_email(msg_to=msg_to, msg_txt=msg_txt)
                logger.info('Письмо отправлено')
            case ['sheet', 'mail']:
                write_to_sheet(sheet_data)
                logger.info("Данные успешно записаны в таблицу!")
                msg_to = get_mail_address()
                send_email(msg_to=msg_to, msg_txt=msg_txt)
                logger.info('Письмо отправлено')
    except Exception as e:
        logger.error(f'Ошибка {repr(e)}')
    finally:
        if db_connection:
            db_connection.disconnect()


if __name__ == '__main__':
    main()
