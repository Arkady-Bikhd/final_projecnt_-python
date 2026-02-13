import requests
import logging

from datetime import datetime as dt


logger = logging.getLogger(__name__) 


def fetch_students_data(client, client_key, start_date, end_date):
    api_url = "https://b2b.itresume.ru/api/statistics"
    payload = {
        'client': client,
        'client_key': client_key,  # - M2MGWS (регистр важен)
        # - дата-время начала в формате `2023-04-01 12:46:47.860798`` (время на сервере в нулевом часовом поясе)
        'start': start_date,
        'end': end_date
    }
    response = requests.get(api_url, params=payload)
    logger.info('Данные получены')
    response.raise_for_status()
    return response.json()


def format_for_db(students_data):    
    formatted_data = list()
    for student_data in students_data:        
        db_data = dict()        
        db_data['user_id'] = student_data.get('lti_user_id')
        if not db_data.get('user_id'):
            continue
        passback_params = student_data.get('passback_params')
        if not passback_params:
            continue
        passback_params = passback_params.split(',')
        passback_params = [passback_param.strip().replace("'", '').replace('}', '').split(' ')
                           for passback_param in passback_params]
        db_data['oauth_consumer_key'] = passback_params[0][1] if passback_params[0][1] else None
        db_data['lis_result_sourcedid'] = passback_params[1][1]
        db_data['lis_outcome_service_url'] = passback_params[2][1] if len(
            passback_params) == 3 else None
        db_data['is_correct'] = student_data.get('is_correct')
        db_data['attempt_type'] = student_data.get('attempt_type')
        db_data['created_at'] = dt.strptime(
            student_data.get('created_at'), "%Y-%m-%d %H:%M:%S.%f")
        formatted_data.append(db_data)
    logger.info('Данные обработаны')
    return formatted_data


# a = fetch_students_data('Skillfactory', 'M2MGWS',
#                          '2023-04-05 12:46:47', '2023-04-06 12:46:47.860798')
# # print(a)
# b = format_for_db(a)
# print(b)

# import re

# zamena = {'яблоко': 'апельсин', 'банан': 'ягода'}
# pattern = re.compile("|".join(re.escape(key) for key in zamena))

# tekst = "яблоко и банан в саду яблок"
# tekst = pattern.sub(lambda match: zamena[match.group(0)], tekst)

# print(tekst)  # Вывод: апельсин и ягода в саду апельсин