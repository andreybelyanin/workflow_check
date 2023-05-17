from itertools import groupby
import time
import requests
import argparse
import sys
import datetime
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

status_codes = dict(DONE=0, ERROR=1, FAILED=2, KILLED=3, OK=4, RUNNING=5, START_MANUAL=6, START_RETRY=7, SUCCEEDED=8,
                    SUSPENDED=9, WAITING=10)


def parse_args():
    """ Парсинг параметров """
    parser = argparse.ArgumentParser(
        description='Discover all unique oozie jobs since piculiar time in past via oozie api')
    parser.add_argument(
        '-s', '--server',
        help='DNS name or IP address of oozie host. Localhost is default.',
        default='localhost')
    parser.add_argument(
        '-p', '--port',
        help='Oozie server port number. 11000 is default',
        default=11000)
    parser.add_argument(
        '-d', '--depth',
        help='History depth for jobs analyzing. 0 is default',
        default=0)
    parser.add_argument(
        '-D', '--delay',
        help='Pause before script restart. 300 seconds is default',
        default=300)
    args = parser.parse_args()
    return vars(args)


def get_config():
    """ Получение параметров """
    config = parse_args()
    return config


def construct_url(o_host, o_port, rewind_days):
    """ Функция формирования url'а """
    url = 'http://' + o_host + ':' + str(
        o_port) + '/oozie/v2/jobs?len=10000&jobtype=wf&filter=%3BendCreatedTime%3D-' + str(rewind_days) + 'd'
    return url


def get_response_data():
    """ Функция, возвращающая ответ от запроса к api oozie """
    url = construct_url(get_config()['server'], get_config()['port'], get_config()['depth'])
    wf_raw_data = requests.get(url)
    if wf_raw_data.status_code != 200:
        print('Response status code = ' + str(wf_raw_data.status_code) + ' on URL: ' + url)
        sys.exit()
    wf_data = wf_raw_data.json()['workflows']
    if len(wf_data) == 0:
        sys.exit(
            'ERROR. Jobs are not been observed in selected time period ' + str(get_config()['depth']) + ' minute(s).')
    return wf_data


def get_wf_time(data):
    """ Функция получения времени статуса """
    if data['endTime'] is None:
        return data['startTime']
    else:
        return data['endTime']


def get_key_by_value(value):
    """ Функция получения ключа по значению """
    for k, v in status_codes.items():
        if v == value:
            return k


def grouper(item):
    return item['wf_name']


def preparing_info_for_pushing():
    wf_data = get_response_data()
    data_list = []  # список словарей с нужными данными полученными из запроса к oozie api
    for wf in wf_data:  # формирование словарей и добавление их в список data_list
        wf_name = lambda data: data['appName']
        oozie_current_state = lambda data: status_codes[data['status']]
        console_url = lambda data: data['consoleUrl']
        start_time = lambda data: data['startTime']
        end_time = lambda data: data['endTime']
        metrics_data = {'wf_name': wf_name(wf),
                        'oozie_current_state': oozie_current_state(wf),
                        'console_url': console_url(wf), 'start_time': start_time(wf),
                        'end_time': end_time(wf),
                        'time': datetime.datetime.strptime(get_wf_time(wf), '%a, %d %b %Y %H:%M:%S %Z')
                        .strftime("%d %b %Y %H:%M:%S")}
        data_list.append(metrics_data)
    data_list = sorted(data_list, key=grouper)
    return data_list


def push_by_pushgateway():
    registry = CollectorRegistry()
    data_list = preparing_info_for_pushing()
    grouped_metrics_data = {}  # словарь отсортированных и сгруппированных данных
    labels = ['app_name', 'console_url', 'start_time', 'status']  # ярлыки для меток

    # создание метрики для статуса
    status_metric = Gauge('oozie_current_state', 'wf_status', labels[0:3], registry=registry)

    # создание метрики для времени действия статуса
    status_time_metric = Gauge('oozie_last_status_time', 'wf_last_status_time', labels, registry=registry)

    # создание метрики с последним временем выполнения завершенного потока
    completed_time_metric = Gauge('oozie_completed_time', 'wf_last_completed_time', labels, registry=registry)

    # итерация по сгруппированным данным и внесение данных в словарь grouped_metrics_data
    for job, group_items in groupby(data_list, key=grouper):
        status_metric.clear()
        status_time_metric.clear()
        completed_time_metric.clear()

        for item in group_items:
            grouped_metrics_data[job] = item
            break

        start_time = item['start_time']
        end_time = item['end_time']
        console_url = item['console_url']
        oozie_current_state = item['oozie_current_state']

        # присвоение ярлыков и значения метрике статуса
        status_metric.labels(job, console_url, start_time).set(oozie_current_state)

        # присвоение ярлыков и значения метрике времени действия статуса
        now = datetime.datetime.now()
        last_status_time = now - datetime.datetime.strptime(item['time'], "%d %b %Y %H:%M:%S")
        status_time_metric.labels(job, console_url, start_time, get_key_by_value(oozie_current_state)) \
            .set(str(last_status_time.seconds))

        # присвоение ярлыков и значения метрике с последним временем выполнения завершенного потока
        if end_time is not None:
            end_time_datetime_form = datetime.datetime.strptime(end_time, '%a, %d %b %Y %H:%M:%S %Z')
            start_time_datetime_form = datetime.datetime.strptime(start_time, '%a, %d %b %Y %H:%M:%S %Z')
            execution_time = (end_time_datetime_form - start_time_datetime_form)
            completed_time_metric.labels(job, console_url, start_time, get_key_by_value(oozie_current_state)) \
                .set(execution_time.seconds)

        push_to_gateway('localhost:9091', job=job, registry=registry)


if __name__ == "__main__":
    push_by_pushgateway()
    time.sleep(int(get_config()['delay']))
