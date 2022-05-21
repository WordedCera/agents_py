import urllib
from urllib import response
import requests
import time
from datetime import datetime
import sys
import signal
import argparse
import schedule
import json
import configparser
from urllib import parse
import psycopg2
import logging
import string
import random

def argumentParser():
    parser = argparse.ArgumentParser(description='Agent for parsing prometheus metrics and storing them in PostgreSQL DB. \n'
                                    'By default, scraping starts every 15 seconds')
    
    parser.add_argument('--minutes', action='store_true',
                        help='Starting every minute')
    parser.add_argument('--seconds', action='store_true',
                        help='Starting every second')
    parser.add_argument('--hours', action='store_true',
                        help='Starting every hour')
    parser.add_argument('--time', type=int,
                        help='Time interval')
    parser.add_argument('--db_host', type=str,
                        help='Specify DB HOST')
    parser.add_argument('--db_port', type=str,
                        help='Specify DB port')
    parser.add_argument('--db_pass', type=str,
                        help='Specify DB password')
    parser.add_argument('--db_user', type=str,
                        help='Specify DB user')
    parser.add_argument('--db_scheme', type=str,
                        help='Db scheme with table to write to'
                        'Example: scheme.table')
    parser.add_argument('--db_name', type=str,
                        help='Specify DB Name')
    parser.add_argument('--log_level', type=str,
                        help='Specify log level. Default - info')
    args = parser.parse_args()
    if args.minutes == True:
        vremya = 'minutes'
    elif args.seconds == True:
        vremya = 'seconds'
    elif args.hours == True:
        vremya = 'hours'
    else:
        vremya = 'seconds'

    if args.time:
        chisla = args.time
    else:
        chisla = 15
    
    if args.log_level == 'info':
        log_level = 'INFO'
    elif args.log_level == 'error':
        log_level = 'ERROR'
    elif args.log_level == 'debug':
        log_level = 'DEBUG'
    else:
        log_level = 'INFO'
    return args, vremya, chisla, log_level

def logirovanie(log_level):
    lenght = 25
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    num = string.digits
    all_symbols = lower + upper + num
    np = random.sample(all_symbols, lenght)
    trace_id = ""
    log_func = getattr(logging, log_level)
    for s in np:
        trace_id += s
    logging.basicConfig(
        level = log_func,
        format=f"%(asctime)s - [%(levelname)s] - ({trace_id}) (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    )
    logger = logging.getLogger(__name__)
    return logger

args, vremya, chisla, log_level = argumentParser()

logger = logirovanie(log_level)

def signalHandler(sig,frame):
    logger.info("Agent stopped")
    sys.exit(0)

def tableConstructor(dicter):
    logger.debug("Constructing table to insert in PGSQL")
    d = json.loads(dicter)
    metric_table = []
    dic = d["data"]["result"]
    for i in dic:
        try:
            if str(i["value"][1]) == "nan":
                value = 0.000000000
            else:
                value = format(int(1["value"][1]), '.9f')
            vremya = datetime.now().replace(second=0,microsecond=0)
            name_is_dict = i["metric"]
            label_name = f'{str(name_is_dict["alt_name"])}_{str(name_is_dict["as"].replace(" ", "_"))}'
            label_name = label_name.replace("-","_").lower()
            whole_metric = [label_name, value, vremya]
            metric_table.append(whole_metric)
            logger.info("json is not empty")
        except Exception as e:
            logger.error(str(e))
            if e is not None:
                return None
    return metric_table

def takeConfig(file='config.cfg'):
    config = configparser.RawConfigParser()
    config.read(file)
    logger.info("Config parsed")
    endpoint = []
    for i, j in config["settings"].items():
        value = j.replace('\"', "")
        endpoint.append(value)
    options = list(config.sections())
    options.pop(0)
    diction = {}
    for i in options:
        diction[i] = dict(config[i].items())
        sub_diction = config[i].items()
        for keys, values in sub_diction:
            if values.startswith("\""):
                new_value = values.replace("\"","")
            elif values.startswith("\'"):
                new_value = values.startswith("\'", "")
            new_value2 = parse.quote_plus(new_value)
            diction[i][keys] = new_value2
    return endpoint, diction

def endpointsCreater(endpoint, diction):
    strin = endpoint[0] + endpoint[1]
    endpoints = []
    for keys, values in diction.items():
        as_name = keys.split("_")
        for key, value in values.items():
            endpoints.append(strin + value + endpoint[2] + as_name[0] + endpoint[3])
    return endpoints

def scrapePrometheus(endpoints, retries=3):
    logger.info("Working with endpoints")
    content = []
    for endpoint in endpoints:
        logger.info(f'Working with {endpoint} with {retries} retries')
        err = None
        for _ in range(retries):
            try:
                if endpoint.startswith('http'):
                    logger.info("Endpoint starts with http")
                    response = requests.get(endpoint)
                    if response.status_code == requests.status_codes.oh:
                        content.append(response.content.decode('utf-8', 'strict'))
                        break
                else:
                    response = urllib.request.urlopen(endpoint)
                    content.append(response.read().decode('utf-8', 'strict'))
                    break
            except (requests.exceptions.ConnectionError, ValueError) as e:
                logger.error(str(e))
                time.sleep(1)
    return content

def jsonCreater(list):
    times_unix = int(list[2].timestamp())
    v = int(float(list[1]))
    json_data = {"source": "prometheus", "name":list[0], "datetime":times_unix, "value":v}
    j = json.dumps(json_data)
    return j

def prometheusParser(endpoints, db_host, db_password, db_port="5432", dbname="postgres", db_user="postgres", schemaname="metrics.prometheus"):
    json_list = []
    try:
        content = scrapePrometheus(endpoints)
    except Exception as e:
        logger.error(str(e))
    for dicter in content:
        if tableConstructor(dicter) != None:
            lister = tableConstructor(dicter)
            for i in lister:
                json_list.append(jsonCreater(i))
        else:
            continue
    try:
        conn = psycopg2.connect(dbname=dbname, user=db_user,
                                password=db_password, host=db_host, port=db_port)
        cur = conn.cursor()
        logger.info('Connection to DB is successful')
        for js in json_list:
            j = json.loads(js)
            logger.info(f'Starting request')
            try:
                logger.debug("Started insert to first table")
                data1 = int(j['datetime'])
                cur.execute(f"insert into {schemaname} (metric_name, datetime, value) values ('{j['name']}', TO_TIMESTAMP('{data1}')::timestampz, {j['value']})")
                logger.debug("successful insert")
                conn.commit()
            except psycopg2.OperationalError as e:
                logger.error(str(e))
    except Exception as e:
        logger.error(str(e))
    cur.close()
    conn.close()
    logger.info("Connection to PostgreSQL closed")

if __name__ == "__main__":
    logger.info(f'Starting every {chisla} {vremya}')
    func = getattr(schedule.every(chisla), vremya)
    signal.signal(signal.SIGINT, signalHandler)
    endpoint, diction = takeConfig()
    endpoints = endpointsCreater(endpoint, diction)
    db_host = args.db_host
    db_password = args.db_pass
    db_user = args.db_user
    db_port = args.db_port
    if args.db_name == None:
        db_name = 'postgres'
    else:
        db_name = args.db_name
    prometheusParser(endpoints=endpoints, db_host=db_host, db_password=db_password, db_user=db_user, db_port=db_port, dbname=db_name)
    func.do(prometheusParser, endpoints=endpoints, db_host=db_host, db_password=db_password, db_user=db_user, db_port=db_port, dbname=db_name)

    while True:
        schedule.run_pending()
        time.sleep(0)
