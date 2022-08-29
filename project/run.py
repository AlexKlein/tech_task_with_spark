"""
Entrypoint for the database preparation process.
"""
import sys
import warnings
from datetime import datetime
from time import sleep

import psycopg2

from common import postgres_wrapper
from common.logger import logging
from etl_core.launcher import start_core_layer
from etl_datamart.launcher import start_datamart_layer
from utils.generator import start_generator
from utils.db_migration import start_migration


logger = logging.getLogger(__name__)


def check_data(conn):
    try:
        conn.execute(raw_sql='select 1 from datamart.feature_retention_rate limit 0')

        logger.info(f"""Migration has been done""")
        print(f"""Migration has been already done""")
    except psycopg2.ProgrammingError as e:
        logger.info(f"""Start migration""")
        print(f"""Start migration""")

        start_migration()


def delay_db(conn):
    i = 1
    run_flag = False

    while i <= 10 and not run_flag:
        try:
            conn.execute(raw_sql='select 1')
            run_flag = True
            break
        except psycopg2.OperationalError as e:
            sleep(i)
            i += 1

    if run_flag:
        logger.info(f"""Database started""")
        print(f"""Database started""")
    else:
        logger.error(f"""Database didn't start""")
        print(f"""Database didn't start""")
        sys.exit(1)


def start_app():
    warnings.simplefilter(action='ignore')

    logger.info(f"""Start database preparation in {datetime.now()}""")
    print('Start database preparation in', datetime.now())

    connection = postgres_wrapper.PostgresWrapper()
    delay_db(connection)
    check_data(connection)
    start_generator()
    start_core_layer()
    start_datamart_layer()

    logger.info(f"""Finish database preparation in {datetime.now()}""")
    print('Finish database preparation in', datetime.now())
