"""
ETL process for running queries and filling tables in datamart area.
"""
from os import getcwd, walk
from os.path import join

from datetime import datetime

from common import postgres_wrapper
from common.logger import logging


logger = logging.getLogger(__name__)


def commands_launcher(sql_file, conn):
    sql_commands = sql_file.split(';')

    for command in sql_commands:

        if len(command.strip()) > 0:
            try:
                conn.execute(raw_sql=command)
            except Exception as e:
                e = str(e).replace('\n', '')
                logger.error(f"""Crashed when running the command: {e}""")

    conn.execute(raw_sql='commit')


def start_datamart_layer():
    logger.info(f"""Start datamart layer filling in {datetime.now()}""")

    connection = postgres_wrapper.PostgresWrapper()

    for path, dirs, files in walk(getcwd()):

        if path.find('etl_datamart') > 0:
            if path.find('sql') > 0:

                for f in files:
                    full_path = join(path, f)
                    logger.info(f"""Commands from {full_path}""")

                    with open(full_path, 'r', encoding='UTF-8') as file:
                        try:
                            commands_launcher(file.read(), connection)
                        except:
                            logger.error(f"""Troubles with {full_path}""")

    connection.close()

    logger.info(f"""Finish datamart layer filling in {datetime.now()}""")
