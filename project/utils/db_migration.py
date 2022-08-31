"""
Util for running SQL statements for the first step of working with the database.
"""
from os import getcwd, walk
from os.path import join

import psycopg2

from common import postgres_wrapper
from common.logger import logging


logger = logging.getLogger(__name__)


def commands_launcher(sql_file, conn):
    sql_commands = sql_file.split(';')

    for command in sql_commands:

        if len(command.strip()) > 0:
            try:
                conn.execute(raw_sql=command)
            except psycopg2.ProgrammingError as e:
                e = str(e).replace('\n', '')
                logger.error(f"""Crashed when running the command: {e}""")
            except Exception as e:
                logger.error(f"""Crashed when running the command: {e}""")

    conn.execute(raw_sql='commit')


def start_migration():
    connection = postgres_wrapper.PostgresWrapper()

    for path, dirs, files in walk(getcwd()):

        if path.find('migrations') > 0:

            for f in files:
                full_path = join(path, f)
                logger.info(f"""Commands from {full_path}""")

                with open(full_path, 'r', encoding='UTF-8') as file:
                    try:
                        commands_launcher(file.read(), connection)
                    except:
                        logger.error(f"""Troubles with {full_path}""")

    connection.close()
