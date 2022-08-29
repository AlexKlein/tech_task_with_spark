from os.path import join
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

import settings as config
from common.logger import logging

from pyspark.sql import SparkSession


logger = logging.getLogger(__name__)


FILES_DIR_PATH = config.FILES_DIR_PATH
POSTGRES_HOST = config.DATABASE['HOST']
POSTGRES_PORT = int(config.DATABASE['PORT'])
POSTGRES_DATABASE = config.DATABASE['DATABASE']
POSTGRES_USER = config.DATABASE['USER']
POSTGRES_PASSWORD = config.DATABASE['PASSWORD']


def get_list_of_files():
    return sorted(
        [join(FILES_DIR_PATH,
         f"""{(datetime.today() - timedelta(days=i)).strftime("%Y%m%d")}_events.parquet""") for i in range(5)])


def get_engine():
    return create_engine(
        f"""postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"""
    ).connect()


def clean_data():
    with open(join('sql', 'delete_fraction_of_users_upgraded_in_one_month.sql')) as file:
        query = text(file.read())
        get_engine().execute(query)


def insert_data(df):
    result = df.toPandas()
    engine = get_engine()
    result.to_sql(schema='core',
                  name='number_of_users_upgraded_in_one_month',
                  con=engine,
                  if_exists='append',
                  index=False)


def start_computing():
    logger.info(f"""Start core fraction_of_users_upgraded_in_one_month ETL process {datetime.now()}""")
    print('Start core fraction_of_users_upgraded_in_one_month ETL process', datetime.now())

    spark = SparkSession.builder.appName("jbTechTest").getOrCreate()

    union_events_df = None

    for file in get_list_of_files():

        events_df = spark.read.parquet(file)
        if union_events_df is None:
            union_events_df = events_df
        else:
            union_events_df = union_events_df.union(events_df).distinct()

    union_events_df.createOrReplaceTempView('events')

    users_df = spark.read.parquet(join(FILES_DIR_PATH, 'users.parquet'))
    users_df.createOrReplaceTempView('users')

    releases_df = spark.read.parquet(join(FILES_DIR_PATH, 'releases.parquet'))
    releases_df.createOrReplaceTempView('releases')

    query = """
    with
        total_users
        as (select count(usr.user_id) as number_of_users
            from   users usr)
    select cast(e.timestamp as date) as value_date,
           e.product                 as product,
           r.release_id              as release_id,
           count(distinct e.user_id) as number_of_users_upgraded_version,
           t.number_of_users         as number_of_users
    from   events e
    inner join releases r
            on e.event_context['additional']['product']['product_id'] = r.product_id and
               e.timestamp between r.release_date and
                                   add_months(r.release_date, 1) - 1
    inner join total_users t
            on 1 = 1
    where  e.group_id = 'upgrade' and
           cast(e.timestamp as date) between current_date - 5 and
                                             current_date - 1
    group by e.product,
             r.release_id,
             t.number_of_users,
             cast(e.timestamp as date)
    """

    logger.info(f"""Finish core fraction_of_users_upgraded_in_one_month ETL process {datetime.now()}""")
    print('Finish core fraction_of_users_upgraded_in_one_month ETL process', datetime.now())

    return spark.sql(query)


def start_fraction_of_users_upgraded_in_one_month():
    df = start_computing()
    clean_data()
    insert_data(df)
