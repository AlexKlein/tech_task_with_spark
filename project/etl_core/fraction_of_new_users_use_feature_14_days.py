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
    with open(join('sql', 'delete_fraction_of_new_users_use_feature_14_days.sql')) as file:
        query = text(file.read())
        get_engine().execute(query)


def insert_data(df):
    result = df.toPandas()
    engine = get_engine()
    result.to_sql(schema='core',
                  name='number_of_new_users_use_feature',
                  con=engine,
                  if_exists='append',
                  index=False)


def start_computing():
    logger.info(f"""Start core fraction_of_new_users_use_feature_14_days ETL process {datetime.now()}""")
    print('Start core fraction_of_new_users_use_feature_14_days ETL process', datetime.now())

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

    query = """
    with
        total_new_users
        as (select count(usr.user_id) as number_of_users
            from   users usr
            where  usr.sign_in_date >= trunc(add_months(current_date, -1), 'mm'))
    select cast(e.timestamp as date)                              as value_date,
           e.product                                              as product,
           e.event_context['additional']['product']['feature_id'] as feature_id,
           count(distinct u.user_id)                              as number_of_users_use_feature, 
           t.number_of_users                                      as number_of_new_users
    from   events e
    inner join users u
            on e.user_id = u.user_id and
               u.sign_in_date >= trunc(add_months(current_date, -1), 'mm')
    inner join total_new_users t
            on 1 = 1
    where  e.event_context['additional']['product']['feature_id'] != 0 and
           e.timestamp between current_date - 14 and
                               current_date - 1
    group by e.product,
             t.number_of_users,
             cast(e.timestamp as date),
             e.event_context['additional']['product']['feature_id']
    """

    logger.info(f"""Finish core fraction_of_new_users_use_feature_14_days ETL process {datetime.now()}""")
    print('Finish core fraction_of_new_users_use_feature_14_days ETL process', datetime.now())

    return spark.sql(query)


def start_fraction_of_new_users_use_feature_14_days():
    df = start_computing()
    clean_data()
    insert_data(df)
