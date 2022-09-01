from os import getcwd
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
    with open(join(join(join(join(getcwd(), 'app'), 'etl_core'), 'sql'),
                   'delete_feature_week_retention_rate_diff.sql')) as file:
        query = text(file.read())
        get_engine().execute(query)

    logger.info(f"""Script delete_feature_week_retention_rate_diff has been executed {datetime.now()}""")


def insert_data(df):
    df.write.mode("append") \
        .format("jdbc") \
        .option("url", f"""jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/""") \
        .option("dbtable", "core.feature_retention_rate") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .save()


def start_computing():
    spark = SparkSession.builder.appName("jbTechTest").getOrCreate()

    union_events_df = None

    for file in get_list_of_files():

        events_df = spark.read.parquet(file)
        if union_events_df is None:
            union_events_df = events_df
        else:
            union_events_df = union_events_df.union(events_df).distinct()

    union_events_df.createOrReplaceTempView('events')

    feature_releases_df = spark.read.parquet(join(FILES_DIR_PATH, 'feature_releases.parquet'))
    feature_releases_df.createOrReplaceTempView('feature_releases')

    query = """
    with day_zero
    as (select min(e.timestamp) as day_zero_dt,
               e.user_id        as user_id,
               e.product        as product,
               f.feature_id     as feature_id
        from   events e
        inner join feature_releases f
                on e.event_context['additional']['product']['feature_id'] = f.feature_id and
                   cast(e.timestamp as date) >= f.release_date
        group by e.user_id,
                 e.product,
                 f.feature_id),
    day_seven
    as (select cast(e.timestamp as date) as day_seven_entry,
               d.user_id                 as user_id,
               d.product                 as product,
               d.feature_id              as feature_id
        from   events e
        inner join day_zero d
                on e.user_id = d.user_id and
                   e.product = d.product and
                   e.event_context['additional']['product']['feature_id'] = d.feature_id and
                   cast(e.timestamp as date) = date_add(d.day_zero_dt, 7))
    select dz.user_id         as user_id,
           dz.product         as product,
           dz.feature_id      as feature_id,
           dz.day_zero_dt     as day_zero_dt,
           ds.day_seven_entry as day_seven_entry
    from   day_zero dz
    left outer join day_seven ds
                 on dz.user_id = ds.user_id and
                    dz.product = ds.product and
                    dz.feature_id = ds.feature_id
    where  dz.day_zero_dt between current_date - 5 and
                                  current_date - 1
    """

    return spark.sql(query)


def start_feature_week_retention_rate_diff():
    logger.info(f"""Start core feature_week_retention_rate_diff ETL process {datetime.now()}""")
    clean_data()
    df = start_computing()
    insert_data(df)
    logger.info(f"""Finish core feature_week_retention_rate_diff ETL process {datetime.now()}""")
