"""
ETL process for running ETL processes for tables in core area.
"""
from datetime import datetime

from common.logger import logging
from etl_core.feature_week_retention_rate_diff import start_feature_week_retention_rate_diff
from etl_core.fraction_of_new_users_use_feature_14_days import start_fraction_of_new_users_use_feature_14_days
from etl_core.fraction_of_users_upgraded_in_one_month import start_fraction_of_users_upgraded_in_one_month

logger = logging.getLogger(__name__)


def start_core_layer():
    logger.info(f"""Start core layer filling in {datetime.now()}""")
    print('Start core layer filling in ', datetime.now())

    start_feature_week_retention_rate_diff()
    start_fraction_of_new_users_use_feature_14_days()
    start_fraction_of_users_upgraded_in_one_month()

    logger.info(f"""Start core layer filling in {datetime.now()}""")
    print('Start core layer filling in ', datetime.now())
