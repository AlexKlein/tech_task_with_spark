from os.path import join
from datetime import datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import settings as config
from common.logger import logging


logger = logging.getLogger(__name__)


FILES_DIR_PATH = config.FILES_DIR_PATH


def generate_events():
    for i in range(5):
        gen_date = (datetime.today() - timedelta(days=i)).strftime("%Y%m%d")

        events_df = pd.DataFrame({'product': ['pyCharm', 'DataGrip', 'IDEA', 'YouTrack', 'DataGrip'],
                                  'product_version': ['2021.2', '2022.1', '2019.4', '2020.3', '2022.2'],
                                  'timestamp': ['2022-08-28 22:22:23.173210',
                                                '2022-08-28 12:57:16.456874',
                                                '2022-09-01 09:12:35.597613',
                                                '2022-08-31 16:14:59.156497',
                                                '2022-09-02 15:12:12.684348'],
                                  'group_id': ['update', 'update', 'upgrade', 'upgrade', 'update'],
                                  'event_id': [65564, 65564, 54684, 13449, 65564],
                                  'event_type': ['change_color',
                                                 'change_color',
                                                 'version_upgrade',
                                                 'version_upgrade',
                                                 'change_color'],
                                  'user_id': [165489, 654891, 795643, 654891, 6846365],
                                  'project_id': [3649461, 6546454, 561321, 654649, 6546454],
                                  'event_context': [
                                      {'has_license': False,
                                       'prev_state': 'light',
                                       'curr_state': 'dark',
                                       'additional': {'user': {'country': 'Canada',
                                                               'timezone': 'GMT-5',
                                                               'OS': 'Windows'},
                                                      'product': {'product_id': 8542135,
                                                                  'build_id': 321654864,
                                                                  'license_type': 'Community',
                                                                  'runtime_version_id': 321354,
                                                                  'feature_id': 3321516484}}},
                                      {'has_license': False,
                                       'prev_state': "dark",
                                       'curr_state': "light",
                                       'additional': {'user': {'country': 'Norway',
                                                               'timezone': 'GMT+2',
                                                               'OS': 'RedHat'},
                                                      'product': {'product_id': 3113551,
                                                                  'build_id': 4654684,
                                                                  'license_type': 'Trial',
                                                                  'runtime_version_id': 5458468,
                                                                  'feature_id': 213265484}}},
                                      {'has_license': True,
                                       'prev_state': "2021.4",
                                       'curr_state': "2022.2",
                                       'additional': {'user': {'country': 'Latvia',
                                                               'timezone': 'GMT+3',
                                                               'OS': 'MacOS'},
                                                      'product': {'product_id': 565498,
                                                                  'build_id': 5568464,
                                                                  'license_type': 'Indefinite',
                                                                  'runtime_version_id': 32132164,
                                                                  'feature_id': 0}}},
                                      {'has_license': True,
                                       'prev_state': "2018.3",
                                       'curr_state': "2022.4",
                                       'additional': {'user': {'country': 'Australia',
                                                               'timezone': 'GMT+10',
                                                               'OS': 'MacOS'},
                                                      'product': {'product_id': 313586484,
                                                                  'build_id': 213564,
                                                                  'license_type': 'Indefinite',
                                                                  'runtime_version_id': 321354684,
                                                                  'feature_id': 0}}},
                                      {'has_license': True,
                                       'prev_state': "dark",
                                       'curr_state': "light",
                                       'additional': {'user': {'country': 'Brazil',
                                                               'timezone': 'GMT-3',
                                                               'OS': 'Windows'},
                                                      'product': {'product_id': 3113551,
                                                                  'build_id': 4654684,
                                                                  'license_type': 'One Year License',
                                                                  'runtime_version_id': 5458468,
                                                                  'feature_id': 213265484}}}]})
        events_table = pa.Table.from_pandas(events_df)
        pq.write_table(events_table, join(FILES_DIR_PATH, f"""{gen_date}_events.parquet"""))

    logger.info(f"""Set of events data parquet files has created""")


def generate_dicts():
    users_df = pd.DataFrame({'user_id': [65654565, 6846365, 795643, 654891, 65489132, 44864684],
                             'sign_in_date': ['2022-07-27',
                                              '2022-08-21',
                                              '2022-08-24',
                                              '2022-08-19',
                                              '2018-12-31',
                                              '2022-07-12'],
                             'user_login': ['Cody', 'Bjorn', 'Dmitriy', 'Brooklyn', 'Caio', 'Priya']})
    users_table = pa.Table.from_pandas(users_df)
    pq.write_table(users_table, join(FILES_DIR_PATH, 'users.parquet'))

    releases_df = pd.DataFrame({'release_id': ['2022.2', '2022.3', '2022.2.1', '2022.2'],
                                'product_id': [8542135, 3113551, 565498, 313586484],
                                'release_date': ['2022-07-27',
                                                 '2022-07-20',
                                                 '2022-07-26',
                                                 '2022-08-05']})
    releases_table = pa.Table.from_pandas(releases_df)
    pq.write_table(releases_table, join(FILES_DIR_PATH, 'releases.parquet'))

    feature_releases_df = pd.DataFrame({'feature_release_id': [3321516484, 8792135484, 213265484, 2315115561],
                                        'product_id': [8542135, 3113551, 565498, 313586484],
                                        'feature_id': [8799561321, 23154494, 456513518, 654641313],
                                        'release_date': ['2022-08-14',
                                                         '2022-08-09',
                                                         '2022-08-28',
                                                         '2022-08-27']})
    feature_releases_table = pa.Table.from_pandas(feature_releases_df)
    pq.write_table(feature_releases_table, join(FILES_DIR_PATH, 'feature_releases.parquet'))

    logger.info(f"""Set of dictionaries data parquet files has created""")


def start_generator():
    generate_events()
    generate_dicts()
