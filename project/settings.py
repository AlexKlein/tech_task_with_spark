"""
Settings for the app.
"""
import os

VERSION = '0.1'
DATABASE = {
    'ENGINE': 'psycopg2',
    'HOST': os.getenv('POSTGRES_HOST'),
    'PORT': os.getenv('POSTGRES_PORT'),
    'DATABASE': os.getenv('POSTGRES_DB'),
    'USER': os.getenv('POSTGRES_USER'),
    'PASSWORD': os.getenv('POSTGRES_PASSWORD')
}
LOG_FILE = '/tmp/etl_project/log/output.log'
FILES_DIR_PATH = '/tmp/etl_project/data'
