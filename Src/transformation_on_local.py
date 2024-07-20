import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date
import os
import numpy as np
import logging


def setup_logger(log_file_path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    log_dir = os.path.dirname(log_file_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger

script_dir = os.path.dirname(os.path.abspath(__file__))
config_file_rel_path = '../config'
path = os.path.join(script_dir, config_file_rel_path, 'local_config.json')
csv_file_rel_path = "../data"
csv_file_path = os.path.join(script_dir, csv_file_rel_path, "AB_NYC_2019.csv")
log_file_path = os.path.join(script_dir, '../log', 'etl_process.log')
logger = setup_logger(log_file_path)

try:
    with open(path) as config_file:
        config = json.load(config_file)

    pg_credentials = config['postgresql']
    logger.info("Configuration file read successfully")

except FileNotFoundError:
    logger.error(f"Config file not found at {path}")
    raise
except json.JSONDecodeError:
    logger.error(f"Error parsing JSON in config file: {path}")
    raise

try:
    df = pd.read_csv(csv_file_path)
    logger.info("CSV file read successfully")

except FileNotFoundError:
    logger.error(f"CSV file not found at {csv_file_path}")
    raise
except pd.errors.EmptyDataError:
    logger.error(f"CSV file is empty or contains no data: {csv_file_path}")
    raise
except pd.errors.ParserError:
    logger.error(f"Error parsing CSV file: {csv_file_path}")
    raise

try:
    pg_connection_string = f"postgresql://{pg_credentials['user']}:{pg_credentials['password']}@{pg_credentials['host']}:{pg_credentials['port']}/{pg_credentials['dbname']}"
    engine = create_engine(pg_connection_string)
    metadata = MetaData()

    table = Table(
        'AB_NYC_2019', metadata,
        Column('id', Integer, primary_key=True, unique=True),
        Column('name', String),
        Column('host_id', Integer),
        Column('host_name', String),
        Column('neighbourhood_group', String),
        Column('neighbourhood', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('room_type', String),
        Column('price', Integer),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('last_review', Date),
        Column('reviews_per_month', Float),
        Column('calculated_host_listings_count', Integer),
        Column('availability_365', Integer)
    )

    metadata.create_all(engine)
    logger.info("Table schema created successfully")

except Exception as e:
    logger.error(f"Error creating table schema: {str(e)}")
    raise

try:
    df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
    df.to_sql('AB_NYC_2019', engine, if_exists='replace', index=False, method='multi')
    logger.info("Data loaded successfully into AB_NYC_2019 table")

except Exception as e:
    logger.error(f"Error loading data to PostgreSQL: {str(e)}")
    raise

try:
    df = pd.read_sql_table('AB_NYC_2019', engine)
    df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
    df['last_review_date'] = df['last_review'].dt.date
    df['last_review_time'] = df['last_review'].dt.time
    avg_price_per_neighborhood = df.groupby('neighbourhood')['price'].mean().reset_index()
    avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)
    df = df.merge(avg_price_per_neighborhood, on='neighbourhood', how='left')
    df['reviews_per_month'].fillna(0, inplace=True)
    df['missing_reviews_per_month'] = np.where(df['reviews_per_month'].isna(), 1, 0)
    transformed_table = Table(
        'transformed_AB_NYC_2019', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('host_id', Integer),
        Column('host_name', String),
        Column('neighbourhood_group', String),
        Column('neighbourhood', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('room_type', String),
        Column('price', Integer),
        Column('minimum_nights', Integer),
        Column('number_of_reviews', Integer),
        Column('last_review_date', Date),
        Column('last_review_time', String),
        Column('reviews_per_month', Float),
        Column('calculated_host_listings_count', Integer),
        Column('availability_365', Integer),
        Column('avg_price', Float),
        Column('missing_reviews_per_month', Integer)
    )

    metadata.create_all(engine)
    df.to_sql('transformed_AB_NYC_2019', engine, if_exists='replace', index=False)
    logger.info("Transformed data loaded successfully into transformed_AB_NYC_2019 table")

except Exception as e:
    logger.error(f"Error processing data: {str(e)}")
    raise

logger.info("ETL process completed successfully")
