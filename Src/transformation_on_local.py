import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, Boolean
import os
import numpy as np
import logging


def setup_logger(log_file_path):
    """Setup logger configuration."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setFormatter(log_format)
    if not logger.handlers:
        logger.addHandler(file_handler)
    return logger

def read_config(config_path):
    """Reads the configuration file."""
    try:
        with open(config_path) as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found at {config_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error parsing JSON in config file: {config_path}")
        raise

def read_csv(csv_file_path):
    """Reads the CSV file into a DataFrame."""
    try:
        df = pd.read_csv(csv_file_path)
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_file_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"CSV file is empty or contains no data: {csv_file_path}")
        raise
    except pd.errors.ParserError:
        logger.error(f"Error parsing CSV file: {csv_file_path}")
        raise

def create_postgres_engine(pg_credentials):
    """Creates a SQLAlchemy engine for PostgreSQL."""
    try:
        pg_connection_string = (
            f"postgresql://{pg_credentials['user']}:{pg_credentials['password']}@"
            f"{pg_credentials['host']}:{pg_credentials['port']}/{pg_credentials['dbname']}"
        )
        engine = create_engine(pg_connection_string)
        return engine
    except Exception as e:
        logger.error(f"Error creating PostgreSQL engine: {str(e)}")
        raise

def get_sqlalchemy_type(column):
    """Returns SQLAlchemy type based on DataFrame column dtype."""
    dtype = column.dtype
    if pd.api.types.is_integer_dtype(dtype):
        return Integer
    elif pd.api.types.is_float_dtype(dtype):
        return Float
    elif pd.api.types.is_bool_dtype(dtype):
        return Boolean
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return Date
    else:
        return String

def create_table_schema(metadata, table_name, df, engine):
    """Creates table schema based on DataFrame columns."""
    try:
        columns = [Column(col, get_sqlalchemy_type(df[col])) for col in df.columns]
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        return table
    except Exception as e:
        logger.error(f"Error creating table schema: {str(e)}")
        raise

def transform_data(df):
    """Transforms DataFrame."""
    try:
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce')
        df['last_review_date'] = df['last_review'].dt.date
        df['last_review_time'] = df['last_review'].dt.time
        avg_price_per_neighborhood = df.groupby('neighbourhood')['price'].mean().reset_index()
        avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)
        df = df.merge(avg_price_per_neighborhood, on='neighbourhood', how='left')
        df['reviews_per_month'].fillna(0, inplace=True)
        df['missing_reviews_per_month'] = np.where(df['reviews_per_month'].isna(), 1, 0)
        return df
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def load_data_to_postgres(df, table_name, engine):
    """Loads DataFrame into PostgreSQL."""
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"Data loaded successfully into {table_name} table.")
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise

def main():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file_rel_path = '..\\config'
        log_file_rel_path = '..\\log'
        csv_file_rel_path = '..\\data'
        config_path = os.path.join(script_dir, config_file_rel_path, 'local_config.json')
        log_file_path = os.path.join(script_dir, log_file_rel_path, 'etl_pipeline.log')
        csv_file_path = os.path.join(script_dir, csv_file_rel_path, "AB_NYC_2019.csv")
        global logger
        logger = setup_logger(log_file_path)
        config = read_config(config_path)
        pg_credentials = config['postgresql']
        df = read_csv(csv_file_path)
        engine = create_postgres_engine(pg_credentials)

        metadata = MetaData()
        table_name = 'AB_NYC_2019'
        table = create_table_schema(metadata, table_name, df, engine)

        load_data_to_postgres(df, table_name, engine)
        df_transformed = transform_data(df)

        transformed_table_name = 'transformed_AB_NYC_2019'
        transformed_table = create_table_schema(metadata, transformed_table_name, df_transformed, engine)

        load_data_to_postgres(df_transformed, transformed_table_name, engine)
        logger.info("ETL process completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during ETL process: {str(e)}")
        raise

if __name__ == "__main__":
    main()
