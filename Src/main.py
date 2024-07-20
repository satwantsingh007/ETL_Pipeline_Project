import json
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Date, Boolean
import numpy as np
import logging
from metaflow import FlowSpec, step, Parameter

def setup_logger(log_file_path):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_format = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
    file_handler = logging.FileHandler(log_file_path, mode='w')
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    return logger

class ETLFlow(FlowSpec):
    config_path = Parameter('config_path', default='/mnt/c/Users/satwa/OneDrive/Desktop/Project_AI_Planet/Project_AI_Planet/Config/config.json')
    log_file_path = Parameter('log_file_path', default='/mnt/c/Users/satwa/OneDrive/Desktop/Project_AI_Planet/Project_AI_Planet/log/log_file.log')
    @step
    def start(self):
        print("Starting ETL process")
        self.logger = setup_logger(self.log_file_path)
        self.logger.info("Starting ETL process")
        self.next(self.read_config)
    @step
    def read_config(self):
        """Reads the configuration file."""
        print("Reading configuration file")
        try:
            with open(self.config_path) as config_file:
                self.config = json.load(config_file)
            self.logger.info("Configuration file read successfully")
        except FileNotFoundError:
            self.logger.error(f"Config file not found at {self.config_path}")
            raise
        except json.JSONDecodeError:
            self.logger.error(f"Error parsing JSON in config file: {self.config_path}")
            raise
        self.next(self.read_csv)
    @step
    def read_csv(self):
        """Reads the CSV file into a DataFrame."""
        print("Reading CSV file")
        csv_file_path = self.config['csv_file_path']
        try:
            self.df = pd.read_csv(csv_file_path)
            self.logger.info("CSV file read successfully")
        except FileNotFoundError:
            self.logger.error(f"CSV file not found at {csv_file_path}")
            raise
        except pd.errors.EmptyDataError:
            self.logger.error(f"CSV file is empty or contains no data: {csv_file_path}")
            raise
        except pd.errors.ParserError:
            self.logger.error(f"Error parsing CSV file: {csv_file_path}")
            raise
        self.next(self.create_postgres_engine)
    @step
    def create_postgres_engine(self):
        """Creates a SQLAlchemy engine for PostgreSQL."""
        print("Creating PostgreSQL connection string")
        pg_credentials = self.config['postgresql']
        self.pg_connection_string = (
            f"postgresql://{pg_credentials['user']}:{pg_credentials['password']}@"
            f"{pg_credentials['host']}:{pg_credentials['port']}/{pg_credentials['dbname']}"
        )
        self.logger.info(f"PostgreSQL connection string: {self.pg_connection_string}")
        self.next(self.create_table_schema)

    @step
    def create_table_schema(self):
        """Creates table schema based on DataFrame columns."""
        print("Creating table schema")
        metadata = MetaData()
        table_name = 'AB_NYC_2019'
        try:
            engine = create_engine(self.pg_connection_string)
            columns = [Column(col, self.get_sqlalchemy_type(self.df[col])) for col in self.df.columns]
            self.table = Table(table_name, metadata, *columns)
            metadata.create_all(engine)
            self.logger.info("Table schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating table schema: {str(e)}")
            raise
        self.next(self.load_data_to_postgres)
    def get_sqlalchemy_type(self, column):
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
    @step
    def load_data_to_postgres(self):
        """Loads DataFrame into PostgreSQL."""
        print("Loading data into PostgreSQL")
        try:
            engine = create_engine(self.pg_connection_string)
            self.df.to_sql('AB_NYC_2019', engine, if_exists='replace', index=False)
            self.logger.info("Data loaded successfully into AB_NYC_2019 table")
        except Exception as e:
            self.logger.error(f"Error loading data to PostgreSQL: {str(e)}")
            raise
        self.next(self.transform_data)
    @step
    def transform_data(self):
        """Transforms DataFrame."""
        print("Transforming data")
        try:
            self.df['last_review'] = pd.to_datetime(self.df['last_review'], errors='coerce')
            self.df['last_review_date'] = self.df['last_review'].dt.date
            self.df['last_review_time'] = self.df['last_review'].dt.time
            avg_price_per_neighborhood = self.df.groupby('neighbourhood')['price'].mean().reset_index()
            avg_price_per_neighborhood.rename(columns={'price': 'avg_price'}, inplace=True)
            self.df = self.df.merge(avg_price_per_neighborhood, on='neighbourhood', how='left')
            self.df['reviews_per_month'].fillna(0, inplace=True)
            self.df['missing_reviews_per_month'] = np.where(self.df['reviews_per_month'].isna(), 1, 0)
            self.logger.info("Data transformation completed successfully")
        except Exception as e:
            self.logger.error(f"Error transforming data: {str(e)}")
            raise
        self.next(self.create_transformed_table_schema)

    @step
    def create_transformed_table_schema(self):
        """Defines and creates the schema for the transformed data."""
        print("Creating transformed table schema")
        metadata = MetaData()
        transformed_table_name = 'transformed_AB_NYC_2019'
        try:
            engine = create_engine(self.pg_connection_string)
            columns = [Column(col, self.get_sqlalchemy_type(self.df[col])) for col in self.df.columns]
            self.transformed_table = Table(transformed_table_name, metadata, *columns)
            metadata.create_all(engine)
            self.logger.info("Transformed table schema created successfully")
        except Exception as e:
            self.logger.error(f"Error creating transformed table schema: {str(e)}")
            raise
        self.next(self.load_transformed_data_to_postgres)

    @step
    def load_transformed_data_to_postgres(self):
        """Loads the transformed DataFrame into PostgreSQL."""
        print("Loading transformed data into PostgreSQL")
        try:
            engine = create_engine(self.pg_connection_string)
            self.df.to_sql('transformed_AB_NYC_2019', engine, if_exists='replace', index=False)
            self.logger.info("Transformed data loaded successfully into transformed_AB_NYC_2019 table")
        except Exception as e:
            self.logger.error(f"Error loading transformed data to PostgreSQL: {str(e)}")
            raise
        self.next(self.end)
    @step
    def end(self):
        print("ETL process completed successfully")
        self.logger.info("ETL process completed successfully")

if __name__ == "__main__":
    ETLFlow()
