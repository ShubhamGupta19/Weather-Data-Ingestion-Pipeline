from typing import Set
import os
import sys
import csv
import time
from datetime import datetime
import concurrent.futures
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from src.logger import logging  # Assuming logging setup in 'src/logger.py'
from src.exception import CustomException
from src.components.data_modelling import WeatherData, Base
from src.config.database_config import SQLALCHEMY_DATABASE_URI

# Create the SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URI)

class DataIngestionConfig:
    """
    Configuration class for data ingestion operations.

    Attributes:
        folder_path (str): Path to the folder containing data files.
        batch_size (int): Number of records to process in each batch.
    """
    def __init__(self, folder_path="wx_data", batch_size=5000):
        """
        Initialize DataIngestionConfig instance.

        Args:
            folder_path (str, optional): Path to the folder containing data files. Default is "wx_data".
            batch_size (int, optional): Number of records to process in each batch. Default is 5000.
        """
        self.folder_path = folder_path
        self.batch_size = batch_size

class DataIngestion:
    """
    Class for handling data ingestion operations.

    Attributes:
        ingestion_config (DataIngestionConfig): Configuration object containing data ingestion settings.
        existing_records (Set[(str, datetime.date)]): Set to store existing (station_id, date) tuples.
    """
    def __init__(self, ingestion_config: DataIngestionConfig):
        """
        Initialize DataIngestion instance.

        Args:
            ingestion_config (DataIngestionConfig): Configuration object containing data ingestion settings.
        """
        self.ingestion_config = ingestion_config
        self.existing_records = set()  # To store existing (station_id, date) tuples
        self._fetch_existing_records()

    def _fetch_existing_records(self):
        """
        Fetch existing (station_id, date) tuples from the database and store them in self.existing_records.
        """
        try:
            # Establish session to query existing records
            Session = sessionmaker(bind=engine)
            session = Session()
            existing_records = session.query(WeatherData.station_id, WeatherData.date).all()
            self.existing_records = {(record.station_id, record.date) for record in existing_records}
            
            session.close()
        except Exception as e:
            logging.error(f"Error fetching existing records from database: {e}")

    def create_temporary_table(self):
        """
        Create temporary table 'temp_weather_data' in the database for staging data ingestion.
        """
        try:
            with engine.connect() as connection:
                connection.execute("""
                    CREATE TABLE temp_weather_data (
                        station_id VARCHAR,
                        date DATE,
                        max_temp FLOAT,
                        min_temp FLOAT,
                        precipitation FLOAT
                    )
                """)
                logging.info("Temporary table 'temp_weather_data' created successfully")
        except Exception as e:
            logging.error(f"Error creating temporary table: {e}")

    def process_file(self, file_path):
        """
        Process each file in the specified folder path:
        - Parse data from each line.
        - Validate and convert data types.
        - Check for duplicates and insert valid records into the temporary table.

        Args:
            file_path (str): Path to the file to be processed.

        Returns:
            int: Number of processed records.
            int: Number of duplicate records skipped.
        """
        processed_count = 0
        duplicate_count = 0
        
        try:
            with open(file_path, 'r') as file:
                for line in file:
                    data = line.strip().split('\t')
                    if len(data) == 4:
                        date_str, max_temp_str, min_temp_str, precipitation_str = data
                        try:
                            date = datetime.strptime(date_str, '%Y%m%d').date()
                            max_temp = float(max_temp_str) / 10.0
                            min_temp = float(min_temp_str) / 10.0
                            precipitation = float(precipitation_str) / 10.0
                            station_id = os.path.basename(file_path).split('.')[0]

                            # Check if (station_id, date) already exists in database
                            if (station_id, date) in self.existing_records:
                                logging.debug(f"Skipping duplicate record: {station_id}, {date}")
                                duplicate_count += 1
                                continue

                            # Insert into temporary table
                            with engine.connect() as connection:
                                connection.execute("""
                                    INSERT INTO temp_weather_data (station_id, date, max_temp, min_temp, precipitation)
                                    VALUES (%s, %s, %s, %s, %s)
                                """, (station_id, date, max_temp if max_temp != -9999 else None,
                                      min_temp if min_temp != -9999 else None,
                                      precipitation if precipitation != -9999 else None))
                            
                            processed_count += 1
                        except Exception as e:
                            logging.error(f"Error processing line: {line}. Error: {e}")
                            exit(1)
        
            logging.info(f"Finished processing file: {file_path}. Processed: {processed_count}, Duplicates: {duplicate_count}")
            
            return processed_count, duplicate_count
        except Exception as e:
            logging.error(f"Error in processing file: {file_path}. Error: {e}")
            return processed_count, duplicate_count

    def insert_into_main_table(self):
        """
        Insert data from the temporary table 'temp_weather_data' into the main table 'weather_data'.
        """
        try:
            with engine.connect() as connection:
                connection.execute("""
                    INSERT INTO weather_data (station_id, date, max_temp, min_temp, precipitation)
                    SELECT station_id, date, max_temp, min_temp, precipitation
                    FROM temp_weather_data
                    ON CONFLICT (station_id, date) DO NOTHING
                """)
                logging.info("Data successfully inserted into 'weather_data' table from temporary table")
        except exc.ProgrammingError as e:
            logging.error(f"Error inserting data into 'weather_data' table: {e}")
        except Exception as e:
            logging.error(f"Unexpected error inserting data into 'weather_data' table: {e}")

    def initiate_data_ingestion(self):
        """
        Initiates the data ingestion process:
        - Creates a temporary table.
        - Processes each file in the specified folder concurrently.
        - Inserts valid data into the temporary table.
        - Inserts data from the temporary table into the main table.
        - Logs ingestion progress and completion.
        """
        start_time = time.time()
        logging.info("Entered the data ingestion component")

        try:
            self.create_temporary_table() 
            folder_path = self.ingestion_config.folder_path
            logging.info(f"Reading files from folder: {folder_path}")

            files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.txt')]
            
            total_duplicates = 0
            total_processed = 0
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(self.process_file, file_path) for file_path in files]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        processed_count, duplicate_count = future.result()
                        total_duplicates += duplicate_count
                        total_processed += processed_count
                    except Exception as e:
                        logging.error(f"Error in future result: {e}")
            
            self.insert_into_main_table()

            end_time = time.time()
            logging.info(f"Data ingestion Started at {start_time} and Ended at {end_time}")
            logging.info(f"Data ingestion completed in {end_time - start_time:.2f} seconds")
            logging.info(f"Total records processed: {total_processed}")
            logging.info(f"Total duplicate records skipped: {total_duplicates}")

        except Exception as e:
            logging.error(f"Error in data ingestion process: {e}")
            raise CustomException(e, sys)
        finally:
            logging.info("Dropping temporary table 'temp_weather_data'")
            try:
                with engine.connect() as connection:
                    connection.execute("DROP TABLE IF EXISTS temp_weather_data")
                logging.info("Temporary table 'temp_weather_data' dropped successfully")
            except Exception as e:
                logging.error(f"Error dropping temporary table 'temp_weather_data': {e}")

