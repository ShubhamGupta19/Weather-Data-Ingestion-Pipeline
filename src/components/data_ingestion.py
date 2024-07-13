# src/components/data_ingestion.py

import logging
from src.exception import CustomException
import os
import sys
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.components.data_modelling import WeatherData, Base
from src.config.database_config import SQLALCHEMY_DATABASE_URI

# Create the SQLAlchemy engine and session
engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)

@dataclass
class DataIngestionConfig:
    folder_path: str = "wx_data"
    batch_size: int = 1000  #batch size for batch processing

class DataIngestion:
    def __init__(self,ingestion_config:DataIngestionConfig):
        self.ingestion_config = ingestion_config
        logging.info(f"Data Ingestion Configuration: {self.ingestion_config}")

    def initiate_data_ingestion(self):
        logging.info("Entered the data ingestion component")
        session = Session()
        records_processed = 0
        batch_records = []

        try:
            folder_path = self.ingestion_config.folder_path
            logging.info(f"Reading files from folder: {folder_path}")
            for filename in os.listdir(folder_path):
                if filename.endswith('.txt'):
                    file_path = os.path.join(folder_path, filename)
                    logging.info(f"Processing file: {file_path}")
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
                                    weather_entry = WeatherData(
                                        station_id=filename.split('.')[0],  # Assuming filename is station_id
                                        date=date,
                                        max_temp=max_temp if max_temp != -9999 else None,
                                        min_temp=min_temp if min_temp != -9999 else None,
                                        precipitation=precipitation if precipitation != -9999 else None
                                    )
                                    batch_records.append(weather_entry)
                                    if len(batch_records) >= self.ingestion_config.batch_size:
                                        session.bulk_save_objects(batch_records)
                                        session.commit()
                                        records_processed += len(batch_records)
                                        logging.info(f"Committed {len(batch_records)} records to the database.")
                                        batch_records.clear()
                                except Exception as e:
                                    logging.error(f"Error processing line: {line}. Error: {e}")
            if batch_records:
                session.bulk_save_objects(batch_records)
                session.commit()
                records_processed += len(batch_records)
                logging.info(f"Committed {len(batch_records)} remaining records to the database.")
            logging.info(f"Total records processed: {records_processed}")
        except Exception as e:
            logging.error(f"Error in data ingestion process: {e}")
            session.rollback()
            raise CustomException(e, sys)
        finally:
            logging.info("Closing session")
            session.close()
