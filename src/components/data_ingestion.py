# src/components/data_ingestion.py

import logging
from exception import CustomException
import os
import sys
from datetime import datetime
from dataclasses import dataclass
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.components.data_modelling import WeatherData, Base
from config.database_config import SQLALCHEMY_DATABASE_URI

engine = create_engine(SQLALCHEMY_DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

@dataclass
class DataIngestionConfig:





class DataIngestion:
    def __init__(self):
        self.ingestion_config= DataIngestionConfig()
    

    def initiate_data_ingestion(self):
        
        logging.info("Entered the data ingestion component")

        try: 
            folder_path="wx_data"
            for filename in os.listdir(folder_path):
                if filename.endswith('.txt'):
                    file_path = os.path.join(folder_path, filename)
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
                                        max_temp=max_temp if max_temp != -999.9 else None,
                                        min_temp=min_temp if min_temp != -999.9 else None,
                                        precipitation=precipitation if precipitation != -999.9 else None
                                    )
                                    session.add(weather_entry)
                                except Exception as e:
                                    print(f"Error processing line: {line}. Error: {e}")
                    session.commit()
        except Exception as e:
            raise CustomException(e,sys)


