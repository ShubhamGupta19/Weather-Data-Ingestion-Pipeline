from sqlalchemy import Column, Integer, String, Float, Date, create_engine, DateTime, CheckConstraint, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL
from sqlalchemy import MetaData,Table
from datetime import date
from src.logger import logging
from src.exception import CustomException
import sys
# Define SQLAlchemy Base class
Base = declarative_base()

# Define SQLAlchemy model for weather data
class WeatherData(Base):
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, default=date.today, index=True)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float, default=0.0)
    
# Configuration class for data modelling
class DataModellingConfig:
    def __init__(self, database_uri):
        self.database_uri = database_uri
       

# Main class for data modelling operations
class DataModelling:
    def __init__(self, config):
        try:
            self.engine = create_engine(config.database_uri)
            self.Base = Base
            self.Session = sessionmaker()
            # Bind session to engine
            self.Session.configure(bind=self.engine)
            # Reflect existing tables and check for 'weather_data'
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect()
            if 'weather_data' not in self.metadata.tables:
                logging.info("Creating 'weather_data' table...")
                self.Base.metadata.create_all(self.engine)
                logging.info("'weather_data' table created.")
            else:
                logging.info("'weather_data' table already exists.")

        except Exception as e:
            logging.error(f"Error initializing DataModelling: {e}")
            raise CustomException(e, sys)
        
        
    # def check_schema_update(self):
    #     try:
    #         if not self.engine.is_up_to_date():
    #             logging.warning("Database schema may need updating.")

    #         logging.info("Database operations complete.")
        
    #     except Exception as e:
    #         logging.error(f"Error checking schema update: {e}")
    #         raise CustomException(e,sys)

