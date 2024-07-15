from sqlalchemy import Column, Integer, String, Float, Date, create_engine, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData
from datetime import date
from src.logger import logging  # Assuming logging setup in 'src/logger.py'
from src.exception import CustomException
import sys

# Define SQLAlchemy Base class
Base = declarative_base()

class WeatherData(Base):
    """
    SQLAlchemy model for weather data.

    Attributes:
        __tablename__ (str): Name of the database table.
        id (int): Primary key for the weather data record.
        station_id (str): Identifier for the weather station.
        date (datetime.date): Date of the weather data record.
        max_temp (float): Maximum temperature recorded.
        min_temp (float): Minimum temperature recorded.
        precipitation (float): Precipitation recorded.
    """
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, default=date.today, index=True)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float, default=0.0)

    __table_args__ = (
        UniqueConstraint('station_id', 'date', name='unique_station_date'),
    )

    def to_dict(self):
        """
        Convert WeatherData instance to a dictionary representation.

        Returns:
            dict: Dictionary containing weather data attributes.
        """
        return {
            'station_id': self.station_id,
            'date': self.date.isoformat(),
            'max_temp': self.max_temp,
            'min_temp': self.min_temp,
            'precipitation': self.precipitation
        }

class DataModellingConfig:
    """
    Configuration class for data modeling operations.

    Attributes:
        database_uri (str): URI for connecting to the database.
    """
    def __init__(self, database_uri):
        """
        Initialize DataModellingConfig instance.

        Args:
            database_uri (str): URI for connecting to the database.
        """
        self.database_uri = database_uri

class DataModelling:
    """
    Main class for data modeling operations.

    Attributes:
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database operations.
        Base (sqlalchemy.ext.declarative.declarative_base): Base class for SQLAlchemy models.
        Session (sqlalchemy.orm.session.sessionmaker): Session maker for database transactions.
        metadata (sqlalchemy.MetaData): Metadata for database schema reflection.
    """
    def __init__(self, config):
        """
        Initialize DataModelling instance.

        Args:
            config (DataModellingConfig): Configuration object containing database URI.
        """
        try:
            # Connect to the database using the provided URI
            self.engine = create_engine(config.database_uri)
            
            # Assign SQLAlchemy base class and session maker
            self.Base = Base
            self.Session = sessionmaker(bind=self.engine)
            
            # Reflect existing tables and check if 'weather_data' table exists
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect()
            
            if 'weather_data' not in self.metadata.tables:
                # Create 'weather_data' table if it does not exist
                logging.info("Creating 'weather_data' table...")
                self.Base.metadata.create_all(self.engine)
                logging.info("'weather_data' table created.")
            else:
                logging.info("'weather_data' table already exists.")

        except Exception as e:
            # Log and raise a custom exception for any initialization errors
            logging.error(f"Error initializing DataModelling: {e}")
            raise CustomException(e, sys)
