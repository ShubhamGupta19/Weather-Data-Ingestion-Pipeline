from sqlalchemy import Column, Integer, String, Float, Date, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import date
from src.logger import logging

# Define SQLAlchemy Base class
Base = declarative_base()

# Define SQLAlchemy model for weather data
class WeatherData(Base):
    __tablename__ = 'weather_data'

    id = Column(Integer, primary_key=True)
    station_id = Column(String, nullable=False)
    date = Column(Date, nullable=False, default=date.today)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)

# Configuration class for data modelling
class DataModellingConfig:
    def __init__(self, database_uri):
        self.database_uri = database_uri
       

# Main class for data modelling operations
class DataModelling:
    def __init__(self, config):
        self.engine = create_engine(config.database_uri)
        self.Base = Base
        self.Session = sessionmaker(bind=self.engine)

        # Check if the database exists and create it if not
        if not self.engine.has_table('weather_data'):
            logging.info("Creating database tables...")
            self.Base.metadata.create_all(self.engine)
            logging.info("Database tables created.")
        else:
            logging.info("Database tables already exist.")

    def query_weather_records(self):
        with self.Session() as session:
            try:
                # Query all weather data records
                weather_records = session.query(WeatherData).all()
                logging.info("Retrieved weather records:")

                for record in weather_records:
                    logging.info(f"Record: {record.id}, {record.station_id}, {record.date}, {record.max_temp}, {record.min_temp}, {record.precipitation}")

            except Exception as e:
                logging.error(f"Error retrieving weather records: {e}")
                session.rollback()
                raise e

    def check_schema_update(self):
        if not self.engine.is_up_to_date():
            logging.warning("Database schema may need updating.")

        logging.info("Database operations complete.")
