from dataclasses import dataclass
from sqlalchemy import Column, Integer, String, Float, UniqueConstraint, func, case, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from src.components.data_modelling import WeatherData
from src.config.database_config import SQLALCHEMY_DATABASE_URI
from src.logger import logging
from src.exception import CustomException
import sys

Base = declarative_base()

# Define the new data model to store the results
class WeatherStationYearlyStats(Base):
    """
    ORM class representing yearly statistics for weather stations.

    Attributes:
        __tablename__ (str): Table name in the database.
        id (int): Primary key, auto-incremented.
        station_id (str): Station identifier, indexed and non-nullable.
        year (int): Year of the statistics, indexed and non-nullable.
        avg_max_temp (float): Average maximum temperature.
        avg_min_temp (float): Average minimum temperature.
        total_precipitation (float): Total precipitation.
    """
    __tablename__ = 'weather_station_yearly_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    avg_max_temp = Column(Float(precision=3, asdecimal=True))
    avg_min_temp = Column(Float(precision=3, asdecimal=True))
    total_precipitation = Column(Float(precision=3, asdecimal=True))

    __table_args__ = (
        UniqueConstraint('station_id', 'year', name='unique_station_year'),
    )

    def to_dict(self):
        """
        Convert object attributes to a dictionary format.

        Returns:
            dict: Dictionary representation of object attributes.
        """
        return {
            'station_id': self.station_id,
            'year': self.year,
            'avg_max_temp': self.avg_max_temp,
            'avg_min_temp': self.avg_min_temp,
            'total_precipitation': self.total_precipitation
        }


@dataclass
class DataAnalysisConfig:
    """
    Configuration class for data analysis operations.

    Attributes:
        database_uri (str): URI for the database connection.
    """
    def __init__(self, database_uri):
        """
        Initialize DataAnalysisConfig instance.

        Args:
            database_uri (str): URI for the database connection.
        """
        self.database_uri = database_uri


class DataAnalysis:
    """
    Class for handling data analysis operations.

    Attributes:
        engine (create_engine): SQLAlchemy engine instance.
        Session (sessionmaker): SQLAlchemy sessionmaker instance.
        metadata (MetaData): SQLAlchemy MetaData instance.
    """
    def __init__(self, analysis_config: DataAnalysisConfig):
        """
        Initialize DataAnalysis instance.

        Args:
            analysis_config (DataAnalysisConfig): Configuration object containing analysis settings.
        """
        try:
            self.engine = create_engine(analysis_config.database_uri)
            self.Session = sessionmaker(bind=self.engine)
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect()
        except Exception as e:
            logging.error(f"Error initializing DataAnalysis: {e}")
            raise CustomException(e, sys)

    def create_weather_station_yearly_stats_table(self):
        """
        Create or update the 'weather_station_yearly_stats' table in the database.

        Checks if the table exists and compares its schema with the defined ORM model.
        If not exists or schema differs, creates or updates the table accordingly.
        """
        try:
            if 'weather_station_yearly_stats' not in self.metadata.tables:
                logging.info("Creating 'weather_station_yearly_stats' table...")
                Base.metadata.create_all(self.engine, tables=[WeatherStationYearlyStats.__table__])
                logging.info("'weather_station_yearly_stats' table created.")
            else:
                # Compare schema and reflect changes if any
                current_table = self.metadata.tables['weather_station_yearly_stats']
                if not current_table.compare(WeatherStationYearlyStats.__table__):
                    logging.info("Updating 'weather_station_yearly_stats' table schema...")
                    current_table.drop(self.engine)
                    Base.metadata.create_all(self.engine, tables=[WeatherStationYearlyStats.__table__])
                    logging.info("'weather_station_yearly_stats' table schema updated.")
                else:
                    logging.info("'weather_station_yearly_stats' table already exists and schema matches.")

        except Exception as e:
            logging.error(f"Error creating 'weather_station_yearly_stats' table: {e}")
            raise CustomException(e, sys)

    def calculate_yearly_stats(self):
        """
        Calculate yearly statistics for weather stations based on weather data.

        Returns:
            list: List of tuples containing yearly statistics for each station.
        """
        try:
            # Create a session
            session = self.Session()

            # Query to calculate the statistics
            yearly_stats = session.query(
                WeatherData.station_id,
                func.extract('year', WeatherData.date).label('year'),
                func.avg(case([(WeatherData.max_temp != None, WeatherData.max_temp)], else_=None)).label('avg_max_temp'),
                func.avg(case([(WeatherData.min_temp != None, WeatherData.min_temp)], else_=None)).label('avg_min_temp'),
                func.sum(case([(WeatherData.precipitation != None, WeatherData.precipitation)], else_=0.0)).label('total_precipitation')
            ).group_by(
                WeatherData.station_id,
                func.extract('year', WeatherData.date)
            ).all()

            return yearly_stats

        except Exception as e:
            logging.error(f"Error in calculating yearly statistics: {e}")
            raise CustomException(e, sys)
        finally:
            session.close()

    def store_yearly_stats(self, yearly_stats):
        """
        Store calculated yearly statistics into the 'weather_station_yearly_stats' table.

        Args:
            yearly_stats (list): List of tuples containing yearly statistics for each station.

        Raises:
            CustomException: If there is an error in storing the statistics.
        """
        try:
            # Create a session
            session = self.Session()
            skipped_records = 0
            for stat in yearly_stats:
                # Check if the record already exists
                existing_record = session.query(WeatherStationYearlyStats).filter_by(
                    station_id=stat.station_id,
                    year=int(stat.year)
                ).first()

                if existing_record:
                    skipped_records += 1
                    #logging.info(f"Record for station_id {stat.station_id} and year {stat.year} already exists. Skipping.")
                else:
                    new_record = WeatherStationYearlyStats(
                        station_id=stat.station_id,
                        year=int(stat.year),
                        avg_max_temp=stat.avg_max_temp if stat.avg_max_temp is not None else None,
                        avg_min_temp=stat.avg_min_temp if stat.avg_min_temp is not None else None,
                        total_precipitation=stat.total_precipitation if stat.total_precipitation is not None else None
                    )
                    session.add(new_record)

            # Commit the transaction
            logging.info(f"{skipped_records} records already exist. Skipping.")
            session.commit()
            logging.info("Yearly statistics stored successfully")

        except Exception as e:
            logging.error(f"Error in storing yearly statistics: {e}")
            raise CustomException(e, sys)
        finally:
            session.close()
