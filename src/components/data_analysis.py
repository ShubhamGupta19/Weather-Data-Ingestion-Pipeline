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


class DataAnalysisConfig:
    def __init__(self, database_uri):
        self.database_uri = database_uri


class DataAnalysis:
    def __init__(self, analysis_config: DataAnalysisConfig):
        try:
            self.engine = create_engine(analysis_config.database_uri)
            self.Session = sessionmaker(bind=self.engine)
            self.metadata = MetaData(bind=self.engine)
            self.metadata.reflect()
        except Exception as e:
            logging.error(f"Error initializing DataAnalysis: {e}")
            raise CustomException(e, sys)

    def create_weather_station_yearly_stats_table(self):
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
            logging.info(f"{skipped_records} records already exists. Skipping.")
            session.commit()
            logging.info("Yearly statistics stored successfully")

        except Exception as e:
            logging.error(f"Error in storing yearly statistics: {e}")
            raise CustomException(e, sys)
        finally:
            session.close()
