from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

SQLALCHEMY_DATABASE_URI = 'postgresql://postgres:123456@localhost:5433/weather_db'

Base = declarative_base()
engine = create_engine(SQLALCHEMY_DATABASE_URI)