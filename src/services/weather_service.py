from decimal import Decimal
from flask import jsonify, request
from sqlalchemy import func
from src.components.data_analysis import WeatherStationYearlyStats
from src.config.database_config import engine
from src.components.data_modelling import WeatherData
from sqlalchemy.orm import sessionmaker
from src.logger import logging
Session = sessionmaker(bind=engine)
session = Session()



def get_weather_data(station_id, date, page, per_page):
    query = session.query(WeatherData)
    
    if station_id:
        query = query.filter(WeatherData.station_id == station_id)
    if date:
        query = query.filter(WeatherData.date == date)

    total = query.count()
    data = query.offset((page - 1) * per_page).limit(per_page).all()

    result = {
        'total': total,
        'page': page,
        'per_page': per_page,
        'data': [record.to_dict() for record in data]
    }
    return result

def get_weather_stats(station_id, page, per_page):
    query = session.query(WeatherStationYearlyStats)
    if station_id:
        query = query.filter(WeatherStationYearlyStats.station_id == station_id)
    
    total = query.count()
    stats_data = query.offset((page - 1) * per_page).limit(per_page).all()
    #print(stats_data)
    result = {
        'total': total,
        'page': page,
        'per_page': per_page,
        'data': [stats.to_dict() for stats in stats_data]
    }

    print(result)
    return result
