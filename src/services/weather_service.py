from decimal import Decimal
import sys
from flask import jsonify, request
from sqlalchemy import func
from src.components.data_analysis import WeatherStationYearlyStats
from src.config.database_config import engine
from src.components.data_modelling import WeatherData
from sqlalchemy.orm import sessionmaker
from src.exception import CustomException
from src.logger import logging

# Create a session to interact with the database
Session = sessionmaker(bind=engine)
session = Session()

def get_weather_data(station_id, date, page, per_page):
    """
    Retrieve weather data records based on optional filters.

    Args:
        station_id (str): ID of the weather station.
        date (str): Date in YYYY-MM-DD format to filter records by date.
        page (int): Page number of results for pagination.
        per_page (int): Number of records per page for pagination.

    Returns:
        dict: Dictionary containing paginated weather data records and metadata.
              Keys: 'total', 'page', 'per_page', 'data'.
    Raises:
        CustomException: If there is an error while querying the database.
    """
    try:
        query = session.query(WeatherData)

        # Apply filters if provided
        if station_id:
            query = query.filter(WeatherData.station_id == station_id)
        if date:
            query = query.filter(WeatherData.date == date)

        # Count total matching records
        total = query.count()

        # Paginate and retrieve data
        data = query.offset((page - 1) * per_page).limit(per_page).all()

        # Prepare result dictionary
        result = {
            'total': total,
            'page': page,
            'per_page': per_page,
            'data': [record.to_dict() for record in data]  # Convert each record to dictionary format
        }
        return result
    except Exception as e:
        # Raise a custom exception with detailed error information
        raise CustomException(e, sys)

def get_weather_stats(station_id, page, per_page):
    """
    Retrieve yearly weather statistics based on optional filters.

    Args:
        station_id (str): ID of the weather station.
        page (int): Page number of results for pagination.
        per_page (int): Number of records per page for pagination.

    Returns:
        dict: Dictionary containing paginated weather statistics and metadata.
              Keys: 'total', 'page', 'per_page', 'data'.
    Raises:
        CustomException: If there is an error while querying the database.
    """
    try:
        query = session.query(WeatherStationYearlyStats)

        # Apply filters if provided
        if station_id:
            query = query.filter(WeatherStationYearlyStats.station_id == station_id)

        # Count total matching records
        total = query.count()

        # Paginate and retrieve statistics data
        stats_data = query.offset((page - 1) * per_page).limit(per_page).all()

        # Prepare result dictionary
        result = {
            'total': total,
            'page': page,
            'per_page': per_page,
            'data': [stats.to_dict() for stats in stats_data]  # Convert each stats record to dictionary format
        }

        #print(result)  # Temporary print statement for debugging purposes

        return result
    except Exception as e:
        # Raise a custom exception with detailed error information
        raise CustomException(e, sys)
