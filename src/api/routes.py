from flask import Blueprint, request, jsonify
from src.services.weather_service import get_weather_data, get_weather_stats

api_blueprint = Blueprint('api', __name__)

@api_blueprint.route('/api/weather', methods=['GET'])
def weather():
    station_id = request.args.get('station_id')
    date = request.args.get('date')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)

    data = get_weather_data(station_id, date, page, per_page)
    return jsonify(data)

@api_blueprint.route('/api/weather/stats', methods=['GET'])
def weather_stats():
    station_id = request.args.get('station_id')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)

    data = get_weather_stats(station_id, page, per_page)
    return jsonify(data)
