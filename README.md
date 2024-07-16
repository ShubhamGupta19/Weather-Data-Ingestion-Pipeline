# Weather Data Ingestion and Analysis Pipeline

## Business Understanding

The goal of this project is to build a robust data ingestion pipeline that processes weather data from multiple files, stores it in a database, and performs analysis to derive valuable insights. The weather data includes daily records of maximum temperature, minimum temperature, and precipitation for different weather stations. This processed data can be used for various analytical purposes, including understanding weather patterns, climate change studies, and supporting agricultural decision-making.

## Tech Stack

- **Python**: Programming language used for scripting and data processing.
- **Flask**: Micro-framework used for building web APIs.
- **SQLAlchemy**: SQL toolkit and Object-Relational Mapping (ORM) library used for database interactions.
- **PostgreSQL**: Relational database management system used for storing weather data.
- **Git**: Version control system used for managing project source code.

## Project Plan

### Phase 0: Setting up PostgreSQL Database
- Install and configure PostgreSQL for storing weather data.

### Phase 1: Data Modeling
- Define SQLAlchemy models (`WeatherData` and `WeatherStationYearlyStats`) for weather data storage.
- Create tables in PostgreSQL database using SQLAlchemy.

### Phase 2: Data Ingestion
- Implement a data ingestion pipeline to fetch existing records and avoid duplicates.
- Create a temporary staging table for processing data files.
- Parse, validate, and clean weather data from files.
- Insert validated records into the temporary table.
- Transfer data from the staging table to the main `weather_data` table in PostgreSQL.

### Phase 3: Data Analysis
- Define models (`WeatherStationYearlyStats`) to store calculated yearly weather statistics.
- Calculate average maximum temperature, average minimum temperature, and total precipitation for each weather station and year.
- Store calculated statistics in the `weather_station_yearly_stats` table.

### Phase 4: API Development
- Develop RESTful APIs using Flask to expose weather data and statistics.
- Implement endpoints for retrieving weather data by station ID, date, and statistical summaries.
- Ensure robust error handling and validation for API requests.


## Initial Data Collection Report

The initial dataset consists of weather data files stored in the "wx_data" folder. Each file is named after a weather station and contains tab-separated values representing the date, maximum temperature, minimum temperature, and precipitation.

## Data Description Report

### Weather Data Fields

- `station_id` (str): Identifier for the weather station.
- `date` (date): Date of the weather data record.
- `max_temp` (float): Maximum temperature recorded (in tenths of degrees Celsius).
- `min_temp` (float): Minimum temperature recorded (in tenths of degrees Celsius).
- `precipitation` (float): Precipitation recorded (in tenths of millimeters).

## Data Quality Report

- **Duplicates**: Checked by comparing (station_id, date) pairs.
- **Missing Values**: Represented by -9999 and handled during data processing.
- **Data Types**: Ensured correct data types for each field during ingestion.
- **Integrity Constraints**: Unique constraint on (station_id, date) to avoid duplicate entries.

## Data Selection Report

### Selection Criteria

- Only valid records with proper date formatting.
- Exclude records with missing values represented by -9999.
- Ensure no duplicate records based on (station_id, date).

## Data Cleaning Report

### Cleaning Steps

- Convert date from YYYYMMDD format to datetime.date object.
- Scale temperature and precipitation values by dividing by 10.
- Replace -9999 with None for missing values.

## Data Derivation Report

### Derived Fields

- `avg_max_temp` (float): Average maximum temperature for each year and station.
- `avg_min_temp` (float): Average minimum temperature for each year and station.
- `total_precipitation` (float): Total precipitation for each year and station.

## Data Modeling Report

### SQLAlchemy Models

#### WeatherData

```python
class WeatherData(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    station_id = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float, default=0.0)
    __table_args__ = (
        UniqueConstraint('station_id', 'date', name='unique_station_date'),
    )
```

#### WeatherStationYearlyStats
```python
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
```

## Data Ingestion Process

    Fetch Existing Records: Loaded existing (station_id, date) pairs to avoid duplicates.
    Temporary Table Creation: Created a temporary table for staging.
    File Processing: Parsed, validated, and cleaned data from files parallely, and inserted valid records into the temporary table.
    Insert Into Main Table: Transferred data from the temporary table into the main table.

## Data Analysis Process

    Calculate Yearly Stats: Computed average maximum temperature, average minimum temperature, and total precipitation for each station and year.
```python
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
```

    
    Store Yearly Stats: Inserted the computed statistics into the weather_station_yearly_stats table.



## Directory Structure

- **app.py**: Entry point of the application.
- **main.py**: Main script or module for the project.
- **README.md**: Project documentation file.
- **requirements.txt**: File listing dependencies required for the project.
- **setup.py**: Script for setting up the project environment.
- **src/**: Root directory for source code.

  - **api/**: Module for handling API routes and logic.
    - **routes.py**: Defines API endpoints and their corresponding functions.
    - **__init__.py**: Initialization script for the `api` module.

  - **components/**: Module for different components of the project.
    - **data_analysis.py**: Contains logic for analyzing weather data.
    - **data_ingestion.py**: Handles the ingestion of weather data.
    - **data_modelling.py**: Defines SQLAlchemy models for weather data.
    - **__init__.py**: Initialization script for the `components` module.

  - **config/**: Configuration files for database and possibly other settings.
    - **database_config.py**: Configuration for database connection.
    - **__init__.py**: Initialization script for the `config` module.

  - **exception.py**: Module defining custom exceptions for error handling.
  - **logger.py**: Module defining logging configuration and utilities.
  
  - **models/**: Directory possibly for additional data models.
    - **__init__.py**: Initialization script for the `models` module.

  - **services/**: Contains business logic or service layers.
    - **weather_service.py**: Implements services related to weather data.
    - **__init__.py**: Initialization script for the `services` module.

  - **__init__.py**: Initialization script for the `src` package.
  
- **static/**: Directory possibly for static files.
  - **swagger.json**: Swagger API specification file.

- **__init__.py**: Initialization script for the entire project.
- **wx_data**: Contains the data in multiple files of .txt format.




# Project Setup and Usage

## Phase 0: Setting Up PostgreSQL

1. **Install PostgreSQL:**
   - Download and install PostgreSQL from the [official website](https://www.postgresql.org/download/) or package manager.

2. **Create Database:**
   - Open a terminal or PostgreSQL client.
   - Create a new database for the project:
     ```sql
     CREATE DATABASE weather_db;
     ```
3. **Update Database configurations:**
   ```python
   from sqlalchemy import create_engine
    db_username = 'your_username'
    db_password = 'your_password'
    db_host = 'localhost'
    db_port = '5433'
    db_name = 'weather_db'
    
    SQLALCHEMY_DATABASE_URI = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(SQLALCHEMY_DATABASE_URI)```

   

## Phase 1: Data Modelling and Ingestion

1. **Clone Git Repository:**
   - Clone the project repository from Git:
     ```bash
     git clone https://github.com/ShubhamGupta19/Weather-Data-Ingestion-Pipeline.git
     ```

2. **Install Dependencies:**
   - Install required Python packages listed in `requirements.txt`:
     ```bash
     pip install -r requirements.txt
     ```

3. **Run Data Ingestion Script:**
   - Execute `main.py` to start the data ingestion process:
     ```bash
     python main.py
     ```
   - The tables will be created in the database and the data from folder `wx_data` will be ingested into those tables.

## Phase 2: API Implementation

1. **Run Flask Application:**
   - Start the Flask application to deploy API endpoints:
     ```bash
     python app.py
     ```

2. **Access Swagger UI:**
   - Open a web browser and go to `http://localhost:5000/swagger` to access Swagger UI.
   - Use Swagger UI to test the implemented API endpoints (`/api/weather` and `/api/weather/stats`).

## Verification

- **Explore API Endpoints:**
  - Test each API endpoint using Swagger UI by providing appropriate query parameters (`station_id`, `date`, `page`, `per_page`).
  - Verify responses to ensure correct functionality and data retrieval.

---

### Additional Notes

- **Swagger Integration:** Ensure `swagger.json` accurately reflects the API endpoints defined in `routes.py`.
- **Testing:** Use tools like Postman or curl commands for more extensive testing beyond Swagger UI.

By following these steps, you can set up the project, ingest and analyze weather data, deploy API endpoints, and verify their functionality using Swagger UI.


