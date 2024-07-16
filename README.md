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

### Phase 1: Data Modeling

- Define a data model using SQLAlchemy to store weather data.
- Create necessary tables in the database.

### Phase 2: Data Ingestion

- Fetch existing records from the database to avoid duplicates.
- Create a temporary table for staging data.
- Process each weather data file, validate and clean the data, and insert valid records into the temporary table.
- Transfer data from the temporary table to the main weather data table.

### Phase 3: Data Analysis

- Define a new data model to store yearly weather statistics.
- Calculate yearly statistics for each weather station.
- Store the calculated statistics in the database.

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

    Fetch Existing Records: Load existing (station_id, date) pairs to avoid duplicates.
    Temporary Table Creation: Create a temporary table for staging.
    File Processing: Parse, validate, and clean data from files, and insert valid records into the temporary table.
    Insert Into Main Table: Transfer data from the temporary table to the main table.

## Data Analysis Process

    Calculate Yearly Stats: Compute average maximum temperature, average minimum temperature, and total precipitation for each station and year.
    Store Yearly Stats: Insert the computed statistics into the weather_station_yearly_stats table.
