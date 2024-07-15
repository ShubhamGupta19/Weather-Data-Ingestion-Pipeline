from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

# Define the components of the PostgreSQL URI
db_username = 'postgres'
db_password = '123456'
db_host = 'localhost'
db_port = '5433'
db_name = 'weather_db'

# Construct the SQLAlchemy database URI
SQLALCHEMY_DATABASE_URI = f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'

# Create a base class for declarative class definitions
Base = declarative_base()

# Create an SQLAlchemy engine
engine = create_engine(SQLALCHEMY_DATABASE_URI)


# db_username, db_password, db_host, db_port, db_name:
# - Individual variables representing components of the PostgreSQL database URI.
# - Replace with actual database credentials and details.

# SQLALCHEMY_DATABASE_URI:
# - Constructs the connection URI for PostgreSQL using formatted strings.
# - Incorporates the individual variables for clarity and manageability.

# Base:
# - Base class for declarative class definitions using SQLAlchemy.
# - All models should inherit from this base class.

# engine:
# - SQLAlchemy engine object that manages database connections and executes SQL commands.

# Usage Notes:
# - Ensure the PostgreSQL server is running and accessible with the provided URI.
# - Replace placeholder values in db_username, db_password, db_host, db_port, and db_name with actual database credentials and details.
# - Customize and extend the code based on specific application requirements.

