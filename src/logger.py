import logging
import os
from datetime import datetime

# Create a unique log file name based on current timestamp
LOG_FILE = f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"

# Define the path to store logs
logs_path = os.path.join(os.getcwd(), "logs")

# Ensure the logs directory exists, creating it if necessary
os.makedirs(logs_path, exist_ok=True)

# Define the full path to the log file
LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

# Configure logging settings
logging.basicConfig(
    filename=LOG_FILE_PATH,              # Log file path
    format="[ %(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s",  # Log format
    level=logging.INFO                   # Log level (INFO or higher)
)

