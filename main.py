import os
import sys
from src.logger import logging
from src.exception import CustomException
from src.components.data_ingestion import DataIngestionConfig,DataIngestion
from src.components.data_modelling import DataModellingConfig,DataModelling
from src.config.database_config import SQLALCHEMY_DATABASE_URI

def main():
    try:
        # Step 1: Data Modelling
        logging.info("Starting data modelling process...")
        modelling_config = DataModellingConfig(database_uri=SQLALCHEMY_DATABASE_URI)
        data_modeller = DataModelling(config=modelling_config)
        #data_modeller.check_schema_update()

       

        # Step 2: Data Ingestion
        logging.info("Starting data ingestion process...")
        ingestion_config = DataIngestionConfig()
        data_ingester = DataIngestion(ingestion_config=ingestion_config)
        data_ingester.initiate_data_ingestion()

        logging.info("Data modelling and ingestion processes completed.")

    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise CustomException(e,sys)

if __name__ == "__main__":
    main()



