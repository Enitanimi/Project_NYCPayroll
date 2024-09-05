

import logging
import os
from dotenv import load_dotenv
from extract import extract_files_from_adls
from transform import transform_data
from load import load_data_to_snowflake, get_snowflake_engine
from pipeline_log import logging
from sqlalchemy import text


def main():
    logging.info("Starting ETL pipeline.")
    
    # Load environment variables
    load_dotenv(override=True)
    
    try:
        logging.info("ETL Pipeline started.")
        
        # Step 1: Extract and move files from ADLS
        logging.info("ETL Step 1: Extracting data from ADLS.")
        extract_files_from_adls(destination_folder="Dataset/Raw_payroll_Data")
        logging.info("Data extraction and movement completed.")
        logging.info("ETL Step 1: Data extraction completed.")
        

        # Check if raw data file exists
        raw_data_path = "Dataset/Raw_payroll_Data/nycpayroll_2020.csv"
        if os.path.exists(raw_data_path):

            # Step 2: Transform
            logging.info("ETL Step 2: Transforming data.")
            transformed_data_path = "Dataset/Cleaned_payroll_Data/nycpayroll_2020.csv"
            dim_date_path = "Dataset/Cleaned_payroll_Data/dim_date.csv"
            transform_data(
                input_file_path=raw_data_path,
                output_file_path=transformed_data_path,
                dim_date_output_path=dim_date_path
            )
            logging.info("ETL Step 2: Data transformation completed.")
            
            # Check if transformed data files exist
            if os.path.exists(transformed_data_path) and os.path.exists(dim_date_path):
                # Step 3: Load
                logging.info("ETL Step 3: Loading data into Snowflake.")
                load_data_to_snowflake(
                    transformed_file_path=transformed_data_path,
                    dim_date_file_path=dim_date_path
                )
                logging.info("ETL Step 3: Data loading completed.")

                # Executing the stored procedure
                engine = get_snowflake_engine()
                with engine.connect() as conn:
                    conn.execute(text('CALL "STG".PAYROLL_AGGREGATES();'))
                    logging.info("Stored procedure executed successfully.")

            else:
                logging.error("Files not found: transformed_data_path or dim_date_path")
                logging.error(f"transformed_data_path: {transformed_data_path}")
                logging.error(f"dim_date_path: {dim_date_path}")
                raise FileNotFoundError("Files not found: transformed_data_path or dim_date_path")
            
        else:
            logging.error(f"File not found: {raw_data_path}")
            raise FileNotFoundError(f"File not found: {raw_data_path}")
        
        logging.info("ETL pipeline completed successfully.")
    
    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
