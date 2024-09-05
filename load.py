

import sqlalchemy
import snowflake.sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import os
from dotenv import load_dotenv
from pipeline_log import logging
from dask.distributed import Client
from sqlalchemy import text


load_dotenv(override=True)


# Define the Snowflake connection parameters
def get_snowflake_engine():
    try:
        engine = create_engine(
            "snowflake://{user}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}".format(
                user=os.getenv("sn_user"),
                password=os.getenv("sn_pword"),
                account_identifier=os.getenv("sn_Acct_Id"),
                database=os.getenv("sn_DB"),
                schema=os.getenv("sn_schema"),
                warehouse=os.getenv("sn_DWH")
            )
        )
        logging.info("Connected to Snowflake successfully.")
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        raise



def load_data_to_snowflake(transformed_file_path, dim_date_file_path):

    try:

         # Initialize Dask client with 1 worker
        client = Client(n_workers=1)
        logging.info("Dask client initialized with 1 worker.")
        # Get Snowflake engine
        engine = get_snowflake_engine()


         # Define table structures
       
        tables = [
            {
                "name": "Dim_Employee",
                "columns": [
                    {"name": "EmployeeID", "type": "INT"},
                    {"name": "LastName", "type": "VARCHAR(50)"},
                    {"name": "FirstName", "type": "VARCHAR(50)"},
                    {"name": "AgencyStartDate", "type": "DATE"},
                    {"name": "TitleCode", "type": "INT"},
                    {"name": "TitleDescription", "type": "TEXT"},
                    {"name": "WorkLocationBorough", "type": "VARCHAR(255)"},
                    {"name": "LeaveStatusasofJune30", "type": "VARCHAR(50)"}
                ]
            },
            {
                "name": "Dim_Agency",
                "columns": [
                    {"name": "AgencyID", "type": "INT"},
                    {"name": "AgencyName", "type": "VARCHAR(255)"}
                ]
            },
            {
                "name": "Dim_PayBasis",
                "columns": [
                    {"name": "PayBasisID", "type": "INT"},
                    {"name": "PayBasis", "type": "VARCHAR(255)"}
                ]
            },
            {
                "name": "Dim_Date",
                "columns": [
                    {"name": "DateID", "type": "INT"},
                    {"name": "Date", "type": "DATE"},
                    {"name": "Year", "type": "INT"},
                    {"name": "Month", "type": "INT"},
                    {"name": "Day", "type": "INT"},
                    {"name": "Quarter", "type": "INT"}
                ]
            },
            {
                "name": "FactPayroll",
                "columns": [
                    {"name": "FactID", "type": "INT"},
                    {"name": "EmployeeID", "type": "INT"},
                    {"name": "AgencyID", "type": "INT"},
                    {"name": "DateID", "type": "INT"},
                    {"name": "PayBasisID", "type": "INT"},
                    {"name": "FiscalYear", "type": "INT"},
                    {"name": "PayrollNumber", "type": "INT"},
                    {"name": "BaseSalary", "type": "FLOAT"},
                    {"name": "RegularHours", "type": "FLOAT"},
                    {"name": "OTHours", "type": "FLOAT"},
                    {"name": "RegularGrossPaid", "type": "FLOAT"},
                    {"name": "TotalOTPaid", "type": "FLOAT"},
                    {"name": "TotalOtherPay", "type": "FLOAT"}
                ]
            }
        ]

        # Step 1: Load the main transformed CSV file
        transformed_nycpayroll_df = pd.read_csv("Dataset/Cleaned_payroll_Data/nycpayroll_2020.csv")
        logging.info("Transformed NycPayroll CSV file loaded successfully.")

        # Step 2: Load data into Snowflake

        with engine.connect() as conn:

            # Step 3: Truncate the tables before loading new data
            for table in tables:
                table_name = table["name"]
                columns = table["columns"]
                column_defs = ', '.join([f'"{col["name"]}" {col["type"]}' for col in columns])
                create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({column_defs});'
                conn.execute(text(create_table_sql))
                logging.info(f'Table "{table_name}" created or already exists.')

            # Step 2: Truncate the tables before loading new data
            for table in tables:
                truncate_table_sql = f'TRUNCATE TABLE "{table["name"]}";'
                conn.execute(text(truncate_table_sql))
                logging.info(f'Truncated "{table["name"]}" successfully.')

            # Dim_Employee
            Dim_Employee_df = transformed_nycpayroll_df[[
                "EmployeeID", "LastName", "FirstName", "AgencyStartDate", 
                "TitleCode", "TitleDescription", "WorkLocationBorough", 
                "LeaveStatusasofJune30"
            ]]
            Dim_Employee_df.to_sql("Dim_Employee", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Employee data loaded successfully!!')

            # Dim_Agency
            Dim_Agency_df = transformed_nycpayroll_df[["AgencyID", "AgencyName"]]
            Dim_Agency_df.to_sql("Dim_Agency", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Agency data loaded successfully!!')

            # Dim_PayBasis
            dim_paybasis_df = transformed_nycpayroll_df[["PayBasisID", "PayBasis"]]
            dim_paybasis_df.to_sql("Dim_PayBasis", con=conn, if_exists="replace", index=False)
            logging.info('Dim_PayBasis data loaded successfully!!')

            # Dim_Date (from dim_date.csv)
            dim_date_df = pd.read_csv("Dataset/Cleaned_payroll_Data/dim_date.csv")
            dim_date_df.to_sql("Dim_Date", con=conn, if_exists="replace", index=False)
            logging.info('Dim_Date data loaded successfully.')

            # FactPayroll
            # Merge with dim_date_df to include DateID
            factpayroll_df = transformed_nycpayroll_df.merge(dim_date_df, left_on="FiscalYear", right_on="Year", how="inner") \
                                                        [["EmployeeID", "AgencyID", "DateID", "PayBasisID", "FiscalYear", \
                                                        "PayrollNumber", "BaseSalary", "RegularHours", "OTHours", \
                                                        "RegularGrossPaid", "TotalOTPaid", "TotalOtherPay"]]

            # Creating the FactID after the merging
            factpayroll_df["FactID"] = range(1, len(factpayroll_df) + 1)

            # Reorder columns to have FactID first
            factpayroll_df = factpayroll_df[[
                "FactID", "EmployeeID", "AgencyID", "DateID", "PayBasisID", "FiscalYear", \
                "PayrollNumber", "BaseSalary", "RegularHours", "OTHours", "RegularGrossPaid", \
                "TotalOTPaid", "TotalOtherPay"
            ]]

            factpayroll_df.to_sql("FactPayroll", con=conn, if_exists="replace", index=False)
            logging.info('FactPayroll data loaded successfully!!')

            # Commit the changes
            conn.commit()
            logging.info("All data loaded successfully!!")

        # Close the Dask client
        client.close()
        logging.info("Dask client closed.")

    except Exception as e:
        logging.error(f"Error loading data into Snowflake: {e}")
        raise
