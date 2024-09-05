

import dask.dataframe as dd
import pandas as pd
import logging
from pipeline_log import logging

def generate_Dim_Date_Table(fiscal_years):
    # Generate dim_date DataFrame based on fiscal years

    date_data = []
    for year in fiscal_years:
        DateID = int(f"{year}0000")  # Use 0000 for general fiscal year mapping
        date_data.append({
            "DateID": DateID,
            "Date": pd.Timestamp(f"{year}-01-01"),
            "Year": year,
            "Month": 1,  # Set to None for fiscal year-based mapping
            "Day": 1,    # Set to None for fiscal year-based mapping
            "Quarter": 1 # Set to None for fiscal year-based mapping
        })
    return pd.DataFrame(date_data)


def transform_data(input_file_path, output_file_path, dim_date_output_path):

    try:
        logging.info("Starting data transformation!!")

        # Define data types for columns
        dtypes = {
            "FiscalYear": "int64",
            "PayrollNumber": "int64",
            "AgencyID": "int64",
            "AgencyName": "object",
            "EmployeeID": "int64",
            "LastName": "object",
            "FirstName": "object",
            "AgencyStartDate": "object",
            "WorkLocationBorough": "object",
            "TitleCode": "int64",
            "TitleDescription": "object",
            "LeaveStatusasofJune30": "object",
            "BaseSalary": "float64",
            "PayBasis": "object",
            "RegularHours": "float64",
            "RegularGrossPaid": "float64",
            "OTHours": "float64",
            "TotalOTPaid": "float64",
            "TotalOtherPay": "float64"
        }

        # Load the data
        NYCpayroll_df = dd.read_csv(input_file_path, dtype=dtypes)
    
        # Drop duplicates
        NYCpayroll_df = NYCpayroll_df.drop_duplicates()

        # Convert date columns to datetime
        NYCpayroll_df['AgencyStartDate'] = dd.to_datetime(NYCpayroll_df['AgencyStartDate'], errors='coerce')

        # Rename columns
        NYCpayroll_df = NYCpayroll_df.rename(columns={'AgencyCode': 'AgencyID'})

        # Create a unique identifier (PayBasisID) for each PayBasis
        paybasis_unique = NYCpayroll_df['PayBasis'].unique().compute()  # Extract unique PayBasis values
        paybasis_mapping = {pb: i + 1 for i, pb in enumerate(paybasis_unique)}  # Create a mapping dictionary
        NYCpayroll_df['PayBasisID'] = NYCpayroll_df['PayBasis'].map(paybasis_mapping, meta=('PayBasis', 'int64'))

        # Fill missing values
        BaseSalary_mean = NYCpayroll_df['BaseSalary'].mean().compute()
        RegularHours_mean = NYCpayroll_df['RegularHours'].mean().compute()
        RegularGross_paid_mean = NYCpayroll_df['RegularGrossPaid'].mean().compute()
        OTHours_mean = NYCpayroll_df['OTHours'].mean().compute()
        TotalOTPaid_mean = NYCpayroll_df['TotalOTPaid'].mean().compute()
        TotalOtherPay_mean = NYCpayroll_df['TotalOtherPay'].mean().compute()

        NYCpayroll_df = NYCpayroll_df.fillna({
            "FiscalYear": 0,
            "PayrollNumber": 0,
            "AgencyID": 0,
            "AgencyName": "Unknown",
            "EmployeeID": 0,
            "LastName": "Unknown",
            "FirstName": "Unknown",
            "AgencyStartDate": pd.Timestamp('1900-01-01'),  # Default date for missing values
            "WorkLocationBorough": "Unknown",
            "TitleCode": 0,
            "TitleDescription": "Unknown",
            "LeaveStatusasofJune30": "Unknown",
            "BaseSalary": BaseSalary_mean,
            "PayBasis": "Unknown",
            "RegularHours": RegularHours_mean,
            "RegularGrossPaid": RegularGross_paid_mean,
            "OTHours": OTHours_mean,
            "TotalOTPaid": TotalOTPaid_mean,
            "TotalOtherPay": TotalOtherPay_mean,
            "PayBasisID": 0
        })

        # Extract distinct fiscal years
        fiscal_years = NYCpayroll_df['FiscalYear'].drop_duplicates().compute().tolist()

        # Generate dim_date DataFrame
        dim_date_df = generate_Dim_Date_Table(fiscal_years)

        # Save dim_date_df to CSV (for loading into Snowflake later)
        dim_date_df.to_csv(dim_date_output_path, index=False)

        logging.info("dim_date data generated and saved!!")

        # converting dask to pandas to enable loading to Snowflake
        NYCpayroll_df = NYCpayroll_df.compute()

        # Save the transformed data back to disk
        NYCpayroll_df.to_csv(output_file_path, index=False)

        logging.info(f"Transformed data saved to {output_file_path}")

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

    return NYCpayroll_df


# Trigger the transformation

transform_data(
    r"Dataset/Raw_payroll_Data/nycpayroll_2020.csv", 
    r"Dataset/Cleaned_payroll_Data/nycpayroll_2020.csv",
    r"Dataset/Cleaned_payroll_Data/dim_date.csv"
)