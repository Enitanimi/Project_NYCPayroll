
import dask
import dask.dataframe as dd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import os
import logging
from pipeline_log import logging
from dotenv import load_dotenv

load_dotenv(override=True)


account_name = os.getenv('AzureAN')
account_key = os.getenv('AzureAK')
container_name = os.getenv('AzureCN')

blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
container_client = blob_service_client.get_container_client(container_name)

# Extracting the Data and moving to another folder



def move_blob(blob_name, destination_folder):
    try:
        logging.info(f'Moving {blob_name} to {destination_folder}.')
        
        # Create the destination blob path
        destination_blob_name = f'{"payroll-extracteddata"}/{blob_name.split("/")[-1]}'  # Placeholder: 'destination_folder' should be the folder name where you want to move processed files
        
        # Get the source and destination blob clients
        source_blob = container_client.get_blob_client(blob_name)
        destination_blob = container_client.get_blob_client(destination_blob_name)

        # Copy the blob to the destination
        destination_blob.start_copy_from_url(source_blob.url)
        
        # Delete the original blob
        source_blob.delete_blob()

        logging.info(f'Moved {blob_name} to {destination_folder}.')
    except Exception as e:
        logging.error(f'Failed to move {blob_name}: {e}')



def extract_files_from_adls(destination_folder):
    logging.info("Starting extract_files_from_adls function")
    try:
        # List blobs with a specific prefix (Placeholder: 'new-data/' should be replaced with the prefix of your new data folder)
        blobs_list = container_client.list_blobs(name_starts_with="payroll-sourcedata/")
        
        for blob in blobs_list:
            blob_name = blob.name
            
            # Define the local download path
            download_path = os.path.join("Dataset/Raw_payroll_Data", blob_name.split('/')[-1])  # Placeholder: 'destination_folder' should be the local directory where files will be downloaded
            
            logging.info(f'Downloading {blob_name} from ADLS to {download_path}.')
            
            # Download the blob to the local path
            blob_client = container_client.get_blob_client(blob_name)
            with open(download_path, "wb") as file:
                file.write(blob_client.download_blob().readall())
                
            logging.info(f'Downloaded {blob_name} successfully.')
            
            # Move the blob to the processed-data folder after downloading (Placeholder: 'processed-data' should be the folder where you want to move processed files)
            move_blob(blob_name, 'payroll-extracteddata')
            
    except Exception as e:
        logging.error(f'Error extracting files from ADLS: {e}')


extract_files_from_adls(destination_folder="Dataset/Raw_payroll_Data")



