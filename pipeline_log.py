
import logging


# Set up logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s\n\n',  # Adjust the format as needed
    handlers=[
        logging.FileHandler("etl_pipeline.log"),  # Logs to a file named transformation.log
        logging.StreamHandler()  # Also prints logs to the console
    ]
)

