import logging
import os

LOG_FILE = "./resources/logs/recon.log"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)  # Checking if logs directory exists

# Configure global logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG / Other levels for more / less details
    format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),  # Append logs to a file
        logging.StreamHandler()  # For console
    ]
)

# Get a central logger instance
logger = logging.getLogger(__name__)  # Use the module name dynamically
