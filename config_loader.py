import pandas as pd
from logger_config import logger  # Using Central logger config, instead of module level logger config

class ConfigLoader:
    @staticmethod
    def read_config(config_path):
        try:
            logger.info(f"Loading configuration from {config_path}...")
            config_df = pd.read_excel(config_path)

            if config_df.empty:
                logger.warning("Warning: The configuration file is empty!")

            logger.info(f"Successfully loaded {len(config_df)} configuration rows.")
            return config_df

        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise  # Stop execution if config loading fails
