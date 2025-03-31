import pandas as pd
import os
import xml.etree.ElementTree as ET
from db_connector import DBConnector
from logger_config import logger  # Import global logger


class DataFetcher:
    @staticmethod
    def fetch_data(config, is_source=True):
        system_type = "source" if is_source else "target"

        # Fetch configurations safely
        system_type_key = f"{system_type}_type"
        file_path = config.get(f"{system_type}_detail")
        db_type = config.get(f"{system_type}_db_type", "").lower()
        host = config.get(f"{system_type}_host")
        port = config.get(f"{system_type}_port")
        database = config.get(f"{system_type}_database")
        user = config.get(f"{system_type}_user")
        password = config.get(f"{system_type}_password")
        query_file = config.get(f"{system_type}_query_file")

        if not config.get(system_type_key):
            logger.error(f"Missing '{system_type_key}' in configuration")
            return None

        data = None

        if config[system_type_key].lower() == 'file':
            if not file_path or not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return None

            file_ext = os.path.splitext(file_path)[1].lower()
            logger.info(f"Loading file {file_path} with extension {file_ext}...")

            try:
                if file_ext == '.csv':
                    data = pd.read_csv(file_path)
                elif file_ext in ['.xls', '.xlsx']:
                    data = pd.read_excel(file_path)
                elif file_ext == '.txt':
                    data = pd.read_csv(file_path, delimiter=",")  # Adjust delimiter as needed
                elif file_ext == '.json':
                    data = pd.read_json(file_path)
                elif file_ext == '.xml':
                    data = DataFetcher.load_xml(file_path)
                elif file_ext == '.fwf':  # Fixed Width File
                    data = pd.read_fwf(file_path)
                else:
                    logger.warning(f"Unsupported file type: {file_ext}. Attempting to load as CSV.")
                    data = pd.read_csv(file_path)

                logger.info(f"Successfully loaded file: {file_path}")
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {e}")
                return None

        elif config[system_type_key].lower() == 'database':
            if not query_file or not os.path.exists(query_file):
                logger.error(f"Query file not found: {query_file}")
                return None

            conn = None
            try:
                if db_type == 'oracle':
                    conn = DBConnector.connect_to_oracle(user, password, host, port, database)
                elif db_type == 'mssql':
                    conn = DBConnector.connect_to_mssql(user, password, host, port, database)
                elif db_type == 'mysql':
                    conn = DBConnector.connect_to_mysql(user, password, host, port, database)
                elif db_type == 'postgresql':
                    conn = DBConnector.connect_to_postgresql(user, password, host, port, database)
                else:
                    logger.error(f"Unsupported database type: {db_type}")
                    return None

                if conn:
                    with open(query_file, 'r') as f:
                        query = f.read()
                    logger.info(f"Executing query from {query_file} on {db_type} database...")
                    data = pd.read_sql(query, conn)
                    logger.info(f"Query execution successful for {system_type} system.")

            except Exception as e:
                logger.error(f"Error executing query for {system_type} system: {e}")
                return None
            finally:
                if conn:
                    conn.close()

        return data

    @staticmethod
    def load_xml(file_path):
        """Load XML file and convert it to DataFrame"""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()

            data = []
            for elem in root:
                row_data = {child.tag: child.text for child in elem}
                data.append(row_data)

            df = pd.DataFrame(data)
            logger.info(f"Successfully loaded XML file: {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading XML file {file_path}: {e}")
            return None
