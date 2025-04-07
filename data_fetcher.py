import os
import xml.etree.ElementTree as ET

import pandas as pd

from cloud_connector import CloudConnector
from db_connector import DBConnector
from logger_config import logger

class DataFetcher:
    @staticmethod
    def fetch_data(config, is_source=True):
        system_type = "source" if is_source else "target"

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
            raise DataLoadError(f"Missing '{system_type_key}' in configuration")

        data = None
        system_source = config[system_type_key].lower()

        if system_source == 'file':
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
                    data = pd.read_csv(file_path, delimiter=",")
                elif file_ext == '.json':
                    data = pd.read_json(file_path)
                elif file_ext == '.xml':
                    data = DataFetcher.load_xml(file_path)
                elif file_ext == '.fwf':
                    data = pd.read_fwf(file_path)
                else:
                    logger.warning(f"Unsupported file type: {file_ext}. Attempting to load as CSV.")
                    data = pd.read_csv(file_path)

                logger.info(f"Successfully loaded file: {file_path}")
            except Exception as e:
                logger.error(f"Error loading file {file_path}: {e}")
                return None

        elif system_source == 'database':
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

        elif system_source == 'adls':
            try:
                conn_str, container, folder_path, file_type = file_path.split('|')
                logger.info(f"Reading ADLS for {system_type}: container={container}, path={folder_path}, type={file_type}")
                CloudConnector.read_all_files_from_adls_folder_with_connection_string(conn_str,container,folder_path,file_type)
                logger.info(f"ADLS load successful for {system_type}")
            except Exception as e:
                raise DataLoadError(f"Failed to load from ADLS for {system_type}: {e}")

        elif system_source == 'adx':
            try:
                cluster_uri, db, query_file_path, client_id, client_secret, authority_id = file_path.split('|')
                CloudConnector.fetch_adx_data(cluster_uri,db,query_file_path,client_id,client_secret,authority_id)
                logger.info(f"ADX query executed successfully for {system_type}")
            except Exception as e:
                raise DataLoadError(f"Failed to load from ADX for {system_type}: {e}")

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
class DataLoadError(Exception):
    pass