import pandas as pd
from db_connector import DBConnector

class DataFetcher:
    @staticmethod
    def fetch_data(config, is_source=True):
        system_type = "source" if is_source else "target"
        file_path = config[f"{system_type}_detail"]
        db_type = config[f"{system_type}_db_type"]
        host = config[f"{system_type}_host"]
        port = config[f"{system_type}_port"]
        database = config[f"{system_type}_database"]
        user = config[f"{system_type}_user"]
        password = config[f"{system_type}_password"]
        query_file = config[f"{system_type}_query_file"]

        if config[f"{system_type}_type"].lower() == 'file':
            return pd.read_csv(file_path)

        elif config[f"{system_type}_type"].lower() == 'database':
            conn = None
            if db_type.lower() == 'oracle':
                conn = DBConnector.connect_to_oracle(user, password, host, port, database)
            elif db_type.lower() == 'mssql':
                conn = DBConnector.connect_to_mssql(user, password, host, port, database)

            if conn:
                with open(query_file, 'r') as f:
                    query = f.read()
                return pd.read_sql(query, conn)

        return None