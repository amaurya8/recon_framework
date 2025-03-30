import oracledb
import pyodbc

class DBConnector:
    @staticmethod
    def connect_to_oracle(username, password, host, port, service_name):
        try:
            # Construct the connection string
            dsn = f"{host}:{port}/{service_name}"

            # Establish a connection
            connection = oracledb.connect(user=username, password=password, dsn=dsn)

            print("Connected to Oracle successfully!")
            return connection
        except Exception as exp:
            print(f"Unable to connect to Oracle: {exp}")
            return None

    @staticmethod
    def connect_to_mssql(username, password, host, port, database):
        try:
            conn_str = f'DRIVER={{SQL Server}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password}'
            connection = pyodbc.connect(conn_str)
            return connection
        except Exception as exp:
            print(f"Unable to connect to MSSQL: {exp}")
            return None