import pymysql
import psycopg2
import oracledb  # Replaces cx_Oracle
import pyodbc
from logger_config import logger
from crypto_util import CryptoUtil


class DBConnector:
    @staticmethod
    def connect_to_mysql(user, encrypted_password, host, port, database):
        """Connect to MySQL using PyMySQL with decrypted password"""
        try:
            password = CryptoUtil.decrypt_password(encrypted_password)
            conn = pymysql.connect(
                host=host,
                user=user,
                password=password,
                database=database,
                port=int(port),
                cursorclass=pymysql.cursors.DictCursor  # Returns query results as dictionaries
            )
            logger.info(f"Connected to MySQL at {host}:{port}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to MySQL: {e}")
            return None

    @staticmethod
    def connect_to_postgresql(user, password, host, port, database):
        """Connect to PostgreSQL Database"""
        try:
            password = CryptoUtil.decrypt_password(password)
            conn = psycopg2.connect(
                user=user, password=password, host=host, port=port, dbname=database
            )
            logger.info(f"Connected to PostgreSQL database: {database} at {host}:{port}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return None

    @staticmethod
    def connect_to_oracle(user, password, host, port, database):
        """Connect to Oracle Database using `oracledb`"""
        try:
            password = CryptoUtil.decrypt_password(password)
            dsn = f"{host}:{port}/{database}"
            oracledb.init_oracle_client()  # Optional, needed for thick mode
            conn = oracledb.connect(user=user, password=password, dsn=dsn)
            logger.info(f"Connected to Oracle database: {database} at {host}:{port}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to Oracle: {e}")
            return None

    @staticmethod
    def connect_to_mssql(user, password, host, port, database):
        """Connect to Microsoft SQL Server"""
        try:
            password = CryptoUtil.decrypt_password(password)
            conn_str = f"DRIVER={{SQL Server}};SERVER={host},{port};DATABASE={database};UID={user};PWD={password}"
            conn = pyodbc.connect(conn_str)
            logger.info(f"Connected to MSSQL database: {database}")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to MSSQL: {e}")
            return None
