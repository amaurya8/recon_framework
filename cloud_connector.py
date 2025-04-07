from adlfs import AzureBlobFileSystem
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
import pandas as pd
from azure.kusto.data.helpers import dataframe_from_result_table

class CloudConnector:
    @staticmethod
    def read_all_files_from_adls_folder_with_connection_string(
        connection_string, container_name, folder_path, file_type="csv"
    ):
        try:
            # Create the ADLS filesystem using connection string
            fs = AzureBlobFileSystem(connection_string=connection_string)

            # Clean up the path and list matching files
            folder_path = folder_path.strip("/")
            full_path = f"{container_name}/{folder_path}"
            file_list = fs.glob(f"{full_path}/**/*.{file_type}")  # recursive search

            # Filter out zero-byte files
            file_list = [f for f in file_list if fs.info(f)['size'] > 0]

            if not file_list:
                print("No valid (non-empty) files found.")
                return pd.DataFrame()

            # Read and combine files
            dfs = []
            for file in file_list:
                with fs.open(file, "rb") as f:
                    if file_type == "csv":
                        df = pd.read_csv(f)
                    elif file_type == "parquet":
                        df = pd.read_parquet(f)
                    else:
                        raise ValueError("Unsupported file type")
                    dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

        except Exception as e:
            print(f"Error reading files from ADLS: {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_adx_data(cluster_uri: str, database: str, query: str, client_id: str, client_secret: str, authority_id: str) -> pd.DataFrame:
        try:
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                cluster_uri, client_id, client_secret, authority_id
            )
            client = KustoClient(kcsb)
            response = client.execute(database, query)

            # Convert using helper
            df = dataframe_from_result_table(response.primary_results[0])
            return df

        except Exception as e:
            print(f"Error fetching data from ADX: {e}")
            return pd.DataFrame()