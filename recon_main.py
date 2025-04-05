import pandas as pd
from config_loader import ConfigLoader
from recon_engine import ReconEngine
import time
from logger_config import logger

""" This module contains logic for recon orchestration ( looping ) for multiple data sets ( source and target systems ) """

if __name__ == "__main__":
    logger.info("## Starting Recon Engine...! ##")
    pd.set_option("display.max_columns", None)

    driver_config_file_path = "resources/Recon_Driver_Config.xlsx"
    config_df = ConfigLoader.read_config(driver_config_file_path)

    for _, row in config_df.iterrows():
        try:
            config = {
                'source_name': str(row['Source Name']).strip(),
                'source_type': str(row['Source Type']).strip(),
                'source_detail': str(row['Source Detail']).strip(),
                'source_db_type': str(row['Source DB Type']).strip(),
                'source_host': str(row['Source Host']).strip(),
                'source_port': str(row['Source Port']).strip() if pd.notna(row['Source Port']) else '',
                'source_database': str(row['Source Database']).strip(),
                'source_user': str(row['Source User ID']).strip(),
                'source_password': str(row['Source Password']).strip(),
                'source_query_file': str(row['Source Query File']).strip(),
                'target_name': str(row['Target Name']).strip(),
                'target_type': str(row['Target Type']).strip(),
                'target_detail': str(row['Target Detail']).strip(),
                'target_db_type': str(row['Target DB Type']).strip(),
                'target_host': str(row['Target Host']).strip(),
                'target_port': str(row['Target Port']).strip() if pd.notna(row['Target Port']) else '',
                'target_database': str(row['Target Database']).strip(),
                'target_user': str(row['Target User ID']).strip(),
                'target_password': str(row['Target Password']).strip(),
                'target_query_file': str(row['Target Query File']).strip(),
                'comparison_keys': [key.strip() for key in str(row['Comparison Keys']).split(',')]
            }

            logger.info(f"## Starting reconciliation for {config['source_name']} vs {config['target_name']} ##")

            start_time = time.time()
            recon_engine = ReconEngine(config)
            recon_engine.run_recon()
            end_time = time.time()

            execution_time_ms = round((end_time - start_time) * 1000, 2)
            logger.info(f"## Recon completed for {config['source_name']} vs {config['target_name']} in {execution_time_ms} ms ##")

        except Exception as e:
            logger.error(f"Error processing reconciliation for {row.get('Source Name', 'Unknown')} vs {row.get('Target Name', 'Unknown')}: {e}")

    logger.info("## Recon Engine Completed for All Configurations! ##")
