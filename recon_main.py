import pandas as pd
from config_loader import ConfigLoader
from recon_engine import ReconEngine

if __name__ == "__main__":
    print("## Starting Recon Engine...! ##")
    pd.set_option("display.max_columns", None)

    file_path = "resources/Recon_Driver.xlsx"
    config_df = ConfigLoader.read_config(file_path)

    for _, row in config_df.iterrows():
        config = {
            'source_name': row['Source Name'],
            'source_type': row['Source Type'],
            'source_detail': row['Source Detail'],
            'source_db_type': row['Source DB Type'],
            'source_host': row['Source Host'],
            'source_port': row['Source Port'],
            'source_database': row['Source Database'],
            'source_user': row['Source User ID'],
            'source_password': row['Source Password'],
            'source_query_file': row['Source Query File'],
            'target_name': row['Target Name'],
            'target_type': row['Target Type'],
            'target_detail': row['Target Detail'],
            'target_db_type': row['Target DB Type'],
            'target_host': row['Target Host'],
            'target_port': row['Target Port'],
            'target_database': row['Target Database'],
            'target_user': row['Target User ID'],
            'target_password': row['Target Password'],
            'target_query_file': row['Target Query File'],
            'comparison_keys': row['Comparison Keys'].split(',')
        }

        recon_engine = ReconEngine(config)
        recon_engine.run_recon()

    print("## Recon Completed...! ##")