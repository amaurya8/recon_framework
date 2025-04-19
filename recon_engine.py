import datacompy
import pandas as pd
from data_fetcher import DataFetcher
from recon_reporter import ReconReportGenerator
from logger_config import logger

""" This Class contains logic for recon — makes debugging simpler and will evolve over time, we can other pre recon logic / method when-ever required."""

class ReconEngine:

    def __init__(self, config,config_path):
        self.config = config
        self.config_path = config_path
        self.source_data = DataFetcher.fetch_data(self.config, is_source=True)
        if self.source_data is None:
            raise DataLoadError("Source data could not be loaded. Please check the source configuration.")
        self.target_data = DataFetcher.fetch_data(self.config, is_source=False)
        if self.target_data is None:
            raise DataLoadError("Target data could not be loaded. Please check the target configuration.")

        # Checking if column mapping sheet exist, and apply
        mapping_dict = self.check_and_apply_col_mapping()
        if mapping_dict:
            self.source_data.rename(columns=mapping_dict, inplace=True)
            logger.info("Source DataFrame columns renamed using mapping.")

    # def check_and_apply_col_mapping(self):
    #     mapping = self.config.get('Use_Case_Id')
    #     try:
    #         logger.info(f"Checking for {mapping} mapping sheet in config file: {self.config_path}")
    #         xl = pd.ExcelFile(self.config_path)
    #
    #         if mapping not in xl.sheet_names:
    #             logger.info(f"{mapping} sheet not found. No column mapping will be applied.")
    #             return None
    #
    #         logger.info( f"Found {mapping} sheet. Loading mapping data...")
    #         mapping_df = xl.parse(mapping).fillna('')
    #
    #         required_cols = {'Source_Column', 'Target_Column'}
    #         if not required_cols.issubset(mapping_df.columns):
    #             raise ValueError(f"The {mapping} sheet must contain 'Source_Column' and 'Target_Column' columns.")
    #
    #         # Build mapping dict {source_column_name: target_column_name}
    #         mapping_dict = dict(zip(mapping_df['Source_Column'], mapping_df['Target_Column']))
    #         logger.info(f"Successfully loaded column mappings: {mapping_dict}")
    #         return mapping_dict
    #
    #     except Exception as e:
    #         logger.error(f"Error loading column mappings: {e}")
    #         return None
    def check_and_apply_col_mapping(self):
        mapping = self.config.get('Use_Case_Id')
        try:
            logger.info(f"Checking for '{mapping}' mapping sheet in config file: {self.config_path}")
            xl = pd.ExcelFile(self.config_path)

            if mapping not in xl.sheet_names:
                logger.info(f"'{mapping}' sheet not found. No column mapping will be applied.")
                return None

            logger.info(f"Found '{mapping}' sheet. Loading mapping data...")
            mapping_df = xl.parse(mapping).fillna('')

            required_cols = {'Source_Column', 'Target_Column'}
            if not required_cols.issubset(mapping_df.columns):
                raise ValueError(f"The '{mapping}' sheet must contain 'Source_Column' and 'Target_Column' columns.")

            # Validate each mapping entry
            valid_mappings = {}
            for _, row in mapping_df.iterrows():
                source_col = row['Source_Column']
                target_col = row['Target_Column']

                if source_col not in self.source_data.columns:
                    logger.warning(f"Source column '{source_col}' not found in source DataFrame. Skipping mapping.")
                    continue
                if target_col not in self.target_data.columns:
                    logger.warning(f"Target column '{target_col}' not found in target DataFrame. Skipping mapping.")
                    continue

                valid_mappings[source_col] = target_col

            if not valid_mappings:
                logger.info("No valid column mappings found. Skipping renaming.")
                return None

            logger.info(f"Valid column mappings applied: {valid_mappings}")
            return valid_mappings

        except Exception as e:
            logger.error(f"Error loading OR applying column mappings: {e}")
            return None

    def run_recon(self):
        if self.source_data is None or self.target_data is None:
            logger.error("## Data could not be loaded, please check configurations. ##")
            return

        comparison = datacompy.Compare(
            self.source_data, self.target_data,
            join_columns=self.config['comparison_keys']
        )

        logger.info("Reconciliation completed. Generating report...")

        # Generate and save the HTML report
        report_generator = ReconReportGenerator(comparison, self.source_data, self.target_data, self.config)
        report_generator.recon_report()

        logger.info("Report generation completed successfully.")

class DataLoadError(Exception):
    pass
