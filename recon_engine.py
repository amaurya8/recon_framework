import datacompy
from data_fetcher import DataFetcher
from recon_reporter import ReconReportGenerator
import logging


class ReconEngine:
    def __init__(self, config):
        self.config = config
        self.source_data = DataFetcher.fetch_data(self.config, is_source=True)
        self.target_data = DataFetcher.fetch_data(self.config, is_source=False)

    def run_recon(self):
        if self.source_data is None or self.target_data is None:
            logging.error("## Data could not be loaded, please check configurations. ##")
            return

        comparison = datacompy.Compare(
            self.source_data, self.target_data,
            join_columns=self.config['comparison_keys']
        )

        logging.info("Reconciliation completed. Generating report...")

        # Generate and save the HTML report
        report_generator = ReconReportGenerator(comparison, self.source_data, self.target_data, self.config)
        report_generator.recon_report()

        logging.info("Report generation completed successfully.")
