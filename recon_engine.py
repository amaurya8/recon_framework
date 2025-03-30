import datacompy
from data_fetcher import DataFetcher

class ReconEngine:
    def __init__(self, config):
        self.config = config
        self.source_data = DataFetcher.fetch_data(self.config, is_source=True)
        self.target_data = DataFetcher.fetch_data(self.config, is_source=False)

    def run_recon(self):
        if self.source_data is not None and self.target_data is not None:
            comparison = datacompy.Compare(
                self.source_data, self.target_data,
                join_columns=self.config['comparison_keys']
            )
            print(comparison.report())
        else:
            print("## Data could not be loaded, please check configurations. ##")