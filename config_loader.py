import pandas as pd

class ConfigLoader:
    @staticmethod
    def read_config(config_path):
        return pd.read_excel(config_path)