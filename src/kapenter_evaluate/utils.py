import importlib
import os
from pathlib import Path
import pandas as pd


def fix_arrow_ns_timestamp(df):
    cols_to_fix = [x for x in df.columns if df[x].dtype.name == 'timedelta64[ns]']
    temp_df = df
    for time_col in cols_to_fix:
        df[time_col] = temp_df[[time_col]].astype(int).loc[:, time_col] / (10 ** 9)
    return df


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Args:
            *args:
            **kwargs:
        """
        if 'load' in kwargs.keys():
            cls._instances[cls] = kwargs['load']
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CachingAgent(metaclass=Singleton):
    def __init__(self):
        super().__init__()
        self.root_path = self.ensure_directory('./local_cache/')

    def check_cache(self, key) -> bool:
        if '.parquet' not in key:
            key = key + '.parquet'
        file_path = Path(f'{self.root_path}{key}')
        if file_path.exists():
            return True
        else:
            return False

    def cache_dataframe(self, key, df) -> bool:
        if '.parquet' not in key:
            key = key + '.parquet'
        try:
            full_path = f'{self.root_path}{key}'
            path_comps = full_path.split('/')
            dirs = '/'.join(path_comps[:-1])
            self.ensure_directory(dirs)
            df.to_parquet(full_path)
            return self.check_cache(key)
        except Exception as ex:
            print(ex)
            return False

    def get_dataframe(self, key) -> pd.DataFrame:
        if '.parquet' not in key:
            key = key + '.parquet'
        if self.check_cache(key):
            return pd.read_parquet(f'{self.root_path}{key}')
        else:
            return pd.DataFrame()

    @staticmethod
    def ensure_directory(dir_path):
        # Convert the path to an absolute path relative to the current working directory
        abs_dir_path = os.path.abspath(dir_path)

        if not os.path.exists(abs_dir_path):
            os.makedirs(abs_dir_path)
            print(f"Directory created: {abs_dir_path}")
        else:
            print(f"Directory already exists: {abs_dir_path}")
        return abs_dir_path + "/"
