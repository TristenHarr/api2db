import pickle
import os
import pandas as pd


class StoreExternal(object):

    def __init__(self, name, seconds, path, load_format, drop_duplicate_exclude=None, move_shards_path=None):
        self.name = name
        self.seconds = seconds
        self.path = path
        self.load_format = load_format
        self.drop_duplicate_exclude = drop_duplicate_exclude
        self.move_shards_path = move_shards_path
        self.dtypes = self.build_dtypes()

    def store(self):
        """
        OVERRIDDEN
        :return:
        """
        pass

    def build_dtypes(self):
        dtypes_path = os.path.join("CACHE", f"{self.name}_dtypes.pkl")
        dtypes = None
        if os.path.isfile(dtypes_path):
            with open(dtypes_path, "rb") as f:
                dtypes = pickle.load(f)
        return dtypes

    def compose_files(self, files):
        df = None
        for file in files:
            path = os.path.join(self.path, file)
            if df is None:
                df = self.load_df(path)
            else:
                add_df = self.load_df(path)
                if add_df is not None:
                    df = df.append(add_df)
                    if self.drop_duplicate_subset is None:
                        df = df.drop_duplicates()
                    else:
                        df = df.drop_duplicates(subset=df.columns.difference(self.drop_duplicate_exclude))
        return df

    def load_df(self, path):
        df = None
        if self.load_format == "pickle":
            df = StoreExternal.pickle_load(path)
        elif self.load_format == "json":
            df = StoreExternal.json_load(path)
        elif self.load_format == "csv":
            df = StoreExternal.csv_load(path)
        elif self.load_format == "parquet":
            df = StoreExternal.parquet_load(path)
        if df is not None:
            df = df.astype(self.dtypes)
        return df

    def store_df(self, path, df):
        if self.load_format == "pickle":
            StoreExternal.pickle_store(path, df)
        elif self.load_format == "json":
            StoreExternal.json_store(path, df)
        elif self.load_format == "csv":
            StoreExternal.csv_store(path, df)
        elif self.load_format == "parquet":
            StoreExternal.parquet_store(path, df)

    @staticmethod
    def pickle_load(path):
        with open(path, "rb") as f:
            df = pickle.load(f)
        return df

    @staticmethod
    def pickle_store(path, df):
        with open(path, "wb") as f:
            pickle.dump(df, f)

    @staticmethod
    def json_load(path):
        return pd.read_json(path, orient="table")

    @staticmethod
    def json_store(path, df):
        df.to_json(path, orient="table", index=False)

    @staticmethod
    def csv_load(path):
        return pd.read_csv(path)

    @staticmethod
    def csv_store(path, df):
        df.to_csv(path, index=False)

    @staticmethod
    def parquet_load(path):
        return pd.read_parquet(path)

    @staticmethod
    def parquet_store(path, df):
        df.to_parquet(path, index=False)
