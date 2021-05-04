# -*- coding: utf-8 -*-
"""
Contains the ApiForm class
==========================
"""
from .pre_process.pre import Pre
from .data_feature.feature import Feature
from .post_process.post import Post
from typing import Optional, List, Any
import os
import pickle
import pandas as pd
import json


class ApiForm(object):
    """Used to clean and process incoming data arriving from an Api"""

    def __init__(self,
                 name: str,
                 pre_process: Optional[List[Pre]]=None,
                 data_features: Optional[List[Feature]]=None,
                 post_process: Optional[List[Post]]=None):
        """
        Creates an ApiForm

        NOTE:
            The ApiForm is used by api2db to do the processing and cleaning of data.
            Incoming data goes through 3 phases.

                1. Pre-Processing

                    * Extract global data-features

                    * Extract a list of data-points that will serve as the rows in a database

                    * Flatten nested arrays of data

                    * Swap extraneous rows returned from poorly implemented APIs

                2. Feature Extraction

                    * Extracts the data features for each row that will be stored in a database

                3. Post-Processing

                    * Add new columns of data that will be the same globally for the arriving data.
                      I.e. arrival timestamps

                    * Apply functions across data columns, replacing the data with the calculated value.
                      I.e. Reformat strings, strip whitespace, etc.

                    * Add new columns of data that are derived from performing calculations on existing columns.
                      I.e. Use a `latitude` and `longitude` column to calculate a new column called `country`

                    * Cast columns that contain datetime data from strings to date times.

                    * Drop columns that should not contain null values.

                    * Perform merging of incoming data with locally stored reference tables.
                      I.e. Incoming data has column `location_id` field, a reference table contains location info with
                      the `location_id` field being a link between the two.
                      This allows for data to be merged on column `location_id` in order to contain all data in a
                      single table.

        Args:
            name: The name of the collector the ApiForm is associated with
            pre_process: An array pre-processing objects to be applied sequentially on incoming data
            data_features: An array of data features to be extracted from the incoming data.
                           The programmer can choose which data features they require, and keep only those.
            post_process: An array of post-processing objects to be applied sequentially on the data after data has been
                          cleaned and extracted to a `pandas.DataFrame`
        """
        self.name = name
        self.pre_process = [] if pre_process is None else pre_process
        self.data_features = [] if data_features is None else data_features
        self.post_process = [] if post_process is None else post_process
        if name == "lab":
            print("building laboratory...")
            self.pre_process = pre_process
            self.data_features = data_features
            self.post_process = post_process

    def add_pre(self, pre: Pre) -> None:
        """
        Allows the programmer to manually add a item to the pre-processing array.

        Args:
            pre: The pre-processing object to add

        Returns:
            None
        """
        self.pre_process.append(pre)

    def add_feature(self, feat: Feature) -> None:
        """
        Allows the programmer to manually add a item to the data-features array.

        Args:
            feat: The feature object to add

        Returns:
            None
        """
        self.data_features.append(feat)

    def add_post(self, post: Post) -> None:
        """
        Allows the programmer to manually add a item to the post-processing array.

        Args:
            post: The post-processing object to add

        Returns:
            None
        """
        self.post_process.append(post)

    def pandas_typecast(self) -> dict:
        """
        Performs typecasting from python native types to their pandas counterparts.
        Currently supported types are:

            * int
            * float
            * bool
            * str

        Since API data is inconsistent, all typecasting makes the values nullable inside the DataFrame. Null values can
        be removed during post-processing.

        Returns:
            A dictionary that can be used to cast a DataFrames types using DataFrame.astype()
        """
        res = {}
        for feat in self.data_features:
            res[feat.key] = ApiForm.typecast(feat.dtype)
        return res

    @staticmethod
    def typecast(dtype: Any) -> str:
        """
        Yields a string containing the pandas dtype when given a python native type.

        Args:
            dtype: The python native type

        Returns:
            The string representing the type that the native type converts to when put into a DataFrame
        """
        if dtype is int:
            return "Int64"
        elif dtype is float:
            return "Float64"
        elif dtype is bool:
            return "bool"
        return "string"

    def experiment(self, CACHE, import_target) -> bool:
        """
        Tool used to build an ApiForm

        NOTE:

            The laboratory is an experimental feature and does not currently support the StaticMerge post-processor.

        Args:
            CACHE: If the data imports should be cached. I.e. Only call the API once
            import_target: The target function that performs an API import

        Returns:
            True if experiment is ready for export otherwise False
        """
        cache_path = os.path.join(os.getcwd(), "lab_cache.pickle")
        if CACHE and os.path.isfile(cache_path):
            with open(cache_path, "rb") as f:
                data = pickle.load(f)
        else:
            data = import_target()
            if CACHE and data is not None and type(data) is list and type(data[0]) is dict:
                with open(cache_path, "wb") as f:
                    pickle.dump(data, f)

        pre_2_post = {}
        if type(data) is not list or (type(data) is list and len(data) == 0) or (
                type(data) is list and type(data[0]) is not dict):
            print(f"import_target must return a list of dictionary data.\nimport_target:\n{data}\n")
            return False
        data = data[0]
        with open("data_without_preprocess.json", "w") as f:
            json.dump(data, f, indent=10)
        if self.pre_process is None or type(self.pre_process) is not list:
            print(f"data:\n{data}\n")
            print(f"data keys:\n{data.keys()}\n")
            print(f"pre_process must return a list of 0 or more pre-processors.\npre_process:\n{self.pre_process}\n")
            return False
        for pre in self.pre_process:
            if pre.ctype == "global_extract":
                pre_2_post[pre.key] = pre(lam_arg=data)
            else:
                data = pre(lam_arg=data)
        with open("data_after_preprocess.json", "w") as f:
            json.dump(data, f, indent=10)
        if type(data) is not list:
            print(f"data should be a list of dicts once pre-processing has been completed.\ndata:\n{data}\n")
            return False
        if type(self.data_features) is not list or len(self.data_features) == 0:
            for i in range(3 if len(data) > 3 else len(data)):
                print(f"data point {i+1}:\n{data[i]}\n")
            print(f"data_features must return a list of data-features.\ndata_features:\n{self.data_features}\n")
            return False
        rows = []
        for d in data:
            row = {}
            for feat in self.data_features:
                row[feat.key] = feat(d)
            rows.append(row)
        df = pd.DataFrame(rows)
        for feat in self.data_features:
            df[feat.key] = df[feat.key].astype(ApiForm.typecast(feat.dtype))
        for k, v in pre_2_post.items():
            df[k] = v["value"]
            df[k] = df[k].astype(ApiForm.typecast(v["dtype"]))
        with open("data_after_feature_extraction.json", "w") as f:
            df.to_json(path_or_buf=f, orient="records", indent=10)
        if type(self.post_process) is not list:
            for i in range(3 if len(data) > 3 else len(data)):
                print(f"data point {i+1}:\n{data[i]}\n")
            print(f"data:\n{df}\n")
            print(f"data dtypes:\n{df.dtypes}\n")
            return False
        for post in self.post_process:
            df = post(df)
        for i in range(3 if len(data) > 3 else len(data)):
            print(f"data point {i+1}:\n{data[i]}\n")
        print(f"finalized data:\n{df}\n")
        print(f"finalized data dtypes:\n{df.dtypes}\n")
        return True
