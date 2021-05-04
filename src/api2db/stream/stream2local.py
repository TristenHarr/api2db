# -*- coding: utf-8 -*-
"""
Contains the Stream2Local class
==================================
"""
from .stream import Stream
from ..app.log import get_logger
import os
import time
import pandas as pd
from typing import Optional, List


class Stream2Local(Stream):
    """Streams data from the associated collector directly to a local file in real-time"""

    def __init__(self,
                 name: str,
                 path: Optional[str]=None,
                 mode: str="shard",
                 fmt: str="parquet",
                 drop_duplicate_keys: Optional[List[str]]=None):
        """
        Creates a Stream2Local object and attempts to build its dtypes

        Args:
            name: The name of the collector associated with the stream
            path: The path to either a single file or a file directory dictated by the `mode` parameter
            mode:

                * `mode="shard"` (default) will store each incoming file independently in the specified `path`
                  In shard mode the file will be named **timestamp_ns**.fmt
                * `mode="update"` will update the file located at the specified `path` with the new data
                * `mode="replace"` will replace the file located at the specified `path` with the new data

            fmt:

                * `fmt="parquet"` (default/recommended) stores the files using parquet format
                * `fmt="json"` stores the files using JSON format
                * `fmt="pickle"` stores the files using pickle format
                * `fmt="csv"` stores the files using csv format

            drop_duplicate_keys:
                * `drop_duplicate_keys=None` -> DataFrame.drop_duplicates() performed before storage
                * `drop_duplicate_keys=["uuid"]` -> DataFrame.drop_duplicates(subset=drop_duplicate_keys) performed
                  before storage
        """
        if path is None and mode == "shard":
            path = os.path.join("STORE/", f"{name}/", f"{fmt}/")
        elif path is None:
            path = os.path.join("CACHE/", f"{name}_static.{fmt}")
        super().__init__(name=name, path=path, fmt=fmt, stream_type=f"local.{fmt}")
        self.mode = mode
        self.drop_duplicate_keys = drop_duplicate_keys

    def stream(self, data: pd.DataFrame) -> None:
        """
        Stores the incoming data into its stream target using the specified `mode`

        Args:
            data: The data to be stored

        Returns:
            None
        """
        if self.mode == "shard":
            self.stream_shard(data)
        elif self.mode == "update":
            self.stream_update(data)
        elif self.mode == "replace":
            self.stream_replace(data)

    def stream_shard(self, data: pd.DataFrame) -> None:
        """
        Stores the incoming data to the specified directory path using the file naming schema **timestamp_ns**.fmt

        Args:
            data: The data to store to the file

        Returns:
            None
        """
        logger = get_logger()
        if not os.path.isdir(self.path):
            try:
                os.makedirs(self.path)
            except Exception as e:
                logger.exception(e)
        if not os.path.isdir(self.path) or self.fmt is None:
            return
        logger.debug(f"storing {len(data)} rows to {self.path}")
        data = data.drop_duplicates(subset=self.drop_duplicate_keys)
        self.static_store_df(df=data,
                             path=os.path.join(self.path, f"{int(time.time()*1000.0)}.{self.fmt}"),
                             fmt=self.fmt)

    def stream_update(self, data: pd.DataFrame) -> None:
        """
        Updates the existing data at the specified file path and adds the incoming data

        Args:
            data: The data to add to the file

        Returns:
            None
        """
        logger = get_logger()
        self.dtypes = self.build_dtypes()
        dir_path = os.path.split(self.path)[0]
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        if self.dtypes is not None:
            df = self.static_load_df(path=self.path, fmt=self.fmt, dtypes=self.dtypes)
            logger.debug(f"adding {len(data)} rows to {self.path}")
            if df is not None:
                df = df.append(data).drop_duplicates(subset=self.drop_duplicate_keys)
                self.static_store_df(df, path=self.path, fmt=self.fmt)
            else:
                self.static_store_df(data, path=self.path, fmt=self.fmt)

    def stream_replace(self, data: pd.DataFrame) -> None:
        """
        Replaces the existing data at the specified file path with the incoming data

        Args:
            data: The data to replace the file with

        Returns:
            None
        """
        if os.path.isfile(self.path):
            os.remove(self.path)
        data = data.drop_duplicates(subset=self.drop_duplicate_keys)
        self.static_store_df(data, path=self.path, fmt=self.fmt)
