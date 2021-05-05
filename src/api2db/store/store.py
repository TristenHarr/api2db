# -*- coding: utf-8 -*-
"""
Contains the Store class
==================================
"""
from ..stream.stream import Stream
from ..app.log import get_logger
import os
from typing import Optional, List


class Store(Stream):
    """Used for storing data into a local or external source periodically"""

    def __init__(self,
                 name: str,
                 seconds: int,
                 path: Optional[str]=None,
                 fmt: str="parquet",
                 drop_duplicate_exclude: Optional[List[str]]=None,
                 move_shards_path: Optional[str]=None,
                 move_composed_path: Optional[str]=None,
                 chunk_size: int=0
                 ):
        """
        Creates a Store object and attempts to build its dtypes.

        Args:
            name: The name of the collector the store is associated with
            seconds: The number of seconds between storage cycles
            path: The path to the directory that will contain sharded files that should be recomposed for storage
            fmt: The file format of the sharded files

                * `fmt="parquet"` (recommended) stores the DataFrame using parquet format
                * `fmt="json"` stores the DataFrame using JSON format
                * `fmt="pickle"` stores the DataFrame using pickle format
                * `fmt="csv"` stores the DataFrame using csv format

            drop_duplicate_exclude:

                * `drop_duplicate_exclude=None`

                  DataFrame.drop_duplicates() performed before storage

                * `drop_duplicate_exclude=["request_millis"]`

                  .drop_duplicates(subset=df.columns.difference(drop_duplicate_exclude))
                  performed before storage.

                  *Primarily used for arrival timestamps. I.e. API sends the same data
                  on sequential requests but in most applications the programmer will want to timestamp the arrival
                  time of data, which would lead to duplicate data with the only difference being arrival timestamps*


            move_shards_path: :py:meth:`Documentation and Examples found here
                              <api2db.stream.file_converter.FileConverter.static_compose_df_from_dir>`

            move_composed_path: :py:meth:`Documentation and Examples found here
                                <api2db.stream.file_converter.FileConverter.static_compose_df_from_dir>`

            chunk_size: CURRENTLY NOT SUPPORTED


        """
        super().__init__(name=name,
                         path=path,
                         fmt=fmt,
                         chunk_size=chunk_size,
                         store=True
                         )
        self.seconds = seconds
        self.drop_duplicate_exclude = drop_duplicate_exclude
        self.move_shards_path = move_shards_path
        self.move_composed_path = move_composed_path
        self.build_dependencies()
        self.stream = None
        """Optional[api2db.stream.stream.Stream]: The stream instance used to store data"""
        self.store_str = None
        """Optional[str]: A string used for logging"""

    def build_dependencies(self) -> None:
        """
        Builds the dependencies for the storage object. I.e. Makes the directories for the ``move_shards_path`` and
        the ``move_composed_path``

        Returns:
            None
        """
        if self.move_shards_path is not None:
            if not os.path.isdir(self.move_shards_path):
                os.makedirs(self.move_shards_path)
        if self.move_composed_path is not None:
            if not os.path.isdir(self.move_composed_path):
                os.makedirs(self.move_composed_path)

    def store(self) -> None:
        """
        Composed a DataFrame from the files in the stores path, and stores the data to the storage target.

        Returns:
            None
        """
        logger = get_logger()
        self.dtypes = self.build_dtypes()
        if self.dtypes is None:
            return
        df = self.static_compose_df_from_dir(path=self.path,
                                             fmt=self.fmt,
                                             move_shards_path=self.move_shards_path,
                                             move_composed_path=self.move_composed_path)
        if df is None:
            logger.warning(f"no files found at {self.path} to store, api may be down")
            return
        try:
            df = df.astype(self.dtypes)
        except Exception as e:
            logger.debug(e)
            return
        if self.drop_duplicate_exclude is not None:
            df = df.drop_duplicates(subset=df.columns.difference(self.drop_duplicate_exclude))
        else:
            df = df.drop_duplicates()
        logger.info(self.store_str.format(len(df)))
        self.stream.stream(df)

    def start(self):
        """
        Store objects subclass Stream but do not contain a start method. Stores should NEVER use start

        Raises:
            AttributeError: 'Store' object has no attribute 'start'
        """
        raise AttributeError("'Store' object has no attribute 'start'")

    def stream_start(self):
        """
        Store objects subclass Stream but do not contain a stream_start method. Stores should NEVER use stream_start

        Raises:
            AttributeError: 'Store' object has no attribute 'stream_start'
        """
        raise AttributeError("'Store' object has no attribute 'stream_start'")
