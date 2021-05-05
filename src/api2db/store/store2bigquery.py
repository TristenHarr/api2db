# -*- coding: utf-8 -*-
"""
Contains the Store2Bigquery class
==================================
"""
from .store import Store
from ..stream.stream2bigquery import Stream2Bigquery
from typing import Optional, List


class Store2Bigquery(Store):
    """Used for storing data to bigquery periodically"""

    def __init__(self,
                 name: str,
                 seconds: int,
                 auth_path: str,
                 pid: str,
                 did: str,
                 tid: str,
                 path: Optional[str]=None,
                 fmt: str="parquet",
                 drop_duplicate_exclude: Optional[List[str]]=None,
                 move_shards_path: Optional[str]=None,
                 move_composed_path: Optional[str]=None,
                 location: str="US",
                 if_exists: str="append",
                 chunk_size: int =0):
        """
        Creates a Store2Bigquery object and attempts to build its dtypes.

        Args:
            name: The name of the collector the store is associated with
            seconds: The number of seconds between storage cycles
            auth_path: The path to the Google provided authentication file. I.e. AUTH/google_auth_file.json
            pid: Google project ID
            did: Google dataset ID
            tid: Google table ID
            path: The path to the directory that will contain sharded files that should be recomposed for storage
            fmt: The file format of the sharded files

                * `fmt="parquet"` (recommended) loads the sharded files using parquet format
                * `fmt="json"` loads the sharded files using JSON format
                * `fmt="pickle"` loads the sharded files using pickle format
                * `fmt="csv"` loads the sharded files using csv format

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

            location: Location of the Bigquery project
            if_exists:

                * `if_exists="append"` Adds the data to the table
                * `if_exists="replace"` Replaces the table with the new data
                * `if_exists="fail"` Fails to upload the new data if the table exists

            chunk_size: CURRENTLY NOT SUPPORTED
        """
        super().__init__(name=name,
                         seconds=seconds,
                         path=path,
                         fmt=fmt,
                         drop_duplicate_exclude=drop_duplicate_exclude,
                         move_shards_path=move_shards_path,
                         move_composed_path=move_composed_path)
        self.stream = Stream2Bigquery(name=name,
                                      auth_path=auth_path,
                                      pid=pid,
                                      did=did,
                                      tid=tid,
                                      location=location,
                                      if_exists=if_exists,
                                      chunk_size=chunk_size,
                                      store=True
                                      )
        self.store_str = (
            "storage files composed, attempting to store {} "
            f"rows to {self.stream.pid}.{self.stream.did}.{self.stream.tid}"
        )
