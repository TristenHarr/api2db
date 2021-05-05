# -*- coding: utf-8 -*-
"""
Contains the Store2Sql class
==================================
"""
from ..store.store import Store
from ..stream.stream2sql import Stream2Sql
from typing import Optional, List


class Store2Sql(Store):
    """Used for storing data to an SQL database periodically"""

    def __init__(self,
                 name: str,
                 seconds: int,
                 db_name: str,
                 dialect: str,
                 username: Optional[str]=None,
                 password: Optional[str]=None,
                 host: Optional[str]=None,
                 auth_path: Optional[str]=None,
                 port: str="",
                 path: Optional[str]=None,
                 fmt: str="parquet",
                 drop_duplicate_exclude: Optional[List[str]]=None,
                 move_shards_path: Optional[str]=None,
                 move_composed_path: Optional[str]=None,
                 if_exists: str="append",
                 chunk_size: int=0,
                 ):
        """
        Creates a Store2Sql object and attempts to build its dtypes.

        NOTE:

            :py:meth:`See documentation for Stream2Sql <api2db.stream.stream2sql.Stream2Sql.__init__>`

        Args:
            name: The name of the collector the store is associated with
            seconds: The number of seconds between storage cycles
            db_name: The name of the database to connect to
            dialect:

                * `dialect="mysql"` -> Use this to connect to a mysql database
                * `dialect="mariadb"` -> Use this to connect to a mariadb database
                * `dialect="postgresql"` -> Use this to connect to a postgresql database
                * `dialect="amazon_aurora"` -> COMING SOON
                * `dialect="oracle"` -> COMING SOON
                * `dialect="microsoft_sql"` -> COMING SOON
                * `dialect="Something else?"` -> Submit a feature request... or even better build it!

            username: The username to authenticate with the database
            password: The password to authenticate with the database
            host: The host of the database
            auth_path: The path to the authentication credentials.
            port: The port to connect to the database with
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

            chunk_size: CURRENTLY NOT SUPPORTED
        """
        super().__init__(name=name,
                         seconds=seconds,
                         path=path,
                         fmt=fmt,
                         drop_duplicate_exclude=drop_duplicate_exclude,
                         move_shards_path=move_shards_path,
                         move_composed_path=move_composed_path)
        self.stream = Stream2Sql(name=name,
                                 db_name=db_name,
                                 dialect=dialect,
                                 username=username,
                                 password=password,
                                 host=host,
                                 auth_path=auth_path,
                                 port=port,
                                 if_exists=if_exists,
                                 chunk_size=chunk_size,
                                 store=True)
        self.store_str = (
            "storage files composed, attempting to store {} "
            f"rows to {self.stream.dialect}://{self.stream.host}/{self.stream.db_name}"
        )
