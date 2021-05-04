# -*- coding: utf-8 -*-
"""
Contains the Stream2Omnisci class
==================================

WARNING:

    Due to dependency conflicts and issues with the current published branch of the pymapd library the following steps
    must be taken to support streaming/storing data to Omnisci

    ::

        > pip install pymapd==0.25.0

        > pip install pandas --upgrade

        > pip install pyarrow --upgrade

    This occurs because of issues with the dependencies of the pymapd library being locked in place.
    I've opened an issue on this, and they appear to be working on it. The most recent publish seemed to break other
    things. Until this gets fixed this is a simple work-around. This will allow api2db to work with Omnisci, however
    there may be issues with attempts to utilize features of the pymapd library outside of what api2db uses,
    so use with caution. --Tristen

"""
from .stream import Stream
from ..app.log import get_logger
from ..app.auth_manager import auth_manage
from pymapd import connect
from pymapd import Connection
import pandas as pd
import time
from typing import Optional, Union


class Stream2Omnisci(Stream):
    """Streams data from the associated collector directly to Omnisci in real-time"""

    def __init__(self,
                 name: str,
                 db_name,
                 username: Optional[str]=None,
                 password: Optional[str]=None,
                 host: Optional[str]=None,
                 auth_path: Optional[str]=None,
                 protocol: str="binary",
                 chunk_size: int=0,
                 store: bool=False
                 ):
        """
        Creates a Stream2Omnisci object and attempts to build its dtypes

        If dtypes can successfully be created I.e. Data arrives from the API for the first time the following occurs:

            * Auto-generates the table schema
            * Casts all `string` columns to `categories` as required by Omnisci
            * Creates a Omnisci table with the name ``collector_name``\_stream

        NOTE:

            Data-attribute fields will have a ``_t`` appended to them due to naming conflicts encountered.

            Example:

            =  =  =
            A  B  C
            =  =  =
            1  2  3
            4  5  6
            =  =  =

            Will become the following in the Omnisci database. (This is only applied to data in the database)

            ===  ===  ===
            A_t  B_t  C_t
            ===  ===  ===
              1    2    3
              4    5    6
            ===  ===  ===

        **Authentication Methods:**

            * Supply ``auth_path`` with a path to an authentication file. Templates for these files can be found in
              your projects `AUTH/` directory

            **OR**

            * Supply the ``username``, ``host``, and ``password``

        Args:
            name: The name of the collector associated with the stream
            db_name: The name of the database to connect to
            username: The username to authenticate with the database
            password: The password to authenticate with the database
            host: The host of the database
            auth_path: The path to the authentication credentials.
            protocol: The protocol to use when connecting to the database
            chunk_size: CURRENTLY NOT SUPPORTED
            store: True if the super class is a Store object, otherwise False

        Raises:
            ValueError: If ``auth_path`` is provided but is invalid or has incorrect values
            ValueError: If ``auth_path`` is not provided and ``username``, ``password`` or ``host`` is missing
        """
        super().__init__(name=name,
                         chunk_size=chunk_size,
                         stream_type="omnisci",
                         store=store
                         )
        if auth_path is not None:
            auth = auth_manage(auth_path)
            if auth is None:
                raise ValueError("Authentication file invalid")
            try:
                self.username = auth["username"]
                self.password = auth["password"]
                self.host = auth["host"]
            except KeyError:
                raise ValueError("Provided authentication file missing details. Must include username, password, host")
        else:
            if username is None or password is None or host is None:
                raise ValueError("Must provide a username, password, and host or valid path to an authentication file")
            self.username = username
            self.password = password
            self.host = host
        self.db_name = db_name
        self.protocol = protocol
        self.con = None
        """Optional[pymapd.Connection]: The connection to the database"""
        self.connected = lambda con: False if con is None else not con.closed
        """Callable[Optional[pymapd.Connection], bool]: returns True if connection is established else False"""
        self.log_str = f"{self.stream_type}://{self.host}.{self.db_name}.{self.name}_stream"
        """str: A string used for logging"""

    def connect(self) -> Union[Connection, None]:
        """
        Attempts to establish a connection to a omnisci database

        Returns:
            A connection object if a connection can be established, else None
        """
        logger = get_logger()
        try:
            con = connect(user=self.username,
                          password=self.password,
                          host=self.host,
                          dbname=self.db_name,
                          protocol=self.protocol)
            logger.info(f"connection established {self.log_str}")
            return con
        except Exception as e:
            logger.exception(e)
            logger.warning(f"connection failed {self.log_str}... retrying")
            return

    @staticmethod
    def cast_categorical(data: pd.DataFrame, dtypes: pd.Series) -> pd.DataFrame:
        """
        Casts all columns with type ``str`` to type ``category`` as required by omnisci and appends a ``_t``
        to column names

        Args:
            data: The DataFrame that will be stored into the omnisci database
            dtypes: The dtypes of the DataFrame

        Returns:
            Modified DataFrame
        """
        if dtypes is not None:
            dtypes = dtypes.apply(lambda x: x.name).to_dict()
            name_dict = {}
            for k, v in dtypes.items():
                if v == "string":
                    data[k] = data[k].fillna("").astype("category")
                name_dict[k] = f"{k}_t"
            data = data.rename(columns=name_dict)
        return data

    def stream(self, data, retry_depth=5):
        """
        Attempts to store the incoming data into omnisci

        **Workflow**

            1. If authentication has not been performed, call `self.connect()`
            2. Attempt to store the DataFrame to omnisci

                * If successful, check to see if any previous uploads have failed and attempt to store those as well

            3. If the DataFrame cannot be successfully stored set the con to None
            4. If the retry_depth is not 0 perform a recursive call attempting to store the data again
            5. If the retry_depth has reached zero, log an exception and store the DataFrame locally

        Failed uploads will be stored in

            * STORE/upload_failed/**collector_name**/omnisci/**timestamp_ns**.parquet

        Args:
            data: The DataFrame that should be stored to omnisci
            retry_depth: Used for a recursive call counter should the DataFrame fail to be stored

        Returns:
            None
        """
        logger = get_logger()
        if not self.connected(self.con):
            logger.info(f"establishing connection to {self.log_str}")
            self.con = self.connect()
        try:
            df = Stream2Omnisci.cast_categorical(data, self.dtypes)
            self.con.load_table(f"{self.name}_stream", df)
            logger.debug(f"{len(data)} rows inserted into {self.log_str}")
            self.check_failures()
        except Exception as e:
            logger.exception(e)
            self.con = None
            if retry_depth != 0:
                logger.warning(
                    f"failed to upload {len(data)} rows to {self.log_str} will retry {retry_depth} more times"
                )
                self.stream(data, retry_depth - 1)
            else:
                logger.error((f"failed to upload {len(data)} rows to {self.log_str}\n"
                              f"storing locally to upload when connection is re-established")
                             )
                ts = int(time.time() * 1000)
                failure_path = f"STORE/upload_failed/{self.name}/{self.stream_type}/{ts}.parquet"
                self.static_store_df(df=data, path=failure_path, fmt="parquet")
