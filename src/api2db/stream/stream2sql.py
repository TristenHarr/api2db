# -*- coding: utf-8 -*-
"""
Contains the Stream2Sql class
==================================
"""
from .stream import Stream
from ..app.log import get_logger
from ..app.auth_manager import auth_manage
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import time
from typing import Optional


class Stream2Sql(Stream):
    """Streams data from the associated collector directly to an SQL database target in real-time"""

    def __init__(self,
                 name: str,
                 db_name: str,
                 dialect: str,
                 username: Optional[str]=None,
                 password: Optional[str]=None,
                 host: Optional[str]=None,
                 auth_path: Optional[str]=None,
                 port: str="",
                 if_exists: str="append",
                 chunk_size: int=0,
                 store: bool=False
                 ):
        """
        Creates a Stream2Sql object and attempts to build its dtypes

        If dtypes can successfully be created I.e. Data arrives from the API for the first time the following occurs:

            * Auto-generates the table schema
            * Creates the table in the database if it does not exist

        **Authentication Methods:**

            * Supply ``auth_path`` with a path to an authentication file. Templates for these files can be found in
              your projects `AUTH/` directory

            **OR**

            * Supply the ``username``, ``host``, and ``password``

        Args:
            name: The name of the collector associated with the stream
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
            port: The port used when establishing a connection to the database
            if_exists:

                * `if_exists="append"` Adds the data to the table
                * `if_exists="replace"` Replaces the table with the new data
                * `if_exists="fail"` Fails to upload the new data if the table exists

            chunk_size: CURRENTLY NOT SUPPORTED
            store: True if the super class is a Store object, otherwise False

        Raises:
            ValueError: If ``auth_path`` is provided but is invalid or has incorrect values
            ValueError: If ``auth_path`` is not provided and ``username``, ``password`` or ``host`` is missing
        """
        super().__init__(name=name,
                         chunk_size=chunk_size,
                         stream_type=f"sql.{dialect}",
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
        self.dialect = dialect
        self.port = port
        self.if_exists = if_exists
        self.driver = None
        """str: The driver to use when connecting with SQLAlchemy"""
        self.engine_str = None
        """str: The full string that is used with sqlalchemy.create_engine"""
        self.log_str = None
        """str: A string used for logging"""
        self.con = None
        """sqlalchemy.engine.Engine: The connection to the database"""
        self.connected = False
        """bool: True if connection is established otherwise False"""
        self.load()

    def load(self) -> None:
        """
        Loads the driver and creates the engine string and the log string

        Raises:
            NotImplementedError: Support for amazon_aurora has not been implemented in api2pandas yet
            NotImplementedError: Support for oracle has not been implemented in api2db yet
            NotImplementedError: Support for microsoft_sql has not been implemented in api2db yet

        Returns:
            None
        """
        if self.dialect == "mysql":
            self.driver = "pymysql"
        elif self.dialect == "mariadb":
            self.driver = "mariadbconnector"
        elif self.dialect == "postgresql":
            self.driver = "psycopg2"
        elif self.dialect == "amazon_aurora":
            raise NotImplementedError("Support for amazon_aurora has not been implemented in api2db yet")
        elif self.dialect == "oracle":
            raise NotImplementedError("Support for oracle has not been implemented in api2db yet")
        elif self.dialect == "microsoft_sql":
            raise NotImplementedError("Support for microsoft_sql has not been implemented in api2db yet")
        if self.driver is None:
            return
        self.engine_str = (f"{self.dialect}+"
                           f"{self.driver}://"
                           f"{self.username}:"
                           f"{self.password}@"
                           f"{self.host}"
                           f"{'' if self.port is None else ':'+str(self.port)}/"
                           f"{self.db_name}")
        self.log_str = (f"{self.dialect}://"
                        f"{self.host}/"
                        f"{self.db_name}")

    def connect(self) -> bool:
        """
        Attempts to establish a connection to the database

        Returns:
            True if the connection is established otherwise False
        """
        logger = get_logger()
        try:
            self.con = create_engine(self.engine_str)
            if not database_exists(self.con.url):
                logger.info(f"database not found {self.log_str}... creating database")
                create_database(self.con.url)
            else:
                logger.info(f"loading database {self.log_str}")

                self.con.connect()
            logger.info(f"connection established {self.log_str}")
            return True
        except Exception as e:
            logger.exception(e)
            logger.warning(f"connection failed {self.log_str}... retrying")
            return False

    def stream(self, data, retry_depth=5):
        """
        Attempts to store the incoming data into the SQL database

        **Workflow**

            1. If authentication has not been performed, call `self.connect()`
            2. Attempt to store the DataFrame to the database

                * If successful, check to see if any previous uploads have failed and attempt to store those as well

            3. If the DataFrame cannot be successfully stored set the connected to False
            4. If the retry_depth is not 0 perform a recursive call attempting to store the data again
            5. If the retry_depth has reached zero, log an exception and store the DataFrame locally

        Failed uploads will be stored in

            * STORE/upload_failed/**collector_name**/sql.**dialect**/**timestamp_ns**.parquet

        Args:
            data: The DataFrame that should be stored to the database
            retry_depth: Used for a recursive call counter should the DataFrame fail to be stored

        Returns:
            None
        """
        logger = get_logger()
        if not self.connected:
            logger.info(f"establishing connection to {self.log_str}")
            self.connected = self.connect()
        try:
            data.to_sql(name=f"{self.name}", con=self.con, if_exists=self.if_exists, index=False)
            logger.debug(f"{len(data)} rows inserted into {self.log_str}")
            self.check_failures()
        except Exception as e:
            logger.exception(e)
            self.connected = False
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
