# -*- coding: utf-8 -*-
"""
Contains the Stream2Bigquery class
==================================
"""
from .stream import Stream
from ..app.log import get_logger
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, NotFound
from google.cloud.bigquery import SchemaField
import pandas as pd
import time
from typing import Union, List


class Stream2Bigquery(Stream):
    """Streams data from the associated collector directly to Bigquery in real-time"""

    def __init__(self,
                 name: str,
                 auth_path: str,
                 pid: str,
                 did: str,
                 tid: str,
                 location: str="US",
                 if_exists: str="append",
                 chunk_size: int=0,
                 store: bool=False
                 ):
        """
        Creates a Stream2Bigquery object and attempts to build its dtypes.

        If dtypes can successfully be created I.e. Data arrives from the API for the first time the following occurs:

            * Auto-generates the table schema
            * Creates the dataset if it does not exist within the project
            * Creates the table if it does not exist within the project

        Args:
            name: The name of the collector associated with the stream
            auth_path: The path to the Google provided authentication file. I.e. AUTH/google_auth_file.json
            pid: Google project ID
            did: Google dataset ID
            tid: Google table ID
            location: Location of the Bigquery project
            if_exists:

                * `if_exists="append"` Adds the data to the table
                * `if_exists="replace"` Replaces the table with the new data
                * `if_exists="fail"` Fails to upload the new data if the table exists

            chunk_size: CURRENTLY NOT SUPPORTED
            store: True if the super class is a Store object, otherwise False
        """
        super().__init__(name=name,
                         chunk_size=chunk_size,
                         stream_type="bigquery",
                         store=store
                         )
        self.auth_path = auth_path
        self.pid = pid
        self.did = did
        self.tid = tid
        self.location = location
        self.if_exists = if_exists
        self.schema = self.build_schema()
        """Optional[List[google.cloud.bigquery.SchemaField]]: contains schema if dtypes exist else None"""
        self.bq_schema = self.build_bq_schema()
        """Optional[List[dict]]: contains bq_schema if dtypes exist else None"""
        self.cred = None
        """google.oauth2.service_account.Credentials: contains the credentials used to authenticate with bigquery"""
        self.client = None
        """google.cloud.bigquery.Client: The bigquery client"""
        self.dataset = None
        """google.cloud.bigquery.Dataset: The dataset associated with the collector"""
        self.table = None
        """google.cloud.bigquery.Table: The table associated with the collector"""
        self.connected = False
        """bool: True if a connection has been established I.e. credentials have been authenticated, otherwise False"""

    def connect(self) -> bool:
        """
        Attempts to authenticate with provided credentials.

        **Workflow**

            1. Load the credentials from the service account file
            2. Instantiate the bigquery Client
            3. Attempt to create the dataset and if a Conflict exception is thrown then load the dataset
            4. Attempt to load the table and if a NotFound exception is thrown then create the table

        Returns:
            True if the table is successfully loaded/created otherwise False
        """
        logger = get_logger()
        # If the schema exists
        if self.schema is not None:
            self.cred = service_account.Credentials.from_service_account_file(self.auth_path)
            self.client = bigquery.Client(credentials=self.cred, project=self.pid)
            self.dataset = bigquery.Dataset(f"{self.pid}.{self.did}")
            self.dataset.location = self.location
            # Try to create the dataset and if it already exists then load it
            try:
                self.client.create_dataset(self.dataset, timeout=60)
                logger.info(f"dataset not found {self.pid}.{self.did}... creating dataset")
            except Conflict:
                logger.info(f"dataset loaded {self.pid}.{self.did}")
            # Try to load the table and if it does not exist then create it
            try:
                self.table = self.client.get_table(f"{self.pid}.{self.did}.{self.tid}")
                logger.info(f"table loaded {self.pid}.{self.did}.{self.tid}")
            except NotFound:
                logger.info(f"table not found {self.pid}.{self.did}.{self.tid}... creating table")
                table = bigquery.Table(f"{self.pid}.{self.did}.{self.tid}", schema=self.schema)
                self.table = self.client.create_table(table)
            logger.info(f"connection established {self.pid}.{self.did}.{self.tid}")
            return True
        logger.warning(f"connection failed {self.pid}.{self.did}.{self.tid}... retrying")
        return False

    def stream(self, data: pd.DataFrame, retry_depth: int=5) -> None:
        """
        Attempts to store the incoming data into bigquery

        **Workflow**

            1. If authentication has not been performed, call `self.connect()`
            2. Attempt to store the DataFrame to bigquery

                * If successful, check to see if any previous uploads have failed and attempt to store those as well

            3. If the DataFrame cannot be successfully stored set the connection to False
            4. If the retry_depth is not 0 perform a recursive call attempting to store the data again
            5. If the retry_depth has reached zero, log an exception and store the DataFrame locally

        Failed uploads will be stored in

            * STORE/upload_failed/**collector_name**/bigquery/**timestamp_ns**.parquet

        Args:
            data: The DataFrame that should be stored to bigquery
            retry_depth: Used for a recursive call counter should the DataFrame fail to be stored

        Returns:
            None
        """
        logger = get_logger()
        # If authentication has not been performed, authenticate via self.connect
        if not self.connected:
            logger.info(f"establishing connection to {self.pid}.{self.did}.{self.tid}")
            self.connected = self.connect()
        # Attempt to store the DataFrame to bigquery
        try:
            data.to_gbq(f"{self.did}.{self.tid}",
                        project_id=self.pid,
                        credentials=self.cred,
                        table_schema=self.bq_schema,
                        if_exists="append")
            logger.debug(f"{len(data)} rows inserted into {self.pid}.{self.did}.{self.tid}")
            # Knowing that data was successfully uploaded, check to see if any previous uploads failed and attempt them
            self.check_failures()
        # If storage fails, call recursively 5 times, before storing data locally to be uploaded upon reconnection
        except Exception as e:
            logger.exception(e)
            self.connected = False
            if retry_depth != 0:
                logger.warning(f"failed to upload {len(data)} rows to {self.pid}.{self.did}.{self.tid} "
                               f"will retry {retry_depth} more times")
                self.stream(data, retry_depth - 1)
            else:
                e_str = (f"failed to upload {len(data)} rows to {self.pid}.{self.did}.{self.tid}\n"
                         f"storing locally to upload when connection is re-established"
                         )
                logger.error(e_str)
                ts = int(time.time() * 1000)
                failure_path = f"STORE/upload_failed/{self.name}/{self.stream_type}/{ts}.parquet"
                self.static_store_df(df=data, path=failure_path, fmt="parquet")

    def build_schema(self) -> Union[List[SchemaField], None]:
        """
        Attempts to build the schema that will be used for table creation

        Iterates through the dtypes items and generate the appropriate SchemaFields

        Returns:
            The schema generated if successful otherwise None
        """
        schema = None
        if self.dtypes is not None:
            schema = []
            dtypes = self.dtypes.apply(lambda x: x.name).to_dict()
            for key, value in dtypes.items():
                if value == "string":
                    schema.append(SchemaField(key, "STRING"))
                elif value == "bool":
                    schema.append(SchemaField(key, "BOOL"))
                elif value == "Int64" or value == "int64":
                    schema.append(SchemaField(key, "INTEGER"))
                elif value == "float64":
                    schema.append(SchemaField(key, "FLOAT"))
                elif value == "datetime64[ns]":
                    schema.append(SchemaField(key, "DATETIME"))
        return schema

    def build_bq_schema(self) -> Union[List[dict], None]:
        """
        Attempts to build the schema that will be used to upload data to bigquery via DataFrame.to_gbq()

        Iterates through the dtypes items and generate the appropriate schema dictionary

        Returns:
            The schema generated if successful otherwise None
        """
        schema = None
        if self.dtypes is not None:
            schema = []
            dtypes = self.dtypes.apply(lambda x: x.name).to_dict()
            for key, value in dtypes.items():
                if value == "string":
                    schema.append({"name": key, "type": "STRING"})
                elif value == "bool":
                    schema.append({"name": key, "type": "BOOL"})
                elif value == "Int64" or value == "int64":
                    schema.append({"name": key, "type": "INTEGER"})
                elif value == "float64":
                    schema.append({"name": key, "type": "FLOAT"})
                elif value == "datetime64[ns]":
                    schema.append({"name": key, "type": "DATETIME"})
        return schema
