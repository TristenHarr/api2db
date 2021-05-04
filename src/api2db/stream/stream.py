# -*- coding: utf-8 -*-
"""
Contains the Stream class
================================

Todo:
    * Add support for sharded uploads -> See chunk_size

"""
from .file_converter import FileConverter
from ..app.log import get_logger
from threading import Thread
from threading import Lock as ThreadLock
from queue import Queue as ThreadQueue
import pandas as pd
import time
import os
from typing import Optional


class Stream(FileConverter):
    """
    Used for streaming data into a local or external source
    """

    def __init__(self,
                 name: str,
                 path: Optional[str]=None,
                 dtypes: Optional[dict]=None,
                 fmt: Optional[str]=None,
                 chunk_size: int=0,
                 stream_type: str="stream",
                 store: bool=False
                 ):
        """
        Creates a Stream object and attempts to build its dtypes.
        If store flag is false, spawns a thread that polls the Stream queue for incoming data

        Args:
            name: The name of the collector the stream is associated with
            path: The directory path the stream should store to (Usage dictated by super classes)
            dtypes: A dictionary containing the dtypes that the stream data DataFrame has
            fmt: The file format that the stream data should be stored as

                * `fmt="parquet"` (recommended) stores the DataFrame using parquet format
                * `fmt="json"` stores the DataFrame using JSON format
                * `fmt="pickle"` stores the DataFrame using pickle format
                * `fmt="csv"` stores the DataFrame using csv format

            chunk_size: The size of chunks to send to the stream target. I.e. Insert data in chunks of chunk_size rows
            stream_type: The type of the stream (Primarily used for logging)
            store: This flag indicates whether or not the stream is being called by a Store object

        Raises:
            NotImplementedError: chunk_storage is not yet implemented.
        """
        super().__init__(name=name, dtypes=dtypes, path=path, fmt=fmt)
        if chunk_size != 0:
            raise NotImplementedError("Chunk storage not yet implemented")
        # FIXME: Implement support for sharded uploads
        self.chunk_size = 0
        self.stream_type = stream_type
        self.is_store_instance = store
        """bool: True if the super-class has base-class Store otherwise False"""
        # If the superclass is a Store instance, do not create a lock/queue
        if store:
            return
        self.lock = ThreadLock()
        """threading.Lock: Stream Lock used to signal if the stream has died"""
        self.q = ThreadQueue()
        """queue.Queue: Stream queue used to pass data into"""

    def start(self) -> None:
        """
        Starts the stream running loop in a new thread

        Returns:
            None
        """
        t = Thread(target=self.stream_start)
        t.start()

    def check_failures(self) -> None:
        """
        Checks to see if previous uploads have failed and if so, loads the previous upload data and attempts to upload
        it again.

        This method searches the directory path

            STORE/upload_failed/**collector_name**/**stream_type**/

        This path is the target location for failed uploads. If an upload fails 5 times in a row, it is stored in this
        location with the filename being the timestamp it is stored.

        Returns:
            None
        """
        # If the dtypes do not exist return
        if self.dtypes is None:
            return
        # If the upload_failed path does not exist return
        if not os.path.isdir(f"STORE/upload_failed/{self.name}/{self.stream_type}/"):
            return
        # Get the list of failed uploads
        failed_list = os.listdir(f"STORE/upload_failed/{self.name}/{self.stream_type}/")
        # If there are no failed uploads return
        if len(failed_list) == 0:
            return
        logger = get_logger()
        logger.info((f"uploading {len(failed_list)} failed upload files found in "
                     "STORE/upload_failed/{self.name}/{self.stream_type}/")
                    )
        # Compose all the failed uploads into a single DataFrame
        df = self.static_compose_df_from_dir(path=f"STORE/upload_failed/{self.name}/{self.stream_type}/",
                                             fmt="parquet")
        # If the DataFrame is None return
        if df is None:
            return
        # Attempt to case the DataFrame to its expected types
        try:
            df = df.astype(self.dtypes)
        except Exception as e:
            logger.exception(e)
            return
        if self.is_store_instance:
            self.stream(df)
        else:
            self.q.put(df)

    def stream_start(self) -> None:
        """
        Starts the stream listener that polls the stream queue for incoming data.

        Returns:
            None
        """
        logger = get_logger()
        logger.info(f"stream starting -> ({self.stream_type})")
        # Acquire the stream lock on initial startup
        self.lock.acquire()
        # Set the running flag to true
        running = True
        while running:
            # If the lock can be acquired I.e. Another stream has gone down, and streams must be reset
            if self.lock.acquire(False):
                # Release the lock
                self.lock.release()
                logger.info(f"stream restarting -> ({self.stream_type})")
                # Set running to False
                running = False

            # Get all data from the queue
            while not self.q.empty():
                data = self.q.get()
                if data is not None:
                    # Push all data to its stream target
                    self.stream(data)
            else:
                time.sleep(1)

    def stream(self, data: pd.DataFrame) -> AttributeError:
        """
        Overridden by supers, a Stream object is NEVER directly used to stream data. It is ALWAYS inherited from

        Args:
            data: The data to stream

        Raises:
            AttributeError: `Stream` does not have the ability to stream data. It must be subclassed.
        """
        raise AttributeError("'Stream' object has no attribute 'stream'")
