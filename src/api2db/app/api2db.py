# -*- coding: utf-8 -*-
"""
Contains the Api2Db class
=========================
"""
from ..ingest.api2pandas import Api2Pandas
from .log import get_logger
from ..ingest.collector import Collector
from ..ingest.api_form import ApiForm
from ..store.store import Store
import schedule
from schedule import CancelJob
from multiprocessing import Process
from threading import Thread
from queue import Queue as ThreadQueue
from threading import Lock as ThreadLock
import time
import os
import pickle
from typing import Callable, List, Union

DEV_SHRINK_DATA = 0
"""int: Library developer setting to shrink incoming data to the first DEV_SHRINK_DATA rows"""


class Api2Db(object):
    """Performs data import, passes data to streams, and schedules data storage"""

    def __init__(self, collector: Collector):
        """
        Creates a Api2Db object and attaches the collector

        Args:
            collector: The collector object to attach to

        """
        self.collector = collector

    def wrap_start(self) -> Process:
        """
        Starts the running loop of an Api2Db instance in a spawned process

        Returns:
            The process spawned with target start
        """
        p = Process(target=self.start)
        p.start()
        return p

    def start(self) -> None:
        """
        The target for Api2Db main process running loop

        Returns:
            None
        """
        # Create an initial instance of the logger which will be named using the process's pid
        logger = get_logger(filename=self.collector.name, q=self.collector.q)
        # Perform job scheduling
        self.schedule()
        while True:
            schedule.run_pending()
            # Get the tags for all scheduled jobs
            tags = [next(iter(j.tags)) for j in schedule.jobs]
            tag = self.collector.name
            # If the job for the main collector has ended, reschedule it
            if tag not in tags:
                self.schedule()
                logger.info(f"inactive collector restarted: {tag}")
            time.sleep(1)

    def schedule(self) -> None:
        """
        schedule starts the streams, schedules collector refresh, schedules storage refresh

        Return:
            None

        Raises:
            NameError or ModuleNotFoundError if streams cannot be created
        """
        # Get the logger via get_logger() which will get the logger for the current process pid
        logger = get_logger()
        # Instantiate the stream objects. (Performed here because streams establish persistent external connections)
        try:
            streams = self.collector.streams()
        except NameError as e:
            raise Api2Db.import_handle(e)
        stream_qs = [stream.q for stream in streams]
        stream_locks = [stream.lock for stream in streams]
        freq = self.collector.seconds
        name = self.collector.name
        # Start each stream
        for stream in streams:
            stream.start()
        logger.info(f"import scheduled: [{freq} seconds] (api request data) -> (streams)")
        # Schedule the collector refresh
        schedule.every(freq).seconds.do(lambda:
                                        Api2Db.collect_wrap(
                                            self.collector.import_target,
                                            self.collector.api_form,
                                            stream_qs,
                                            stream_locks)
                                        ).tag(name)

        tags = [next(iter(j.tags)) for j in schedule.jobs]
        tag = f"{name}.refresh"
        # If storage refresh is not running
        if tag not in tags:
            logger.info(f"storage refresh scheduled: [{freq} seconds] -> (check stores)")
            # Schedule the storage refresh
            schedule.every(freq).seconds.do(lambda:
                                            Api2Db.store_wrap(self.collector.stores)
                                            ).tag(tag)
        else:
            logger.info(f"storage refresh already running:\n\t[{freq} seconds] ({name}) -> (skipping)")

    @staticmethod
    def collect_wrap(import_target: Callable[[], Union[List[dict], None]],
                     api_form: Callable[[], ApiForm],
                     stream_qs: List[ThreadQueue],
                     stream_locks: List[ThreadLock]
                     ) -> Union[type(CancelJob), None]:
        """
        Starts/restarts dead streams, and calls method collect to import data

        Args:
            import_target: Function that returns data imported from an Api
            api_form: Function that instantiates and returns an ApiForm object
            stream_qs: A list of queues to pass the incoming data into to be handled by stream targets
            stream_locks: A list of locks that become acquirable if their respective stream has died

        Returns:
            CancelJob if stream has died, restarting the streams, None otherwise
        """
        stream_died = False
        # Check to see if each stream lock can be acquired
        for lock in stream_locks:
            if lock.acquire(False):
                # If the lock can be acquired, then the stream has died
                stream_died = True
                break

        # If one of the streams has died
        if stream_died:
            # Signal to all streams that they must be restarted by releasing the lock
            for lock in stream_locks:
                try:
                    lock.release()
                except ValueError:
                    pass
            # Tell the scheduler to cancel the job
            return CancelJob

        # Spawn a thread with target collect
        t = Thread(target=Api2Db.collect, args=(import_target, api_form, stream_qs,))
        # Start the thread
        t.start()

    @staticmethod
    def collect(import_target: Callable[[], Union[List[dict], None]],
                api_form: Callable[[], ApiForm],
                stream_qs: List[ThreadQueue]
                ) -> None:
        """
        Performs a data-import, cleans the data, and sends the data into
        its stream queues

        Args:
            import_target: Function that returns data imported from an Api
            api_form: Function that instantiates and returns an ApiForm object
            stream_qs: A list of queues to pass the incoming data into to be handled by stream targets

        Returns:
            None
        """
        logger = get_logger()
        # Create an instance of an Api2Pandas object passing the api_form constructor function
        api2pandas = Api2Pandas(api_form)
        if not api2pandas.dependencies_satisfied():
            return
        # Import the data
        data = import_target()
        if data is None or type(data) is not list:
            return
        # For each data point
        for data_point in data:
            if data_point is None:
                return
            # Clean the data and extract it into a Pandas DataFrame
            df = api2pandas.extract(data_point)
            if df is None:
                return
            # DEV OPTION -> Allows data to be shrunk during development of library!
            if DEV_SHRINK_DATA != 0:
                df = df.head(DEV_SHRINK_DATA)
            dtypes_path = os.path.join("CACHE", f"{api2pandas.api_form.name}_dtypes.pkl")
            # If the dtypes file is not created, create a dtypes file
            if not os.path.isfile(dtypes_path):
                logger.info(f"no dtypes found -> making dtypes...")
                with open(dtypes_path, "wb") as f:
                    pickle.dump(df.dtypes, f)
            # Place the Pandas DataFrame into each stream queue
            for q in stream_qs:
                q.put(df)

    @staticmethod
    def store_wrap(stores: Callable[[], List[Store]]) -> None:
        """
        Checks to ensure that storage jobs are scheduled to run and schedules any
        jobs that have been unscheduled

        Args:
            stores: Function that returns a list of Store subclassed objects

        Returns:
            None

        Raises:
            NameError or ModuleNotFoundError if stores cannot be created
        """
        logger = get_logger()
        # Create a list of Store subclasses to perform data storage on
        try:
            stores = stores()
        except NameError as e:
            raise Api2Db.import_handle(e)
        tags = [next(iter(j.tags)) for j in schedule.jobs]
        # For each store
        for store in stores:
            tag = f"{store.name}.{store.path}"
            dtypes_path = os.path.join("CACHE", f"{store.name}_dtypes.pkl")
            # If the storage job is not scheduled
            if tag not in tags and os.path.isfile(dtypes_path):
                logger.info(f"storage scheduled: [{store.seconds} seconds] ({store.path}) -> (store)")
                # Schedule the storage job
                schedule.every(store.seconds).seconds.do(lambda: Api2Db.store(store)).tag(tag)
            elif not os.path.isfile(dtypes_path):
                logger.info(f"dtypes file not found at {dtypes_path}: waiting for data to arrive...")

    @staticmethod
    def store(store: Store) -> None:
        """
        Performs the data storage operation of a Store subclass

        Args:
            store: The Store to perform storage on

        Returns:
            None
        """
        # Spawn a thread to perform the storage operation
        t = Thread(target=store.store)
        t.start()

    @staticmethod
    def import_handle(e: Exception) -> Exception:
        """
        Handles import errors. Informs the user of libraries they need

        Args:
            e: The raised Exception

        Returns:
            ModuleNotFoundError if dependencies missing otherwise the original exception
        """
        e_str = None
        if "2Omnisci" in str(e):
            e_str = ("Storage to Omnisci has additional dependencies\n"
                     "Run the following in this order:\n"
                     "pip install pymapd==0.25.0\n"
                     "pip install pandas --upgrade\n"
                     "pip install pyarrow --upgrade\n"
                     )
        elif "2Sql" in str(e):
            e_str = ("Storage to SQL has additional dependencies\n"
                     "Run the following in this order:\n"
                     "pip install slqalchemy --upgrade\n"
                     "pip install sqlalchemy-utils --upgrade\n"
                     "For MariaDB support: pip install mariadb --upgrade\n"
                     "For MySQL support: pip install pymysql --upgrade\n"
                     "For PostgreSQL support: pip install psycopg2 --upgrade\n"
                     )
        elif "2Bigquery" in str(e):
            e_str = ("Storage to Bigquery has additional dependencies\n"
                     "Run the following in this order:\n"
                     "pip install google-cloud-bigquery --upgrade\n"
                     "pip install pandas-gbq --upgrade\n"
                     )
        if e_str is not None:
            return ModuleNotFoundError(e_str)
        return e
