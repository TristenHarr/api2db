# -*- coding: utf-8 -*-
"""
Contains the Run class
=========================
"""
from ..app.api2db import Api2Db
from ..ingest.collector import Collector
from multiprocessing import Queue
from typing import List
import logging
from logging import StreamHandler, Formatter
from logging.handlers import QueueListener


class Run(object):
    """Serves as the main entry point for the application"""

    def __init__(self, collectors: List[Collector]):
        """
        The Run object is the application entry point

        Args:
            collectors: A list of collector objects to collect data for
        """
        self.collectors = collectors
        self.q = Queue()
        """multiprocessing.Queue: Used for message passing for collectors with debug mode enabled"""

    def run(self):
        """
        Starts the application

        Returns:
            None
        """
        self.multiprocessing_start()

    def multiprocessing_start(self):
        """
        Starts each collector in it's own process

        Returns:
            None
        """
        debug_mode = False
        # For each collector
        for c in self.collectors:
            # If the collector is enabled I.e. should run an import with frequency higher than 0
            if c.seconds != 0:
                # If collector is running in debug mode, pass it the message queue
                if c.debug:
                    c.set_q(self.q)
                    debug_mode = True
                # Create an Api2Db instance passing the collector as parameter
                api2db = Api2Db(c)
                # Start the collector by calling the Api2Db wrap_start method
                api2db.wrap_start()
        if debug_mode:
            print("All collector processes started. Running in development mode.")
            formatter = Formatter(fmt="Pid: %(process)-6d Tid: %(thread)-6d %(asctime)s %(levelname)-7s %(message)s",
                                  datefmt="%Y-%m-%d %I:%H:%M")
            handler = StreamHandler()
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)
            listener = QueueListener(self.q, handler)
            listener.start()
        else:
            print("All collector processes started. Running in production mode.")
