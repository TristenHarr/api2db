# -*- coding: utf-8 -*-
"""
Contains the get_logger function
================================
"""
import logging
from logging import Logger
from logging.handlers import QueueHandler
import os
from multiprocessing import Queue
from typing import Optional


def get_logger(filename: Optional[str] = None, q: Optional[Queue] = None) -> Logger:
    """
    Retrieves the logger for the current process for logging to the log file

    If no filename is provided, the logger for the current process is assumed to already have
    handlers registered, and will be returned.

    If a filename is provided an the logger has no handlers, a handler will be created and registered

    Args:
        filename: The name of the file to log to
        q: The queue used to pass messages if the collector is running in debug mode

    Returns:
        A logger that can be used to log messages
    """
    # Get the logger for the current process id
    logger = logging.getLogger(str(os.getpid()))
    # If the logger does not have any handlers registered i.e. on collector start
    if not logger.hasHandlers() and filename is not None:
        # Create a handler and register it to the logger
        formatter = logging.Formatter(fmt="%(asctime)s %(filename)-25s %(levelname)-5s %(message)s",
                                      datefmt="%Y-%m-%d %I:%H:%M")
        handler = logging.FileHandler(f"LOGS/{filename}.log")
        handler.setFormatter(formatter)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        if q is not None:
            q_handler = QueueHandler(q)
            q_handler.setLevel(logging.DEBUG)
            logger.addHandler(q_handler)
    return logger
