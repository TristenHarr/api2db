# -*- coding: utf-8 -*-
"""
Contains the auth_manage function
=================================
"""
import json
from .log import get_logger
import os
from typing import Union


def auth_manage(path: str) -> Union[dict, None]:
    """
    Loads authentication credentials from the specified path

    Args:
        path: The path where the authentication file resides

    Returns:
        Authentication credentials if file successfully loaded, None otherwise
    """
    res = None
    if os.path.isfile(path):
        try:
            with open(path, "r") as f:
                res = json.load(f)
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
    return res
