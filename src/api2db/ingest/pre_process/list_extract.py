# -*- coding: utf-8 -*-
"""
Contains the ListExtract class
==============================

Summary of ListExtract Usage:
-----------------------------

::

    data = { "actual_data_rows": [{"id": "row1"}, {"id": "row2"}], "erroneous_data": "FooBar" }
    pre = ListExtract(lam=lambda x: x["actual_data_rows"])

Example Usage of ListExtract:
-----------------------------

>>> data = {
...    "Foo": "Metadata",
...    "data_array": [
...            {
...                "data_id": 1,
...                "name": "name_1"
...            },
...            {
...                "data_id": 2,
...                "name": "name_2"
...            }
...        ]
... }
...
... pre = ListExtract(lam=lambda x: x["data_array"])
... pre.lam_wrap(data)
[
    {
        "data_id": 1,
        "name": "name_1"
    },
    {
        "data_id": 2,
        "name": "name_2"
    }
]
"""
from .pre import Pre
from ...app.log import get_logger
from typing import Callable, Union, List


class ListExtract(Pre):
    """Used to extract a list of dictionaries that will each represent a single row in a database"""

    def __init__(self, lam: Callable[[dict], list]):
        """
        Creates a ListExtract object

        Args:
            lam: Anonymous function that attempts to extract a list of data that will become rows in a DataFrame
        """
        self.ctype = "list_extract"
        """str: type of data processor"""
        self.lam = lam
        self.dtype = list
        """type(list): the datatype performing `lam` should yield"""

    def lam_wrap(self, lam_arg: dict) -> Union[List[dict], None]:
        """
        Overrides super class method

        Workflow:

            1. Attempt to perform the ``lam`` operation on the incoming data
            2. Attempt to cast the result ``lam`` operation to a list

                * If an exception occurs, return None

            3. Return the list of data

        Args:
            lam_arg: A dictionary containing a list of dictionaries that will become the rows of a DataFrame

        Returns:
            A list of dictionaries that will become the rows of a DataFrame if successful otherwise None
        """
        try:
            res = self.dtype(self.lam(lam_arg))
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
            res = None
        return res
