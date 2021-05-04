# -*- coding: utf-8 -*-
"""
Contains the GlobalExtract class
================================

Summary of GlobalExtract usage:
-------------------------------

::

    data = {"date": "2021-04-19", "data_array": [{"id": 1, "name": "Foo"}, {"id": 2, "name": "Bar"}]}
    pre = GlobalExtract(key="publish_time",
                        lam=lambda x: x["date"],
                        dtype=str
                        )

**Final DataFrame**

==  ====  ============
id  name  publish_time
==  ====  ============
 1  Foo   2021-04-19
 2  Bar   2021-04-19
==  ====  ============

Example Usage of GlobalExtract:
-------------------------------

>>> # pre-processing operators
... pres = []
... # Dictionary that contains all globally extracted data
... pre_2_post_dict = {}
... # Incoming Data
... data = {"date": "2021-04-19", "data_array": [{"id": 1, "name": "Foo"}, {"id": 2, "name": "Bar"}]}
... # GlobalExtract instance for extracting the "date" from data, but replacing its key with "publish_time"
... pre = GlobalExtract(key="publish_time",
...                     lam=lambda x: x["date"],
...                     dtype=str
...                     )
... # The preprocessor gets added to the list of preprocessors
... pres.append(pre)
... # Each preprocesser gets applied sequentially
... for p in pres:
...     if p.ctype == "global_extract":
...         pre_2_post_dict[p.key] = p.lam_wrap(data)
...     else:
...         pass # See other pre-processors
... pre_2_post_dict
{"publish_time": {"value": "2021-04-19", "dtype": str}}

**Later after the data has been extracted to a DataFrame df**

::

    # Assume df = DataFrame containing extracted data
    # Assume dtype_convert is a function that maps a python native type to a pandas dtype

    # For each globally extracted item
    for k, v in pre_2_post_dict.items():
        # Add the item to the DataFrame -> These are GLOBAL values shared amongst ALL rows
        df[k] = v["value"]
        # Typecast the value to ensure it is the correct dtype
        df[k] = df[k].astype(dtype_convert(v["dtype"]))

**Example of what DataFrame would be:**

==  ====  ============
id  name  publish_time
==  ====  ============
 1  Foo   2021-04-19
 2  Bar   2021-04-19
==  ====  ============

"""
from .pre import Pre
from ...app.log import get_logger
from typing import Callable, Any


class GlobalExtract(Pre):
    """Used to extract a global feature from incoming data"""

    def __init__(self, key: str, lam: Callable[[dict], Any], dtype):
        """
        Creates a GlobalExtract object

        Args:
            key: The desired key of the feature for the storage target
            lam: Anonymous function that specifies where the location of the feature that should be extracted is
            dtype: The python native datatype the feature is expected to be
        """
        self.ctype = "global_extract"
        """str: type of the data processor"""
        self.key = key
        self.lam = lam
        self.dtype = dtype

    def lam_wrap(self, lam_arg: dict) -> dict:
        """
        Overrides super class method

        Workflow:

            1. Attempt to perform the ``lam`` operation on the incoming data
            2. Attempt to cast the result of the ``lam`` operation to the ``dtype``

                * If an exception occurs, returns {"value": None, "dtype": ``dtype``}

            3. Return {"value": ``result``, "dtype": ``dtype``}

        Args:
            lam_arg: A dictionary containing the feature that should be extracted

        Returns:
            A dictionary containing {"value": ``result or None``, "dtype": ``dtype``}
        """
        try:
            res = self.dtype(self.lam(lam_arg))
        except KeyError:
            res = None
        except ValueError:
            res = None
        except Exception as e:
            logger = get_logger()
            logger.exception(e)
            res = None
        res = {"value": res, "dtype": self.dtype}
        return res
