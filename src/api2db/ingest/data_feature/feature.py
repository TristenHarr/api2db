# -*- coding: utf-8 -*-
"""
Contains the Feature class
===========================

Summary of Feature Usage:
--------------------------

::

    data = [{"id": 1, "name": "Foo", "nest0": {"nest1": {"x": True}, "y": 14.3 } }, ... ]
    data_features = [

        Feature(key="uuid", lam=lambda x: x["id"], dtype=int),      # Extracts "id" and rename it to "uuid"

        Feature(key="name", lam=lambda x: x["name"], dtype=str),    # Will extract "name" keeping the key as "name"

        Feature(key="x", lam=lambda x: x["nest0"]["nest1"]["x"], dtype=bool),   # Will extract "x"

        Feature(key="y", lam=lambda x: x["nest0"]["y"], dtype=bool)             # Will extract "y"
    ]


"""
from ..base_lam import BaseLam
from ...app.log import get_logger
from typing import Optional, Callable, Any


class Feature(BaseLam):
    """Used to extract a data-feature from incoming data"""

    def __init__(self,
                 key: str,
                 lam: Callable[[dict], Any],
                 dtype: Any,
                 nan_int: Optional[int]=None,
                 nan_float: Optional[float]=None,
                 nan_bool: Optional[bool]=False,
                 nan_str: Optional[str]=None):
        """
        Creates a Feature object

        NOTE:
            All values default to nulling the data that cannot be type-casted to its expected type.
            For the majority of instances this is going to be the programmers desired effect.
            If there is a way to make it so that the data can be cleaned in order to prevent it from being nulled, that
            should be done using the libraries pre-processing tools. If the data cannot be cleaned in pre-processing and
            it cannot be type-casted to its expected type, then it is by definition worthless. If it is possible to
            clean it, it can be cleaned in pre-processing, although it may require the programmer to subclass
            :py:class:`Pre <api2db.ingest.pre_process.pre.Pre>`

        Args:
            key: The name of the column that will be stored in the storage target
            lam: Function that takes as parameter a dictionary, and returns where the data the programmer wants
                 **should** be. api2db handles null data and unexpected data types automatically
            dtype: The python native type of the data feature
            nan_int: If specified and ``dtype`` is ``int`` this value will be used to replace null values and values
                     that fail to be casted to type ``int``
            nan_float: If specified and ``dtype`` is ``float`` this value will be used to replace null values and values
                       that fail to be casted to type ``float``
            nan_bool: If specified and ``dtype`` is ``bool`` this value will be used to replace null values and values
                      that fail to be casted to type ``bool``
            nan_str: If specified and ``dtype`` is ``str`` this value will be used to replace null values and values
                     that fail to be casted to type ``str``
        """
        self.key = key
        self.dtype = dtype
        self.nan_int = nan_int
        self.nan_float = nan_float
        self.nan_bool = nan_bool
        self.nan_str = nan_str
        self.lam = lam

    def lam_wrap(self, data: dict) -> Any:
        """
        Overrides super class method

        Extracts a feature from incoming data

        Workflow:

            1. Attempt to call ``lam`` on data to get data-feature
            2. Attempt to typecast result to ``dtype``
            3. If ``dtype`` is ``str`` and the result.lower() is "none", "nan", "null", or "nil" replace it with
               ``nan_str``
            4. If an exception occurs when attempting any of the above, set the result to None
            5. Return the result

        Args:
            data: A dictionary of incoming data representing a single row in a DataFrame

        Returns:
            The extracted data-feature
        """
        res = None
        e_flag = False
        try:
            res = self.dtype(self.lam(data))
            if type(res) is str and res.lower() in ["none", "nan", "null", "nil"]:
                res = self.nan_str
        except KeyError:
            e_flag = True
        except ValueError:
            e_flag = True
        except Exception as e:
            e_flag = True
            logger = get_logger()
            logger.exception(e)
        finally:
            if e_flag:
                if self.dtype is int:
                    res = self.nan_int
                elif self.dtype is float:
                    res = self.nan_float
                elif self.dtype is bool:
                    res = self.nan_bool
                else:
                    res = self.nan_str
        return res
