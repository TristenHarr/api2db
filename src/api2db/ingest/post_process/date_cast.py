# -*- coding: utf-8 -*-
"""
Contains the DateCast class
===========================

Summary of DateCast Usage:
--------------------------

**DataFrame** ``df``

===================  =====
        Foo           Bar
===================  =====
2021-04-29 01:39:00  False
2021-04-29 01:39:00  False
Bar!                 True
===================  =====

**DataFrame** ``df.dtypes``

======  ====
 Foo    Bar
======  ====
string  bool
======  ====

::

    post = DateCast(key="Foo", fmt="%Y-%m-%d %H:%M:%S")


**DataFrame** ``df``

===================  =====
        Foo           Bar
===================  =====
2021-04-29 01:39:00  False
2021-04-29 01:39:00  False
NaT                  True
===================  =====

**DataFrame** ``df.dtypes``

==============  ====
     Foo        Bar
==============  ====
datetime64[ns]  bool
==============  ====
"""
from .post import Post
import pandas as pd


class DateCast(Post):
    """Used to cast columns containing dates in string format to pandas DateTimes"""

    def __init__(self, key: str, fmt: str):
        """
        Creates a DateCast object

        Args:
            key: The name of the column containing strings that should be cast to datetimes
            fmt: A string formatter that specifies the datetime format of the strings in the column named ``key``
        """
        self.ctype = "date_cast"
        """str: type of data processor"""
        self.key = key
        self.fmt = fmt

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides super class method

        Workflow:

            1. Attempt to cast ``lam_arg[self.key]`` from strings to datetimes
            2. Return the modified ``lam_arg``

        Args:
            lam_arg: The DataFrame to modify

        Returns:
            The modified DataFrame
        """
        lam_arg[self.key] = pd.to_datetime(lam_arg[self.key], format=self.fmt, errors="coerce")
        return lam_arg
