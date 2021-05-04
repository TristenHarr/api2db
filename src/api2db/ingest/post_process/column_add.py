# -*- coding: utf-8 -*-
"""
Contains the ColumnAdd class
============================

Summary of ColumnAdd Usage:
---------------------------

**DataFrame** ``df``

===  ===
Foo  Bar
===  ===
  1  A
  2  B
  3  C
===  ===

::

    post = ColumnAdd(key="FooBar", lam=lambda: 5, dtype=int)


**DataFrame** ``df``

===  ===  ======
Foo  Bar  FooBar
===  ===  ======
  1  A         5
  2  B         5
  3  C         5
===  ===  ======

Example Usage of ColumnAdd:
---------------------------

>>> import pandas as pd
... def f():
...     return 5
... df = pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"]})   # Setup
...
... post = ColumnAdd(key="timestamp", lam=lambda x: f, dtype=int)
... post.lam_wrap(df)
pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"], "FooBar": [5, 5, 5]})
"""
from .post import Post
from typing import Any, Callable
import pandas as pd


class ColumnAdd(Post):
    """Used to add global values to a DataFrame, primarily for timestamps/ids"""

    def __init__(self, key: str, lam: Callable[[], Any], dtype: Any):
        """
        Creates a ColumnAdd object

        Args:
            key: The column name for the DataFrame
            lam: A function that returns the value that should be globally placed into the DataFrame in ``key`` column
            dtype: The python native type of the functions return
        """
        self.ctype = "column_add"
        """str: type of the data processor"""
        self.key = key
        self.lam = lam
        self.dtype = self.typecast(dtype)

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides super class method

        Workflow:

            1. Assign the ``lam`` function return to ``lam_arg[self.key]``
            2. Typecast ``lam_arg[self.key]`` to ``dtype``
            3. Return ``lam_arg``

        Args:
            lam_arg: The DataFrame to add a column to

        Returns:
            The modified DataFrame
        """
        lam_arg[self.key] = self.lam()
        lam_arg[self.key] = lam_arg[self.key].astype(self.dtype)
        return lam_arg
