# -*- coding: utf-8 -*-
"""
Contains the ColumnApply class
==============================

Summary of ColumnApply Usage:
-----------------------------

**DataFrame** ``df``

===  ===
Foo  Bar
===  ===
  1  A
  2  B
  3  C
===  ===

::

    post = ColumnApply(key="Foo", lam=lambda x: x + 1, dtype=int)


**DataFrame** ``df``

===  ===
Foo  Bar
===  ===
  2  A
  3  B
  4  C
===  ===

Example Usage of ColumnApply:
-----------------------------

>>> import pandas as pd
... df = pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"]})   # Setup
...
... post = ColumnApply(key="Foo", lam=lambda x: x + 1, dtype=int)
... post.lam_wrap(df)
pd.DataFrame({"Foo": [2, 3, 4], "Bar": ["A", "B", "C"]})
"""
from .post import Post
from typing import Callable, Any
import pandas as pd


class ColumnApply(Post):
    """Used to apply a function across the rows in a column of a DataFrame"""

    def __init__(self, key: str, lam: Callable[[Any], Any], dtype: Any):
        """
        Creates a ColumnApply Object

        Args:
            key: The column to apply the function to
            lam: The function to apply
            dtype: The python native type of the function output
        """
        self.ctype = "column_apply"
        """str: type of data processor"""
        self.key = key
        self.lam = lam
        self.dtype = self.typecast(dtype)

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides a super class method

        Workflow:

            1. Apply ``lam`` to ``lam_arg[self.key]``
            2. Cast ``lam_arg[self.key]`` to ``dtype``
            3. Return ``lam_arg``

        Args:
            lam_arg: The DataFrame to modify

        Returns:
            The modified DataFrame
        """
        lam_arg[self.key] = lam_arg[self.key].apply(self.lam)
        lam_arg[self.key] = lam_arg[self.key].astype(self.dtype)
        return lam_arg
