# -*- coding: utf-8 -*-
"""
Contains the ColumnsCalculate class
===================================

NOTE:

    **ColumnsCalculate can be used to**

        1. Replace columns in a DataFrame with calculated values

        2. Add new columns to a DataFrame based on calculations from existing columns

Summary of ColumnsCalculate Usage:
----------------------------------

**DataFrame** ``df``

===  ===
Foo  Bar
===  ===
  1    2
  2    4
  3    8
===  ===

::

    def foobar(df):
        df["Foo+Bar"] = df["Foo"] + df["Bar"]
        df["Foo*Bar"] = df["Foo"] * df["Bar"]
        return df[["Foo+Bar", "Foo*Bar"]]

    post = ColumnsCalculate(keys=["Foo+Bar", "Foo*Bar"], lam=lambda x: foobar(x), dtype=int)


**DataFrame** ``df``

===  ===  =======  =======
Foo  Bar  Foo+Bar  Foo*Bar
===  ===  =======  =======
  1    2        3        2
  2    4        6        8
  3    8       11       24
===  ===  =======  =======

Example Usage of ColumnsCalculate:
----------------------------------

>>> import pandas as pd
... df = pd.DataFrame({"Foo": [1, 2, 3], "Bar": [2, 4, 8]})   # Setup
...
... def foobar(d):
...     d["Foo+Bar"] = d["Foo"] + d["Bar"]
...     d["Foo*Bar"] = d["Foo"] * d["Bar"]
...     return d[["Foo+Bar", "Foo*Bar"]]
...
... post = ColumnsCalculate(keys=["Foo+Bar", "Foo*Bar"], lam=lambda x: foobar(x), dtype=int)
... post.lam_wrap(df)
pd.DataFrame({"Foo+Bar": [3, 6, 11], "Foo*Bar": [2, 8, 24]})
"""
from .post import Post
import pandas as pd
from typing import List, Any, Callable


class ColumnsCalculate(Post):
    """Used to calculate new column values to add to the DataFrame"""

    def __init__(self, keys: List[str], lam: Callable[[pd.DataFrame], pd.DataFrame], dtypes: List[Any]):
        """
        Creates a ColumnsCalculate object

        Args:
            keys: A list of the keys to add/replace in the existing DataFrame
            lam: A function that takes as parameter a DataFrame, and returns a DataFrame with column names matching
                 ``keys`` and the columns having/being castable to ``dtypes``
            dtypes: A list of python native types that are associated with ``keys``
        """
        self.ctype = "columns_calculate"
        """str: type of data processor"""
        self.keys = keys
        self.lam = lam
        self.dtypes = [self.typecast(d) for d in dtypes]

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides super class method

        Workflow:

            1. Create a temporary DataFrame ``tmp_df`` by applying ``lam`` to ``lam_arg``
            2. For each ``key`` in ``self.keys`` set ``lam_arg[key] = tmp_df[key]``
            3. For each ``key`` in ``self.keys`` cast ``lam_arg[key]`` to the appropriate pandas dtype
            4. Return ``lam_arg``

        Args:
             lam_arg: The DataFrame to modify

        Returns:
            The modified DataFrame
        """
        tmp_df = self.lam(lam_arg)
        for key, dtype in zip(self.keys, self.dtypes):
            lam_arg[key] = tmp_df[key]
            lam_arg[key] = lam_arg[key].astype(dtype)
        return lam_arg
