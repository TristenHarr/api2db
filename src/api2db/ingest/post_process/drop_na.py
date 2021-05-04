# -*- coding: utf-8 -*-
"""
Contains the DropNa class
=========================

Simply a shortcut class for a common operation.

Summary of DropNa Usage:
------------------------

See pandas Documentation
"""
from .post import Post
import pandas as pd
from typing import List


class DropNa(Post):
    """Used to drop columns with null values on specified keys"""

    def __init__(self, keys: List[str]):
        """
        Creates a DropNa object

        Args:
            keys: The subset of keys to drop if the keys are null
        """
        self.ctype = "drop_na"
        """str: type of data processor"""
        self.keys = keys

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides super class method

        Shortcut used to drop null values. Performs ``pd.DataFrame.drop_na(subset=self.keys)``

        Args:
            lam_arg: The DataFrame to modify

        Returns:
            The modified DataFrame
        """
        return lam_arg.dropna(subset=self.keys)
