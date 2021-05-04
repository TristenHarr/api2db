# -*- coding: utf-8 -*-
"""
Contains the MergeStatic class
==============================

NOTE:

    MergeStatic is used to merge data together. A common use case of this is in situations where a data-vendor provides
    an API that gives data-points "Foo", "Bar", and "location_id" where "location_id" references a different data-set.

    It is common for data-providers to have a file that does not update very frequently, i.e. is mostly static that
    contains this information.

    The typical workflow of a MergeStatic instance is as follows:

        1. Create a LocalStream with mode set to `update` or `replace` and a target like `CACHE/my_local_stream.pickle`

        2. Set the LocalStream to run periodically (6 hours, 24 hours, 10 days, whatever frequency this data is updated)

        3. Add a MergeStatic object to the frequently updating datas post-processors and set the path to the LocalStream
           storage path.
"""
from .post import Post
import pickle
import pandas as pd


class MergeStatic(Post):
    """Merges incoming data with a locally stored DataFrame"""
    def __init__(self, key: str, path: str):
        """
        Creates a MergeStatic object

        Args:
            key: The key that the DataFrames should be merged on
            path: The path to the locally stored file containing the pickled DataFrame to merge with
        """
        self.ctype = "merge_static"
        """str: type of data processor"""
        self.key = key
        self.path = path

    def lam_wrap(self, lam_arg: pd.DataFrame) -> pd.DataFrame:
        """
        Overrides super class method

        Workflow:

            1. Load DataFrame ``df`` from file specified at ``self.path``
            2. Use ``lam_arg`` to perform left-merge on ``self.key`` merging with ``df``
            3. Return the modified DataFrame

        Args:
            lam_arg: The DataFrame to modify

        Returns:
            The modified DataFrame
        """
        with open(self.path, "rb") as f:
            static = pickle.load(f)
        lam_arg = lam_arg.merge(static, on=self.key, how="left")
        return lam_arg
