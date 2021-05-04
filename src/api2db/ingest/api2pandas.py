# -*- coding: utf-8 -*-
"""
Contains the Api2Pandas class
=============================
"""
from ..app.log import get_logger
from .api_form import ApiForm
import pandas as pd
import os
from typing import Union, Callable


class Api2Pandas(object):
    """Used to extract incoming data from an API into a pandas DataFrame"""

    def __init__(self, api_form: Callable[[], ApiForm]):
        """
        Creates a Api2Pandas object and loads its ApiForm

        Args:
            api_form: The function that generates the ApiForm for the associated collector
        """
        self.api_form = api_form()

    def dependencies_satisfied(self) -> bool:
        """
        Checks to ensure any data-linking dependency files exist

        This feature currently only exists for :py:class:`api2db.ingest.post_process.merge_static.MergeStatic`

        Returns:
            True if all dependencies are satisfied, otherwise False
        """
        logger = get_logger()
        res = True
        for pre in self.api_form.pre_process:
            if pre.ctype in []:
                if not os.path.isfile(pre.path):
                    logger.warning(f"Missing PreProcess Dependency File: {pre.path}")
                    res = False

        for post in self.api_form.post_process:
            if post.ctype in ["merge_static"]:
                if not os.path.isfile(post.path):
                    logger.warning(f"Missing PostProcess Dependency File: {post.path}")
                    res = False
        return res

    def extract(self, data: dict) -> Union[pd.DataFrame, None]:
        """
        Performs data-extraction from data arriving from an API.

        Workflow:

            1. Perform all pre-processing on data
            2. Perform all data-feature extraction
            3. Perform all post-processing on data
            4. Return a DataFrame containing the cleaned data.

        Args:
            data: The data arriving from an API to perform data extraction on.

        Returns:
            The cleaned data if it is possible to clean the data otherwise None
        """
        # Global extraction dictionary
        pre_2_post = {}
        # For each pre-processor
        for pre in self.api_form.pre_process:
            # If the pre-processor is a global extraction, add the feature extracted to the global extraction dictionary
            if pre.ctype == "global_extract":
                pre_2_post[pre.key] = pre(lam_arg=data)
            else:
                # Perform the pre-processor and replace the existing data with the new data
                data = pre(lam_arg=data)
        if data is None:
            return data
        rows = []
        # For each row in the data
        for data_point in data:
            row = {}
            # Extract all the features from the row
            for feat in self.api_form.data_features:
                row[feat.key] = feat(data_point)
            rows.append(row)
        # Create the DataFrame from the rows
        df = pd.DataFrame(rows)
        # Cast the DataFrame to the correct dtypes
        df = df.astype(self.api_form.pandas_typecast())
        # Add all globally extracted data to the DataFrame
        for k, v in pre_2_post.items():
            df[k] = v["value"]
            df[k] = df[k].astype(self.api_form.typecast(v["dtype"]))
        # For each post-processor
        for post in self.api_form.post_process:
            if post.ctype == "futures":                                 # FUTURES MAY REQUIRE DIFFERENT OPERATIONS
                pass
            else:
                # Perform the post-processing operation on the DataFrame
                df = post(df)
        # Get rid of the data index
        df = df.reset_index(drop=True)
        # Return the clean Data Hooray!
        return df
