# -*- coding: utf-8 -*-
"""
Contains the FeatureFlatten class
=================================

NOTE:

    FeatureFlatten should not be used until **AFTER** ListExtract has been performed on the data, unless performing
    a list extract is not necessary on the data.

Summary of FeatureFlatten usage:
--------------------------------

::

    data = [
        {
            "data_id": 1,
            "data_features": [
                                {
                                    "x": 5,
                                    "y": 10
                                },
                                {
                                    "x": 7,
                                    "y": 15
                                },
                                .
                                .
                                .
                             ]
        }
    ]
    pre = FeatureFlatten(key="data_features")

Example Usage of FeatureFlatten:
--------------------------------

>>> data = [
...     {
...         "data_id": 1,
...         "data_features": {
...                             "Foo": 5,
...                             "Bar": 10
...                          }
...     },
...
...     {
...         "data_id": 2,
...         "data_features": [
...                             {
...                                 "Foo": 5,
...                                 "Bar": 10
...                             },
...                             {
...                                 "Foo": 7,
...                                 "Bar": 15
...                             }
...                          ]
...     }
... ]
... pre = FeatureFlatten(key="data_features")
... pre.lam_wrap(data)
[
    {
        "data_id": 1,
        "data_features": {
                            "Foo": 5,
                            "Bar": 10
                         }
    },
    {
        "data_id": 2,
        "data_features": {
                            "Foo": 5,
                            "Bar": 10
                         }
    },
    {
        "data_id": 2,
        "data_features": {
                            "Foo": 7,
                            "Bar": 15
                         }
    }
]
"""
from .pre import Pre
import copy
from typing import Optional, List


class FeatureFlatten(Pre):
    """Used to flatten features containing arrays causing them to be incompatible for storage in a table-based schema"""

    def __init__(self, key: str):
        """
        Creates a FeatureFlatten object

        Args:
            key: The key containing nested data that each needs to have its own row in the final DataFrame
        """
        self.ctype = "feature_flatten"
        """str: type of data processor"""
        self.key = key

    def lam_wrap(self, lam_arg: Optional[List[dict]]) -> List[dict]:
        """
        Overrides super class method

        Workflow:

            * Create an array of ``rows``

            * For each dictionary ``d`` in the array of data-points

                * If the type of ``self.key`` is in ``d.keys()`` and ``type(d[self.key]) == list``

                    * For each item in list

                        * Create a new row containing all data-features and the item by itself and add it to ``rows``

                * If the type of ``self.key`` is in ``d.keys()`` and ``type(d[self.key]) == dict``

                    * Keep the row as it is, and add it to ``rows``

        Args:
            lam_arg: A list of dictionaries that each represent a row in the final DataFrame
                     (Optional to safeguard against if previous pre-processors could not parse data, i.e. No data-points
                     existed)

        Returns:
            An array of dictionaries that each represent a row, with nested data extracted to their own rows
        """
        res = []
        if lam_arg is not None and type(lam_arg) is list:
            for arg in lam_arg:
                if type(arg) is dict and self.key in arg.keys() and type(arg[self.key]) is list:
                    res_dict = {}
                    for k, v in arg.items():
                        if k != self.key:
                            res_dict[k] = v
                    for item in arg[self.key]:
                        res_dict[self.key] = item
                        res.append(copy.deepcopy(res_dict))
                else:
                    if type(arg) is dict and self.key in arg.keys() and type(arg[self.key]) is dict:
                        res.append(arg)
        return res
