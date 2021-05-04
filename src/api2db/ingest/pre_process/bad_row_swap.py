# -*- coding: utf-8 -*-
"""
Contains the BadRowSwap class
=============================

NOTE:

    BadRowSwap should not be used until **AFTER** ListExtract has been performed on the data, unless performing
    a list extract is not necessary on the data.

    When using BadRowSwap, the following conditions must be met:

        1. The value contained at location ``key_1`` must be able to be identified as valid, or in need of being swapped
           without any reference to the value at location ``key_2``. (Typically using regex or performing type-checking)

        2. ``key_1`` and ``key_2`` must be unique within their respective row of data.
           ``data = {"key_1": {"key_1": 1, "key_2": 2}}`` would be invalid.

    BadRowSwap **will potentially drop rows of data**. Rows meeting the following conditions will be dropped:

        * Any row that is missing ``key_1`` as a key will be dropped.

        * Any row that evaluates as needing to be swapped based on ``key_1`` that is missing ``key_2`` will be dropped.

    BadRowSwap will keep rows that meet the following conditions:

        * Any row that evaluates as not needing to be swapped based on ``key_1`` will be kept, regardless of if
          ``key_2`` exists or not.

        * Any row that evaluates as needing to be swapped based on ``key_1`` that also contains ``key_2`` will swap the
          values at the locations of the ``key_1`` and ``key_2`` and the row will be kept.

    Performing BadRowSwap can be computationally expensive, since it walks all nested data until it finds the desired
    keys. So here are a few tips to help you determine if you should be using it or not.

    Usage Tips for using BadRowSwap:

        * If both ``key_1`` and ``key_2`` are unimportant fields, I.e. Nullable fields and keeping them does not add
          significant value to the data consider just allowing the collector to Null them if they do not match the types
          or consider allowing them to simply have the wrong values if they have the same data-types. Otherwise you risk
          both slowing down data-collection, and dropping rows that have good data other than those swapped rows.

        * Always attempt to place the key at location ``key_1`` as the more important value to retain.
          If you need to swap data like a "uuid" and a "description", use the "uuid" as ``key_1``

        * If you cannot place the key at location ``key_1`` as the more important key, consider if the risk of losing
          data with a valid value for the more important key is worth it in instances where the less important key is
          missing

        * Consider the frequency that BadRowSwap would need to be run. If 1 out of 1,000,000 data-points contains values
          with swapped keys, is it worth running the computation on all 1,000,000 rows to save just that 1 row?

        * Analyze the data by hand. Pull it into a pandas DataFrame, and check it.

            * How often are is a row incorrect?

            * Are the erroneous rows ALWAYS the same key?

            * How often is one of the keys for the row missing when the rows have bad data?

Summary of BadRowSwap usage:
----------------------------

::

    data = [
        {
            "id": "17.0",
            "size": "Foo",
        },
        {
            "id": "Bar",
            "size": "10.0"
        }
    ]

    pre = BadRowSwap(key_1="id",
                     key_2="size",
                     lam=lambda x: re.match("[0-9][0-9]\.[0-9]+", x["id"]) is not None
                     )

Example Usage of BadRowSwap:
----------------------------

Occasionally when dealing with an API, the data is not always where it is supposed to be. Oftentimes this results
in the rows containing the misplaced data being dropped altogether. In the instance that for some unknown reason the
incoming data has keys that tend to occasionally have their values swapped so long as it is possible to check to see
if the data has been swapped due to what the data *should* be, use BadRowSwap.

This example assumes that the API occasionally swaps the values for "id" and "latitude". BadRowSwap can handle any level
of nested data in these instances, so long as the keys for the values that are occasionally swapped are
**unique within a single row**

>>> import re
... data = [
...     {
...         "id": "16.53",                                  # NEEDS SWAP = True
...          "place": {
...              "coords": {
...                  "latitude": "ID_1",
...                  "longitude": "-20.43"
...              },
...              "name": "place_1"
...          },
...         "details": "Some details... etc"
...      },
...
...     {
...         "id": "ID_2",                                   # NEEDS SWAP = False
...          "place": {
...              "coords": {
...                  "latitude": "15.43",
...                  "longitude": "-20.43"
...              },
...              "name": "place_2"
...          },
...         "details": "Some details... etc"
...      },
...
...     {
...         "id": "10.21",                                  # NEEDS SWAP = True
...         "place": {
...             "coords": {
...                                                         # Missing "latitude" key, results in row being skipped
...                 "longitude": "-20.43"
...             },
...             "name": "place_2"
...         },
...         "details": "Some details... etc"
...     },
...
...     {
...                                                         # Missing "id" key, results in row being skipped
...         "place": {
...             "coords": {
...                 "latitude": "ID_4",
...                 "longitude": "-20.43"
...             },
...             "name": "place_2"
...         },
...         "details": "Some details... etc"
...     },
...
...     {
...         "id": "ID_5",                                   # NEEDS SWAP = False
...         "place": {
...             "coords": {
...                                                         # Missing "latitude" row is kept, because no row swap needed
...                 "longitude": "-20.43"
...             },
...             "name": "place_2"
...         },
...         "details": "Some details... etc"
...     }
... ]
...
... pre = BadRowSwap(key_1="id",
...                  key_2="latitude",
...                  lam=lambda x: re.match("[0-9][0-9]\.[0-9]+", x["id"]) is not None
...                  )
...
... pre.lam_wrap(data)
[
    {
        "id": "ID_1",  # "id" and "latitude" have been swapped
        "place": {
            "coords": {
                "latitude": "16.53",
                "longitude": "-20.43"
            },
            "name": "place_1"
        },
        "details": "Some details... etc"
    },
    {
        "id": "ID_2",  # No changes required with this row
        "place": {
            "coords": {
                "latitude": "15.43",
                "longitude": "-20.43"
            },
            "name": "place_2"
        },
        "details": "Some details... etc"
    },
    # Row 3, and Row 4 have been dropped because they were missing key_1 or they required a swap and were missing key_2
    {
        "id": "ID_5",  # No changes required with this row
        "place": {
            "coords": {
                # The latitude is still missing but that can be handled later, it may be nullable, so it should be kept
                "longitude": "-20.43"
            },
            "name": "place_2"
        },
        "details": "Some details... etc"
    }
]
"""
from .pre import Pre
from typing import Callable, List


class BadRowSwap(Pre):
    """Used to swap rows arriving from the API that have the values for the given key swapped occasionally"""
    def __init__(self, key_1: str, key_2: str, lam: Callable[[dict], bool]):
        """
        Creates a BadRowSwap object

        Args:
            key_1: The key of a field that occasionally has its value swapped with the data from ``key_2``
            key_2: The key of a field that occasionally has its value swapped with the data from ``key_1``
            lam: A function (anonymous, or not) that when given the value located under ``key_1`` returns True if the
                 keys need their values swapped otherwise returns False
        """
        self.ctype = "bad_row_swap"
        self.key_1 = key_1
        self.key_2 = key_2
        self.lam = lam

    def lam_wrap(self, lam_arg: List[dict]) -> List[dict]:
        """
        Overrides super class method

        Args:
            lam_arg: A list of dictionaries with each dictionary containing what will become a row in a DataFrame

        Returns:
            A modified list of dictionaries with bad rows being either swapped or dropped.
        """
        new_rows = []
        for r in lam_arg:
            try:
                if self.lam(r):  # TODO: Cache the location of keys once found. I.e. Option for 1 key-search
                    v1 = BadRowSwap._find(self.key_1, r)
                    v2 = BadRowSwap._find(self.key_2, r)
                    r = BadRowSwap._swap(self.key_1, v2, r)
                    r = BadRowSwap._swap(self.key_2, v1, r)
                    if v1 is not None and v2 is not None:
                        new_rows.append(r)
                else:
                    new_rows.append(r)
            except KeyError:
                pass
        return new_rows

    @staticmethod
    def _swap(_k, _v, _d):
        return {k: _v if k == _k else v if type(v) is not dict else BadRowSwap._swap(_k, _v, v) for k, v in _d.items()}

    @staticmethod
    def _find(_k, _d):
        res = None
        try:
            res = _d[_k]
        except KeyError:
            for _v in _d.values():
                if type(_v) is dict:
                    res = BadRowSwap._find(_k, _v)
                    if res is not None:
                        break
        return res
