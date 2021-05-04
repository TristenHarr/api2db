Examples
=========

Shell Commands
--------------

pmake
~~~~~

This shell command is used for initial creation of the project structure.

Given a blank project directory

::

    project_dir-----/

**Shell Command:** ``path/to/project_dir> pmake FooCollector BarCollector``

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

.. note::

    This command can also be used without any collector arguments, and collectors can be added using the ``cadd``
    shell command.

cadd
~~~~

This shell command is used to add a collector to an existing api2db project

Given the following project structure

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/procect_dir> cadd BarCollector``

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

crem
~~~~

This shell command is used to remove a collector registered with an existing api2db project

Given the following project

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/project_dir> crem BarCollector``

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

clist
~~~~~

This shell command is used to show a list of collectors registered with an existing api2db project


Given the following project

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/procect_dir> clist``

**Out:** ``["FooCollector", "BarCollector"]``

pclear
~~~~~~

This shell command is used to clear a project and should **ONLY** be used if a complete restart is required.

Given the following project

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/project_dir> pclear``

::

    project_dir-----/

mlab
~~~~

This shell command is used for creation of a lab. Labs offer an easier way to design an ApiForm.

Given a project directory

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- FooCollector.py
                    |        |- BarCollector.py
                    |
                    AUTH-----/
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/project_dir> mlab``

::

    project_dir-----/
                    |
                    apis-------/
                    |          |- __init__.py
                    |          |- FooCollector.py
                    |          |- BarCollector.py
                    |
                    AUTH-------/
                    |          |- bigquery_auth_template.json
                    |          |- omnisci_auth_template.json
                    |          |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    laboratory-/
                    |          |- lab.py    EDIT THIS FILE!
                    |
                    helpers.py
                    |
                    main.py

rlab
~~~~

This shell command is used to run a lab.

clab
~~~~

This shell command is used to clear a lab.

::

    project_dir-----/
                    |
                    apis-------/
                    |          |- __init__.py
                    |          |- FooCollector.py
                    |          |- BarCollector.py
                    |
                    AUTH-------/
                    |          |- bigquery_auth_template.json
                    |          |- omnisci_auth_template.json
                    |          |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    STORE/
                    |
                    laboratory-/
                    |          |- lab.py    EDIT THIS FILE!
                    |
                    helpers.py
                    |
                    main.py

**Shell Command:** ``path/to/project_dir> clab``

::

    project_dir-----/
                |
                apis-----/
                |        |- __init__.py
                |        |- FooCollector.py
                |        |- BarCollector.py
                |
                AUTH-----/
                |        |- bigquery_auth_template.json
                |        |- omnisci_auth_template.json
                |        |- sql_auth_template.json
                |
                CACHE/
                |
                STORE/
                |
                helpers.py
                |
                main.py

Pre-processing
--------------

BadRowSwap
~~~~~~~~~~

.. note::

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

**Summary of BadRowSwap usage:**

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

**Example Usage of BadRowSwap:**

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

FeatureFlatten
~~~~~~~~~~~~~~

.. note::

    FeatureFlatten should not be used until **AFTER** ListExtract has been performed on the data, unless performing
    a list extract is not necessary on the data.

**Summary of FeatureFlatten usage:**

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

**Example Usage of FeatureFlatten:**

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

GlobalExtract
~~~~~~~~~~~~~

**Summary of GlobalExtract usage:**

::

    data = {"date": "2021-04-19", "data_array": [{"id": 1, "name": "Foo"}, {"id": 2, "name": "Bar"}]}
    pre = GlobalExtract(key="publish_time",
                        lam=lambda x: x["date"],
                        dtype=str
                        )

**Final DataFrame**

==  ====  ============
id  name  publish_time
==  ====  ============
 1  Foo   2021-04-19
 2  Bar   2021-04-19
==  ====  ============

**Example Usage of GlobalExtract:**

>>> # pre-processing operators
... pres = []
... # Dictionary that contains all globally extracted data
... pre_2_post_dict = {}
... # Incoming Data
... data = {"date": "2021-04-19", "data_array": [{"id": 1, "name": "Foo"}, {"id": 2, "name": "Bar"}]}
... # GlobalExtract instance for extracting the "date" from data, but replacing its key with "publish_time"
... pre = GlobalExtract(key="publish_time",
...                     lam=lambda x: x["date"],
...                     dtype=str
...                     )
... # The preprocessor gets added to the list of preprocessors
... pres.append(pre)
... # Each preprocesser gets applied sequentially
... for p in pres:
...     if p.ctype == "global_extract":
...         pre_2_post_dict[p.key] = p.lam_wrap(data)
...     else:
...         pass # See other pre-processors
... pre_2_post_dict
{"publish_time": {"value": "2021-04-19", "dtype": str}}

**Later after the data has been extracted to a DataFrame df**

::

    # Assume df = DataFrame containing extracted data
    # Assume dtype_convert is a function that maps a python native type to a pandas dtype

    # For each globally extracted item
    for k, v in pre_2_post_dict.items():
        # Add the item to the DataFrame -> These are GLOBAL values shared amongst ALL rows
        df[k] = v["value"]
        # Typecast the value to ensure it is the correct dtype
        df[k] = df[k].astype(dtype_convert(v["dtype"]))

**Example of what DataFrame would be:**

==  ====  ============
id  name  publish_time
==  ====  ============
 1  Foo   2021-04-19
 2  Bar   2021-04-19
==  ====  ============


ListExtract
~~~~~~~~~~~

**Summary of ListExtract Usage:**

::

    data = { "actual_data_rows": [{"id": "row1"}, {"id": "row2"}], "erroneous_data": "FooBar" }
    pre = ListExtract(lam=lambda x: x["actual_data_rows"])

**Example Usage of ListExtract:**

>>> data = {
...    "Foo": "Metadata",
...    "data_array": [
...            {
...                "data_id": 1,
...                "name": "name_1"
...            },
...            {
...                "data_id": 2,
...                "name": "name_2"
...            }
...        ]
... }
...
... pre = ListExtract(lam=lambda x: x["data_array"])
... pre.lam_wrap(data)
[
    {
        "data_id": 1,
        "name": "name_1"
    },
    {
        "data_id": 2,
        "name": "name_2"
    }
]

Extracting data-features
------------------------

Summary of Feature Usage:
~~~~~~~~~~~~~~~~~~~~~~~~~

::

    data = [{"id": 1, "name": "Foo", "nest0": {"nest1": {"x": True}, "y": 14.3 } }, ... ]
    data_features = [

        Feature(key="uuid", lam=lambda x: x["id"], dtype=int),                  # Extracts "id" and rename it to "uuid"

        Feature(key="name", lam=lambda x: x["name"], dtype=str),                # Will extract "name" keeping the key as "name"

        Feature(key="x", lam=lambda x: x["nest0"]["nest1"]["x"], dtype=bool),   # Will extract "x"

        Feature(key="y", lam=lambda x: x["nest0"]["y"], dtype=bool)             # Will extract "y"
    ]

Post-processing
---------------

ColumnAdd
~~~~~~~~~

**Summary of ColumnAdd Usage:**

DataFrame ``df``

===  ===
Foo  Bar
===  ===
  1  A
  2  B
  3  C
===  ===

::

    post = ColumnAdd(key="FooBar", lam=lambda: 5, dtype=int)


DataFrame ``df``

===  ===  ======
Foo  Bar  FooBar
===  ===  ======
  1  A         5
  2  B         5
  3  C         5
===  ===  ======

**Example Usage of ColumnAdd:**

>>> import pandas as pd
... def f():
...     return 5
... df = pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"]})   # Setup
...
... post = ColumnAdd(key="timestamp", lam=lambda x: f, dtype=int)
... post.lam_wrap(df)
pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"], "FooBar": [5, 5, 5]})

ColumnApply
~~~~~~~~~~~

**Summary of ColumnApply Usage:**

DataFrame ``df``

===  ===
Foo  Bar
===  ===
  1  A
  2  B
  3  C
===  ===

::

    post = ColumnApply(key="Foo", lam=lambda x: x + 1, dtype=int)


DataFrame ``df``

===  ===
Foo  Bar
===  ===
  2  A
  3  B
  4  C
===  ===

**Example Usage of ColumnApply:**

>>> import pandas as pd
... df = pd.DataFrame({"Foo": [1, 2, 3], "Bar": ["A", "B", "C"]})   # Setup
...
... post = ColumnApply(key="Foo", lam=lambda x: x + 1, dtype=int)
... post.lam_wrap(df)
pd.DataFrame({"Foo": [2, 3, 4], "Bar": ["A", "B", "C"]})

ColumnsCalculate
~~~~~~~~~~~~~~~~

.. note::

    **ColumnsCalculate can be used to**

        1. Replace columns in a DataFrame with calculated values

        2. Add new columns to a DataFrame based on calculations from existing columns

**Summary of ColumnsCalculate Usage:**

DataFrame ``df``

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


DataFrame ``df``

===  ===  =======  =======
Foo  Bar  Foo+Bar  Foo*Bar
===  ===  =======  =======
  1    2        3        2
  2    4        6        8
  3    8       11       24
===  ===  =======  =======

**Example Usage of ColumnsCalculate:**

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

DateCast
~~~~~~~~

**Summary of DateCast Usage:**

DataFrame ``df``

===================  =====
        Foo           Bar
===================  =====
2021-04-29 01:39:00  False
2021-04-29 01:39:00  False
Bar!                 True
===================  =====

DataFrame ``df.dtypes``

======  ====
 Foo    Bar
======  ====
string  bool
======  ====

::

    post = DateCast(key="Foo", fmt="%Y-%m-%d %H:%M:%S")


DataFrame ``df``

===================  =====
        Foo           Bar
===================  =====
2021-04-29 01:39:00  False
2021-04-29 01:39:00  False
NaT                  True
===================  =====

DataFrame ``df.dtypes``

==============  ====
     Foo        Bar
==============  ====
datetime64[ns]  bool
==============  ====


DropNa
~~~~~~

Simply a shortcut class for a common operation.

**Summary of DropNa Usage:**

See pandas Documentation

MergeStatic
~~~~~~~~~~~

.. note::

    MergeStatic is used to merge data together. A common use case of this is in situations where a data-vendor provides
    an API that gives data-points "Foo", "Bar", and "location_id" where "location_id" references a different data-set.

    It is common for data-providers to have a file that does not update very frequently, i.e. is mostly static that
    contains this information.

    The typical workflow of a MergeStatic instance is as follows:

        1. Create a LocalStream with mode set to `update` or `replace` and a target like `CACHE/my_local_stream.pickle`

        2. Set the LocalStream to run periodically (6 hours, 24 hours, 10 days, whatever frequency this data is updated)

        3. Add a MergeStatic object to the frequently updating datas post-processors and set the path to the LocalStream
           storage path.
