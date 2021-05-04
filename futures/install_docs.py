"""
Performs data import from !name! API.
Return must be an array of data, I.E. Wrap a JSON/XML -> Dict in an array.

Ex.
res = {"description": "Some complex nasty nested data"}
return [res]

Ex.
res = {
    "request_timestamp": "2021-4-22 00:00:00",
    "request_data": [
        {
            "description": "some complex nasty nested data"
        },
        {
            "description": "more nasty nested data"
        }
    ]
}
return res["request_data"]

IMPORTANT: Anything removed from the data will be inaccessible for collection.
In the above example, "request_timestamp" would be lost. When in doubt return everything wrapped in an array.

Currently, API responses are limited to a single set of data-features.
Ex.

res = {
    "data_1": [
        {"id": 1, "description": "some data that has no correlation with data_2"},
        {"id": 2, "description": "more unrelated data"}
        .
        .
        .
    ],

    "data_2": [
        {"name": "weird_data", "description": "totally different data than data_1", "random_feature": 123}
        .
        .
        .
    ]
}

In the above instance, extracting data_1 and data_2 is possible but would require 2 API calls.
This should occur rarely to never. Good APIs will return well structured data.
"""

"""
Creates !name! data collection form

Collection Forms contain 3 components.

1. Pre-processing
2. Data Feature Extraction
3. Post-processing

Each operation is performed sequentially, starting at the top of the pre_process array, and ending
at the end of the post_process array.

Important Notes about data features:
Error Checking is not required and is performed for you.
If a data-row is missing a feature that feature will be replaced with null.
If a data-row has a feature that is not the correct data-type, it will be replaced with null.

All data-features require 3 things.

1. The desired data-feature key. This must be unique across all features.
2. The expected location of the feature given a single row of data. 
3. The expected datatype of the feature. (Python native types)

Ex.

data_row = {
    "geo": {
            "x": 90.0,
            "y": 25.0
        }
}

data_features = [
    Feature(key="latitude", 
            lam=lambda x: x["geo"]["x"],
            dtype=float
            ),

    Feature(key="longitude", 
            lam=lambda x: x["geo"]["y"],
            dtype=float
            )
]
"""

"""
Links Datastreams to data arriving from !name!

A data-stream allows for data to be stored as it arrives in real-time.
Stream-Chaining is permitted. I.e. streams = [StreamLocal2Csv, StreamLocal2Pickle, ...]

Currently supported Local Streams Modes:
    shard  -> Stores the arriving data locally, primarily used for !name!_stores
    update -> Appends incoming DataFrame to stored data and drops duplicates.
    append -> Appends the arriving data to stored data, ignoring duplicates

Local Stream Storage Formats Supported:         CSV, JSON, Parquet, pickle
Local Stream Storage Formats In Development:    Avro, Feather, HDF5, ORC, SQLite

An External Stream streams data directly to an external source in real-time.

Currently Supported: BigQuery, MySql, MariaDb, Postgresql, Google Cloud Storage (currently supports Parquet format)
In Development:      FirebaseRTDB, Firestore, Amazon Aurora, Oracle, Microsoft SQL Server                       
"""

"""
Stores link to sharded stream files.
Files are first recomposed, and then pushed to the desired storage medium.
Stores also support chaining. See Documentation for details.

Currently Supported: BigQuery, MySql, MariaDB, Postgresql, Google Cloud Storage #FIXME: (gcs supports Parquet only)
In Development: Amazon Aurora, Oracle, Microsoft SQL Server
"""