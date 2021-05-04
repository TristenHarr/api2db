# TODO: The opposite of api_2_db
"""
db_2_api should be able to take a data-format from either collected data, or existing data-sources, and auto-generate
an API using a DbForm()

Dynamically build a flask API with objects such as:

db_form = [
    DbFeature(key="id", type=int, ...)
]

EndPoint(url="get/the/data",
         source_format="bigquery",
         include_formats=["json", "csv", "parquet", "pickle"],
         endpoint_type="REST".. (Support Download URI?, Support Active Streams?)
         )
"""