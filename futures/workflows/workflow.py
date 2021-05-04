# TODO: WorkFlow class
"""
The workflow class should tie everything together. This should become the heart of the application.

A workflow should be focused on being as hands-off as possible.
Workflows can link multiple api_2_db and db_2_api's.

Web Page:

        Add a data source ->  Form based template. Streams in data live, attempts to infer the types,
        allows a user to design the structure of the data.
        Attempt to INFER the structure first, and give a basic setup. Auto-infer typecasting.
        User corrects/re-orders data to their liking.

        Feature Form:
                        Key: -> key
                        Highligh the data location of this key
                        This key is of type -> type
                        .
                        .

        ApiBuilder:
                        Opposite of Collector.
                        Highlight the data, and build an API structure.

USE CASE:

Client XYZ uses api_2_db/db_2_api to stream data in from 5-6 data-sources.
Client XYZ chose the BYOS option, upon which they have a server instance they provided, and api_2_db
workflows were developed that utilize their server for working with the data.

Data is ingested from the 5-6 data sources, and it is cleaned and processed upon arrival.

The data arrives and is handed into a stream object that stores it locally onto the server.
Every hour, this local data is moved into a data-lake, and during the process of the data being moved, the
workflow includes Chains of Extracts, allowing for data-summaries to be obtained.
These summaries are pushed to Redis using a pub/sub model providing logging, interactive charts and graphs, and
even serializable versions of the summaries, which allow their users to embed custom versions of the charts into
their own web-pages.

The stream is chained with another stream that pushed data in real-time into an additional HPC server with a large GPU

This is done via a workflow, that can automate the setup of an entire server. Support for auto-scaling should also be taken into account.

Once the data has arrived on the GPU server, live data-charts are viewable in real-time offering data-insights.
The GPU server is also used to do real-time ML predictions. The client has paid for someone to go through a subset of
the data and hand-label it, and now has a fairly accurate ML model. This workflow allows the client to offer users
live predictions for.. lets say sports betting for example?

Using db_2_api the client has built a workflow that now works in reverse, offering users a paid API that
they can send data to, and get the ML predictions back from.


AutoML
        -> Auto Predictions...

                                Given the input data.. and the extensive workflow capabilities..
                                Provide random data-transformations..

                                I.e.
                                table =
                                id      lat     long        feature_1       feature_2 ...

                                AutoML(table) ->
                                                    try ML model using columns: [1, 2, 3]
                                                    try ML model using columns: [1, 3, 4]
                                                    try ML model using columns: [log(1), matrix_mult(2, 3)]
                                                    .
                                                    .
                                                    .
                                                    Collect Results ->
                                                                        Find best results, find commonalities between
                                                                        training settings -> Repeat

Paid Plans:
                BYOS Self-Serve ->  Bring your own Server
                                    Build a custom solution using the api_2_db API. You are responsible for supporting
                                    the code, workflows, etc.

                BYOS Managed    ->  Bring your own server
                                    But api2db.app will manage your workflows, ensuring things
                                    run smoothly, and if any other api_2_db clients are collecting the same data as you
                                    api_2_db will provide you with that data should your server go down.
                                    Includes support, at a higher cost. Designed for LARGE companies

                BYODB Self-Serve -> Bring your own Database.
                                    Use the api2db website to design your work-flows.
                                    api2db.app will collect your data, and stream it into your database.

                BYODB Managed    -> Bring your own Database.
                                    But, api2db will manage the design and workflows. You say what data you need, and
                                    where you need it from, describe what data formats you want, etc. and api2db.app
                                    will handle the rest.

                api2db hosted   ->  Run your data-collectors directly on api2db's platform.
"""