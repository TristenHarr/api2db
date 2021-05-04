The future of api2db and contributing
=====================================

Installing the project as a developer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Future Plans
~~~~~~~~~~~~

* Add support for more storage targets

    * Oracle
    * Amazon Aurora
    * MariaDB
    * Microsoft SQL Server
    * Firebase RTDB

* Add support for uploading in chunks and resumable uploads

* Add support for sharding data collection across multiple servers.

    * If one server does not send a heartbeat out after a certain period of time, another server begins collecting data.

* Add 100% test coverage for code-base

* Add library support utilizing the ApiForm to create database migration support

    * Treat a database as a stream, and pull data from it in chunks before migrating to a new target
    * Used to switch databases, and also clean messy database data

* Add ML targets that can be attached directly to streams

    * Allow for streams to feed directly into predictive models

* Add support for an api2db implementation

    * Performs things in a manner opposite of api2db
    * Objects such as an EndPoint object used to create Api Endpoints
    * Take any database, and turn it into an API
    * Include role-based authentication

* Remove BaseLam object

    * Since collectors run in a single process, this needs depreciated. No need to serialize the state

* Add additional pre/post processors

    * Listen to what users want upon release, and implement them

* Remove unnecessary strings

    * Fix implementations using strings to represent object types I.e. ctype in processors
    * Use isinstance, and pandas.api.types.is_x_type

* Add support for GPU processing of data

    * Allow for ingestion to be performed on a GPU for high-volume streams

* Rewrite performance critical areas of application in C

* Create a Store2Local object that can be used to aggregate storage in time intervals

* Add support for messaging

    * Redis Pub/Sub
    * Google Cloud Pub/Sub
    * Kafka Pub/Sub
    * Using Firestore

* Add the ability to generate live insights

    * As data arrives, create the ability to perform rolling averages of data
    * Allow for chaining messaging abilities onto streams
