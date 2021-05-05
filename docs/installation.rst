Installation and Quickstart
===========================

Installation
------------

**Install the library**

``pip install api2db``

**To add MySQL support**

``pip install mypysql``

**To add MariaDB support**

``pip install mariadb``

**To add PostgreSQL support**

``pip install psycopg2``

**To add Omnisci support**

::

    pip install pymapd==0.25.0
    pip install pyarrow==3.0.0
    pip install pandas --upgrade

Quickstart
----------

**Create a project with the** ``pmake`` **shell command**

Initial directory structure

::

    project_dir/

``path/to/project_dir/> pmake``

New project directory structure

::

    project_dir-----/
                    |
                    apis-----/                                  # Each API will get its own file
                    |        |- __init__.py
                    |
                    AUTH-----/                                  # These templates are used for adding database targets
                    |        |- bigquery_auth_template.json
                    |        |- omnisci_auth_template.json
                    |        |- sql_auth_template.json
                    |
                    CACHE/                                      # Application cache files will be stored and used here.
                    |
                    STORE/                                      # This is where incoming data can be stored locally.
                    |
                    LOGS/                                       # Each collector will receive it's own log file.
                    |
                    helpers.py                                  # Common helper functions can be written here.
                    |
                    main.py                                     # This is the application entry point.

**Choose an API**
This example will use the CoinCap_ API as it is free, does not require an API key, and seems to have good uptime.
(This project has no affiliation with CoinCap)

.. _CoinCap: https://docs.coincap.io/

**Create a collector with the** ``cadd`` **shell command**

``path/to/project_dir/> cadd coincap``

::

    project_dir-----/
                    |
                    apis-----/
                    |        |- __init__.py
                    |        |- coincap.py                      # This is where you'll write code!
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
                    LOGS/
                    |
                    helpers.py
                    |
                    main.py

**Understanding** ``project_dir/apis/some_api_collector.py``

Each collector has 4 parts.

    1. Data Import
        * Performs a call to an API to request data.
    2. Data Processing
        * Processes and cleans incoming data, making it useful.
    3. Data Streams
        * Streams data as it arrives to a storage target.
    4. Data Stores
        * Stores data periodically to a storage target.

**The base template for the coincap collector looks like this**

::

    from api2db.ingest import *
    from api2db.stream import *
    from api2db.store import *
    from helpers import *

    def coincap_import():
        return None

    def coincap_form():
        pre_process = [
            # Preprocessing Here
        ]

        data_features = [
            # Data Features Here
        ]

        post_process = [
            # Postproccesing Here
        ]

        return ApiForm(name="coincap", pre_process=pre_process, data_features=data_features, post_process=post_process)

    def coincap_streams():
        streams = [

        ]
        return streams

    def coincap_stores():
        stores = [

        ]
        return stores

    coincap_info = Collector(name="coincap",
                            seconds=0,                      # Import frequency of 0 disables collector
                            import_target=coincap_import,
                            api_form=coincap_form,
                            streams=coincap_streams,
                            stores=coincap_stores,
                            debug=True                      # Set to False for production
                            )

Using the lab to build ApiForms
-------------------------------

**To simplify setting up the data import and data-processing first run the** ``mlab`` **shell command**

``path/to/project_dir/> mlab``

::

    project_dir-----/
                    |
                    apis------/
                    |         |- __init__.py
                    |         |- coincap.py
                    |
                    AUTH------/
                    |         |- bigquery_auth_template.json
                    |         |- omnisci_auth_template.json
                    |         |- sql_auth_template.json
                    |
                    CACHE/
                    |
                    laboratory/
                    |         |- lab.py     # This is where you can experiment with pre-processing
                    |
                    STORE/
                    |
                    LOGS/
                    |
                    helpers.py
                    |
                    main.py

**A blank lab.py file will look like this**

::

    from api2db.ingest import *
    CACHE=True      # Caches API data so that only a single API call is made if True

    def import_target():
        return None

    def pre_process():
        return None

    def data_features():
        return None

    def post_process():
        return None

    if __name__ == "__main__":
        api_form = ApiForm(name="lab",
                           pre_process=pre_process(),
                           data_features=data_features(),
                           post_process=post_process()
                           )
        api_form.experiment(CACHE, import_target)

Importing data
--------------

**Perform a data import by writing the code for the** ``import_target`` **function**

lab.py
::

    .
    .
    .

    import requests
    import logging

    def import_target():
        """
        Data returned by the import target must be an array of dicts.
        This allows for either a single API call to be returned, or an array of them.
        """
        data = None
        url = "https://api.coincap.io/v2/assets/"
        try:
            data = [requests.get(url).json()]
        except Exception as e:
            logging.exception(e)
        return data

    .
    .
    .

**Use the** ``rlab`` **shell command to run the lab**

.. note::

    Watch the laboratory directory closely. Data will be dumped into JSON files at different points
    during data-processing to provide the programmer with an easier to read format.

``path/to/project_dir/> rlab``

Output:

::

    data:
    {
        "data": [
            {
                "id": "bitcoin",
                "rank": "1",
                "symbol": "BTC",
                "name": "Bitcoin",
                "supply": "18698850.0000000000000000",
                "maxSupply": "21000000.0000000000000000",
                "marketCapUsd": "1041388865130.8623213956691350",
                "volumeUsd24Hr": "12822561919.6746830356589619",
                "priceUsd": "55692.6690748822693051",
                "changePercent24Hr": "-4.1033665252363403",
                "vwap24Hr": "57708.7312639442977184",
                "explorer": "https://blockchain.info/",
            },
            .
            .
            .
        ],

        "timestamp": 1620100433183,
    }

    data keys:
    dict_keys(['data', 'timestamp'])

    pre_process must return a list of 0 or more pre-processors.
    pre_process:
    None

Performing pre-processing on data
---------------------------------

**Perform pre-processing on data by writing the code for the** ``pre_process`` **function**

lab.py
::

    .
    .
    .

    def pre_process():
        """
        Pre-processors are applied sequentially.
        In this example, we will:

            1. Extract the timestamp and make it a global feature using GlobalExtract
            2. Perform a ListExtract to extract the list of data which will become the rows in the storage target table
        """
        return [
            GlobalExtract(key="timestamp",
                          lam=lambda x: x["timestamp"],
                          dtype=int
                          ),

            ListExtract(lam=lambda x: x["data"])
        ]

    .
    .
    .

**Use the** ``rlab`` **shell command to run the lab**

``path/to/project_dir/> rlab``

Output:

::

    data point 1:
    {'id': 'bitcoin', 'rank': '1', 'symbol': 'BTC', 'name': 'Bitcoin', 'supply': '18698850.0000000000000000', 'maxSupply': '21000000.0000000000000000', 'marketCapUsd': '1
    041388865130.8623213956691350', 'volumeUsd24Hr': '12822561919.6746830356589619', 'priceUsd': '55692.6690748822693051', 'changePercent24Hr': '-4.1033665252363403', 'vw
    ap24Hr': '57708.7312639442977184', 'explorer': 'https://blockchain.info/'}

    data point 2:
    {'id': 'ethereum', 'rank': '2', 'symbol': 'ETH', 'name': 'Ethereum', 'supply': '115729464.3115000000000000', 'maxSupply': None, 'marketCapUsd': '376411190202.66581272
    13330461', 'volumeUsd24Hr': '17656637086.6618270054805080', 'priceUsd': '3252.5095699873722881', 'changePercent24Hr': '6.4420494833790460', 'vwap24Hr': '3234.41835079
    37765772', 'explorer': 'https://etherscan.io/'}

    data point 3:
    {'id': 'binance-coin', 'rank': '3', 'symbol': 'BNB', 'name': 'Binance Coin', 'supply': '153432897.0000000000000000', 'maxSupply': '170532785.0000000000000000', 'marke
    tCapUsd': '98431624817.6777436959489247', 'volumeUsd24Hr': '254674805.8210425908376882', 'priceUsd': '641.5288164550379551', 'changePercent24Hr': '1.1504585233985471'
    , 'vwap24Hr': '653.0516845642682435', 'explorer': 'https://etherscan.io/token/0xB8c77482e45F1F44dE1745F52C74426C631bDD52'}

    data_features must return a list of data-features.
    data_features:
    None

Extracting features from data
-----------------------------

**Extract data-features from data by writing the code for the** ``data_features`` **function**

.. note::

    Pick and choose which data-features you wish to extract from your data. This example will extract the
    ``id``, ``rank``, ``symbol``, ``name``, ``priceUsd``, and ``volumeUsd24Hr``

    Feature extraction will handle null data and data of the wrong type automatically.

lab.py
::

    .
    .
    .

    def data_features():
        return [
            Feature(key="id",
                    lam=lambda x: x["id"],
                    dtype=str),

            Feature(key="rank",
                    lam=lambda x: x["rank"],
                    dtype=int),

            Feature(key="symbol",
                    lam=lambda x: x["symbol"],
                    dtype=str),

            Feature(key="name",
                    lam=lambda x: x["name"],
                    dtype=str),

            Feature(key="price_usd",                    # Keys support renaming
                    lam=lambda x: x["priceUsd"],
                    dtype=float),

            Feature(key="volume_usd_24_hr",
                    lam=lambda x: x["volumeUsd24Hr"],
                    dtype=float)
        ]

    .
    .
    .

**Use the** ``rlab`` **shell command to run the lab**

``path/to/project_dir/> rlab``

Output:

::

    data point 1:
    {'id': 'bitcoin', 'rank': '1', 'symbol': 'BTC', 'name': 'Bitcoin', 'supply': '18698850.0000000000000000', 'maxSupply': '21000000.0000000000000000', 'marketCapUsd': '1
    041388865130.8623213956691350', 'volumeUsd24Hr': '12822561919.6746830356589619', 'priceUsd': '55692.6690748822693051', 'changePercent24Hr': '-4.1033665252363403', 'vw
    ap24Hr': '57708.7312639442977184', 'explorer': 'https://blockchain.info/'}

    data point 2:
    {'id': 'ethereum', 'rank': '2', 'symbol': 'ETH', 'name': 'Ethereum', 'supply': '115729464.3115000000000000', 'maxSupply': None, 'marketCapUsd': '376411190202.66581272
    13330461', 'volumeUsd24Hr': '17656637086.6618270054805080', 'priceUsd': '3252.5095699873722881', 'changePercent24Hr': '6.4420494833790460', 'vwap24Hr': '3234.41835079
    37765772', 'explorer': 'https://etherscan.io/'}

    data point 3:
    {'id': 'binance-coin', 'rank': '3', 'symbol': 'BNB', 'name': 'Binance Coin', 'supply': '153432897.0000000000000000', 'maxSupply': '170532785.0000000000000000', 'marke
    tCapUsd': '98431624817.6777436959489247', 'volumeUsd24Hr': '254674805.8210425908376882', 'priceUsd': '641.5288164550379551', 'changePercent24Hr': '1.1504585233985471'
    , 'vwap24Hr': '653.0516845642682435', 'explorer': 'https://etherscan.io/token/0xB8c77482e45F1F44dE1745F52C74426C631bDD52'}

    data:
                    id  rank symbol            name     price_usd    volume_usd_24_hr      timestamp
    0          bitcoin     1    BTC         Bitcoin  55692.669075  12822561919.674683  1620100433183
    1         ethereum     2    ETH        Ethereum    3252.50957  17656637086.661827  1620100433183
    2     binance-coin     3    BNB    Binance Coin    641.528816    254674805.821043  1620100433183
    3              xrp     4    XRP             XRP      1.461734   1969092162.016667  1620100433183
    4         dogecoin     5   DOGE        Dogecoin      0.419828   2694025432.110168  1620100433183
    ..             ...   ...    ...             ...           ...                 ...            ...
    95       abbc-coin    96   ABBC       ABBC Coin      0.755244       355316.252287  1620100433183
    96          status    97    SNT          Status      0.169848      5966843.243043  1620100433183
    97             nxm    98    NXM             NXM     90.764252      7577199.874023  1620100433183
    98  ocean-protocol    99  OCEAN  Ocean Protocol      1.357968      9131449.423728  1620100433183
    99           iotex   100   IOTX           IoTeX      0.057802       576658.038699  1620100433183

    [100 rows x 7 columns]

    data dtypes:
    id                   string
    rank                  Int64
    symbol               string
    name                 string
    price_usd           Float64
    volume_usd_24_hr    Float64
    timestamp             Int64
    dtype: object

Performing post-processing on data
----------------------------------

**Perform post-processing on data by writing the code for the** ``post_process`` **function**

.. note::

    Post-processors can be applied to alter the data, or extract new information from the data.

lab.py
::

    .
    .
    .

    import time
    def post_process():
        """
        In this example we will add a timestamp for the arrival time of the data.
        """
        return [
            ColumnAdd(key="arrival_time",
                      lam=lambda: int(time.time()*1000),
                      dtype=int
                      )
        ]

    .
    .
    .

**Use the** ``rlab`` **shell command to run the lab**

``path/to/project_dir/> rlab``

Output:

::

    data point 1:
    {'id': 'bitcoin', 'rank': '1', 'symbol': 'BTC', 'name': 'Bitcoin', 'supply': '18698850.0000000000000000', 'maxSupply': '21000000.0000000000000000', 'marketCapUsd': '1
    041388865130.8623213956691350', 'volumeUsd24Hr': '12822561919.6746830356589619', 'priceUsd': '55692.6690748822693051', 'changePercent24Hr': '-4.1033665252363403', 'vw
    ap24Hr': '57708.7312639442977184', 'explorer': 'https://blockchain.info/'}

    data point 2:
    {'id': 'ethereum', 'rank': '2', 'symbol': 'ETH', 'name': 'Ethereum', 'supply': '115729464.3115000000000000', 'maxSupply': None, 'marketCapUsd': '376411190202.66581272
    13330461', 'volumeUsd24Hr': '17656637086.6618270054805080', 'priceUsd': '3252.5095699873722881', 'changePercent24Hr': '6.4420494833790460', 'vwap24Hr': '3234.41835079
    37765772', 'explorer': 'https://etherscan.io/'}

    data point 3:
    {'id': 'binance-coin', 'rank': '3', 'symbol': 'BNB', 'name': 'Binance Coin', 'supply': '153432897.0000000000000000', 'maxSupply': '170532785.0000000000000000', 'marke
    tCapUsd': '98431624817.6777436959489247', 'volumeUsd24Hr': '254674805.8210425908376882', 'priceUsd': '641.5288164550379551', 'changePercent24Hr': '1.1504585233985471'
    , 'vwap24Hr': '653.0516845642682435', 'explorer': 'https://etherscan.io/token/0xB8c77482e45F1F44dE1745F52C74426C631bDD52'}

    finalized data:
                    id  rank symbol            name     price_usd    volume_usd_24_hr      timestamp   arrival_time
    0          bitcoin     1    BTC         Bitcoin  55692.669075  12822561919.674683  1620100433183  1620104839526
    1         ethereum     2    ETH        Ethereum    3252.50957  17656637086.661827  1620100433183  1620104839526
    2     binance-coin     3    BNB    Binance Coin    641.528816    254674805.821043  1620100433183  1620104839526
    3              xrp     4    XRP             XRP      1.461734   1969092162.016667  1620100433183  1620104839526
    4         dogecoin     5   DOGE        Dogecoin      0.419828   2694025432.110168  1620100433183  1620104839526
    ..             ...   ...    ...             ...           ...                 ...            ...            ...
    95       abbc-coin    96   ABBC       ABBC Coin      0.755244       355316.252287  1620100433183  1620104839526
    96          status    97    SNT          Status      0.169848      5966843.243043  1620100433183  1620104839526
    97             nxm    98    NXM             NXM     90.764252      7577199.874023  1620100433183  1620104839526
    98  ocean-protocol    99  OCEAN  Ocean Protocol      1.357968      9131449.423728  1620100433183  1620104839526
    99           iotex   100   IOTX           IoTeX      0.057802       576658.038699  1620100433183  1620104839526

    [100 rows x 8 columns]

    finalized data dtypes:
    id                   string
    rank                  Int64
    symbol               string
    name                 string
    price_usd           Float64
    volume_usd_24_hr    Float64
    timestamp             Int64
    arrival_time          Int64
    dtype: object

Exporting data from the lab to a collector
------------------------------------------

.. note::

    **Once the lab has been used to build the form fields for an ApiForm, move the data to the collector**

    It is not necessary to use the lab feature of the library to perform data-extraction, it just makes
    things a bit easier.

**Move the code from** ``lab.py`` **to** ``coincap.py``

coincap.py

::

    .
    .
    .

    import requests
    import logging
    import time


    def coincap_import():
        data = None
        url = "https://api.coincap.io/v2/assets/"
        try:
            data = [requests.get(url).json()]
        except Exception as e:
            logging.exception(e)
        return data

    def coincap_form():
        pre_process = [
            GlobalExtract(key="timestamp",
                          lam=lambda x: x["timestamp"],
                          dtype=int
                          ),

            ListExtract(lam=lambda x: x["data"])
        ]

        data_features = [
            Feature(key="id",
                    lam=lambda x: x["id"],
                    dtype=str),

            Feature(key="rank",
                    lam=lambda x: x["rank"],
                    dtype=int),

            Feature(key="symbol",
                    lam=lambda x: x["symbol"],
                    dtype=str),

            Feature(key="name",
                    lam=lambda x: x["name"],
                    dtype=str),

            Feature(key="price_usd",           # Keys support renaming
                    lam=lambda x: x["priceUsd"],
                    dtype=float),

            Feature(key="volume_usd_24_hr",
                    lam=lambda x: x["volumeUsd24Hr"],
                    dtype=float)
        ]

        post_process = [
            ColumnAdd(key="arrival_time",
                      lam=lambda: int(time.time()*1000),
                      dtype=int
                      )
        ]

        return ApiForm(name="coincap", pre_process=pre_process, data_features=data_features, post_process=post_process)

    .
    .
    .

**Once the lab has been moved over, you can optionally run the** ``clab`` **shell command to delete the lab**

Setting up an authentication file for database targets
------------------------------------------------------

    1. Create a JSON file in the AUTH directory

    2. Copy the template for the database target you wish to use

    3. Fill out the template

Setting up a stream target for live data
----------------------------------------

**The following code will set up live streaming both to a local file location, and to a MySQL database**

coincap.py
::

    .
    .
    .

    def coincap_streams():
        """
        In this example, we will stream data live into a local file, and directly into a MySQL database.
        """
        streams = [
            Stream2Local(name="coincap",
                         path="STORE/coincap/live"
                         ),

            Stream2Sql(name="coincap",
                       auth_path="AUTH/mysql_auth.json",
                       db_name="stream_coincap",
                       dialect="mysql",
                       port="3306"
                       )
        ]
        return streams

    .
    .
    .

**Yes it is that easy, no you do not have to build the tables.**

Setting up a store target for data
----------------------------------

**The following will set up a storage target that will pull data from** ``STORE/coincap/live`` **and store it to a MariaDB database periodically**

coincap.py
::

    .
    .
    .

    def coincap_stores():
        """
        In this example, we will store data every 10 minutes to a MariaDB database.
        The files we store will then be composed into a single file, and stored in a different storage location.
        """
        stores = [
            Store2Sql(name="coincap",
                      seconds=600,
                      path="STORE/coincap/live",
                      db_name="store_coincap",
                      auth_path="AUTH/mariadb_auth.json",
                      port="3306",
                      dialect="mariadb",
                      move_composed_path="STORE/coincap/ten_minute_intervals/"
            )
        ]
        return stores

    .
    .
    .

Registering a collector to run
------------------------------

**To register a collector, all that needs to be done is set the import frequency by changing the** ``seconds`` **parameter**

coincap.py
::

    .
    .
    .

    coincap_info = Collector(name="coincap",
                            seconds=30,                       # Import data from the API every 30 seconds
                            import_target=coincap_import,
                            api_form=coincap_form,
                            streams=coincap_streams,
                            stores=coincap_stores,
                            debug=True                        # Set to False for production
                            )

    .
    .
    .

Running the application
-----------------------

**Run main.py**

Info Log Outputs:

::

    2021-05-04 01:01:14 stream.py                 INFO  stream starting -> (local.parquet)
    2021-05-04 01:01:14 stream.py                 INFO  stream starting -> (sql.mysql)
    2021-05-04 01:01:14 api2db.py                 INFO  import scheduled: [30 seconds] (api request data) -> (streams)
    2021-05-04 01:01:14 api2db.py                 INFO  storage refresh scheduled: [30 seconds] -> (check stores)
    2021-05-04 01:01:15 api2db.py                 INFO  storage scheduled: [600 seconds] (STORE/coincap/live) -> (store)
    2021-05-04 01:01:15 stream2sql.py             INFO  establishing connection to mysql://***/stream_coincap
    2021-05-04 01:01:15 stream2sql.py             INFO  database not found mysql://***.com/stream_coincap... creating database
    2021-05-04 01:01:15 stream2sql.py             INFO  connection established mysql://***/stream_coincap
    2021-05-04 01:01:25 store.py                  INFO  storage files composed, attempting to store 3600 rows to mariadb://***/store_coincap
    2021-05-04 01:01:25 stream2sql.py             INFO  establishing connection to mariadb://***/store_coincap
    2021-05-04 01:01:25 stream2sql.py             INFO  database not found mariadb://***/store_coincap... creating database
    2021-05-04 01:01:25 stream2sql.py             INFO  connection established mariadb://***/store_coincap


Debug Log Outputs:

::

    .
    .
    .
    2021-05-04 01:01:24 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    2021-05-04 01:01:24 stream2local.py           DEBUG storing 100 rows to STORE/coincap/live
    2021-05-04 01:01:24 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    2021-05-04 01:01:25 stream2local.py           DEBUG storing 100 rows to STORE/coincap/live
    2021-05-04 01:01:25 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    2021-05-04 01:01:25 stream2sql.py             DEBUG 3600 rows inserted into mariadb://***/store_coincap
    2021-05-04 01:01:25 stream2local.py           DEBUG storing 100 rows to STORE/coincap/live
    2021-05-04 01:01:25 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    2021-05-04 01:01:26 stream2local.py           DEBUG storing 100 rows to STORE/coincap/live
    2021-05-04 01:01:26 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    2021-05-04 01:01:26 stream2local.py           DEBUG storing 100 rows to STORE/coincap/live
    2021-05-04 01:01:26 stream2sql.py             DEBUG 100 rows inserted into mysql://***/stream_coincap
    .
    .
    .

