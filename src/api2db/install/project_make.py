# -*- coding: utf-8 -*-
"""
Contains the pmake function
===========================
"""
import os
import sys
from typing import List, Optional

_main_components = '''from api2db import Run
import apis

if __name__ == "__main__":
    r = Run(apis.apis)
    r.run()

'''
"""str: Base String used to construct the main.py file"""

_api_component = '''from api2db.ingest import *
from api2db.stream import *
from api2db.store import *
from helpers import *

def !name!_import():
    return None

def !name!_form():
    pre_process = [
        # pre_processing here
    ]

    data_features = [
        # data_features here
    ]

    post_process = [
        # post_processing here
    ]

    return ApiForm(name="!name!", pre_process=pre_process, data_features=data_features, post_process=post_process)

def !name!_streams():
    streams = [

    ]
    return streams

def !name!_stores():
    stores = [

    ]
    return stores

!name!_info = Collector(name="!name!",
                        seconds=0,                      # Import frequency of 0 disables collector
                        import_target=!name!_import,
                        api_form=!name!_form,
                        streams=!name!_streams,
                        stores=!name!_stores,
                        debug=True                      # Set to false for production
                        )

'''
"""str: Base String used to construct a new collector file"""

_omni_auth_template = '''{
  "username": "user",
  "password": "password",
  "host": "host"
}
'''
"""str: Base String used to construct the omnisci auth template"""

_sql_auth_template = '''{
  "username": "user",
  "password": "password",
  "host": "host"
}
'''
"""str: Base String used to construct the sql auth template"""

_bigquery_auth_template = '''{
  "type": "service_account -- THIS FILE WILL BE PROVIDED BY GOOGLE!",
  "project_id": "pid",
  "private_key_id": "KEY ID",
  "private_key": "PRIVATE KEY",
  "client_email": "service email",
  "client_id": "id",
  "auth_uri": "auth uri",
  "token_uri": "token",
  "auth_provider_x509_cert_url": "certs",
  "client_x509_cert_url": "cert"
}
'''
"""str: Base String used to construct the bigquery auth template"""


def _pmake():
    apis = sys.argv[1:]
    pmake(apis)


def pmake(apis: Optional[List[str]]=None) -> None:
    """
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

    NOTE:

        This command can also be used without any collector arguments, and collectors can be added using the ``cadd``
        shell command.

    Args:
        apis: The collector names that should be created for the project APIs

    Returns:
        None

    """
    _make_apis_dir()
    _make_cache_dir()
    _make_store_dir()
    _make_logs_dir()
    _make_auth_dir()
    _make_omni_auth_template()
    _make_sql_auth_template()
    _make_bigquery_auth_template()
    _make_helpers()
    _make_apis_init(apis)
    for api in apis:
        _make_api_component(api)
    _make_main()


def _make_apis_dir():
    if not os.path.isdir("apis"):
        os.mkdir("apis")
        print("Directory apis/ Created...")
    else:
        print("Directory apis/ Exists... skipping")


def _make_cache_dir():
    if not os.path.isdir("CACHE"):
        os.mkdir("CACHE")
        print("Directory CACHE/ Created...")
    else:
        print("Directory CACHE/ Exists... skipping")


def _make_logs_dir():
    if not os.path.isdir("LOGS"):
        os.mkdir("LOGS")
        print("Directory LOGS/ Created...")
    else:
        print("Directory LOGS/ Exists... skipping")


def _make_store_dir():
    if not os.path.isdir("STORE"):
        os.mkdir("STORE")
        print("Directory STORE/ Created...")
    else:
        print("Directory STORE/ Exists... skipping")


def _make_auth_dir():
    if not os.path.isdir("AUTH"):
        os.mkdir("AUTH")
        print("Directory AUTH/ Created...")
    else:
        print("Directory AUTH/ Exists... skipping")


def _make_omni_auth_template():
    if not os.path.isfile("AUTH/omnisci_auth_template.json"):
        with open("AUTH/omnisci_auth_template.json", "w") as f:
            f.write(_omni_auth_template)


def _make_sql_auth_template():
    if not os.path.isfile("AUTH/sql_auth_template.json"):
        with open("AUTH/sql_auth_template.json", "w") as f:
            f.write(_sql_auth_template)


def _make_bigquery_auth_template():
    if not os.path.isfile("AUTH/bigquery_auth_template.json"):
        with open("AUTH/bigquery_auth_template.json", "w") as f:
            f.write(_bigquery_auth_template)


def _make_helpers():
    if not os.path.isfile("helpers.py"):
        with open("helpers.py", "w") as _:
            pass
        print("File helpers.py Created...")
    else:
        print("File helpers.py Exists... skipping")


def _make_api_component(name):
    if not os.path.isfile(f"apis/{name}.py"):
        comps = _api_component.replace("!name!", name)
        with open(f"apis/{name}.py", "w") as f:
            f.write(comps)
        print(f"File apis/{name}.py Created, template code auto-generated...")
    else:
        print(f"File apis/{name}.py Exists... skipping")


def _make_apis_init(apis):
    if not os.path.isfile("apis/__init__.py"):
        lines = []
        api_lines = ""
        for api in apis:
            lines.append("from .!name! import !name!_info\n".replace("!name!", api))
            api_lines += f"{api}_info, "
        else:
            api_lines = api_lines.strip(", ")
            lines.append(f"apis = [{api_lines}]\n")
        with open("apis/__init__.py", "w") as f:
            f.writelines(lines)
        print("File apis/__init__.py Created, imports auto-generated...")
    else:
        print("File apis/__init__.py Exists... skipping")


def _make_main():
    if not os.path.isfile("main.py"):
        comps = _main_components.replace("!pname!", os.path.split(os.getcwd())[1])
        with open("main.py", "w") as f:
            f.write(comps)
        print("File main.py Created, code auto-generated...")
    else:
        print("File main.py Exists... skipping")
