# -*- coding: utf-8 -*-
"""
Contains the cadd function
==========================
"""
import sys
import os
_api_component = '''from api2db.ingest import *
from api2db.stream import *
from api2db.store import *
from helpers import *

def !name!_import():
    return None

def !name!_form():
    pre_process = [
        # Preprocessing Here
    ]

    data_features = [
        # Data Features Here
    ]

    post_process = [
        # Postproccesing Here
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
                        debug=True                      # Set to False for production
                        )

'''
"""str: Base String used to construct a new collector file"""


def _cadd():
    ad = None if len(sys.argv) == 1 else sys.argv[1]
    cadd(ad)


def cadd(ad: str) -> None:
    """
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

    Args:
        ad: The name of the collector to add.

    Returns:
        None
    """
    if ad is None:
        raise ValueError("You must specify a collector to add.")
    py_path = os.path.join(os.getcwd(), "apis", f"{ad}.py")
    if os.path.isfile(py_path):
        raise AttributeError(f"Collector {ad} already exists.")
    _make_api_component(ad)
    init_path = os.path.join(os.getcwd(), "apis", "__init__.py")
    if os.path.isfile(init_path):
        lines = []
        with open(init_path, "r") as f:
            for line in f.readlines():
                if line.find("apis = [") == 0:
                    lines.append(f"from .{ad} import {ad}_info\n")
                    if "[]" in line:
                        lines.append(line.replace("]", f"{ad}_info]"))
                    else:
                        lines.append(line.replace("]", f", {ad}_info]"))
                else:
                    lines.append(line)
        with open(init_path, "w") as f:
            f.writelines(lines)


def _make_api_component(name: str) -> None:
    if not os.path.isfile(f"apis/{name}.py"):
        comps = _api_component.replace("!name!", name)
        with open(f"apis/{name}.py", "w") as f:
            f.write(comps)
        print(f"File apis/{name}.py Created, template code auto-generated...")
    else:
        print(f"File apis/{name}.py Exists... skipping")
