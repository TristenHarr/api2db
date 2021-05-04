# -*- coding: utf-8 -*-
"""
Contains the clist function
===========================
"""
import os


def _clist():
    clist()


def clist() -> None:
    """
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

    Returns:
        None
    """
    files = os.listdir(os.path.join(os.getcwd(), "apis"))
    files.remove("__init__.py")
    files = [f.replace(".py", "") for f in files]
    print(files)
