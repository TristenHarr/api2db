# -*- coding: utf-8 -*-
"""
Contains the crem function
==========================
"""
import sys
import os


def _crem():
    rem = None if len(sys.argv) == 1 else sys.argv[1]
    crem(rem)


def crem(rem: str) -> None:
    """
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

    Args:
        rem: The name of the collector to remove

    Returns:
        None
    """
    if rem is None:
        raise ValueError("You must specify a collector to remove.")
    clear = input(f"WARNING: ALL FILES ASSOCIATED WITH COLLECTOR {rem} WILL BE DELETED. Continue? (y/n) ")
    if clear != "y":
        return
    sure = input(f"Are you sure you want to delete all items associated with {rem}? This cannot be undone. (y/n) ")
    if sure != "y":
        return
    py_path = os.path.join(os.getcwd(), "apis", f"{rem}.py")
    if os.path.isfile(py_path):
        os.remove(py_path)
    init_path = os.path.join(os.getcwd(), "apis", "__init__.py")
    if os.path.isfile(init_path):
        lines = []
        with open(init_path, "r") as f:
            for line in f.readlines():
                if line == f"from ..apis.{rem} import {rem}_info\n":
                    pass
                elif line.find("apis = [") == 0:
                    lines.append(line.replace(f"{rem}_info, ", "").replace(f"{rem}_info", "").replace(", ]", "]"))
                else:
                    lines.append(line)
        with open(init_path, "w") as f:
            f.writelines(lines)
