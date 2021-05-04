# -*- coding: utf-8 -*-
"""
Contains the pclear function
============================

WARNING:
    Usage of this will completely clear out the project directory. This includes all collectors, all code, and
    all files. This is a nuclear delete option for when your foo doesn't want to bar and so you need to start over.
    **Use with caution.**
"""
import os


def _pclear():
    pclear()


def pclear() -> None:
    """
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

    Returns:
        None
    """
    clear = input(f"WARNING: ALL ITEMS IN DIRECTORY {os.getcwd()} WILL BE DELETED. Continue? (y/n) ")
    if clear != "y":
        return
    sure = input(f"Are you sure you want to delete all items from {os.getcwd()}? This cannot be undone. (y/n) ")
    if sure != "y":
        return
    for root, dirs, files in os.walk(os.getcwd(), False):
        for f in files:
            os.remove(os.path.join(root, f))
        for d in dirs:
            os.rmdir(os.path.join(root, d))
