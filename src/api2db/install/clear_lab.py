import os


def clab():
    """
    This shell command is used to clear a lab.

    ::

        project_dir-----/
                        |
                        apis-------/
                        |          |- __init__.py
                        |          |- FooCollector.py
                        |          |- BarCollector.py
                        |
                        AUTH-------/
                        |          |- bigquery_auth_template.json
                        |          |- omnisci_auth_template.json
                        |          |- sql_auth_template.json
                        |
                        CACHE/
                        |
                        STORE/
                        |
                        laboratory-/
                        |          |- lab.py    EDIT THIS FILE!
                        |
                        helpers.py
                        |
                        main.py

    **Shell Command:** ``path/to/project_dir> clab``

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


    """
    lab_dir_path = os.path.join(os.getcwd(), "laboratory")
    if not os.path.isdir(lab_dir_path):
        print("No lab exists... skipping")
        return
    clear = input(f"WARNING: ALL ITEMS IN DIRECTORY {lab_dir_path} WILL BE DELETED. Continue? (y/n) ")
    if clear != "y":
        return
    sure = input(f"Are you sure you want to delete all items from {lab_dir_path}? This cannot be undone. (y/n) ")
    if sure != "y":
        return
    for root, dirs, files in os.walk(lab_dir_path, False):
        for f in files:
            os.remove(os.path.join(root, f))
        for d in dirs:
            os.rmdir(os.path.join(root, d))
    os.rmdir(lab_dir_path)
