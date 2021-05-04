import os

_lab_components = """from api2db.ingest import *
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

"""



def mlab():
    """
    This shell command is used for creation of a lab. Labs offer an easier way to design an ApiForm.

    Given a project directory

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

    **Shell Command:** ``path/to/project_dir> mlab``

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

    Returns:
        None

    """
    lab_dir_path = os.path.join(os.getcwd(), "laboratory")
    if not os.path.isdir(lab_dir_path):
        os.makedirs(lab_dir_path)
        with open(os.path.join(lab_dir_path, "lab.py"), "w") as f:
            for line in _lab_components:
                f.write(line)
        print("Lab has been created. Edit the file found in laboratory/lab.py")
    else:
        print("Lab already exists!")
