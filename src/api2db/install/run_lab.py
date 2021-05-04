import os


def rlab():
    """
    This shell command is used to run a lab.
    """
    lab_dir_path = os.path.join(os.getcwd(), "laboratory")
    if not os.path.isdir(lab_dir_path):
        print("No lab exists... run the mlab command to make a lab.")
    os.chdir("laboratory")
    os.system("python lab.py")
