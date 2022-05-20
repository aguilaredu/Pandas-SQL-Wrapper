# General utilities used to make database connections easier
from pathlib import Path
import json
import os
import pyodbc

def get_project_root() -> Path:
    """Gets the root directory of this project.

    Returns:
        Path: The filesystem path of the root project
    """
    return Path(__file__).parent.parent



def create_conection(database_name: str) -> pyodbc.Connection:
    """Creates a connection object to a database as defined in secrets/database_config.json file

    Args:
        database_name (str): The name of the database given in the secrets/database_config.json file

    Returns:
        pyodbc.Connection: A Pyodbc connection object
    """
    try:
        ROOT_DIR = get_project_root()
        database_config =  json.load(open(os.path.join(ROOT_DIR, "secrets/database_config.json")))[database_name]
        conn = pyodbc.connect(f'Driver={{{database_config["driver"]}}};'
                            f'Server={database_config["host"]};'
                            f'Database={database_config["database"]};'
                            f'UID={database_config["user"]};'
                            f'PWD={database_config["password"]};')
        return conn
    except:
        raise

