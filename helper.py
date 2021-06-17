"""
Author: Adrian Haerle
Date 17.06.2021
"""
from configparser import ConfigParser

def get_config_param(filepath_ini="/DUMMY.ini", section="DUMMY"):
    """
    Gets database connection parameters saved in the respective .ini file.

    :param filepath_ini: filepath of .ini file that contains relevant connection details
    :param section: section within .ini file that should be returns
    :return: dict containing connection parameters
    """
    parser = ConfigParser()
    parser.read(filepath_ini)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception("Section {0} not found in the {1} file".format(section, filepath_ini))
    return db
