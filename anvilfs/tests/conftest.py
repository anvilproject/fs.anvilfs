import os.path

import pytest
import yaml


conf = {}

conf_file = "config.yml"

path = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
try:
    with open(path + "/" + conf_file) as f:
        conf = yaml.safe_load(f.read())
except FileNotFoundError as fnfe:
    print("Configuration file not found -- are you sure it exists?")
    raise fnfe
except yaml.scanner.ScannerError as se:
    print("Configuration file is not valid yaml")
    raise se


@pytest.fixture(scope="session")
def valid_gs_info():
    return conf["anvil_info"]
