import os.path

import pytest
import yaml
import json

import firecloud.api as fapi
from google.cloud import storage
from google.api_core.exceptions import NotFound

print("!!!!!!!!!!!!!!!!!!!!!!!!!")

conf = {}

# configuration metadata
conf_file = "config.yml"
attr_file = "test_attribs.json"
# test bucket file
bckt_file = "testfiles/text.txt"

# get yaml config file
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

try:
    with open(path + "/" + attr_file) as f:
        attr = json.loads(f.read())
except FileNotFoundError as fnfe:
    print("Attributes file not found")
    raise fnfe


# ensure presence of anvil test workspace
class TestWorkspaceManager:
    WORKSPACE_NAME = "anvilfs_disposable_test_workspace"
    NAMESPACE_NAME = conf["anvil_info"]["Namespace"]

    ATTRIBUTES = attr
    min_attrs = {"a": "1"}

    def __init__(self, ws_name=None, ns_name=None):
        if not ws_name:
            ws_name = self.WORKSPACE_NAME
        if not ns_name:
            ns_name = self.NAMESPACE_NAME
        print(f"initializing test workspace {ns_name}>{ws_name}")
        wsbucket = self.check_workspaces()
        if wsbucket:
            print(f"{ws_name} found, deleting existing workspace")
            self.delete_workspace(wsbucket)
        bucketname = self.make_workspace()


    def check_workspaces(self):
        fields = "workspace.name,workspace.bucketName"
        wspaces = fapi.list_workspaces(fields=fields).json()
        for ws in wspaces:
            if ws["workspace"]["name"] == self.WORKSPACE_NAME:
                return ws["workspace"]["bucketName"]
        return False

    def make_workspace(self):
        print(f"making workspace:\n\tns: {self.NAMESPACE_NAME}\n\tws: {self.WORKSPACE_NAME}\n\tattrs: {self.min_attrs}")
        r = fapi.create_workspace(
            self.NAMESPACE_NAME, self.WORKSPACE_NAME, attributes=self.ATTRIBUTES)
        self.populate_bucket(r.json()["bucketName"])

    def populate_bucket(self, bucket):
        print(f"adding {bckt_file.split('/')[-1]} to {bucket}")
        bucket = storage.Client().bucket(bucket)
        blob = bucket.blob(bckt_file.split("/")[-1])
        blob.upload_from_filename(path + "/" + bckt_file)

    def depopulate_bucket(self, bucket):
        print(f"removing {bckt_file.split('/')[-1]} from {bucket}...", end="")
        bucket = storage.Client().bucket(bucket)
        blob = bucket.blob(bckt_file.split("/")[-1])
        try:
            blob.delete()
        except NotFound as nf:
            print("...file not found! continuing.")
            return
        print("...removed!")

    def delete_workspace(self, bucket):
        print(f"deleting {self.WORKSPACE_NAME}")
        fapi.delete_workspace(self.NAMESPACE_NAME, self.WORKSPACE_NAME)
        self.depopulate_bucket(bucket)


#extract info from config
@pytest.fixture(scope="session")
def valid_gs_info():
    return conf["anvil_info"]

@pytest.fixture(scope="session")
def dummy_attributes():
    return {
      "referenceData_twoGS_links": {
        "itemsType": "AttributeValue",
        "items": [
          "gs://dummybucket/foo/bar/filename.extension",
          "gs://dummybucket/foo/bar/filename2.extension"
        ]
      },
      "referenceData_oneGS_link": "gs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta",
      "nonreference_cruft": "gs://i/should/not/be/listed",
      "referenceData_twoDRS_links": {
        "itemsType": "AttributeValue",
        "items": [
          "drs://dummybucket/foo/bar/filename.extension",
          "drs://dummybucket/foo/bar/filename2.extension"
        ]
      },
      "referenceData_oneDRS_link": "drs://gcp-public-data--broad-references/hg38/v0/Homo_sapiens_assembly38.fasta",
      "bork": "1",
      "tag:tags": {
        "itemsType": "AttributeValue",
        "items": [
          "hey im a tag"
        ]
      }
    }