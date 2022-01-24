import string
from tkinter.tix import INTEGER
import firecloud.api as fapi
from datetime import datetime

class WorkspaceAttributes:
    # NOTES
    # References:
    #   keys: "referenceData_<table name>_<key>"
    #   value: either string gs:// uri or list via dict:
    #   {
    #     "itemsType": "AttributeValue",
    #     "items": [
    #       "gs://uri",
    #       "gs://uri"
    #     ]}       

    def __init__(self, tags=[], kv_pairs={}):
        # key/value pairs
        #  - include gs:// and drs://
        # reference data
        pass

class WorkspaceEntities:
    #
    def __init__(self):
        pass


class AnVILTestWorkspace:
    test_attributes = {
        "a":"b",
        "0":"1"
    }
    def __init__(self):
        # generate timestamp
        timestamp = datetime.now().strftime("%m_%d_%Y_%H%M%S")
        name = (f"{timestamp}_test_WS")
        self.setup(name)

    def get_billing_project(self):
        for bp in fapi.list_billing_projects().json():
            if bp["creationStatus"] == "Ready" and bp["role"] == "Owner":
                return bp["projectName"]

    def setup(self, name="Test AnVIL Workspace"):
        namespace = self.get_billing_project()
        fapi.create_workspace(
            namespace=namespace,
            name=name,
            attributes=AnVILTestWorkspace.test_attributes
        )

    def teardown(self):
        pass

    def create_anvil_table(self, name, column_titles=[], rows=None):
        title = f"entity:{name}_id" + "".join(["\t"+title for title in column_titles])
        columns = len(column_titles) + 1
        # make sure rows match columns
        if not rows:
            rows = ["test" for x in range(columns)]
        assert len(rows) == columns    
