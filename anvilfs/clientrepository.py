import firecloud.api as fapi
from google.cloud import storage
from google.cloud import bigquery


class ClientRepository:
    base_project = None
    workspace_project = None

    _refs = {
        "fapi": fapi,
        "gc_storage_client": None,
        "gc_bigquery_client": None
    }
    _ref_inits = {
        "gc_storage_client": storage.Client,
        "gc_bigquery_client": bigquery.Client
    }

    def __getattr__(self, ref):
        # if client is uninitialized, user isn't owner
        #    and so listed googleProject isn't theirs
        if not self._refs[ref]:
            # get a base project if it wasn't provided
            if not self.base_project:
                self.base_project = self.get_base_project()
            self._refs[ref] = self._ref_inits[ref](project=self.base_project)
        return self._refs[ref]

    def get_base_project(self):
        for bp in self.fapi.list_billing_projects().json():
            if bp["creationStatus"] == "Ready" and bp["role"] == "Owner":
                return bp["projectName"]

    def get_fapi_token(self):
        try:
            sesh = self.fapi.__getattribute__("__SESSION")
        except AttributeError:
            self.fapi._set_session()
        if not sesh or not sesh.credentials.valid:
            self.fapi._set_session()
        return self.fapi.__getattribute__("__SESSION").credentials.token
