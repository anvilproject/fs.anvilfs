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
        if not self._refs[ref]:
            # if it's user's personal workspace use their project
            if self.workspace_project:
                self.base_project = self.workspace_project
                self._refs[ref] = self._ref_inits[ref](project=self.base_project)
            # otherwise use whatever firecloud is using
            elif self.base_project:
                self._refs[ref] = self._ref_inits[ref](project=self.base_project)
            else:
                raise Exception(f"Should have found a project in:\n\t{self.base_project}\n\t{self.workspace_project}")
        return self._refs[ref]

    @classmethod
    def get_default_gcs_client(cls):
        return cls.__getattr__(cls, 'gc_storage_client')

    def get_fapi_creds(self):
        try:
            sesh = self.fapi.__getattribute__("__SESSION")
        except AttributeError:
            self.fapi._set_session()
        if not sesh or not sesh.credentials.valid:
            self.fapi._set_session()
        return self.fapi.__getattribute__("__SESSION").credentials

    def get_fapi_token(self):
        try:
            sesh = self.fapi.__getattribute__("__SESSION")
        except AttributeError:
            self.fapi._set_session()
        if not sesh or not sesh.credentials.valid:
            self.fapi._set_session()
        return self.fapi.__getattribute__("__SESSION").credentials.token
