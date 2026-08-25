import firecloud.api as fapi
from google.cloud import storage
from google.cloud import bigquery


class ClientRepository:
    base_project = None
    workspace_project = None
    # workspace facts needed to pick a billing project, populated by AnVILFS;
    # workspace_google_project hosts the bucket regardless of access level,
    # whereas workspace_project is only set when the caller owns it
    workspace_google_project = None
    workspace_access_level = None
    workspace_label = None
    _resolved_billing_project = None

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
            self._refs[ref] = self._ref_inits[ref](
                project=self.resolve_billing_project())
        return self._refs[ref]

    # a classmethod because get_default_gcs_client() reaches __getattr__ with
    # the class itself in place of an instance
    @classmethod
    def resolve_billing_project(cls):
        if not cls._resolved_billing_project:
            ClientRepository._resolved_billing_project = cls._billing_chain()
        return cls._resolved_billing_project

    @classmethod
    def _billing_chain(cls):
        # this project pays for requester-pays access, so prefer projects the
        # caller is entitled to bill against before falling back to the
        # workspace's own project
        if cls.workspace_project:
            return cls.workspace_project
        if cls.base_project:
            return cls.base_project
        owned = cls.find_owned_billing_project()
        if owned:
            return owned
        # a reader or writer with no billing project of their own can still
        # list and read a workspace bucket that is not requester-pays
        if cls.workspace_google_project:
            return cls.workspace_google_project
        raise Exception((
            "AnVILFS: no billing project available for Google Cloud access "
            f"to workspace {cls.workspace_label}\n"
            f"\taccess level: {cls.workspace_access_level}\n"
            f"\tworkspace google project: {cls.workspace_google_project}\n"
            f"\tfirecloud quota project: {cls.base_project}\n"
            "The authenticated account owns no Ready billing project. Ask a "
            "workspace owner for billing access, or set a quota project on "
            "the credentials in use."))

    @staticmethod
    def find_owned_billing_project():
        try:
            response = fapi.list_billing_projects()
            if response.status_code != 200:
                print("AnVILFS: could not list billing projects "
                      f"({response.status_code}): {response.text}")
                return None
            billing_projects = response.json()
        except Exception as e:
            print(f"AnVILFS: could not list billing projects: {e}")
            return None
        for billing_project in billing_projects:
            if billing_project.get("creationStatus", "Ready") != "Ready":
                continue
            # the API has returned both a single 'role' and a 'roles' list
            roles = billing_project.get("roles")
            if roles is None:
                roles = [billing_project.get("role")]
            if "Owner" in roles:
                return billing_project.get("projectName")
        return None

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
