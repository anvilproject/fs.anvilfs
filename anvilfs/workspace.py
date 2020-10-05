from io import BytesIO
from os import SEEK_END, SEEK_SET
from os.path import commonprefix
import re

from .base import BaseAnVILFolder, BaseAnVILFile
from .bucket import WorkspaceBucket
from .reference import ReferenceDataFile, ReferenceDataFolder
from .tables import RootTablesFolder, TableDataCohort
from .workloadidentitycredentials import WorkloadIdentityCredentials


class WorkspaceData(BaseAnVILFile):
    def __init__(self, name, data_dict):
        self.name = name
        self.buffer = self._dict_to_buffer(data_dict)
        self.last_modified = None
    
    def _dict_to_buffer(self, d):
        # only keys that match the below regex are valid 
        keys = [k for k in d.keys() if bool(re.match("^[A-Za-z0-9_-]*$", k)) ]
        data = ""
        for k in keys:
            data += f"{k}\t{d[k]}\n"
        buffer = BytesIO(data.encode('utf-8'))
        position = buffer.tell()
        buffer.seek(0, SEEK_END)
        self.size = buffer.tell()
        buffer.seek(position, SEEK_SET)
        return buffer

    def get_bytes_handler(self):
        return self.buffer

class Workspace(BaseAnVILFolder):
    def __init__(self, namespace_reference,  workspace_name):
        self.namespace = namespace_reference
        resp = self.fetch_api_info(workspace_name)
        self.bucket_name = resp["workspace"]["bucketName"]
        attributes = resp["workspace"]["attributes"]
        try:
            super().__init__(workspace_name, resp["workspace"]["lastModified"])
        except KeyError as e:
            print("Error: Workspace fetch_api_info({}) fetch failed".format(workspace_name))
        # Tables folder
        table_baf = RootTablesFolder(self.fetch_entity_info(), self)
        self[table_baf.name] = table_baf
        # bucket folder
        bucket_baf = BaseAnVILFolder("Other Data/")
        self[bucket_baf.name] = bucket_baf
        # ref data folder
        refs = self.ref_extractor(attributes)
        ref_baf = ReferenceDataFolder("Reference Data/", refs)
        self[ref_baf.name] = ref_baf
        # populate workspace data
        workspacedata = dict(attributes)
        blocklist_prefixes = [
            "referenceData_",
            "description"
        ]
        for datum in attributes:
            for blocked in blocklist_prefixes:
                if datum.startswith(blocked):
                    del workspacedata[datum]
        if workspacedata:
            _wsd = WorkspaceData("WorkspaceData.tsv", workspacedata)
            bucket_baf[_wsd.name] = _wsd
        _wsb = WorkspaceBucket(self.bucket_name)
        bucket_baf[_wsb.name] = _wsb

    def ref_extractor(self, attribs):
        # structure:
        # { "source": {
        #      "reftype": {urlstr, blob} }}
        result = {}
        google_buckets = {}
        for ref in [r for r in attribs if r.startswith("referenceData_")]:
            val = attribs[ref]
            refsplit = ref.split("_", 2) 
            source = refsplit[1]
            reftype = refsplit[2]
            if source not in result:
                result[source] = {}
            if reftype not in result[source]:
                result[source][reftype] = {}
            root_result_obj = result[source][reftype]
            if isinstance(val, dict):
                val = val["items"]
            elif isinstance(val, str):
                val = [val]
            for v in val:
                parsed = self.url_parser(v)
                if not parsed:
                    continue
                root_result_obj[v] = None # blob placeholder
                if parsed["schema"] == "gs":
                    if parsed["bucket"] not in google_buckets:
                        google_buckets[parsed["bucket"]] = []
                    google_buckets[parsed["bucket"]].append(v)
                else:
                    raise Exception("Other schemas not yet implemented")
        # determine max shared prefix to limit results from api call
        url_to_blob = {}
        for bucket in google_buckets:
            gs_pfx = f"gs://{bucket}/"
            pfxs = [x[len(gs_pfx):] for x in google_buckets[bucket]]
            prefix = commonprefix(pfxs)
            blobs = self.gc_storage_client.list_blobs(bucket, prefix=prefix)
            for blob in blobs:
                url = gs_pfx + blob.name
                url_to_blob[url] = blob
        # go back and add blobs to result
        for source in result:
            src_vals = result[source]
            for reftype in src_vals:
                refs = src_vals[reftype]
                for url in refs:
                    refs[url] = url_to_blob[url]
        return result

    def url_parser(self, url):
        split = url.split("://", 1)
        # if this is not a url, ignore
        if len(split) == 1:
            return None
        schema = split[0]
        path =  split[1]
        components = path.split("/", 1)
        subcomponents = components[1].split("/")
        return {
            "schema": schema,
            "bucket": components[0],
            "path": components[1],
            "source": subcomponents[0],
            "filename": subcomponents[-1]
        }

    def fetch_api_info(self, workspace_name):
        fields = "workspace.attributes,workspace.bucketName,workspace.lastModified"
        resp = self.fapi.get_workspace(namespace=self.namespace.name, workspace=workspace_name, fields=fields)
        if resp.status_code == 200:
            return resp.json()
        else:
            resp.raise_for_status()

    def fetch_entity_info(self):
        resp = self.fapi.list_entity_types(namespace=self.namespace.name, workspace=self.name)
        if resp.status_code != 200:
            resp.raise_for_status()
        return resp.json()
