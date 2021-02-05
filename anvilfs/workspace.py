from .basefolder import BaseAnVILFolder
from .reference import ReferenceDataFolder
from .tables import RootTablesFolder
from .workspacebucket import OtherDataFolder


class Workspace(BaseAnVILFolder):
    def __init__(self, namespace_name,  workspace_name):
        self.namespace_name = namespace_name
        # collect workspace attributes that inform structure
        resp = self.fetch_api_info(workspace_name)
        self.bucket_name = resp["workspace"]["bucketName"]
        self.attributes = resp["workspace"]["attributes"]
        try:
            super().__init__(
                workspace_name, resp["workspace"]["lastModified"])
        except KeyError:
            print("Error: Workspace fetch_api_info({}) fetch failed".format(
                workspace_name))

    def lazy_init(self):
        if self.initialized:
            print(f"{self.name} already initialized!")
            return
        # Tables folder
        table_baf = RootTablesFolder(self)
        self[table_baf.name] = table_baf
        # bucket folder
        bucket_baf = OtherDataFolder(self.attributes, self.bucket_name)
        self[bucket_baf.name] = bucket_baf
        # ref data folder
        refs = self.ref_extractor(self.attributes)
        ref_baf = ReferenceDataFolder("Reference Data/", refs)
        self[ref_baf.name] = ref_baf

    def ref_extractor(self, attribs):
        # structure:
        # { "source": {
        #      "reftype": [urlstr] }}
        result = {}

        for ref in [r for r in attribs if r.startswith("referenceData_")]:
            # e.g.,
            # referenceData_hg38_known_indels_sites_VCFs
            # source = hg38
            # reftype = known_indels_sites_VCFs
            val = attribs[ref]
            refsplit = ref.split("_", 2)
            source = refsplit[1]
            reftype = refsplit[2]

            # create entries if they dont exist...
            if source not in result:
                result[source] = {}
            if reftype not in result[source]:
                result[source][reftype] = []

            root_result_obj = result[source][reftype]

            # ensure its a list even if its a list of one
            if isinstance(val, dict):
                val = val["items"]
            elif isinstance(val, str):
                val = [val]

            # for every URI/URL...
            for v in val:
                allowed_protocols = [
                    "gs"
                ]
                if v.split("://")[0] in allowed_protocols:
                    root_result_obj.append(v)
        return result

    def fetch_api_info(self, workspace_name):
        fields = ("workspace.attributes,workspace.bucketName,"
                  "workspace.lastModified")
        resp = self.fapi.get_workspace(
            namespace=self.namespace_name, workspace=workspace_name,
            fields=fields)
        if resp.status_code == 200:
            return resp.json()
        else:
            resp.raise_for_status()

    def fetch_entity_info(self):
        resp = self.fapi.list_entity_types(
            namespace=self.namespace_name, workspace=self.name)
        if resp.status_code != 200:
            resp.raise_for_status()
        return resp.json()
