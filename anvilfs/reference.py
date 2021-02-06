from .basefile import BaseAnVILFile
from .basefolder import BaseAnVILFolder
from .google import GoogleAnVILFile

import gs_chunked_io as gscio


class ReferenceDataFolder(BaseAnVILFolder):
    def __init__(self, name, refs):
        super().__init__(name)
        self.refs = refs

    def lazy_init(self):
        self.init_references(self.refs)

    def init_references(self, refs):
        for source in refs:
            # source, e.g. hg38
            source_folder = RefereneDataSubfolder(source+"/", refs[source])
            self[source_folder.name] = source_folder
            # reftype, e.g. axiomPoly_resource_vcf


class RefereneDataSubfolder(BaseAnVILFolder):
    def __init__(self, name, refs_source={}):
        super().__init__(name)
        self.refs_source = refs_source

    def lazy_init(self):
        for reftype in self.refs_source:
            reftype_folder = RefereneDataSubfolder(reftype+"/")
            self[reftype_folder.name] = reftype_folder
            contents = ReferenceDataFile.make_rdfs(
                self.refs_source[reftype])
            for c in contents:
                reftype_folder[c.name] = c


class ReferenceDataFile(BaseAnVILFile):
    def __init__(self, blob):
        blob.reload()
        self.blob_handle = blob
        self.name = blob.name.split("/")[-1]
        self.last_modified = blob.updated
        self.size = blob.size
        self.is_dir = False

    @classmethod
    def make_rdfs(cls, objs):
        return GoogleAnVILFile.factory(objs)

    def get_bytes_handler(self):
        return gscio.Reader(self.blob_handle)
