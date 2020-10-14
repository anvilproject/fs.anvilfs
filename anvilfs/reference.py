from io import BytesIO

from .basefile import BaseAnVILFile
from .basefolder import BaseAnVILFolder

class ReferenceDataFolder(BaseAnVILFolder):
    def __init__(self, name, refs):
        super().__init__(name)
        self.refs = refs
    
    def lazy_init(self):
        self.init_references(self.refs)

    def init_references(self, refs):
        for source in refs:
            # source, e.g. hg38
            source_folder = RefereneDataSubfolder(source+"/")
            self[source_folder.name] = source_folder
            # reftype, e.g. axiomPoly_resource_vcf
            for reftype in refs[source]:
                reftype_folder = RefereneDataSubfolder(reftype+"/")
                source_folder[reftype_folder.name] = reftype_folder
                contents = ReferenceDataFile.make_rdfs(refs[source][reftype])
                for c in contents:
                    reftype_folder[c.name] = c

class RefereneDataSubfolder(BaseAnVILFolder):
    def __init__(self, name):
        super().__init__(name)
        self.initialized = True
    
    def lazy_init(self):
        pass

class ReferenceDataFile(BaseAnVILFile):
    def __init__(self, blob):
        self.blob_handle = blob
        self.name = blob.name.split("/")[-1]
        self.last_modified = blob.updated
        self.size = blob.size
        self.is_dir = False

    @classmethod
    def make_rdfs(cls, objs):
        result = []
        for o in objs:
            result.append(cls(objs[o]))
        return result

    def get_bytes_handler(self):
        buffer = BytesIO()
        self.blob_handle.download_to_file(buffer)
        buffer.seek(0)
        return buffer