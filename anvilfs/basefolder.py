from fs.enums import ResourceType
from fs.info import Info

from .baseresource import BaseAnVILResource
from .google import GoogleAnVILFile, DRSAnVILFile


class BaseAnVILFolder(BaseAnVILResource):
    def __init__(self, name, last_modified=None):
        self.initialized = False
        # required as anvil supports files that have same name as directory
        if name[-1] != "/":
            self.name = name + "/"
        else:
            self.name = name
        self.last_modified = last_modified
        self.children = {}

    def lazily_init(fn):
        def lazywrapper(*args, **kwargs):
            self = args[0]
            if not self.initialized:
                self.lazy_init()
                self.initialized = True
            return fn(*args, **kwargs)
        return lazywrapper

    def lazy_init(self):
        raise NotImplementedError(
            f"{self.__class__.__name__}.lazy_init method is abstract")

    def __hash__(self):
        return hash((self.name, self.__class__.__name__, self.last_modified))

    def __eq__(self, other):
        return (self.name, self.last_modified) == (
            other.name, other.last_modified)

    # allow dictionary-style access, with possible objs as keys
    @lazily_init
    def __getitem__(self, key):
        return self.children[key]

    # allow for <item> in :
    def __iter__(self):
        return iter(self.children)

    @lazily_init
    def keys(self):
        def sorter(k):
            if k[-1] == "/":
                k = "0"+k
            return k
        return sorted([k for k in self.children.keys()], key=sorter)

    @lazily_init
    def get_object_from_path(self, path):
        if path == "/" or path == "":
            return self
        # internally, using google-style no initial slash
        if path[0] == "/":
            path = path[1:]
        # if path represents a folder:
        if path[-1] == "/":
            split = path[:-1].split("/")
            for i in range(len(split)):
                split[i] += "/"
        else:
            split = path.split("/")
            for i in range(len(split[:-1])):
                split[i] += "/"
        base_obj = self
        for component in split:
            base_obj = base_obj[component]
        return base_obj

    def __setitem__(self, key, val):
        self.children[key] = val

    def getinfo(self):
        _result = {
            "basic": {
                "name": self.name,
                "is_dir": True,
            },
            "details": {
                "type": ResourceType.directory,
                "modified": self.last_modified
            }
        }
        return Info(_result)

    def is_linkable_file(self, fname):

        protocol = fname.split("://")[0]
        filename = fname.split("/")[-1]
        allowed_protocols = {
            "gs": GoogleAnVILFile,
            "drs": DRSAnVILFile,
        }
        blocked_file_prefixes = [
            "data-explorer?"
        ]
        if protocol in allowed_protocols:
            for bfp in blocked_file_prefixes:
                if filename.startswith(bfp):
                    return None
            return allowed_protocols[protocol]
        else:
            return None
