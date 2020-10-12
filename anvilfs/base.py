from io import BytesIO
from os import SEEK_END, SEEK_SET

from fs.enums import ResourceType
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected
from fs.info import Info

import firecloud.api as fapi
from google.cloud import storage
from google.cloud import bigquery


class ClientRepository:
    base_project = None

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
            self._refs[ref] = self._ref_inits[ref](project = self.base_project)
        return self._refs[ref]
    
    def get_fapi_token(self):
        try:
            sesh = self.fapi.__getattribute__("__SESSION")
        except AttributeError as ae:
            self.fapi._set_session()
        if not sesh or not sesh.credentials.valid:
            self.fapi._set_session()
        return self.fapi.__getattribute__("__SESSION").credentials.token


class BaseAnVILResource(ClientRepository):
    def getinfo(self):
        raise NotImplementedError("Method getinfo() not implemented")
    
    def __hash__(self):
        return hash((self.name, self.__class__.__name__))
    
    def __eq__(self, other):
        return self.getinfo().raw == other.getinfo().raw
    
    def __str__(self):
        return "<{}: {}>".format(self.__class__.__name__, self.name)

    def __ne__(self, other):
        return not(self == other)


class BaseAnVILFile(BaseAnVILResource):
    def __init__(self, name, size, last_modified=None):
        self.name = name
        self.size = size
        self.last_modified = last_modified

    def getinfo(self):
        result = {
            "basic": {
                "name": self.name,
                "is_dir": False,
            },
            "details": {
                "type": ResourceType.file,
                "size": self.size,
                "modified": self.last_modified
            }
        }
        return Info(result)

    def string_to_buffer(self,string):
        buffer = BytesIO(string.encode('utf-8'))
        position = buffer.tell()
        buffer.seek(0, SEEK_END)
        self.size = buffer.tell()
        buffer.seek(position, SEEK_SET)
        return buffer

    def get_bytes_handler(self):
        raise NotImplementedError("Method get_bytes_handler() not implemented")

class BaseAnVILFolder(BaseAnVILResource):
    def __init__(self, name, last_modified=None):
        print(f"super {self.__class__.__name__}.{name}")
        self.initialized = False
        if name[-1] != "/": # required since anvil supports names the same as their containing directories
            self.name = name + "/"
        else:
            self.name = name
        self.last_modified = last_modified
        self.children = {}
    
    def lazily_init(fn):
        def lazywrapper(*args, **kwargs):
            self = args[0]
            print(f"lazy init {self.__class__.__name__}.{self.name}.{fn.__name__}({args},{kwargs})? {self.initialized}")
            if not self.initialized:
                self.lazy_init()
                self.initialized = True
            return fn(*args, **kwargs)
        return lazywrapper


    def lazy_init(self):
        raise NotImplementedError(f"{self.__class__.__name__}.lazy_init method is abstract and must be specified")

    def __hash__(self):
        return hash((self.name, self.__class__.__name__, self.last_modified))

    def __eq__(self, other):
        return (self.name, self.last_modified) == (other.name, other.last_modified)

    # allow dictionary-style access, with possible objs as keys
    @lazily_init
    def __getitem__(self, key):
        print(f"getitem: {key}")
        return self.children[key]

    # allow for <item> in :
    def __iter__(self):
        return iter(self.children)

    @lazily_init
    def keys(self):
        print(f"gettin keys for {self.name}...")
        def sorter(k):
            if k[-1] == "/":
                k = "0"+k
            return k
        print(f"CKs: {self.children.keys()}")
        return sorted([k for k in self.children.keys()], key=sorter)

    @lazily_init
    def get_object_from_path(self, path):
        if path == "/" or path == "":
            return self
        #internally, using google-style no initial slash
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