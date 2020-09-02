from fs.enums import ResourceType
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected
from fs.info import Info

class BaseAnVILResource:
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
    
    def get_bytes_handler(self):
        raise NotImplementedError("Method get_bytes_handler() not implemented")

class BaseAnVILFolder(BaseAnVILResource):
    def __init__(self, name, last_modified=None):
        if name[-1] != "/": # required since anvil supports names the same as their containing directories
            self.name = name + "/"
        else:
            self.name = name
        self.last_modified = last_modified
        self.children = {}

    def __hash__(self):
        return hash((self.name, self.__class__.__name__, self.last_modified))

    def __eq__(self, other):
        return (self.name, self.last_modified) == (other.name, other.last_modified)

    # allow dictionary-style access, with possible objs as keys
    def __getitem__(self, key):
        return self.children[key]

    # allow for <item> in :
    def __iter__(self):
        return iter(self.children)

    def keys(self):
        def sorter(k):
            if k[-1] == "/":
                k = "0"+k
            return k
        return sorted([k for k in self.children.keys()], key=sorter)

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