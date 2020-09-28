from .base import BaseAnVILFolder
from .namespace import Namespace
from .workspace import Workspace

from fs.base import FS
from fs.errors import DirectoryExpected, ResourceNotFound, FileExpected


class AnVILFS(FS):
    def __init__(self, namespace, workspace):
        super(AnVILFS, self).__init__()
        self.namespace = Namespace(namespace)
        self.workspace = self.namespace.fetch_workspace(workspace)
        self.rootobj = self.workspace # leaving the option to make namespace root

    def getinfo(self, path, namespaces=None):
        return self.rootobj.get_object_from_path(path).getinfo()

    # Get a list of resource names (str) in a directory.
    def listdir(self, path):
        if path == "/" or path == "":
            return self.rootobj.keys()
        try:
            maybe_dir = self.rootobj.get_object_from_path(path)
        except KeyError as ke:
            raise ResourceNotFound("Resource {} not found".format(path))
        if isinstance(maybe_dir, BaseAnVILFolder):
            return maybe_dir.keys()
        else:
            raise DirectoryExpected("{} is not a directory".format(path))

    def scandir(self, path):
        if path[-1] != "/":
            path = path + "/"
        result = []
        l = self.listdir(path)
        for o in l:
            result.append(self.getinfo(path+o))
        return result

    def makedir():# Make a directory.
        raise Exception("makedir not implemented")
    
    def openbin(self, path, mode="r", buffering=-1, **options):
        obj = self.rootobj.get_object_from_path(path)
        try:
            return obj.get_bytes_handler()
        except AttributeError as e:
            raise FileExpected("Error: requested object is not a file:\n  {}".format(path))

    def remove():# Remove a file.
        raise Exception("remove not implemented")
    def removedir():# Remove a directory.
        raise Exception("removedir not implemented")
    def setinfo():# Set resource information.
        raise Exception("setinfo not implemented")
    # for network systems, scandir needed otherwise default calls a combination of listdir and getinfo for each file.