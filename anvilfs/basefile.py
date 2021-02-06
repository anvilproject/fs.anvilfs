from io import BytesIO
from os import SEEK_END, SEEK_SET

from fs.enums import ResourceType
from fs.info import Info

from .baseresource import BaseAnVILResource


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

    def string_to_buffer(self, string):
        buffer = BytesIO(string.encode('utf-8'))
        position = buffer.tell()
        buffer.seek(0, SEEK_END)
        self.size = buffer.tell()
        buffer.seek(position, SEEK_SET)
        return buffer

    def get_bytes_handler(self):
        raise NotImplementedError(
            "Abstract method get_bytes_handler() not implemented")
