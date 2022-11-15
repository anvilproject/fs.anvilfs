import getm
import requests

from .basefile import BaseAnVILFile


class HypertextAnVILFile(BaseAnVILFile):
    def __init__(self, url, name=None, size=None, last_modified=None):
        print(f"hypertext URL received: {url}")
        self.url = url
        if not name:
            self.name = url.split("/")[-1]
        if not size or not last_modified:
            r = requests.head(url)
            try:
                self.size = r.headers["Content-Length"]
            except KeyError:
                self.size = 1
            try:
                self.last_modified = r.headers["Last-Modified"]
            except KeyError:
                self.last_modified = ""

    def get_bytes_handler(self):
        return getm.urlopen(self.url)

    @classmethod
    def factory(cls, urllist):
        results = []
        for item in urllist:
            results.append(HypertextAnVILFile(item))
        return results
