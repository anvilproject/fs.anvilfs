from io import BytesIO
from time import sleep

from google.cloud import bigquery

from .base import BaseAnVILFile, BaseAnVILFolder

# terra 'entities' represent tables
# #TODO: refactor to use entity types for lazy load
class TablesFolder(BaseAnVILFolder):
    def __init__(self):
        self.name = "Tables/"


class TableDataCohort(BaseAnVILFile):
    def lazybqclient(fn):
        def wrapper(*args, **kwargs):
            self = args[0]
            if self.bqclient is None:
                self.bqclient = bigquery.Client()
            return fn(*args, **kwargs)
        return wrapper

    def __init__(self, name, query):
        self.name = name
        self.size = 1 # some placeholder? cant lazy load AND init with total size of results
        self.query = query
        self.bqclient = None

    @lazybqclient
    def get_bytes_handler(self):
        job = self.bqclient.query(self.query)
        r = job.result()
        schema_keys = [e.name for e in r.schema]
        string = '\t'.join(schema_keys)
        for row in r:
            string += "\n" + '\t'.join(row.values())
        return self.string_to_buffer(string)
