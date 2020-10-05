from io import BytesIO
from time import sleep

from .base import BaseAnVILFile, BaseAnVILFolder
from .google import GoogleAnVILFile, DRSAnVILFile

class TableEntriesFile(BaseAnVILFile):
    def __init__(self, name, itemsdict):
        self.name = name
        self.last_modified = "" # #TODO determine if this is ok
        string = "\t".join(
                [f"{x}" for x in itemsdict]
            )
        for i in range(len(list(itemsdict.values())[0])):
            string += "\n" + "\t".join(
                [f"{itemsdict[x][i]}" for x in itemsdict]
            )
        self.buffstr = string
        self.size = len(string)
    
    def get_bytes_handler(self):
        return self.string_to_buffer(self.buffstr)

# terra 'entities' represent tables
# #TODO: refactor to use entity types for lazy load                    
class TableFolder(BaseAnVILFolder):
    def __init__(self, etype, eid, attribs, wsref):
        self.name = etype + "/"
        super().__init__(self.name)
        self.type = etype
        self.eid = eid
        self.wsref = wsref
        self.attribs = attribs # column names

    def make_contents(self):
        base_table = {
            name: [] for name in self.attribs
        }
        linked_files = []
        # get remote info
        resp = self.get_entity_info()
        for entry in resp:
            e_attrs = entry["attributes"]
            for attr in self.attribs:
                addendum = ""
                if attr in e_attrs:
                    val = e_attrs[attr]
                    if val is None:
                        val = ""
                    if type(val) == dict: # if theres more processing, e.g. not a string
                        # mere string attributes
                        if val["itemsType"] == "AttributeValue":
                            addendum = ",".join(val["items"])
                        # dict attributes referring to another entity
                        elif val["itemsType"] == "EntityReference": #TODO is this the best way to handle entity references?
                            addendum = ",".join(
                                [x["entityName"] for x in val["items"]]
                            )
                    else:
                        val = str(val) # enforce string
                        addendum = val
                        # check if its a linkable file
                        efiletype = self.is_linkable_file(val)
                        if efiletype:
                            _r = efiletype(val)
                            if type(_r) == list:
                                linked_files.extend(_r)
                            else:
                                linked_files.append(_r)
                base_table[attr].append(addendum)
            # if there are links, make them externally available by entityname_filename.tsv
        linked_files.append(TableEntriesFile(self.type + "_contents.tsv", base_table))
        for f in linked_files:
            self[f.name] = f

    def is_linkable_file(self, fname):
        protocol = fname.split("://")[0]
        allowed_protocols = {
            "gs": GoogleAnVILFile,
            "drs": DRSAnVILFile
        }
        if protocol in allowed_protocols:
            return allowed_protocols[protocol]
        else:
            return None

        
    def get_entity_info(self):
        resp = self.fapi.get_entities(
            self.wsref.namespace.name, 
            self.wsref.name,
            self.type)
        if resp.status_code == 200:
            return resp.json()
        else:
            resp.raise_for_status()


class RootTablesFolder(BaseAnVILFolder):
    def __init__(self, einfo, wsref):
        self.name = "Tables/"
        super().__init__(self.name)
        for ename in einfo:
            attribs = einfo[ename]["attributeNames"]
            eid = einfo[ename]["idName"]
            tf = TableFolder(ename, eid, attribs, wsref)
            self[tf.name] = tf
            # change this for laziness
            tf.make_contents()


class TableDataCohort(BaseAnVILFile):

    def __init__(self, name, attribs):
        query = attribs["query"]
        self.name = name
        self.size = 1 # some placeholder? cant lazy load AND init with total size of results
        self.query = query
        self.last_modified = "" # #TODO determine if this is ok

    def get_bytes_handler(self):
        job = self.gc_bigquery_client.query(self.query)
        r = job.result()
        schema_keys = [e.name for e in r.schema]
        string = '\t'.join(schema_keys)
        for row in r:
            string += "\n" + '\t'.join(row.values())
        return self.string_to_buffer(string)
