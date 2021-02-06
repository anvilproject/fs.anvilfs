from .basefile import BaseAnVILFile
from .basefolder import BaseAnVILFolder

from .google import DRSAnVILFile, LazyDRSAnVILFile


class TableEntriesFile(BaseAnVILFile):
    def __init__(self, name, itemsdict):
        self.name = name
        self.last_modified = ""  # #TODO this or current time?
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


class RootTablesFolder(BaseAnVILFolder):
    def __init__(self, wsref):
        self.name = "Tables/"
        self.wsref = wsref
        super().__init__(self.name)

    def lazy_init(self):
        self.einfo = self.wsref.fetch_entity_info()
        for ename in self.einfo:
            attribs = self.einfo[ename]["attributeNames"]
            eid = self.einfo[ename]["idName"]
            tf = TableFolder(ename, eid, attribs, self.wsref)
            self[tf.name] = tf


# terra 'entities' represent tables
class TableFolder(BaseAnVILFolder):
    def __init__(self, etype, eid, attribs, wsref):
        self.name = etype + "/"
        super().__init__(self.name)
        self.type = etype
        self.eid = eid
        self.wsref = wsref
        self.attribs = attribs  # column names

    def lazy_init(self):
        self.make_contents()

    def make_contents(self):
        base_table = {
            name: [] for name in self.attribs
        }
        has_metadata = False
        if(set(["file_name", "file_size", "updated_datetime"]).issubset(
                set(self.attribs))):
            has_metadata = True
        linked_files = []
        file_links = {}
        # get remote info
        resp = self.get_entity_info()
        # for each entity,
        for entry in resp:
            e_attrs = entry["attributes"]
            # if entry is a cohort with underlying query, add it as a file
            if entry["entityType"] == "cohort" and "query" in e_attrs:
                linked_files.append(
                    TableDataCohort(entry["name"], e_attrs["query"]))
            # for each of the column names
            for attr in self.attribs:
                addendum = ""
                if attr in e_attrs:
                    val = e_attrs[attr]
                    if val is None:
                        val = ""
                    # if theres more processing, e.g. not a string
                    if type(val) == dict and "itemsType" in val:
                        # mere string attributes
                        if val["itemsType"] == "AttributeValue":
                            addendum = ",".join(val["items"])
                        # dict attributes referring to another entity
                        elif val["itemsType"] == "EntityReference":
                            addendum = ",".join(
                                [x["entityName"] for x in val["items"]]
                            )
                    else:
                        if type(val) == dict and "entityName" in val:
                            val = val["entityName"]
                        val = str(val)  # enforce string
                        addendum = val
                        # check if its a linkable file
                        efiletype = self.is_linkable_file(val)
                        # if it's a linkable file...
                        if efiletype is not None:
                            if efiletype not in file_links:
                                file_links[efiletype] = []
                            # if we can lazy-load DRS info
                            if efiletype == DRSAnVILFile and has_metadata:
                                try:
                                    linked_files.append(LazyDRSAnVILFile(
                                        val,
                                        e_attrs["file_name"],
                                        e_attrs["file_size"],
                                        e_attrs["updated_datetime"]
                                    ))
                                # if info doesn't exist, fall back
                                except KeyError:
                                    file_links[efiletype].append(val)
                            else:
                                file_links[efiletype].append(val)
                base_table[attr].append(addendum)
        for method in file_links:
            try:
                fresh_files = method.factory(file_links[method])
                linked_files.extend(fresh_files)
            except Exception as e:
                print(f"AnVILFS ERROR: SKIPPING FILE due to error:")
                print(e)
                continue
        # if there are links, export to tsv
        linked_files.append(
            TableEntriesFile(self.type + "_contents.tsv", base_table))
        for f in linked_files:
            self[f.name] = f

    def get_entity_info(self):
        resp = self.fapi.get_entities(
            self.wsref.namespace_name,
            self.wsref.name,
            self.type)
        if resp.status_code == 200:
            return resp.json()
        else:
            resp.raise_for_status()


class TableDataCohort(BaseAnVILFile):

    def __init__(self, name, query):
        self.name = name + "_query_results.tsv"
        self.size = 1  # a placeholder; cant lazy load AND init with size
        self.query = query
        self.last_modified = ""  # #TODO this or current time?

    def get_bytes_handler(self):
        job = self.gc_bigquery_client.query(self.query)
        r = job.result()
        schema_keys = [e.name for e in r.schema]
        string = '\t'.join(schema_keys)
        for row in r:
            string += "\n" + '\t'.join(row.values())
        return self.string_to_buffer(string)
