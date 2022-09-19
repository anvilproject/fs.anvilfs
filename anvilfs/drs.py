from terra_notebook_utils import drs
import concurrent.futures
import dateutil
from .hypertext import HypertextAnVILFile


class DRSAnVILFile(HypertextAnVILFile):
    # api_url = ("https://us-central1-broad-dsde-prod.cloudfunctions.net/"
    #            "martha_v3")

    # @classmethod
    # def create_sa_creds(cls, sa_info):
    #     sa_creds = service_account.Credentials.from_service_account_info(sa_info)
    #     return sa_creds

    def __init__(self, drs_uri, preloaded_info=None):
        info = preloaded_info or drs.get_drs_info(drs_uri)
        self.uri = drs_uri
        self.size = info.size
        self.last_modified = dateutil.parser.parse(info.updated)
        self.name = info.name

    @classmethod
    def factory(cls, drslist):
        # subfunction for threads
        def _get_info(drs_uri, timeout):
            return drs.get_drs_info(drs_uri)

        # thread pool maker
        def _pooler(inlist, maxworks=50):
            timeout = 60  # seconds
            good_data = []
            bad_uris = []
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=maxworks) as executor:
                # Start the load operations and mark each future with its URL
                future_to_url = {executor.submit(
                    _get_info, url, timeout): url for url in inlist}
                for future in concurrent.futures.as_completed(future_to_url):
                    url = future_to_url[future]
                    try:
                        data = future.result()
                        if "name" not in data._asdict():
                            print(f"DRS resolution error- received:\n\t{data}")
                            bad_uris.append(url)
                        else:
                            good_data.append((url, data))
                    except Exception as exc:
                        print('%r generated an exception: %s' % (url, exc))
                        raise exc
                    else:
                        pass
            return good_data, bad_uris

        # first pass
        file_objects = []
        good, bad = _pooler(drslist)
        # retry
        good_retries, bad_retries = _pooler(bad)
        if bad_retries:
            print(f"Unable to resolve the following URIs:\n{bad_retries}")
        total_goods = good + good_retries
        # make google bucket objects
        for item in total_goods:
            file_objects.append(DRSAnVILFile(item[0], item[1]))
        return file_objects

    def get_bytes_handler(self):
        super().__init__(
            drs.access(self.uri, self.workspace, self.namespace),
            self.name,
            self.size,
            self.last_modified
        )
        return super().get_bytes_handler()


class LazyDRSAnVILFile(DRSAnVILFile):
    def __init__(self, uri, name, size=None, last_modified=None):
        self.uri = uri
        self.name = name
        self.size = size or 1
        self.last_modified = last_modified or ""

    def get_bytes_handler(self):
        super().__init__(self.uri)
        return super().get_bytes_handler()
