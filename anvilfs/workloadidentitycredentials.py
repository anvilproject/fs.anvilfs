import datetime
from google.auth import credentials
import json


class WorkloadIdentityCredentials(credentials.Scoped, credentials.Credentials):
    def __init__(self, scopes):
        super(WorkloadIdentityCredentials, self).__init__()
        self._scopes = scopes

    def with_scopes(self, scopes):
        return WorkloadIdentityCredentials(scopes=scopes)

    @property
    def requires_scopes(self):
        return False

    def refresh(self, request):
        url = ('http://metadata.google.internal/computeMetadata/'
               'v1/instance/service-accounts/default/token')
        if self._scopes:
            url += '?scopes=' + ','.join(self._scopes)
        response = request(url=url, method="GET", headers={
                           'Metadata-Flavor': 'Google'})
        if response.status == 200:
            response_json = json.loads(response.data)
        else:
            raise RuntimeError('bad status from metadata server')
        self.token = response_json['access_token']
        self.expiry = datetime.datetime.utcnow(
        ) + datetime.timedelta(seconds=response_json['expires_in'])
