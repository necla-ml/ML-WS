import json
from collections import defaultdict

from .api import APIClient
from ..aws.secrets import get as get_secret

REPLACE = 'replace'
SECRET_PATH = 'eigen/prod/thirdparty'

class BlueMonitor(APIClient):
    def __init__(self):
        super().__init__()
        self.api = self.__class__.__name__.lower()
        self.secret = get_secret(SECRET_PATH).get(self.api, None)
        assert self.secret, f'Cannot find API credentials for {self.api} at {SECRET_PATH}'
        self.auth_token = defaultdict(lambda: None)

    def _auth_channel(self, channel_id, reload=False):
        """
        curl -X GET "url" \
        -H "accept: application/json"  \
        -H "Authorization: SI "sd4f1sdf41s5d1fs65d1f65sd15f61sd"
        """
        if self.auth_token[channel_id] is not None and not reload:
            pass
        else:
            auth_creds = self.secret['auth']
            url = auth_creds['url'].replace('channel_id', str(channel_id))
            headers = auth_creds['headers']
            # request auth token
            res = self.get(url=url, headers=headers)
            if res.status_code == 200:
                result = res.json()
                self.auth_token[channel_id] = result['token']
            else:
                raise Exception(f'Failed authentication request with status {res.status_code}: {res.text}')

        return self.auth_token[channel_id]

    def stream_url(self, channel_id, stream_type='rtsp', reload=False):
        """
        curl -X GET "url" \
        -H "accept: application/json" \
        -H "Authorization: Acc sdf15sd1f51sd4f1sd1f45sd1f"
        """
        stream_creds = self.secret['stream']
        ch_auth_token = self._auth_channel(channel_id, reload=reload)
        headers = stream_creds['headers']
        url = stream_creds['url']
        headers['Authorization'] = headers['Authorization'].replace('auth_token', ch_auth_token)
        # request stream url
        res = self.get(url=url, headers=headers)
        if res.status_code == 200:
            result = res.json()
            return result.get(stream_type, None)
        else:
            raise Exception(f'Failed to get the stream url {res.status_code}: {res.text}')

    def send_meta(self, channel_id, data, reload=False):
        """
        curl -X POST "url" 
        -H "accept: application/json" 
        -H "Content-Type: application/json" 
        -H "Authorization: Acc sdf15sd1f51sd4f1sd1f45sd1f" 
        -d "json_data"
        """
        meta_creds = self.secret['meta']
        ch_auth_token = self._auth_channel(channel_id, reload=reload)
        headers = meta_creds['headers']
        url = meta_creds['url']
        headers['Authorization'] = headers['Authorization'].replace('auth_token', ch_auth_token)
        # call endpoint
        res = self.post(url, json.dumps(data), headers)
        if res.status_code == 200:
            result = res.json()
            return result
        else:
            raise Exception(f'Failed to send meta {res.status_code}: {res.text}')

    def send_event(self, channel_id, data, reload=False):
        """
        curl -X POST "url" \
        -H "accept: application/json" \
        -H "Content-Type: application/json" 
        -H "Authorization: Acc adijiofenwenfiwenfiwnefwen"
        -d "json_data"
        """
        event_creds = self.secret['event']
        ch_auth_token = self._auth_channel(channel_id, reload=reload)
        headers = event_creds['headers']
        url = event_creds['url']
        headers['Authorization'] = headers['Authorization'].replace('auth_token', ch_auth_token)
        # call endpoint
        res = self.post(url, json.dumps(data), headers)
        if res.status_code in [200, 201]:
            result = res.json()
            return result
        else:
            raise Exception(f'Failed to trigger event {res.status_code}: {res.text}')