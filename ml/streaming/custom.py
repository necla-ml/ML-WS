import requests
from abc import ABC, abstractmethod

from ml import logging
from ..ws.aws.secrets import get as get_secret
from .avsource import AVSource, openAV

REPLACE = 'replace'
SECRET_PATH = 'eigen/prod/custom_streaming'
DEFAULT_TIMEOUT = 20 # sec

class StreamAPIClient(ABC):
    def __init__(self):
        super().__init__()
    
    @staticmethod
    def get_request(url, headers, timeout=DEFAULT_TIMEOUT):
        return requests.get(url=url, headers=headers, timeout=timeout)

    @abstractmethod
    def _authenticate(self):
        pass
    
    @abstractmethod
    def stream_url(self):
        pass

        
class BlueMonitor(StreamAPIClient):
    def __init__(self, creds, channel_id, stream_type='rtsp'):
        super().__init__()
        self.creds = creds
        self.channel_id = channel_id
        self.stream_type = stream_type
        self.auth_token = self._authenticate()

    def _authenticate(self):
        """
        curl -X GET "url" \
        -H "accept: application/json"  \
        -H "Authorization: SI sd4f1sdf41s5d1fs65d1f65sd15f61sd"
        """
        auth_creds = self.creds['auth']
        url = auth_creds['url'].replace(REPLACE, str(self.channel_id))
        headers = auth_creds['headers']
        # request auth token
        res = self.get_request(url=url, headers=headers)
        if res.status_code == 200:
            result = res.json()
            return result['token']
        else:
            raise Exception(f'Failed authentication request with status {res.status_code}: {res.text}')

    def stream_url(self):
        """
        curl -X GET "url" \
        -H "accept: application/json" \
        -H "Authorization: Acc sdf15sd1f51sd4f1sd1f45sd1f
        """
        stream_creds = self.creds['stream']
        if self.auth_token is None:
            self._authenticate()
        headers = stream_creds['headers']
        url = stream_creds['url']
        headers['Authorization'] = headers['Authorization'].replace(REPLACE, self.auth_token)
        # request stream url
        res = self.get_request(url=url, headers=headers)
        if res.status_code == 200:
            result = res.json()
            return result[self.stream_type]
        else:
            raise Exception(f'Failed to get the stream url {res.status_code}: {res.text}')

SUPPORTED_APIS = {'bluemonitor': BlueMonitor}

class CustomSource(AVSource):
    def __init__(self, url, *args, **kwargs):
        '''Single streaming session at a time.
        Params:
            url: string([cs://][api_type]-[channel_id])
                e.g: cs://bluemonitor-25639
        '''
        self.url = url
        self.api, self.channel_id = url.split('://')[-1].split('-')
        assert self.api in SUPPORTED_APIS, f'Provided API: {self.api} is not supported!'
        self.stream_type = kwargs.pop('stream_type', 'rtsp')
        self.secret = get_secret(SECRET_PATH).get(self.api, None)
        assert self.secret, f'Cannot find API credentials for {self.api} at {SECRET_PATH}'
        self.path = None

    def open(self, *args, **kwargs):
        try:
            if not self.path:
                api_client = SUPPORTED_APIS[self.api](self.secret, self.channel_id, self.stream_type)
                self.path = api_client.stream_url()
            session = openAV(self.path, **kwargs)
        except Exception as e:
            logging.error(f"Failed to open session: {e}")
            return None
        else:
            return session