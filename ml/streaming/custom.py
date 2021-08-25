from ml import logging
from .avsource import AVSource, openAV
from ..ws.thirdparty import SUPPORTED_APIS

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
        self.path = None

    def open(self, *args, **kwargs):
        try:
            if not self.path:
                api_client = SUPPORTED_APIS[self.api]()
                self.path = api_client.stream_url(self.channel_id, self.stream_type)
            session = openAV(self.path, **kwargs)
        except Exception as e:
            logging.error(f"Failed to open session: {e}")
            return None
        else:
            return session