import requests
from abc import ABC, abstractmethod

DEFAULT_TIMEOUT = 20

class APIClient(ABC):
    def __init__(self):
        super().__init__()
    
    @staticmethod
    def get(url, headers, timeout=DEFAULT_TIMEOUT):
        return requests.get(url=url, headers=headers, timeout=timeout)

    @staticmethod
    def post(url, data, headers, timeout=DEFAULT_TIMEOUT):
        return requests.post(url=url, data=data, headers=headers, timeout=timeout)
    
    @abstractmethod
    def stream_url(self):
        """ All third party apis must implement this method which returns the stream url in supported format """
        pass