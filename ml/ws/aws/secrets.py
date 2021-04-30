from . import utils

__all__ = [
    'get',
    'aws_env',
    'kvs_private_key'
]

_CACHE = {}

def get(name):
    if name not in _CACHE:
        secret = utils.get_secret(name)
        if secret:
            _CACHE[name] = secret
    return _CACHE.get(name, None)

def aws_env():
    return get('aws/env')

def kvs_private_key():
    secret = get('KinesisVideoStreamProducerCredentials')
    return secret['PRIVATE_KEY']
