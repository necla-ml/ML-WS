import pytest

from ml.ws.aws.secrets import *
from ml.ws.aws.rds import site_info, site_stream_url

@pytest.fixture
def sites():
    return dict(
        DEV=['nuuo_ca'],
        PROD=[
            'latham',
            'quickbuy',
            'ernie',
            'calstore'
        ]
    )

def test_aws_env():
    env = aws_env()
    print('env:', env)

def test_credentials(sites):
    for env, names in sites.items():
        for name in names:
            info = site_info(name, env)
            url = site_stream_url(name, ch=0, profile=1, env=env)
            print(info, url)