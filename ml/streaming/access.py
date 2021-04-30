'''
Public streaming access urls by vendors.
'''

_URL = dict(
    wyzecam='rtsp://{auth}{ip}:{port}/live',
    dahua='rtsp://{auth}{ip}:{port}/cam/realmonitor?channel={ch}&subtype={profile}',
    wisenet='rtsp://{auth}{ip}:{port}/LiveChannel/{ch}/media.smp',
    nuuo='nuuo://{auth}{ip}:{port}',
)

def stream_url(vendor, ip, user=None, passwd=None, port=None, ch=None, profile=None):
    '''Formulate the streaming url given the vendor and necessary information.
    '''
    auth = (user and passwd) and f"{user}:{passwd}@" or ''
    url = _URL[vendor]
    if port is None:
        if url.startswith('rtsp'):
            port = 554
        elif url.startswith('nuuo'):
            port = 5250
        else:
            raise ValueError('port unspecified')
    return url.format(auth=auth, ip=ip, port=port, ch=ch, profile=profile)
