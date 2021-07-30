'''Demonstrate streaming from NUUO/NVR over HTTP.

Run essential tests as follows:

```python
python -m pytest tests/test_nuuo_titan.py -m essential  -s
```
'''

import pytest

from ml import av, cv, sys, logging
from ml.av.h264 import NALU_t, H264Framer
from ml.streaming import AVSource
from ml.streaming.nuuo import NVR, Titan8040R, Crystal
'''
@pytest.fixture
def ip():
    return '12.31.246.50'

@pytest.fixture
def port():
    return 5250

@pytest.fixture
def user():
    return 'admin'

@pytest.fixture
def passwd():
    return 'admin'
'''

cred = None

@pytest.fixture
def rds():
    from ml.ws.aws.rds import RDS
    return RDS()

@pytest.fixture
def site():
    return 3

@pytest.fixture
def credentials(rds, site):
    global cred
    if cred is None:
        from ml.ws.aws import utils
        from ml.ws.aws.rds import get_nvr_credentials
        secret = utils.get_secret()
        #self.region = region or secret.get('AWS_DEFAULT_REGION', ffi.NULL)
        #self.accessKey = accessKey or secret.get('AWS_ACCESS_KEY_ID', ffi.NULL)
        #self.secretKey = secretKey or secret.get('AWS_SECRET_ACCESS_KEY', ffi.NULL)
        privateKey = secret.get('PRIVATE_KEY', None)
        cred = get_nvr_credentials(rds, site, privateKey)
        print(secret)
        print(cred)
        return cred
    else:
        return cred

@pytest.fixture
def areas():
    return [
        'First Floor Entry',        # nuuo-50341658: 1920p@8-12FPS
        'First Floor Books',        # nuuo-50341665: 1080p@15FPS
        'First Floor Reg. 1 and 2', # nuuo-50341650: 1080p@15FPS
        'First Floor Reg. 3 and 4', # nuuo-50341660: 1080p@30FPS
        'First Floor Reg. 5 and 6', # nuuo-50341668: 1080p@15FPS
        'First Floor Reg. 7 and 8', # nuuo-50341661: 1080p@15FPS
        # 'First Floor PTZ',          # nuuo-50341659:  720p@30FPS
        'Second Floor Fisheye'    , # 
    ]

@pytest.fixture
def FPS(areas):
    return {
        areas[0]:  8,
        areas[1]: 15,
        areas[2]: 15,
        areas[3]: 30,
        areas[4]: 15,
        areas[5]: 15,
        areas[6]:  8,
    }

def url(ip, port):
    return f"nuuo://{ip}:{port}"

def startStreaming(nvr, cfg, duration=None):
    cam, profile, codec = cfg
    host = f'{nvr.ip}:{nvr.port}'
    url = f'http://{host}'
    show = sys.x_available()
    decoder = av.CodecContext.create(codec.replace('.', '').lower(), "r")
    stream = nvr.startStreaming(cfg, debug=not True)
    logging.info(f"##### {cam['area']}: streaming({profile}/{codec}) for {duration} frames #####")
    frames = 0
    for i, (pkt, (media, cc)) in enumerate(stream):
        try:
            # Unsupported codecs, e.g.:
            #   - b'audio/x-g711-mulaw'
            if media != 'video' or decoder.name not in cc:
                print(f"[{i}] Skipped unsupported frame: {media}/{cc}")
                continue

            packet = av.Packet(pkt['payload'])
            frame = decoder.decode(packet)[0].to_rgb().to_ndarray()[:,:,::-1]
        except Exception as e:
            logging.error(f"[{frames}] Failed to decode frame: {e}")
            break
        else:
            if show:
                cv.imshow('Live: {url}', frame)
                cv.waitKey(1)
            frames += 1
            logging.info(f"[{i}] {media}/{cc} of {len(pkt)} bytes in {frame.shape}")
            if duration is not None and frames == duration:
                cv.destroyAllWindows()
                break


def test_streaming_all(credentials, duration=5):
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Titan8040R)
    nvr.connect()
    logging.info(f"##### Probing all streaming profiles for {duration} frames #####")
    for cam in nvr:
        if cam['area'] == 'First Floor PTZ':
            logging.warning(f"Skipping 'First Floor PTZ'")            
            continue

        cfgs = nvr.query(area=cam['area'], exact=True)
        assert len(cfgs) == 1
        cfg = cfgs[0]
        assert len(cfg) == 3
        startStreaming(nvr, cfg, duration)
        print()

# @pytest.mark.essential
def test_connect(credentials):
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Titan8040R)
    nvr.connect()
    for device in nvr:
        logging.info(device)

# @pytest.mark.essential
def test_query_streaming_one(credentials):
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    area = 'First Floor Entry'
    area = 'Second Floor Fisheye'   # w/ audio/x-g711-mulaw
    profile = 'Original' 
    codec = 'H.264'
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Titan8040R)
    nvr.connect()
    res = nvr.query(area=area, profile=profile, codec=codec, exact=True)
    assert len(res) == 1, f"No exact matching for camera stream but got {len(res)}: {[cam for cam in res]} for ({area}, {profile}/{codec})"
    cam, profile, codec = cfg = res[0]
    logging.info(f"##### Queried to start streaming profile: {profile}/{codec} from {cam['area']} #####")
    startStreaming(nvr, cfg, duration=5)
    print()

'''
@pytest.mark.essential
def test_query_PTZ(ip, port, user, passwd, areas):
    nvr = nuuo.Titan8040R(ip, port, user=user, passwd=passwd)
    nvr.connect()
    srcs = nvr.query(area=areas[-1], exact=False)
    assert len(srcs) == 1
    src = srcs[0]
    logging.info(src)
'''

# @pytest.mark.essential
def test_queries(credentials):
    # Connect to VMS/NVR
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Titan8040R)
    nvr.connect()

    # Query for matched camera streams offering the specified profiel/codec in the area
    # Fuzzy criteria by default and exact area matching if required
    # Case insensitive for all
    srcs = nvr.query(area='fisheye', exact=False)
    logging.info(f"Non-exact matching for 'fisheye'({len(srcs)}): {[(src[0]['area'], '/'.join(src[1:])) for src in srcs]}")
    assert len(srcs) == 3
    srcs = nvr.query(area='fisheye', exact=True)
    logging.info(f"Exact matching for 'fisheye'({len(srcs)}): {[(src[0]['area'], '/'.join(src[1:])) for src in srcs]}")
    assert len(srcs) == 0
    srcs = nvr.query(area='First Floor Fisheye', exact=True)
    logging.info(f"Exact matching for 'First Floor Fishey'({len(srcs)}): {[(src[0]['area'], '/'.join(src[1:])) for src in srcs]}")
    assert len(srcs) == 1
    srcs = nvr.query(profile='Original', codec='mpeg4')
    logging.info(f"Non-exact matching for all with 'mpeg4'({len(srcs)}): {[(src[0]['area'], '/'.join(src[1:])) for src in srcs]}")
    assert len(srcs) == 1
    srcs = nvr.query(profile='Minimum', codec='mjpeg')
    logging.info(f"Non-exact matching for all with 'mjpeg'({len(srcs)}):")
    for src in srcs:
        logging.info(f"\t{src[0]['area']}: {'/'.join(src[1:])}")
    assert len(srcs) == 26


# @pytest.mark.essential
def test_single_session(credentials, areas, FPS):
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    area = areas[2]
    fps = FPS[area]
    decoding=True
    source = AVSource.create(url(ip, port), user=user, passwd=passwd)
    sessions = source.open(area, 'Original', fps=fps, decoding=decoding, exact=True, with_audio=True)
    assert len(sessions) == 1

    session = sessions[0]
    video = session['video']
    logging.info(f"Session: \n{session}")

    total = 30
    X = sys.x_available()
    for i in range(total):
        res = source.read(session, media=None)
        assert res is not None
        m, media, frame = res
        if decoding:
            # NUUO H.264 contains no pts info
            logging.info(f"{m}[{media['count']}]: {frame.shape} of {frame.dtype} at {media['time']:.3f}s")
        else:
            logging.info(f"{m}[{media['count']}]: {media['keyframe'] and 'key ' or ''}({frame.size} bytes) at {media['time']:.3f}s")
    logging.info(f"{m}/{media['format']} stream FPS={video['fps_rt']:.2f}")
    source.close(session)
    assert not session

# @pytest.mark.essential
def test_multi_sessions(credentials, FPS):
    ip = credentials['ip']
    port = credentials['port']
    user = credentials['username']
    passwd = credentials['passwd']
    source = AVSource.create(url(ip, port), user=user, passwd=passwd)
    sessions = source.open('fisheye', 'Original', decoding=True, with_audio=False)
    logging.info(f"{[session['cam']['area'] for session in sessions]}")
    assert len(sessions) == 3
    areas = set()
    for session in sessions:
        area = session['cam']['area']
        if 'video' in session:
            video = session['video']
            if video['fps'] is None:
                video['fps'] = area in FPS and FPS[area] or 15
        frame = source.read(session)
        areas.add(area)
        source.close(session)
        assert frame is not None, f"Failed to read frame from '{session['cam']['area']}'"
        assert not session

    assert len(areas) == 3, f"Three distinc areas are expected but only got {len(areas)}: {areas}"    