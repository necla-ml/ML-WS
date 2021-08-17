'''Demonstrate streaming from NUUO/NVR over HTTP.

```python
python -m pytest tests/test_nuuo_crystal.py -m essential  -s
```
'''
from time import time
import pytest

from ml import av, cv, sys, logging
from ml.av.h264 import NALU_t, H264Framer
from ml.streaming import AVSource
from ml.streaming.nuuo import NVR, Titan8040R, Crystal

@pytest.fixture
def ip():
    return '73.222.32.72' # Latham
    return '47.181.12.10' # NUUO-CA

@pytest.fixture
def port():
    return 5250
    return 3456

@pytest.fixture
def user():
    return 'admin'

@pytest.fixture
def passwd():
    return 'admin'
    return 'Localhost69'

@pytest.fixture
def areas():
    return [
        'Cam 1',                # 1080p@30FPS, 960p@15FPS, 480p@15FPS
        'Cam 2',                # 1080p@30FPS, 480p@15FPS, D1@10FPS(MJPG)
        'Cam 3',                # 1080p@30FPS, 960p@15FPS, 480p@15FPS
        'Cam 4',                # 1080p@30FPS, 960p@15FPS, 480p@15FPS
    ]
    return [
        'Medical Entry',        # nuuo-B8220_camera:   1080p@30FPS     ~2.9s fragment duration
        'convenience store',    # nuuo-B8220_camera:   1080p@30FPS     ~8.3s fragment duration
        'Mall Entrance',        # nuuo-B8220_camera: 720x576@25FPS     0.5 fragment duration
        'Highway',              # nuuo-B8220_camera:    720p@30FPS     ~8.3s fragment duration
    ]

@pytest.fixture
def FPS(areas):
    return {
        areas[0]: 20,
        areas[1]: 30,
        areas[2]: 20,
        areas[3]: 20,
    }
    return {
        areas[0]: 30,
        areas[1]: 30,
        areas[2]: 25,
        areas[3]: 30,
    }

@pytest.fixture
def site_id():
    return 873
    return 100

def url(ip, port):
    return f"nuuo://{ip}:{port}"

def startStreaming(nvr, cfg, duration=None, debug=False):
    serverId, cam, profile, codec = cfg
    host = f'{nvr.ip}:{nvr.port}'
    url = f'http://{host}'
    show = sys.x_available()
    decoder = av.CodecContext.create(codec.replace('.', '').lower(), "r")
    stream = nvr.startStreaming(cfg, debug=debug)
    logging.info(f"##### {cam['area']}: streaming({profile}/{codec}) for {duration} frames #####")
    frames = 0
    for i, (pkt, (media, cc)) in enumerate(stream):
        # Unsupported codecs, e.g.:
        #   - b'audio/x-g711-mulaw'
        if media != 'video' or decoder.name not in cc:
            print(f"[{i}] Skipped unsupported frame: {media}/{cc}")
            continue

        packet = av.Packet(pkt['payload'])
        timestamp = pkt['time']
        frame = decoder.decode(packet)[0].to_rgb().to_ndarray()[:,:,::-1]
        if show:
            cv.imshow('Live: {url}', frame)
            cv.waitKey(1)
        frames += 1
        logging.info(f"[{i}] {media}/{cc} of {packet.size} bytes in {frame.shape} at {timestamp:.3f}s, now={time():.3f}s")
        if duration is not None and frames == duration:
            cv.destroyAllWindows()
            break

# @pytest.mark.essential
def test_connect(ip, port, user, passwd):
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Crystal)
    nvr.connect()
    for _, setup in nvr:
        logging.info(setup)

# @pytest.mark.essential
def test_queries(ip, port, user, passwd):
    # Connect to VMS/NVR
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Crystal)
    nvr.connect()

    # Query for matched camera streams offering the specified profiel/codec in the area
    # Fuzzy criteria by default and exact area matching if required
    # Case insensitive for all
    srcs = nvr.query(area='Medical', exact=False)
    logging.info(f"Non-exact matching for 'Medical Entry'({len(srcs)}): {[(src[1]['area'], '/'.join(src[2:])) for src in srcs]}")
    assert len(srcs) == 1
    assert srcs[0][1]['area'] == 'Medical Entry'

    srcs = nvr.query(area='convenience store', exact=True)
    logging.info(f"Exact matching for 'convenience store'({len(srcs)}): {[(src[1]['area'], '/'.join(src[2:])) for src in srcs]}")
    assert len(srcs) == 1
    assert srcs[0][1]['area'] == 'convenience store'

    srcs = nvr.query(area='Mall Entrance', exact=True)
    logging.info(f"Exact matching for 'Mall Entrance'({len(srcs)}): {[(src[1]['area'], '/'.join(src[2:])) for src in srcs]}")
    assert len(srcs) == 1
    assert srcs[0][1]['area'] == 'Mall Entrance'

    srcs = nvr.query(area='high', exact=False)
    logging.info(f"Non-exact matching for 'high'({len(srcs)}): {[(src[1]['area'], '/'.join(src[2:])) for src in srcs]}")
    assert len(srcs) == 1
    assert srcs[0][1]['area'] == 'Highway'

# @pytest.mark.essential
def test_streaming_one(ip, port, user, passwd):
    area = 'Medical Entry'
    area = 'Mall Entrance'
    area = 'Highway'
    area = 'convenience store'
    area = 'Cam 1'
    profile = 'Original' 
    profile = 'Low' 
    codec = 'H.264'
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Crystal)
    nvr.connect()
    res = nvr.query(area=area, profile=profile, codec=codec, exact=True)
    assert len(res) == 1, f"Exact matching for camera stream but got {len(res)}: {[cam for cam in res]} for ({area}, {profile}/{codec})"
    
    cfg = res[0]
    startStreaming(nvr, cfg, duration=150, debug=not True)
    print()

# @pytest.mark.essential
def test_streaming_all(ip, port, user, passwd, duration=5):
    nvr = NVR.create(ip, port, user=user, passwd=passwd)
    assert isinstance(nvr, Crystal)
    nvr.connect()
    logging.info(f"##### Probing all streaming profiles for {duration} frames #####")
    for _, setup in nvr:
        for cam in setup['devices']:
            cfgs = nvr.query(area=cam['area'], profile=0)
            assert len(cfgs) == 1
            cfg = cfgs[0]
            assert len(cfg) == 4
            startStreaming(nvr, cfg, duration)
            print()

# @pytest.mark.essential
def test_single_session(ip, port, user, passwd, areas, FPS):
    area = areas[1]
    fps = FPS[area]
    decoding=not True
    source = AVSource.create(url(ip, port), user=user, passwd=passwd)
    sessions = source.open(area, 'Original', fps=fps, decoding=decoding, exact=True, with_audio=True)
    assert len(sessions) == 1

    session = sessions[0]
    video = session['video']
    logging.info(f"Session: \n{session}")

    total = 300
    X = sys.x_available()
    for i in range(total):
        res = source.read(session, media=None)
        if res is None:
            logging.warning(f"Skipped invalid frame")
            continue

        m, media, frame = res
        if decoding:
            duration = float(media['duration'] * media['timbe_base']) if m == 'video' else frame.size / 2 / media['sample_rate']
            logging.info(f"{m}[{media['count']}]: {frame.shape} of {frame.dtype} with duraton {duration:.3f}s at {media['time']:.3f}s, now={time():.3f}s")
        else:
            duration = float(media['duration'] * media['time_base']) if m == 'video' else frame.size / media['sample_rate']
            logging.info(f"{m}[{media['count']}]: {media['keyframe'] and 'key ' or ''}({frame.size} bytes) with duration {duration:.3f}s at {media['time']:.3f}s, now={time():.3f}s")
    logging.info(f"{m}/{video['format']} stream FPS={video['fps_rt']:.2f}")
    source.close(session)
    assert not session


# @pytest.mark.essential
def test_multi_sessions(ip, port, user, passwd, areas, FPS):
    source = AVSource.create(url(ip, port), user=user, passwd=passwd)
    sessions = source.open('Ent', 'Original', decoding=True, with_audio=False)
    logging.info(f"sessions for {[session['cam']['area'] for session in sessions]}")
    assert len(sessions) == 2

    areas = set()
    for session in sessions:
        area = session['cam']['area']
        if 'video' in session:
            video = session['video']
            if video['fps'] is None:
                video['fps'] = area in FPS and FPS[area] or 30
        frame = source.read(session)
        areas.add(session['cam']['area'])
        source.close(session)
        assert frame is not None, f"Failed to read frame from '{session['cam']['area']}'"
        assert not session

    assert len(areas) == 2, f"Two distinc areas are expected but only got {len(areas)}: {areas}"    