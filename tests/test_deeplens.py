import pytest

from ml import av, cv, logging
from ml.utils import Config
from ml.iot import AWSCam

@pytest.fixture
def resolution():
    return 720

@pytest.fixture
def fps():
    return 15 * 2

@pytest.fixture
def gop():
    return 15 * 2

@pytest.fixture
def bitrate():
    return 2000000

@pytest.fixture
def ch():
    return 1

from time import time, sleep
def test_awscam_info(resolution, fps, gop, bitrate, ch):
    cam = AWSCam(resolution=resolution, fps=fps, gop=gop, bitrate=bitrate, ch=ch)
    cam.open()

    total = 100
    start = time()
    for i in range(total):
        f = cam.read()
        now = time()
        logging.info(f"frame[{i}] {f.is_keyframe and 'key ' or '    '}with {cam.time:.3f}s read at {now:.3f}s")
    logging.info(f"RT FPS: {cam.frames / (now-start):.2f}")

    encoder = cam.encoder
    cfg = Config()
    cfg.bit_rate = encoder.bit_rate
    cfg.duration = encoder.duration
    cfg.format = encoder.format
    cfg.size = encoder.size
    cfg.name = encoder.name
    cfg.start_time = encoder.start_time
    logging.info(f"Encoder info: \n{cfg}")

    vid = encoder.streams[0]
    cfg.clear()
    cfg.average_rate = vid.average_rate
    cfg.duration = vid.duration
    cfg.index = vid.index
    cfg.frames = vid.frames
    cfg.profile = vid.profile
    cfg.start_time = vid.start_time
    cfg.time_base = vid.time_base
    cfg.type = vid.type
    logging.info(f"Video info: \n{cfg}")

    cfg.clear()
    cc = cam.codec
    cfg.name = cc.name
    cfg.type = cc.type
    cfg.bit_rate = cc.bit_rate
    cfg.framerate = cc.framerate
    cfg.format = cc.format
    cfg.width = cc.width
    cfg.height = cc.height
    cfg.gop_size = cc.gop_size
    cfg.rate = cc.rate
    cfg.time_base = cc.time_base
    cfg.ticks_per_frame = cc.ticks_per_frame
    logging.info(f'Codec info: \n{cfg}')

    cfg.clear()
    cfg.size = f.size
    cfg.is_keyframe = f.is_keyframe
    cfg.dts = f.dts
    cfg.pts = f.pts
    cfg.time_base = f.time_base
    cfg.duration = f.duration
    logging.info(f'Frame info: \n{cfg}')