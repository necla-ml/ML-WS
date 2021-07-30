from time import time
import pytest

from ml.streaming import AVSource
from ml.streaming import youtube
from ml import av, cv, sys, logging

@pytest.fixture
def url():
    # FIXME must be a live stream url
    return 'https://www.youtube.com/watch?v=DDU-rZs-Ic4'
    return 'https://www.youtube.com/watch?v=P87CdXLBC0Y'
    return 'https://www.youtube.com/watch?v=KGEekP1102g'

@pytest.fixture
def url_short():
    # FIXME must be a live stream url
    return 'https://youtu.be/KGEekP1102g'

@pytest.mark.essential
def test_youtube_live_av(url):
    source = AVSource.create(url)
    assert type(source) is youtube.YTSource
    assert source.url == url
    hls = youtube.yt_hls_url(url)
    options = dict(rtsp_transport='http',       # required for VPN
                    rtsp_flags='prefer_tcp',
                    stimeout='2000000')         # in case of network down
    logging.info(f"av.open({hls}, {options})")
    s = av.open(hls, options=options, timeout=5.0 * 2)
    v = s.demux(video=0)
    for i in range(100):
        f = next(v)
        pts = float(f.pts * f.time_base)
        duration = float(f.duration * f.time_base)
        logging.info(f"frame[{i}] time={pts:.3f}s, duration={duration:.3f}s, now={time():.3f}s")

@pytest.mark.essential
def test_youtube_live(url):
    source = AVSource.create(url)
    assert type(source) is youtube.YTSource
    assert source.url == url
    session = source.open(mode='LIVE', decoding=False)
    assert session is not None
    assert 'video' in session
    logging.info(session)
    for i in range(100):
        m, meta, frame = source.read(session, media='video')
        assert meta == session['video']
        assert frame is not None
        pts = float(frame.pts * frame.time_base)
        duration = float(frame.duration * frame.time_base)
        logging.info(f"frame[{meta['count']}] fpts={pts:.3f}s, fduration={duration:.3f}s, duration={meta['duration']:.3f}s, time={meta['time']:.3f}s, now={time():.3f}s")
        #assert media['height'] == frame.shape[0]
        #assert media['width'] == frame.shape[1]
        #assert 3 == frame.shape[2]
    
    if 'audio' in session:
        for i in range(5):
            m, meta, frame = source.read(session, media='audio')
            media = session['audio']
            logging.info(f"[{i}] {media}")
            assert frame is not None
            assert media == meta
            assert media['sample_rate'] == 48000
            assert media['channels'] == 2
            # assert 1024 == frame.shape[1]
    source.close(session)
    assert not session