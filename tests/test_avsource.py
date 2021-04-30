from pathlib import Path
import pytest

from ml.streaming import AVSource
from ml import av, cv, sys, logging

from fixtures import assets

@pytest.fixture
def bitstream():
    return assets.bitstream_short.path.__str__()
    return (Path(__file__).parent / 'assets' / 'store720p.264').absolute().__str__()

@pytest.fixture
def video_mp4():
    return assets.video_mp4.path.__str__()
    return (Path(__file__).parent / 'assets' / 'store720p.mp4').absolute().__str__()

@pytest.mark.essential
def test_video_h264_time(video_mp4):
    src = AVSource.create(video_mp4)
    frames = src.open()
    packets = src.open(decoding=False)

    names = ('frames', 'packets')
    sessions = (frames, packets)
    stats = {'frames': [], 'packets': []}

    for name, session in zip(names, sessions):
        start = session['start']
        video = session['video']
        elapse = 0

        time_all = []
        duration_all = []
        elapse_all = []
        for i in range(5):
            elapse += video.get('duration') or 0
            res = src.read(session, media='video')
            if res is None:
                break
            
            time = video['time']
            duration = video['duration']
            time_all.append(time)
            duration_all.append(duration)
            elapse_all.append(elapse)
            assert duration > 0
            if i == 0:
                assert time == start
                assert elapse == 0
            else:
                import math
                assert math.isclose(time - start, elapse, rel_tol=1e-5), f"{time - start} != {elapse}"
        print()
        print('duration:', duration_all)
        print('elapse:', elapse_all)
        print('time:', time_all)
        print(f"start={start}")
    
# @pytest.mark.essential
def test_webcam(src=0):
    source = AVSource.create(0)
    assert type(source) is AVSource
    assert source.src == src

    # open(self, start=None, end=None, expires=5 * 60, mode='LIVE', offset=4.75):
    session = source.open(fps=15, resolution='720p', decoding=True)
    assert session is not None
    assert 'video' in session
    assert 'audio' not in session
    logging.info(session)

    total = 5
    X = sys.x_available()
    while True:
        res = source.read(session, media='video')
        assert res
        m, media, frame = res

        count = media['count']
        if m == 'video':
            logging.info(f"[{count}] Decoded video frame{frame.shape}@{media['fps']:.2f}FPS")
        elif m == 'audio':
            logging.info(f"[{count}] Decoded audio frame{frame.shape}@{media['fps']:.2f}FPS")
        else:
            assert False, f"Unknown media {m}"
        
        if X:
            cv.imshow(frame, title='LIVE')
            cv.waitKey(1)
        
        if count == total:
            break