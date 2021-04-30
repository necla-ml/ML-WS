import pytest
from ml import av


@pytest.fixture
def bitstream():
    return 'stream661track47649.264'
    return 'store720p.264'


@pytest.fixture
def rtsp(bitstream):
    return f"rtsp://96.248.115.171/{bitstream}"
    return f"rtsp://138.15.169.58:8554/{bitstream}"


@pytest.fixture
def store_rtsp():
    # return 'rtsp://admin:a1b2c3d4@99.44.172.181:554/chID=1&streamType=substream&linkType=tcp'
    return 'rtsp://eigen:Sinet@1234@quickbuy.ddns.net:554/cam/realmonitor?channel=1&subtype=1'


def av_dump(src, count=60, last=None, **options):
    print()
    print(f'========== av: {src} ============')
    s = av.open(src, options=options)
    v = s.demux(video=0)
    pkts = [next(v) for _ in range(count)][last:]
    print('key:', [pkt.is_keyframe for pkt in pkts])
    print('corrupt:', [pkt.is_corrupt for pkt in pkts])
    print('dts:', [pkt.dts for pkt in pkts])
    print('pts:', [pkt.pts for pkt in pkts])
    print('duration:', [pkt.duration for pkt in pkts])
    print('size:', [pkt.size for pkt in pkts])
    s.close()
    return pkts


def avsrc_dump(src, count=60, last=None, **kwargs):
    from ml.streaming import AVSource
    print()
    print(f'========== AVSource: {src} ============')
    src = AVSource.create(src)
    session = src.open(decoding=False, **kwargs)
    pkts = [(src.read(session)[-1], session['video']['keyframe'], session['video']['duration']) for _ in range(count)][last:]
    print('key:', [key for _, key, _ in pkts])
    print('corrupt:', [pkt.is_corrupt for pkt, _, _ in pkts])
    print('pkt.dts:', [pkt.dts for pkt, _, _ in pkts])
    print('pkt.pts:', [pkt.pts for pkt, _, _ in pkts])
    print('pkt.duration:', [pkt.duration for pkt, _, _ in pkts])
    print('frm.duration:', [duration for _, _, duration in pkts])
    print('pkt.size:', [pkt.size for pkt, _, _ in pkts])
    src.close(session)
    return pkts


def test_store_rtsp_pkts(store_rtsp):
    count = 60
    last = -count
    pkts_avsrc = avsrc_dump(store_rtsp, count, last, rtsp_transport='http')
    pkts_av = av_dump(store_rtsp, count, last, rtsp_transport='http')


def test_rtsp_pkts(bitstream, rtsp):
    count = 200
    last = 50
    pkts_bs = av_dump(bitstream, count, -last)
    pkts_av = av_dump(rtsp, count, -last)
    pkts_avsrc = avsrc_dump(rtsp, count, -last)

    for i in range(last):
        mpkt_bs = memoryview(pkts_bs[i])
        mpkt_av = memoryview(pkts_av[i])
        mpkt_avsrc = memoryview(pkts_avsrc[i])
        assert mpkt_bs == mpkt_av, f"[{i}] mpkt_bs != mpkt_av"
        assert mpkt_av == mpkt_avsrc, f"[{i}] mpkt_av != mpkt_avsrc"