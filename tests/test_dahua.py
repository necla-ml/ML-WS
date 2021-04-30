import pytest
from ml import av, logging
from ml.av import NALUParser, NALU_t

@pytest.fixture
def rtsp():
    return "rtsp://eigen:Sinet@1234@quickbuy.ddns.net:554/cam/realmonitor?channel=1&subtype=1"

def test_rtsp(rtsp):
    workaround = True
    s = av.open(rtsp, options=dict(rtsp_transport='http'))
    v = s.demux(video=0)
    codec = s.streams[0].codec_context
    if True:
        pkts = []
        for _ in range(15 * 2):
            pkt = next(v)
            NALUs = []
            for (pos, _, _, type), nalu in NALUParser(memoryview(pkt), workaround=False):
                NALUs.append(nalu[-1] == 0x00 and nalu[:-1] or nalu)
            packet = av.Packet(b''.join(NALUs))
            packet.dts = pkt.dts
            packet.pts = pkt.pts
            packet.time_base = pkt.time_base
            pkt = packet
            pkts.append(pkt)
    else:
        pkts = [next(v) for _ in range(15*2)]
    print(pkts)
#   frames = [pkt.decode()[0] for pkt in pkts]

    print()
    if codec.extradata is not None:
        for (pos, _, _, type), nalu in NALUParser(codec.extradata):
            logging.info(f"CPD {NALU_t(type).name} at {pos}: {nalu[:8]} ending with {nalu[-1:]}")
        with open(f"tmp/cpd.264", 'wb') as f:
            f.write(codec.extradata)
    for i, pkt in enumerate(pkts):
        print(f"pkt[{i}] {pkt.is_keyframe and 'key ' or ''}{pkt}")
        for (pos, _, _, type), nalu in NALUParser(memoryview(pkt), workaround=False):
            logging.info(f"frame[{i}] {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()} ending with {nalu[-1:].tobytes()}")
        with open(f"tmp/frame{i:02d}.264", 'wb') as f:
            f.write(pkt)
#    for i, frame in enumerate(frames):
#        print(f"frame[{i}] {frame.key_frame and 'key' or ''}{frame}")

    decoded = []
    h264 = av.CodecContext.create('h264', 'r')
    for i, pkt in enumerate(pkts):
        res = h264.decode(pkt)
        print(f"pkt[{i}] {len(res)} frames decoded")
        if res:
            assert len(res) == 1
            decoded.append(res[0])
    res = h264.decode()
    if res:
        print(f"pkt[-1] {len(res)} frames decoded")
        assert len(res) == 1
        decoded.append(res[0])
    
    if workaround:
        print(decoded)
        from ml import cv
        for i, f in enumerate(decoded):
            cv.save(f.to_rgb().to_ndarray()[:,:,::-1], f'tmp/images/frame{i:02d}.jpg')
    else:
        for i, f in enumerate(decoded):
            assert (f.to_ndarray() == frames[i].to_ndarray()).all()
