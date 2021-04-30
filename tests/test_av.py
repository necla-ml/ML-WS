from pathlib import Path
import array
import pytest

from ml import av, logging
from ml.av.h264 import NALU_t, NALUParser
from fixtures import assets

@pytest.fixture
def bitstream():
    import os
    from ml import io
    path = assets.bitstream_workaround.path
    size = os.path.getsize(path)
    with io.FileIO(path, 'rb') as f:
        buf = bytearray(size)
        f.readinto(buf)
        # print(len(buf))
        return buf

@pytest.fixture
def bitstream_short():
    return str(assets.bitstream_short.path)

@pytest.fixture
def video_mp4():
    return str(assets.video_mp4.path)

@pytest.mark.essential
def test_source_time(video_mp4):
    sources = dict(file=video_mp4,
                    # cam=0,
                    # rtsp_qb="rtsp://eigen:Sinet@1234@quickbuy.ddns.net:554/cam/realmonitor?channel=1&subtype=1",
                    # rtsp_qb="rtsp://eigen:Sinet@1234@174.62.105.212:554/cam/realmonitor?channel=1&subtype=1",
                    # rtsp_ernie="rtsp://admin:Password123@50.211.198.158:558/LiveChannel/0/media.smp",
                    # nuuo_latham="nuuo://admin:admin@73.222.32.72:5250", # Cam 1
                    # kvs_awscam="aws_cam-5302",
                    )
    import av
    from time import time, localtime, strftime
    from datetime import datetime
    for src, path in sources.items():
        s = av.open(path)
        s_start = s.start_time  # us
        
        vs = s.streams[0]
        v_start = vs.start_time # pts
        vtb = vs.time_base      # per frame time base in pts
        rates = (float(vs.guessed_rate), float(vs.average_rate), float(vs.base_rate))

        cc = vs.codec_context
        fps = cc.framerate
        tps = cc.ticks_per_frame
        ctb = cc.time_base      # per frame time base in secs
        
        v = s.demux()
        pkts = [next(v) for i in range(3)]
        frames = [p.decode()[0] for p in pkts]

        pkt_ts = [(p.dts, p.pts) for p in pkts]
        pkt_rts = [(f"{float(p.dts * vtb):.3f}", f"{float(p.pts * vtb):.3f}") for p in pkts]
        pkt_durations = [(p.duration, round(float(p.duration * vtb), 3)) for p in pkts]
        frame_ts = [(f.dts, f.pts) for f in frames]
        frame_rts = [(f"{float(f.dts * vtb):.3f}s", f"{float(f.pts * vtb):.3f}s") for f in frames] 

        #assert s.start_time == vs.start_time
        #assert vs.time_base == cc.time_base

        s_timestamp = strftime("%X %x", localtime(s_start / 1e6))
        v_timestamp = strftime("%X %x", localtime(float(v_start * vtb)))
        print()
        print(f'##### {src}={path} #####')
        print(f'stream start: {s_timestamp}, {s_start/1e6:.3f}s, {s_start}us')
        print(f'video start: {v_timestamp}, {float(v_start * vtb):.3f}s, {v_start}pts')
        print(f"FPS={fps}({1 / (ctb * tps)}), rates={rates}, time_base={vtb}({ctb}), ticks_per_frame={tps}")
        print(f"pkt time: {pkt_ts}, {pkt_rts}")
        print(f"pkt duration: {list(zip(*pkt_durations))}")
        print(f"frame time: {frame_ts}, {frame_rts}")


@pytest.mark.essential
@pytest.mark.parametrize("workaround", [False, True])
def test_NALUParser(bitstream, workaround):
    prev = None
    logging.info(f"Read H264 bitstream of {len(bitstream)} bytes")
    for i, ((start, forbidden, idc, type), nalu) in enumerate(NALUParser(bitstream, workaround)):
        logging.info(f"[{i}] pos: {start}, forbidden: {forbidden}, idc: {idc}, type: {type}, size: {len(nalu)}")
        if prev is not None:
            if workaround:
                assert prev[-1] != 0x00
            else:
                assert prev[-1] >= 0x00
        prev = nalu

@pytest.mark.essential
def test_motion_vectors(bitstream_short):
    s = av.open(bitstream_short)
    codec = s.streams.video[0].codec_context
    codec.options = dict(flags2='+export_mvs')
    v = s.decode(video=0)
    frames = [next(v) for _ in range(15)]
    #print([list(f.side_data.keys()) for f in frames[:3]])
    MVs = [f.side_data.get('MOTION_VECTORS') for f in frames]
    print()
    for i, mv in enumerate(MVs):
        if mv is None:
            continue
        print(f"frame[{i}]:", len(mv), 'MVs')
        print(mv.to_ndarray()[:10])