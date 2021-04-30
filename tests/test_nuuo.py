#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import os
from time import time, sleep

from ml.av.h264 import NALU_t
from ml import logging

from ml.streaming import AVSource
from ml.streaming.producers import KVProducer, DEFAULT_FPS_VALUE, HUNDREDS_OF_NANOS_IN_A_SECOND
from ml.streaming._C import ffi, lib

from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process
from functools import partial

import sys
import argparse

class NUUOProducerTest(KVProducer):
    def __init__(self, ip=None, port=None, user='admin', passwd='admin', accessKey=None, secretKey=None):
        # AWS/KVS credentials given or from ENV
        super(NUUOProducerTest, self).__init__(accessKey, secretKey)
        
        # NUUO credentials given or from ENV
        NUUO_IP = ip or os.getenv('NUUO_IP', None)
        NUUO_PORT = port or os.getenv('NUUO_PORT', None)
        NUUO_USER = user or os.getenv('NUUO_USER', None)
        NUUO_PASSWD = passwd or os.getenv('NUUO_PASSWD', None)
        self.nuuo = AVSource.create(f"nuuo://{NUUO_IP}:{NUUO_PORT}", user=NUUO_USER, passwd=NUUO_PASSWD)
        
    def download_test(self, area, profile='Original', fps=DEFAULT_FPS_VALUE, duration=None):
        '''Start streaming from the source area camera.
        '''
        from time import time
        pFrame = ffi.new('PFrame')
        pFrame.version = ffi.integer_const('FRAME_CURRENT_VERSION')
        pFrame.trackId = ffi.integer_const('DEFAULT_VIDEO_TRACK_ID')
        pFrame.duration = int(HUNDREDS_OF_NANOS_IN_A_SECOND / (fps or DEFAULT_FPS_VALUE))

        frameIndex = 0
        start_time = time()
        retStatus = ffi.integer_const('STATUS_SUCCESS')

        streamStopTime = None
        if duration is not None:
            streamingDuration = duration * HUNDREDS_OF_NANOS_IN_A_SECOND
            streamStopTime = lib.defaultGetTime() + streamingDuration
            print(f"Streaming stops in {streamStopTime / 10000000:.3f}s")
        else:
            print(f"Streaming indefinitely")

        stream_time = lib.defaultGetTime()
    
        # Python signal delivery depends on the main thread not being blocked on system calls
        import signal
        signaled = False
        def handler(sig, frame):
            nonlocal signaled
            print(f"Interrupted or stopped by {signal.Signals(sig).name}")
            signaled = True
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

        sessions = self.nuuo.open(area, profile, decoding=False, exact=True, with_audio=False)
        assert len(sessions) == 1, f"Only one streaming session at a time"
        session = sessions.pop()
        while (streamStopTime is None or lib.defaultGetTime() < streamStopTime) and not signaled:
            # TODO audio streaming
            res = self.nuuo.read(session, media='video')
            if res is None:
                logging.error(f"Failed to read frame from NUUO NVR")
                break
            
            # TODO non-H.264 keyframe requires additional parsing
            m, media, frame = res
            keyframe = media.get('keyframe', False)
            pFrame.flags = ffi.integer_const('FRAME_FLAG_KEY_FRAME') if keyframe else ffi.integer_const('FRAME_FLAG_NONE')
            pFrame.frameData = ffi.cast('void*', frame.buffer_ptr)
            pFrame.size = frame.size
            pFrame.index = frameIndex
            logging.info(f'FPS {area}: {frameIndex/(time() - start_time)}')
            frameIndex += 1
        self.nuuo.close(session)


def main(cfg, streams):
    print(f"Streaming on behalf of user={cfg.user} from {cfg.ip}:{cfg.port} to KV stream={streams}")
    area, stream = streams
    print(area, stream)
    stream = f"{stream}"
    streamer = NUUOProducerTest(cfg.ip, cfg.port, cfg.user, cfg.passwd)
    streamer.connect(stream)
    streamer.download_test(area=area, profile=cfg.profile, fps=cfg.fps)
    streamer.disconnect()

def multiprocess(streams):
    p = []
    for stream in streams:
        process = Process(target=main_cfg, args=(stream,))
        p.append(process)
    for process in p:
        process.start()
    for process in p:
        process.join()

def processpool(streams, area_len):
    # Multiple process pool workers to fetch and upload to multiple streams
    pool = ProcessPoolExecutor(max_workers=area_len)
    results = pool.map(main_cfg, streams)
    for result in results:
        print(result)
    pool.close()
    pool.terminate()
    pool.join()

def async_thread(streams, area_len):
    # TODO: test with async loop
    raise NotImplementedError


if __name__ == '__main__':
    parser = argparse.ArgumentParser('NUUO Streamer')
    parser.add_argument('--ip', default='12.31.246.50', help='NVR IP address')
    parser.add_argument('--port', type=int, default=5250, help='NVR port')
    parser.add_argument('-u', '--user', default='admin', help='username')
    parser.add_argument('--passwd', default='admin', help='password')
    parser.add_argument('--profile', default='Original', help='Video compression quality')
    parser.add_argument('--fps', type=int, default=15, help='NVR source FPS')

    parser.add_argument('--stream', default='calstore-ucb,NUUO_TEST,MyKVStream,MyKVStream2', help='Destination AWS KV stream name')
    parser.add_argument('-a', '--area', default='First Floor Books,First Floor Entry,First Floor Reg. 1 and 2,First Floor Reg. 5 and 6', help='Camera area query')
 
    cfg = parser.parse_args()
    assert cfg.user, f"username unspecified"
    assert cfg.passwd, f"password unspecified"
    assert cfg.stream, f"destination stream name unspecified"

    main_cfg = partial(main, cfg)
    areas = cfg.area.split(',')
    streams = cfg.stream.split(',')

    if len(areas) != len(streams):
        print(f'Number of areas should equal number of streams')
        sys.exit()

    streams = list(zip(areas, streams))

    # Multiprocess
    multiprocess(streams)

    # Processpool
    #processpool(streams, len(areas))

   

  