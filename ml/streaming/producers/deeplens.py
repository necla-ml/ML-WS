# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import os
from time import time, sleep
from ml.time import HUNDREDS_OF_NANOS_SEC

from ...iot import AWSCam
from .._C import ffi, lib
from . import KVProducer, DEFAULT_FPS_VALUE

class DeepLensProducer(KVProducer):
    def __init__(self, accessKey=None, secretKey=None):
        # AWS/KVS credentials given or from ENV
        super(DeepLensProducer, self).__init__(accessKey, secretKey)

    def upload(self, resolution=720, gop=None, bitrate=2000000, fps=DEFAULT_FPS_VALUE, ch=1, duration=None):
        cam = AWSCam(resolution=resolution, fps=fps, gop=gop, bitrate=bitrate, ch=ch)
        cam.open()

        import signal
        signaled = False
        def handler(sig, frame):
            nonlocal signaled
            print(f"Interrupted or stopped by {signal.Signals(sig).name}")
            signaled = True
        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        
        frameIndex = 0
        retStatus = ffi.integer_const('STATUS_SUCCESS')
        
        pFrame = ffi.new('PFrame')
        pFrame.version = ffi.integer_const('FRAME_CURRENT_VERSION')
        pFrame.trackId = ffi.integer_const('DEFAULT_VIDEO_TRACK_ID')
        pFrame.duration = int(HUNDREDS_OF_NANOS_SEC / cam.fps)

        streamStartTime = lib.defaultGetTime()
        streamStopTime = None
        if duration is not None:
            streamingDuration = duration * HUNDREDS_OF_NANOS_SEC
            streamStopTime = streamStartTime + streamingDuration
            print(f"Streaming stops in {streamStopTime / HUNDREDS_OF_NANOS_SEC:.3f}s")
        else:
            print(f"Streaming indefinitely")

        while (streamStopTime is None or lib.defaultGetTime() < streamStopTime) and not signaled:
            frame = cam.read()
            now = time()
            
            pFrame.frameData = ffi.cast('void*', frame.buffer_ptr)
            pFrame.size = frame.size
            pFrame.index = frameIndex
            pFrame.flags = ffi.integer_const('FRAME_FLAG_KEY_FRAME') if frame.is_keyframe else ffi.integer_const('FRAME_FLAG_NONE')
            pFrame.duration = int(cam.duration * HUNDREDS_OF_NANOS_SEC)
            pFrame.presentationTs = pFrame.decodingTs = int(cam.time * HUNDREDS_OF_NANOS_SEC)            
            ret = lib.putKinesisVideoFrame(self.streamHandle, pFrame)
            if ret > 0:
                print(f"Failed to send a frame to KVS with ret={ret:#04x}")
                break
            else:    
                print(f"Sent {'key ' if frame.is_keyframe else ''}frame[{frameIndex}] of duration {cam.duration:.3f}s to KVS with timestamp {cam.time:.3f}s at {now:.3f}s", )
            frameIndex += 1

        cam.close()
