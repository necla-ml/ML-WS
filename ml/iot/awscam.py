# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

from pathlib import Path
from time import time, sleep
import os

from ml import av, logging

MXUVC_BIN = "/opt/awscam/camera/installed/bin/mxuvc"
RESOLUTION = {1080 : (1920, 1080), 720 : (1280, 720), 480 : (858, 480)}

DEFAULT_CH_H264 = '/opt/awscam/out/ch1_out.h264'
DEFAULT_CH_MJPG = '/opt/awscam/out/ch2_out.mjpeg'
ENCODERS = { 1: DEFAULT_CH_H264, 2: DEFAULT_CH_MJPG }

class AWSCam(object):
    def __init__(self, resolution=None, fps=None, gop=None, bitrate=None, ch=1):
        if fps:
            ret = os.system(f"{MXUVC_BIN} --ch {ch} framerate {fps}")
            if ret > 0:
                self.fps = None
                logging.error(f"Failed to set FPS to {fps} w/ ret={ret}, typically permission denied")
            else:
                self.fps = fps

        if resolution:
            res = RESOLUTION[resolution]
            ret = os.system(f"{MXUVC_BIN} --ch {ch} resolution {res[0]} {res[1]}")
            if ret > 0:
                self.resolution = None
                logging.error(f"Failed to set resolution to {res} w/ ret={ret}, typically permission denied")
            else:
                self.resolution = resolution

        gop = gop or fps
        if gop:
            ret = os.system(f"{MXUVC_BIN} --ch {ch} gop {gop}")
            if ret > 0:
                self.gop = None
                logging.error(f"Failed to set GOP to {gop} w/ ret={ret}, typically permission denied")
            else:
                self.gop = gop

        if bitrate:
            ret = os.system(f"{MXUVC_BIN} --ch {ch} bitrate {bitrate}")
            if ret > 0:
                self.bitrate = None
                logging.error(f"Failed to set bitrate to {bitrate} w/ ret={ret}, typically permission denied")
            else:
                self.bitrate = bitrate

        ret = os.system(f"{MXUVC_BIN} --ch {ch} iframe")
        if ret > 0:
            logging.error(f"Failed to force generating an IFrame")
        
        self.stream = None
        self.ch = ch

    def open(self):
        if self.stream is not None:
            logging.warning("Already opened")
            return False
        
        self.encoder = av.open(ENCODERS[self.ch])
        self.video = self.encoder.streams[0]
        self.codec = self.video.codec_context
        self.stream = self.encoder.demux()
        
        self.duration = float(self.codec.time_base * self.codec.ticks_per_frame)
        self.fps = 1 / self.duration        # nominal FPS
        self.rate = float(self.codec.rate)  # average FPS

        self.started = False
        self.start = None
        self.time = None
        self.frames = 0
        return True

    def close(self):
        if self.stream is not None:
            self.encoder.close()
            self.encoder = self.stream = self.video = self.codec = None
            logging.info(f"{self.frames} fresh frames read at {self.frames / (time() - self.start):.2f}FPS since open")

    def read(self):
        if self.stream is None:
            logging.warning('No stream open')
            return None

        if not self.started:
            # Read until the first fresh key frame w.r.t. the specified FPS
            prev = time()
            for i, frame in enumerate(self.stream):
                now = time()
                if (now - prev) < 9 * self.duration / 10 or not frame.is_keyframe:
                    logging.warning(f"frame[{i}] Skipped a buffered stale {frame.is_keyframe and 'key ' or '    '}frame for short duration of {now - prev:.3f}s < {self.duration:.3f}s")
                    prev = now
                    continue
                else:
                    self.started = True
                    self.time = self.start = now
                    logging.info(f"frame[{i}] First fresh key frame")
                    break
        else:
            # Compensate unexpected encoder latency
            frame = next(self.stream)
            now = time()
            prev = self.time
            self.time += self.duration
            duration = float(frame.time_base * frame.duration)
            self.duration = duration + (now - self.time) / 2
            '''            
            actual = time() - self.time
            if actual > duration:
                self.time += (duration + actual) / 2
                logging.warning(f"Unexpected frame latency: {actual:.6f}s > {duration:.6f}s")
            else:
                self.time += duration
            '''
        self.frames += 1
        return frame