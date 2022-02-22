from dataclasses import dataclass
from typing import Any
from collections import namedtuple
from queue import Queue, Empty
import threading
import sys
import math
import numpy as np

from time import time
from datetime import datetime
from ml import logging
from ml.gst import RTSPPipeline, MESSAGE_TYPE, RTSP_CONFIG
'''
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtp', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import GObject, GLib, Gst, GstRtp, GstApp, GstVideo
'''

def test_nvh264_leak():
    def run(pipeline):
        frame_idx = 0
        try:
            while True:
                msg_type, message = pipeline.read()
                if msg_type == MESSAGE_TYPE.FRAME:
                    frame = message.data
                    timestamp = message.timestamp
                    duration = message.duration
                    timestamp_frame = datetime.utcfromtimestamp(timestamp).strftime('%m-%d-%Y %H:%M:%S.%f')
                    timestamp_now = datetime.utcfromtimestamp(time()).strftime('%m-%d-%Y %H:%M:%S.%f')
                    logging.info(f'Frame: {frame_idx} | Current: {timestamp_now} | Frame: {timestamp_frame}')
                    frame_idx +=1
                    if frame_idx > 50:
                        break
                elif msg_type == MESSAGE_TYPE.EOS:
                    raise message
                elif msg_type == MESSAGE_TYPE.ERROR:
                    raise message
                else:
                    logging.warning('Unknown message type')
                    break
        except KeyboardInterrupt:
            logging.info('Stopping stream...')
        except Exception as e:
            logging.error(f'{e}')

    # pipeline config
    cfg = RTSP_CONFIG(
        location = 'rtsp://wowzaec2demo.streamlock.net/vod/mp4:BigBuckBunny_115k.mp4',
        latency = 5000,
        protocols = 'udp', # tcp | udp | http | udp_mcast | unknown | tls
        encoding = 'H264', # h264 | h265
        encoding_device = 'gpu', # cpu | gpu
        framerate = 10,
        scale = (720, 1280) # (H, W)
    )

    count = 0
    while True:
        # init pipeline
        pipeline = RTSPPipeline(cfg)
        logging.info(f"RTSP pipelinie[{count}] created with queue size={len(pipeline.queue)}")
        # start
        pipeline.start()
        # run 
        run(pipeline)
        # close 
        pipeline.close()
        logging.info(f"RTSP pipelinie[{count}] closed with {len(pipeline.queue)} frames left in queue")
        del pipeline
        count += 1