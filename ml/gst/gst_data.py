import time
import collections
from enum import Enum
from typing import Any
from dataclasses import dataclass 

import numpy as np

class MESSAGE_TYPE(Enum):
    EOS = 0
    ERROR = 1
    FRAME = 2

@dataclass
class StreamInfo: 
    frame_count: int = 0
    rtcp_ntp_time_epoch_ns: int = -1
    rtcp_buffer_timestamp: int = -1

@dataclass
class FRAME:
    data: np.ndarray = None
    timestamp: float = -1
    pts: int = -1
    duration: int = -1

@dataclass
class RTSP_CONFIG:
    """
    Config for RTSP pipeline
    """
    location: str = ''
    latency: int = 100
    protocols: int = 'tcp' # tcp | udp | http | udp-mcast | unknown | tls
    user_id: str = ''
    user_pw: str = ''

    encoding: str = 'H264' # h264 | h265
    encoding_device: str = 'cpu' # cpu | gpu

    framerate: int = 10
    scale: tuple = () # (H, W)


class STATE(Enum):
    """
    VOID_PENDING     no pending state.
    NULL             the None state or initial state of an element.
    READY            the element is ready to go to PAUSED.
    PAUSED           the element is PAUSED, it is ready to accept and process data. Sink elements however only accept one buffer and then block.
    PLAYING          the element is PLAYING, the Gst.Clock is running and the data is flowing.
    """
    VOID_PENDING = 0
    NULL = 1
    READY = 2
    PAUSED = 3
    PLAYING = 4
    
    def __str__(self):
        return str(self.name)

@dataclass
class STATE_CHANGE:
    """
    oldstate	the previous state, or None
    newstate	the new (current) state, or None
    pending	    the pending (target) state, or None
    """
    oldstate: str
    newstate: str
    pending: str
    
@dataclass
class RTSP_CAPS:
    """
    RTSP Caps
    """
    payload: int
    clock_rate: int
    packetization_mode: str
    encoding_name: str
    profile_level_id: str
    framerate: float

class FPS_CALCULATOR:
    """
    Calculate avg fps
    """
    def __init__(self,avarageof=50):
        self.frametimestamps = collections.deque(maxlen=avarageof)
    
    def __call__(self):
        self.frametimestamps.append(time.time())
        if(len(self.frametimestamps) > 1):
            return round(len(self.frametimestamps)/(self.frametimestamps[-1]-self.frametimestamps[0]))
        else:
            return 0.0