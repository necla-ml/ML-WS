from collections import namedtuple
from queue import Queue, Empty
from dataclasses import dataclass
from typing import Any
import threading
import sys
import math
import time
import traceback
import numpy as np
import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstRtp', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstVideo', '1.0')

from gi.repository import GObject, GLib, Gst, GstRtp, GstApp, GstVideo

@dataclass
class StreamInfo:
    id: int = -1 
    frameCount: int = 0
    anomaly_count: int = 0 
    meta_number: int = -1 
    frames: Queue = Queue()
    timespec_first_frame_ns: int = -1 
    gst_ts_first_frame_ns: int = -1 
    lock_stream_rtcp_sr: Any = None
    rtcp_ntp_time_epoch_ns: int = -1
    rtcp_buffer_timestamp: int = -1
    last_ntp_time_ns: int = -1 
    done: bool = False

URL = 'rtsph://127.0.0.1:8554/test'
DEFAULT_CMD = f"gst-launch-1.0 -v rtspsrc ignore-x-server-reply=true location={URL} ! fakesink"
DEFAULT_PIPELINE = f"rtspsrc latency=2000 ignore-x-server-reply=true ntp-sync=true ntp-time-source=0 buffer-mode=1 location={URL} name=src ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videoscale ! video/x-raw,format=RGB,width=640,height=480 ! appsink emit-signals=True name=output"

def has_flag(value: GstVideo.VideoFormatFlags,
             flag: GstVideo.VideoFormatFlags) -> bool:

    # in VideoFormatFlags each new value is 1 << 2**{0...8}
    return bool(value & (1 << max(1, math.ceil(math.log2(int(flag))))))

def _get_num_channels(fmt: GstVideo.VideoFormat) -> int:
    """
        -1: means complex format (YUV, ...)
    """
    frmt_info = GstVideo.VideoFormat.get_info(fmt)
    
    # temporal fix
    if fmt == GstVideo.VideoFormat.BGRX:
        return 4
    
    if has_flag(frmt_info.flags, GstVideo.VideoFormatFlags.ALPHA):
        return 4

    if has_flag(frmt_info.flags, GstVideo.VideoFormatFlags.RGB):
        return 3

    if has_flag(frmt_info.flags, GstVideo.VideoFormatFlags.GRAY):
        return 1

    return -1

_ALL_VIDEO_FORMATS = [GstVideo.VideoFormat.from_string(
    f.strip()) for f in GstVideo.VIDEO_FORMATS_ALL.strip('{ }').split(',')]
_ALL_VIDEO_FORMAT_CHANNELS = {fmt: _get_num_channels(fmt) for fmt in _ALL_VIDEO_FORMATS}
_DTYPES = {
    16: np.int16,
}

def get_num_channels(fmt: GstVideo.VideoFormat):
    return _ALL_VIDEO_FORMAT_CHANNELS[fmt]


def get_np_dtype(fmt: GstVideo.VideoFormat) -> np.number:
    format_info = GstVideo.VideoFormat.get_info(fmt)
    return _DTYPES.get(format_info.bits, np.uint8)

def on_message(bus: Gst.Bus, message: Gst.Message, loop: GLib.MainLoop):
    mtype = message.type
    """
        Gstreamer Message Types and how to parse
        https://lazka.github.io/pgi-docs/Gst-1.0/flags.html#Gst.MessageType
    """
    if mtype == Gst.MessageType.EOS:
        print("End of stream")
        loop.quit()

    elif mtype == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print(err, debug)
        loop.quit()

    elif mtype == Gst.MessageType.WARNING:
        err, debug = message.parse_warning()
        print(err, debug)

    return True

def extract_buffer(sample: Gst.Sample) -> np.ndarray:
    """Extracts Gst.Buffer from Gst.Sample and converts to np.ndarray"""
    buffer = sample.get_buffer()  # Gst.Buffer
    # print(buffer.pts, buffer.dts, buffer.offset)
    caps_format = sample.get_caps().get_structure(0)  # Gst.Structure
    if caps_format.get_value('format') is None:
        buffer_size = buffer.get_size()
        array = np.ndarray(shape=buffer_size, buffer=buffer.extract_dup(0, buffer_size),
                        dtype=get_np_dtype(video_format))
        return np.squeeze(array), buffer.pts  # remove single dimension if exists
    else:
        video_format = GstVideo.VideoFormat.from_string(caps_format.get_value('format'))
        w, h = caps_format.get_value('width'), caps_format.get_value('height')
        c = get_num_channels(video_format)
        buffer_size = buffer.get_size()
        shape = (h, w, c) if (h * w * c == buffer_size) else buffer_size
        array = np.ndarray(shape=shape, buffer=buffer.extract_dup(0, buffer_size),
                           dtype=get_np_dtype(video_format))

        return np.squeeze(array), buffer.pts  # remove single dimension if exists

def on_buffer(sink: GstApp.AppSink, vCtx: StreamInfo) -> Gst.FlowReturn:
    """Callback on 'new-sample' signal"""
    sample = sink.emit("pull-sample")  # Gst.Sample
    if isinstance(sample, Gst.Sample):
        sink_time = sink.get_clock().get_time()
        frame, pts = extract_buffer(sample)
        vCtx.frames.put((frame, pts))
        print(f"{pts}: Received frame{frame.shape} of type {frame.dtype}")
        return Gst.FlowReturn.OK
    return Gst.FlowReturn.ERROR

def on_handle_sync(rtpjitterbuffer, properties, vCtx):
    clock_base = properties.get_value('clock-base')
    clock_rate = properties.get_value('clock-rate')
    base_time = properties.get_value('base-time')
    base_rtptime = properties.get_value('base-rtptime')
    sr_ext_rtptime = properties.get_value('sr-ext-rtptime')
    buffer = properties.get_value('sr-buffer')
    gstreamer_time = base_time + Gst.util_uint64_scale(sr_ext_rtptime - base_rtptime, Gst.SECOND, clock_rate)

    rtcp = GstRtp.RTCPBuffer()
    pkt = GstRtp.RTCPPacket()
    GstRtp.rtcp_buffer_map(buffer, Gst.MapFlags.READ, rtcp)
    available = rtcp.get_first_packet(pkt)
    while available:
        if pkt.get_type() == GstRtp.RTCPType.SR:
            # FIXME: check if video stream
            ssrc, ntptime, rtptime, _, _ = pkt.sr_get_sender_info()
            ntp = ((ntptime >> 32) - (70 * 365 + 17) * 86400) * Gst.SECOND # unix time in ns
            ntp += ((ntptime & 0xFFFFFFFF) * Gst.SECOND) >> 32
            vCtx.rtcp_ntp_time_epoch_ns = ntp
            vCtx.rtcp_buffer_timestamp = gstreamer_time # from rtptime
            print(f"on_handle_sync(): SR ntp_unix_ns={ntp}, rtptime={rtptime}")
            print(f"on_handle_sync(): base_time={base_time}, base_rtptime={base_rtptime}, sr_ext_rtptime={sr_ext_rtptime}, clock_rate={clock_rate} => time={gstreamer_time / Gst.SECOND}s, pts={buffer.pts}")    
        available = pkt.move_to_next()
    
def on_new_jitterbuffer(rtpbin, jitterbuffer, session, ssrc, udata):
    print(f"on_new_jitterbuffer(): jitterbuffer={jitterbuffer.get_name()}")
    jitterbuffer.set_property('max-rtcp-rtp-time-diff', -1)
    jitterbuffer.connect('handle-sync', on_handle_sync, udata)

def on_rtspsrc_new_manager(rtspsrc, manager, udata):
    print(f"on_rtspsrc_new_manager(): manager={type(manager)}")
    manager.connect('new-jitterbuffer', on_new_jitterbuffer, udata)

def on_rtspsrc_select_stream (rtspsrc, num, caps, udata):
    properties = caps.get_structure(0)
    media = properties.get_value('media')
    if media == 'video':
        print(f"on_rtspsrc_select_stream(): to accept media[{num}]={media}")
        return True
    else:
        print(f"on_rtspsrc_select_stream(): to reject media[{num}]={media}")
        return False

def work(vCtx):
    import threading
    name = threading.current_thread().name
    print(f"{name}[{threading.get_ident()}] running...")
    i = 0
    while not vCtx.done:
        #print(vCtx)
        #print(vCtx.frames)
        try:
            frame, pts = vCtx.frames.get(block=True, timeout=1)
        except Empty as e:
            print(f"No frames yet")
        else:
            if vCtx.rtcp_ntp_time_epoch_ns < 0:
                print(f"frame[{i}] out of sync ntp={vCtx.rtcp_ntp_time_epoch_ns} processed at {time.ctime()}")
            else:
                # print(f"Got frame pts={pts}, rtcp_ntp={vCtx.rtcp_ntp_time_epoch_ns}, rtcp_pts={vCtx.rtcp_buffer_timestamp}")
                ntp = (vCtx.rtcp_ntp_time_epoch_ns + (pts - vCtx.rtcp_buffer_timestamp)) / Gst.SECOND
                print(f"frame[{i}] ntp={time.ctime(ntp)} processed at {time.ctime()}")
            i += 1
    print(f"{name}[{threading.get_ident()}] exiting...")

def test_rtsp():
    Gst.init(None)
    #Gst.debug_set_active(True)
    #Gst.debug_set_default_threshold(Gst.DebugLevel.LOG)
    print()

    pipeline = Gst.parse_launch(DEFAULT_PIPELINE)
    src = pipeline.get_by_name('src')
    appsink = pipeline.get_by_name('output')  # get AppSink
    loop = GLib.MainLoop()

    vCtx = StreamInfo()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", on_message, loop)
    src.connect("select-stream", on_rtspsrc_select_stream, vCtx)
    src.connect("new-manager", on_rtspsrc_new_manager, vCtx)
    appsink.connect("new-sample", on_buffer, vCtx)
    
    worker = threading.Thread(name="Consumer", target=work, args=(vCtx,))
    worker.start()
    pipeline.set_state(Gst.State.PLAYING)
    try:
        loop.run()
    except KeyboardInterrupt as e:
        print('Interrupted to quit...')
    except Exception:
        traceback.print_exc()
    finally:
        pipeline.set_state(Gst.State.NULL)
        loop.quit()
        vCtx.done = True
        print(f"Joining worker {worker.name}")
        worker.join()
        print(f"Joined worker {worker.name}")