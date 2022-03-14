import math
from threading import Thread
from abc import abstractmethod

import numpy as np

from ml import logging
from ml.ws.common import Dequeue

import gi
gi.require_version('GLib', '2.0')
gi.require_version('GObject', '2.0')
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
gi.require_version('GstRtp', '1.0')
gi.require_version('GstVideo', '1.0')
from gi.repository import Gst, GstRtp, GObject, GLib, GstApp, GstVideo
GObject.threads_init()
Gst.init(None)

from .gst_data import *

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

def get_np_dtype(fmt: GstVideo.VideoFormat):
    format_info = GstVideo.VideoFormat.get_info(fmt)
    return _DTYPES.get(format_info.bits, np.uint8)

def extract_buffer(sample: Gst.Sample):
    """Extracts Gst.Buffer from Gst.Sample and converts to np.ndarray"""
    buffer = sample.get_buffer()  # Gst.Buffer
    caps_format = sample.get_caps().get_structure(0)  # Gst.Structure  
    if caps_format.get_value('format') is None:
        buffer_size = buffer.get_size()
        array = np.ndarray(shape=buffer_size, buffer=buffer.extract_dup(0, buffer_size),
                        dtype=get_np_dtype(video_format))
        return np.squeeze(array), buffer.pts, buffer.duration, (-1, -1, -1)  # remove single dimension if exists
    else:
        video_format = GstVideo.VideoFormat.from_string(caps_format.get_value('format'))
        w, h = caps_format.get_value('width'), caps_format.get_value('height')
        c = get_num_channels(video_format)
        buffer_size = buffer.get_size()
        shape = (h, w, c) if (h * w * c == buffer_size) else buffer_size
        array = np.ndarray(shape=shape, buffer=buffer.extract_dup(0, buffer_size),
                           dtype=get_np_dtype(video_format))

        return np.squeeze(array), buffer.pts, buffer.duration, shape  # remove single dimension if exists

def make_element(factory_name, element_name):
    logging.debug(f'Creating element {element_name} of type {factory_name}')
    element = Gst.ElementFactory.make(factory_name, element_name)
    if not element:
        raise Exception(f'Unable to create element {element_name} of type {factory_name}')
    return element

def add_probe(pipeline, element_name, callback, pad_name="sink", probe_type=Gst.PadProbeType.BUFFER):
    logging.info("add_probe: Adding probe to %s pad of %s" % (pad_name, element_name))
    element = pipeline.get_by_name(element_name)
    if not element:
        raise Exception("Unable to get element %s" % element_name)
    sinkpad = element.get_static_pad(pad_name)
    if not sinkpad:
        raise Exception("Unable to get %s pad of %s" % (pad_name, element_name))
    sinkpad.add_probe(probe_type, callback, 0)

class GSTPipeline(Thread):
    def __init__(self, cfg, name=None, max_buffer=100, queue_timeout=10, daemon=True):
        super().__init__(daemon=daemon)
        self.name = name or self.__class__.__name__
        self.cfg = cfg
        self.state = STATE.NULL
        self.pipeline = None
        self.loop = None

        self.queue = Dequeue(maxlen=max_buffer) # Queue(maxsize=max_buffer)
        self.queue_timeout = queue_timeout

        self.setup()

    @abstractmethod
    def setup_elements(self):
        pass

    @abstractmethod
    def connect_element_signals(self):
        pass

    def on_status_changed(self, bus, message):
        """
        STATUS Change Bus Callback
        """
        state = message.parse_state_changed()
        self.state = STATE_CHANGE(
            STATE(state.oldstate),
            STATE(state.newstate),
            STATE(state.pending)
        )
        logging.debug(f"State | {self.state}")

    def on_eos(self, bus, message):
        """
        EOS Bus Callback
        """
        self.put(MESSAGE_TYPE.EOS, Exception(message))
        
    def on_error(self, bus, message):
        """
        ERROR Bus Callback
        """
        err, debug = message.parse_error()
        logging.debug(debug)
        message = f'Error received from element {message.src.get_name()}: {err}'
        self.put(MESSAGE_TYPE.ERROR, Exception(message))
        
    def connect_bus(self):
        self.bus.connect('message::error', self.on_error)
        self.bus.connect('message::state-changed', self.on_status_changed)
        self.bus.connect('message::eos', self.on_eos)

    def setup(self):
        """ Setup GST pipeline """
        self.pipeline = Gst.Pipeline()
        
        if not self.pipeline:
            raise Exception(f'Unable to create Pipeline')
        
        self.bus = self.pipeline.get_bus()
        self.bus.add_signal_watch()
        self.connect_bus()

        # add elements to pipeline
        try:
            self.setup_elements()
        except Exception as e:
            raise e
    
    def run(self):
        self.loop = GLib.MainLoop()
        # change state to PLAYING
        ret = self.pipeline.set_state(Gst.State.PLAYING)
        if ret == Gst.StateChangeReturn.FAILURE:
            message = f'Unable to set the pipeline to the playing state.'
            self.put(MESSAGE_TYPE.ERROR, Exception(message))
        self.loop.run()
        state = self.pipeline.set_state(Gst.State.NULL)
        if state != Gst.StateChangeReturn.SUCCESS:
            logging.warning('GST state change to NULL failed')

    def read(self):
        """
        Returns:
            tuple(message_type: MESSAGE_TYPE, message: FRAME | Exception)
        Raises:
            TimeoutError
        """
        try:
            value = self.queue.get(timeout=self.queue_timeout)
        except TimeoutError as e:
            raise e
        else:
            return value

    def put(self, message_type, message):
        self.queue.put((message_type, message))

    def close(self):
        logging.info(f"CLOSE {self.name}")
        self.loop.quit()
        '''
        state = self.pipeline.set_state(Gst.State.NULL)
        if state != Gst.StateChangeReturn.SUCCESS:
            logging.warning('GST state change to NULL failed')
        '''
        self.join(timeout=None)

class RTSPPipeline(GSTPipeline):
    def __init__(self, cfg, name=None, max_buffer=100, queue_timeout=10, daemon=True):
        super().__init__(cfg, name, max_buffer, queue_timeout, daemon)
        self._video_caps = None

    def on_new_sample(self, sink, udata):
        """Callback on 'new-sample' signal"""
        sample = sink.emit("pull-sample")  # Gst.Sample
        if isinstance(sample, Gst.Sample):
            frame, pts, duration, shape = extract_buffer(sample)
            if udata.rtcp_ntp_time_epoch_ns < 0:
                logging.warning(f"frame[{udata.frame_count}] out of sync ntp={udata.rtcp_ntp_time_epoch_ns} processed at {time.ctime()}")
                ntp = -1
            else:
                ntp = (udata.rtcp_ntp_time_epoch_ns + (pts - udata.rtcp_buffer_timestamp)) / Gst.SECOND
            current_frame = FRAME(
                data=frame,
                timestamp=ntp,
                pts=pts,
                duration=duration
            )
            self.put(MESSAGE_TYPE.FRAME, current_frame)
            udata.frame_count += 1
            return Gst.FlowReturn.OK
        return Gst.FlowReturn.ERROR

    def on_handle_sync(self, rtpjitterbuffer, properties, udata):
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
                udata.rtcp_ntp_time_epoch_ns = ntp
                udata.rtcp_buffer_timestamp = gstreamer_time # from rtptime
                logging.debug(f"on_handle_sync(): SR ntp_unix_ns={ntp}, rtptime={rtptime}")
                logging.debug(f"on_handle_sync(): base_time={base_time}, base_rtptime={base_rtptime}, sr_ext_rtptime={sr_ext_rtptime}, clock_rate={clock_rate} => time={gstreamer_time / Gst.SECOND}s, pts={buffer.pts}")    
            available = pkt.move_to_next()

    def on_new_jitterbuffer(self, rtpbin, jitterbuffer, session, ssrc, udata):
        logging.debug(f"on_new_jitterbuffer(): jitterbuffer={jitterbuffer.get_name()}")
        jitterbuffer.set_property('max-rtcp-rtp-time-diff', -1)
        jitterbuffer.connect('handle-sync', self.on_handle_sync, udata)

    def on_rtspsrc_new_manager(self, rtspsrc, manager, udata):
        logging.debug(f"on_rtspsrc_new_manager(): manager={type(manager)}")
        manager.connect('new-jitterbuffer', self.on_new_jitterbuffer, udata)

    def select_stream_callback(self, rtspsrc, num, caps, udata):
        media = caps.get_structure(0).get_value('media')
        encoding = caps.get_structure(0).get_value('encoding-name')

        # if encoding == 'H264':
        #     self.depay = make_element('rtph264depay', 'm_rtpdepay')
        #     self.pipeline.add(self.depay)
            
        # elif encoding == 'H265':
        #     self.depay = make_element('rtph265depay', 'm_rtpdepay')
        #     self.pipeline.add(self.depay)
        # else:
        #     logging.warning(f'{encoding} is not supported')
        #     return False

        # skip stream other than video
        if media == 'video':
            logging.debug(f"on_rtspsrc_select_stream(): to accept media[{num}]={media}")
            return True
        else:
            logging.debug(f"on_rtspsrc_select_stream(): to reject media[{num}]={media}")
            return False

    def rtspsrc_pad_callback(self, rtspsrc, pad, udata):
        caps = pad.get_current_caps()
        structure = caps.get_structure(0)
        if structure.get_string("media") == "video":
            self._video_caps = RTSP_CAPS(
                structure.get_int("payload").value,
                structure.get_int("clock-rate").value,
                structure.get_string("packetization-mode"),
                structure.get_string("encoding-name"),
                structure.get_string("profile-level-id"),
                structure.get_string("a-framerate")
            )
            logging.info(f"RTSP CAPS (VIDEO) | {self._video_caps}")
        name = structure.get_name()
        if name == 'application/x-rtp':
            # link depay element here since the pad is open now
            rtspsrc.link(self.depay)
    
    def connect_element_signals(self):
        self.source.connect('select-stream', self.select_stream_callback, self._v_ctx)
        self.source.connect("new-manager", self.on_rtspsrc_new_manager, self._v_ctx)
        self.source.connect('pad-added', self.rtspsrc_pad_callback, self._v_ctx)
        self.sink.connect("new-sample", self.on_new_sample, self._v_ctx)

    def setup_elements(self):
        
        self._v_ctx = StreamInfo()

        ''' SOURCE '''
        self.source = make_element('rtspsrc', 'source')
        # add to pipeline
        self.pipeline.add(self.source)
        # source properties
        self.source.set_property('location', self.cfg.location)
        self.source.set_property('protocols', self.cfg.protocols)
        self.source.set_property('latency', self.cfg.latency)
        self.source.set_property('user-id', self.cfg.user_id)
        self.source.set_property('user-pw', self.cfg.user_pw)
        self.source.set_property('ntp-sync', True)
        self.source.set_property('ntp-time-source', 0)
        # self.source.set_property('ignore-x-server-reply', True) gst-good > 1.19
        self.source.set_property('buffer-mode', 1)

        encoding = self.cfg.encoding
        assert encoding in ['H264', 'H265'], f'Invalid encoding option: {encoding}'

        ''' DEPAY '''
        self.depay = make_element(f'rtp{encoding.lower()}depay', 'depay')
        self.pipeline.add(self.depay)

        ''' PARSER '''
        self.parser = make_element(f'{encoding.lower()}parse', 'parse')
        self.pipeline.add(self.parser)

        ''' DECODE '''
        encoding_device = self.cfg.encoding_device
        if encoding_device == 'cpu':
            self.decode = make_element(f'avdec_{encoding.lower()}', 'decode')
        elif encoding_device == 'gpu':
            self.decode = make_element(f'nv{encoding.lower()}dec', 'decode')
        else:
            raise Exception(f'Invalid encoding device option: {encoding_device}')
        self.pipeline.add(self.decode)

        ''' CONVERT '''
        self.convert = make_element('videoconvert' ,'convert')
        self.pipeline.add(self.convert)

        ''' FRAME RATE '''
        self.framerate = make_element('videorate' ,'rate')
        self.pipeline.add(self.framerate)
        framerate = self.cfg.framerate
        if framerate:
            self.framerate.set_property('max-rate', framerate / 1)
            self.framerate.set_property('drop-only', 'true')

        ''' SCALE '''
        self.videoscale = make_element('videoscale', 'scale')
        self.pipeline.add(self.videoscale)
        self.filter = make_element("capsfilter", "filter")
        self.pipeline.add(self.filter)
        if self.cfg.scale:
            H, W = self.cfg.scale
            caps = Gst.caps_from_string(f'video/x-raw,width={W},height={H}')
            self.filter.set_property("caps", caps)

        ''' APP SINK'''
        self.sink = make_element('appsink', 'sink')
        self.pipeline.add(self.sink)
        # emit new-preroll and new-sample signals flags
        self.sink.set_property('emit-signals', True)
        # The allowed caps for the sink padflags: readable, writable Caps (NULL)
        colorformat = self.cfg.colorformat
        caps = Gst.caps_from_string('video/x-raw, format=(string){{{cf}}}'.format(cf=colorformat))
        self.sink.set_property('caps', caps)

        ''' LINK ELEMENTS '''
        assert self.depay.link(self.parser), f'Failed to link {self.depay.name} to {self.parser.name}'
        assert self.parser.link(self.decode), f'Failed to link {self.parser.name} to {self.decode.name}'
        assert self.decode.link(self.convert), f'Failed to link {self.decode.name} to {self.convert.name}'
        assert self.convert.link(self.framerate), f'Failed to link {self.convert.name} to {self.framerate.name}'
        assert self.framerate.link(self.videoscale), f'Failed to link {self.framerate.name} to {self.videoscale.name}'
        assert self.videoscale.link(self.filter), f'Failed to link {self.videoscale.name} to {self.filter.name}'
        assert self.filter.link(self.sink), f'Failed to link {self.filter.name} to {self.sink.name}'

        ''' SIGNALS '''
        self.connect_element_signals()
    
    def close(self):
        super().close()
        elements = []
        for elem in self.pipeline.children:
            elements.append(elem)
        for elem in elements:
            self.pipeline.remove(elem)
            elem.run_dispose()
            logging.debug(f"{elem.name} removed and disposed")
        self.pipeline.run_dispose()
        logging.debug(f"{self.pipeline.name} disposed")

class LocalPipeline(GSTPipeline):
    def __init__(self, cfg, name=None):
        super().__init__(cfg, name)
        raise NotImplementedError

class KinesisPipeline(GSTPipeline):
    def __init__(self, cfg, name=None):
        super().__init__(cfg, name)
        raise NotImplementedError

class YoutubePipeline(GSTPipeline):
    def __init__(self, cfg, name=None):
        super().__init__(cfg, name)
        raise NotImplementedError