from threading import Thread
from abc import abstractmethod
from turtle import heading, width

import numpy as np

from ml import logging
from ml.ws.common import Dequeue

import gi
gi.require_version('Gst', '1.0')
gi.require_version('GLib', '2.0')
gi.require_version('GstRtp', '1.0')
gi.require_version('GstRtsp', '1.0')
from gi.repository import Gst, GstRtp, GstRtsp, GObject, GLib
Gst.init(None)

from .gst_data import *

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
        self.pipeline.unref()

class RTSPPipeline(GSTPipeline):
    def __init__(self, cfg, name=None, max_buffer=100, queue_timeout=10, daemon=True):
        super().__init__(cfg, name, max_buffer, queue_timeout, daemon)
        self.video_caps = None
        self.rtcp_ntp_time_epoch_ns = None
        self.rtcp_buffer_timestamp = None

    def on_new_sample(self, sink, udata):
        sample = sink.emit('pull_sample')
        buffer = sample.get_buffer()

        # get read access to the buffer data
        success, map_info = buffer.map(Gst.MapFlags.READ)
        if not success:
            self.put(MESSAGE_TYPE.ERROR, RuntimeError("Could not map buffer data!"))

        # extract the width and height info from the sample's caps
        caps = sample.get_caps()    
        shape = (caps.get_structure(0).get_value('height'), caps.get_structure(0).get_value('width'), 3)

        # calculate GstBuffer's NTP Time
        ntp_timestamp = 0
        '''
        ==> buffer_ntp_ns = rtcp_ntp_time_epoch_ns + (GST_BUFFER_PTS(buffer) - rtcp_buffer_timestamp)
        @rtcp_ntp_time_epoch_ns [IN] The 64-bit RTCP NTP Timestamp (IETF RFC 3550; RTCP)
            converted to epoch time in nanoseconds - GstClockTime 
        @rtcp_buffer_timestamp [IN] The Buffer PTS (as close as possible to the RTCP buffer
            timestamp which carried the Sender Report); This timestamp is 
            synchronized with the stream's RTP buffer timestamps on GStreamer clock
        '''
        duration = buffer.duration
        if self.rtcp_ntp_time_epoch_ns is not None:
            # calc buffer ntp timestamp
            # FIXME: time jump due to sudden increase in rtcp_buffer_timestamp
            self.rtcp_ntp_time_epoch_ns += duration
            buffer_ntp_ns = self.rtcp_ntp_time_epoch_ns #+ (buffer.pts - self.rtcp_buffer_timestamp)
            # nsec to sec
            ntp_timestamp = buffer_ntp_ns / 10 ** 9

        # NOTE: gst buffer is not writable 
        arr = np.ndarray(
            shape=shape,
            buffer=map_info.data,
            dtype=np.uint8,
            order='C'
        )

        current_frame = FRAME(
            data=arr,
            timestamp=ntp_timestamp,
            height=shape[0],
            width=shape[1],
            channels=shape[-1],
            duration=duration
        )

        if ntp_timestamp:
            self.put(MESSAGE_TYPE.FRAME, current_frame)

        # clean up the buffer mapping
        buffer.unmap(map_info)

        return Gst.FlowReturn.OK

    def handle_sync_callback(self, jitter_buffer, struct, udata):
        """
        struct: 
            application/x-rtp-sync, base-rtptime=(guint64)2869166795, base-time=(guint64)2375045731, clock-rate=(uint)90000, clock-base=(guint64)2869166795, sr-ext-rtptime=(guint64)2869304534, 
            sr-buffer=(buffer)80c80006f1f21e54e4f7076c1bdd872fab061cd60000021d000a9eb081ca000cf1f21e54011c757365723135353632343635363340686f73742d313335643865316506094753747265616d6572000000;
        """
        # get buffer and time values from struct
        sr_buffer = struct.get_value('sr-buffer')
        gstreamer_time = struct.get_value('base-time')
        base_rtptime = struct.get_value('base-rtptime')
        sr_ext_rtptime = struct.get_value('sr-ext-rtptime') 
        clock_rate = struct.get_value('clock-rate')

        gstreamer_time += Gst.util_uint64_scale(sr_ext_rtptime - base_rtptime, Gst.SECOND, clock_rate)

        if(not sr_buffer): 
            return

        rtcp = GstRtp.RTCPBuffer()
        GstRtp.RTCPBuffer.map(sr_buffer, Gst.MapFlags.READ, rtcp)
        packet = GstRtp.RTCPPacket()
        res = rtcp.get_first_packet(packet)
        pkt_exists = True
        while pkt_exists and res:
            pkt_type = packet.get_type()
            if pkt_type == GstRtp.RTCPType.SR:  # SR
                # get NTP and RTP times 
                sr_info = packet.sr_get_sender_info()
                '''
                rtptime - which is the timestamp synchronized with corresponding
                RTP Stream is dropped as we have the same mapped to Gstreamer clock
                in rtpbin (manager) plugin which is eventually saved in
                gstreamer_time as "base-time".
                NOTE: This is the latest incoming RTP-buffer-time aligned to
                gstreamer clock + clock-skew between receiver and sender
                FOR OSS REFERENCE: Specific code in rtpmanager:
                do_handle_sync(), rtp_jitter_buffer_get_sync() in
                gst-plugins-good/gst/rtpmanager/gstrtpjitterbuffer.c
                '''
                '''
                RTCP RFC 3550; The full-resolution
                NTP timestamp is a 64-bit unsigned fixed-point number with
                the integer part in the first 32 bits and the fractional part in the
                last 32 bits.
                The NTP timescale wraps around every 2^32 seconds (136 years);
                the first rollover will occur in 2036.
                The higher 32-bits carry epoch time + 2208988800LL (later is a const introduced by gstrtpbin.c)
                To convert fractional part of NTP:
                NTP fraction * (fraction of seconds) / 2 ^ 32.
                For example of NTP fraction 1329481807, to convert to microsecond: = 1329481807 * (10 ^ 6) / 2 ^ 32 = 309544us (roughly)
                '''
                # Extract higher 32-bit epoch into tv_sec
                tv_sec = ((sr_info.ntptime >> 32) - 2208988800) 
                # Extract lower 32-bit fraction into tv_nsec
                tv_nsec = ((sr_info.ntptime & (0xFFFFFFFF)) * Gst.SECOND) >> 32
                # Total NTP timestamp
                rtcp_ntp_time_epoch_ns = tv_sec * Gst.SECOND + tv_nsec * Gst.NSECOND

                self.rtcp_ntp_time_epoch_ns = rtcp_ntp_time_epoch_ns
                self.rtcp_buffer_timestamp =  gstreamer_time
            # else: pass # INVALID, SDES, RR, BYE, APP, RTPFB, PSFB, XR

            # move pointer to next packet
            pkt_exists = packet.move_to_next()

    def new_jitter_buffer_callback(self, rtpbin, jitterbuffer, session, ssrc, udata):
        # request for the `handle-sync` signal in jitterbuffer to lawfully tap RTCP Sender Report
        jitterbuffer.connect('handle-sync', self.handle_sync_callback, udata)
        # allow RTCP SR reports to be infinitely ahead than the data stream (useful for very low fps streams)
        jitterbuffer.set_property('max-rtcp-rtp-time-diff', -1)
        return Gst.FlowReturn.OK

    def element_added_callback(self, rtspsrc, manager, udata):
        # rtpbin: request new-jitterbuffer signal
        if manager.name == 'manager':
            manager.connect('new-jitterbuffer', self.new_jitter_buffer_callback, udata)
        return Gst.FlowReturn.OK

    def select_stream_callback(self, rtspsrc, num, caps, udata):
        media = caps.get_structure(0).get_value('media')
        encoding = caps.get_structure(0).get_value('encoding-name')

        # skip stream other than video
        if media != 'video':
            return False

        # if encoding == 'H264':
        #     self.depay = make_element('rtph264depay', 'm_rtpdepay')
        #     self.pipeline.add(self.depay)
            
        # elif encoding == 'H265':
        #     self.depay = make_element('rtph265depay', 'm_rtpdepay')
        #     self.pipeline.add(self.depay)
        # else:
        #     logging.warning(f'{encoding} is not supported')
        #     return False

        return True

    def rtspsrc_pad_callback(self, rtspsrc, pad, udata):
        caps = pad.get_current_caps()
        structure = caps.get_structure(0)
        if structure.get_string("media") == "video":
            self.video_caps = RTSP_CAPS(
                structure.get_int("payload").value,
                structure.get_int("clock-rate").value,
                structure.get_string("packetization-mode"),
                structure.get_string("encoding-name"),
                structure.get_string("profile-level-id"),
                structure.get_string("a-framerate")
            )
            logging.info(f"RTSP CAPS (VIDEO) | {self.video_caps}")
        name = structure.get_name()
        if name == 'application/x-rtp':
            # link depay element here since the pad is open now
            rtspsrc.link(self.depay)
    
    def connect_element_signals(self):
        self.source.connect('element-added', self.element_added_callback, None)
        self.source.connect('select-stream', self.select_stream_callback, None)
        self.source.connect('pad-added', self.rtspsrc_pad_callback, None)
        self.sink.connect("new-sample", self.on_new_sample, None)

    def setup_elements(self):
        ''' SOURCE '''
        self.source = make_element('rtspsrc', 'source')
        # add to pipeline
        self.pipeline.add(self.source)
        # source properties
        self.source.set_property('location', self.cfg.location)
        self.source.set_property('protocols', self.cfg.protocols)
        self.source.set_property('ntp-sync', True)
        self.source.set_property('buffer-mode', 0)
        self.source.set_property('latency', self.cfg.latency)
        self.source.set_property('user-id', self.cfg.user_id)
        self.source.set_property('user-pw', self.cfg.user_pw)

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

        self.filter = Gst.ElementFactory.make("capsfilter", "filter")
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
        caps = Gst.caps_from_string('video/x-raw, format=(string){BGR, GRAY8}')
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
        self.decode.unref()

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