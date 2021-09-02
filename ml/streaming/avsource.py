# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import os
from pathlib import Path
from datetime import datetime

from ml import av, time, logging
from ml.av import NALU_t, NALUParser, hasStartCode
from ml.time import localtime, strftime

obj_type = type

def openAV(src, decoding=False, with_audio=False, **kwargs):
    try:
        format = None
        options = None
        fps = float(kwargs.get('fps', 10))
        if str(src).startswith('rtsp'):
            # ffmpeg RTSP options:
            # rtsp_transport: tcp, http, udp_multicast, udp
            # rtsp_flags: prefer_tcp, filter_src, listen, none
            # allowed_media_types: video, audio, data
            # stimeout: socket TCP I/O timeout in us
            # RTSP/HTTP required for VPN but not necessarily supported
            options = dict(rtsp_transport=kwargs.get('rtsp_transport', 'tcp'),
                           rtsp_flags='prefer_tcp',
                           stimeout=kwargs.get('stimeout', '5000000')) # in case of network down

            # NOTE: retry with different rtsp transport types if unspecified
            if options and options.get('rtsp_transport', None):
                source = None
                for transport in ['tcp', 'http']:
                    try:
                        options['rtsp_transport'] = transport
                        source = av.open(src, format=format, options=options, timeout=(15, 5))
                    except Exception as e:
                        logging.warning(f'Failed with rtsp_transport={transport}: {e}')
                    else:
                        options['rtsp_transport'] = transport
                        break
                assert source is not None, f"Failed to open RTSP source over TCP/HTTP"
            else:
                source = av.open(src, format=format, options=options, timeout=(15, 5))
        else:
            if isinstance(src, int) or (isinstance(src, str) and src.startswith('/dev/video')):
                # XXX webcam: high FPS with MJPG
                import platform
                system = platform.system()
                resolution = av.resolution_str(*kwargs.get('resolution', ['720p']))
                options = {'framerate': str(fps), 'video_size': resolution, 'input_format':'mjpeg'}
                decoding = True
                if system == 'Darwin':
                    src = str(src)
                    format = 'avfoundation'
                elif system == 'Linux':
                    src = f"/dev/video{src}" if isinstance(src, int) else src
                else:
                    raise ValueError(f"Webcam unsupported on {system}")
            source = av.open(src, format=format, options=options)

        # timeout: maximum timeout (in secs) to wait for incoming connections and soket reading
        # XXX HLS connection potential time out for taking more than 5s
        logging.info(f"av.open({src}, format={format}, options={options}, timeout=(15, 5))")
    except Exception as e:
        logging.error(e)
        raise e
    else:
        '''
        H.264 NALU formats:
        Annex b.: 
            RTSP/RTP: rtsp, 'RTSP input', set()
            bitstream: h264, 'raw H.264 video', {'h26l', 'h264', '264', 'avc'}
            webcam: /dev/videoX, ...
            DeepLens: /opt/.../...out
            NUUO/NVR: N/A
        AVCC:
            avi: avi, 'AVI (Audio Video Interleaved)', {'avi'}
            mp4: 'mov,mp4,m4a,3gp,3g2,mj2', 'QuickTime / MOV', {'m4a', 'mov', 'mp4', 'mj2', '3gp', '3g2'}
            webm/mkv: 'matroska,webm', 'Matroska / WebM', {'mks', 'mka', 'mkv', 'mk3d'}
            DASH/KVS(StreamBody): file-like obj
        '''
        now = time.time()
        start_time = source.start_time / 1e6 # us
        relative = abs(start_time - now) > 60*60*24*30          # Too small to be absolute
        rt = not(isinstance(src, str) and os.path.isfile(src))  # regular file or not
        if rt:
            logging.info(f"Assume real-time source: {src}")
        else:
            logging.info(f"Simulating local source as real-time: {src}")

        # XXX start_time may be negative (webcam), zero if unavailable, or a small logical timestamp
        session = dict(
            src=src,
            streams=source,
            format=source.format.name,
            decoding=decoding,
            start=relative and now or start_time,
            rt=rt,
        )
        session_start_local = strftime('%X', localtime(session['start']))
        source_start_local = strftime('%X', localtime(start_time))
        logging.info(f"Session start: {session['start']:.3f}s({session_start_local}), source start: {start_time:.3f}s({source_start_local})")
        if source.streams.video:
            # FIXME RTSP FPS might be unavailable or incorrectly set
            video0 = source.streams.video[0] 
            codec = video0.codec_context
            FPS = 1 / (codec.time_base * codec.ticks_per_frame)
            fps = FPS > 60 and (codec.framerate and float(codec.framerate)) or fps
            session['video'] = dict(
                stream=source.demux(video=0),
                start=video0.start_time,        # same as 1st frame in pts
                codec=codec,
                format=video0.name,
                width=video0.width,
                height=video0.height,
                fps=fps,
                count=0,
                time=0,                         # pts in secs
                duration=None,                  # frame duration in secs
                drifting=False,
                adaptive=kwargs.get('adaptive', True),
                workaround=kwargs.get('workaround', True),
                thresholds=dict(
                    drifting=10,
                ),
                prev=None,
            )
            logging.info(f"codec.framerate={codec.framerate}, codec.time_base={codec.time_base}, codec.ticks_per_frame={codec.ticks_per_frame}, fps={session['video']['fps']}, FPS={FPS}")
        if source.streams.audio:
            audio0 = source.streams.audio[0]
            codec=audio0.codec_context
            logging.warning(f"No audio streaming supported yet")
            '''
            if codec.name == 'aac':
                logging.warning(f"AAC is not supported yet")
            else:
                session['audio'] = dict(
                    stream=decoding and source.decode(audio=0) or source.demux(audio=0),
                    start=audio0.start_time,        # same as 1st frame in pts
                    format=audio0.name,
                    codec=codec,
                    sample_rate=codec.sample_rate,
                    channels=len(codec.layout.channels),
                    count=0,
                    time=0,
                )
            '''
        return session

class AVSource(object):
    '''Media stream source from an URL supported by FFMPEG.

    Each time open() returns one or more streaming sessions.
    It is the caller's responsibility to manage the returned sessions.
    A specific session is required to read frames from the source.
    '''

    @classmethod
    def create(cls, url, *args, **kwargs):
        '''Generic source creation by url.
        Args:
            url: unified path from filesystem device to particular streaming protocols including rtsp/kvs/nuuo
            args: necessary args for the URL spec
            kwargs: necessary keyward args for the URL spec

        local: ('/dev/video0' | 0 | 1 | path, ...)

        YT: ('https://www.youtube.com/watch?v=1MszDslSzBg', ...)
            url: [ 'https://www.youtube.com/watch?v=1MszDslSzBg' | 'https://youtu.be/1MszDslSzBg' ]

        FB: ('https://www.facebook.com/thenational/videos/695563637931892', ...)

        KVS: ('kvs://aws_cam-1102', timestamp='PRODUCER', protocol='HLS')
            timestamp: [ 'PRODUCER' | 'SERVER']
            protocol: [ 'HLS' | 'DASH' ]

        NUUO: ('nuuo://ip:port', user='admin', passwd='admin')
            user: login username
            passwd: login password
        '''

        url = str(url)
        if url.startswith('kvs://'):
            from .kinesis import KVSource
            return KVSource(url, *args, **kwargs)
        elif url.startswith('nuuo://'):
            from .nuuo import NUUOSource
            return NUUOSource(url, *args, **kwargs)
        elif url.startswith('https://www.youtube.com') or url.startswith('https://youtu.be'):
            from .youtube import YTSource
            return YTSource(url, *args, **kwargs)
        elif url.startswith('https://www.facebook.com'):
            from .facebook import FBSource
            return FBSource(url, *args, **kwargs)
        elif url.startswith('s3://'):
            from .s3 import S3Source
            return S3Source(url, *args, **kwargs)
        elif url.startswith('cs://'):
            from .custom import CustomSource
            return CustomSource(url, *args, **kwargs)
        else:
            # local file path or other supported remote URL
            # XXX Must be in annex b format if from H.264 NALU bitstream
            return cls(url, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        src = args[0]
        if src.isnumeric():
            # camera index in the system
            self.src = int(src)
        else:
            # filesystem path
            path = Path(src)
            if path.exists():
                logging.info(f"Local source path {path}")
            else:
                logging.info(f"Assume remote source URL {src}")
            self.src = src
    
    def open(self, *args, **kwargs):
        """
        KWArgs:
            fps(int): framerate for webcam
            resolution(str): resolution for webcam
        """
        return openAV(self.src, *args, **kwargs)
    
    def close(self, session):
        if 'streams' in session:
            session['streams'].close()
        session.clear()

    def read_audio(self, session):
        meta = session['audio']
        stream = meta['stream']
        codec = meta['codec']
        meta['keyframe'] = True
        for i, frame in enumerate(stream):
            if session['decoding']:
                meta['duration'] = frame.samples / frame.sample_rate
            else:
                # Only g711: one byte per sample
                meta['duration'] = frame.buffer_size / codec.sample_rate
            meta['time'] = session['start'] + i * meta['duration']
            meta['count'] += 1
            yield meta, frame.to_ndarray() if session['decoding'] else frame
        return None

    def read_video(self, session, format='BGR'):
        meta = session['video']
        stream = meta['stream']
        codec = meta['codec']
        workaround = meta['workaround']
        while True:
            try:
                pkt = next(stream)
            except StopIteration:
                pkt = None
            now = time.time()
            prev = meta.get('prev', None)
            if prev is None:
                if not pkt.is_keyframe:
                    # Some RTSP source may not send key frame to begin with e.g. wisecam
                    logging.warning(f"No key frame to begin with, skip through")
                    session['start'] = now
                    continue
                meta['keyframe'] = pkt.is_keyframe
                meta['time'] = session['start']
                streams = session['streams']
                sformat = session['format']
                
                # XXX Stream container package format determines H.264 NALUs in AVCC or Annex B.
                # TODO Streaming NALUs in AVCC
                if 'hls' in sformat or 'rtsp' in sformat or '264' in sformat:
                    # XXX In case of out of band CPD: SPS/PPS in AnnexB.
                    CPD = []
                    if codec.extradata is not None:
                        for (pos, _, _, type), nalu in NALUParser(codec.extradata, workaround=workaround):
                            if hasStartCode(nalu):
                                CPD.append(nalu)
                                logging.info(f"CPD {NALU_t(type).name} at {pos}: {nalu[:8]} ending with {nalu[-1:]}")
                            else:
                                logging.warning(f"Invalid CPD NALU({type}) at {pos}: {nalu[:8]} ending with {nalu[-1:]}")
                                if not CPD:
                                    # Skip all
                                    break
                    NALUs = []
                    if workaround:
                        # FIXME workaround before KVS MKVGenerator deals with NALUs ending with a zero byte
                        #   https://github.com/awslabs/amazon-kinesis-video-streams-producer-sdk-cpp/issues/491
                        for (pos, _, _, type), nalu in NALUParser(memoryview(pkt), workaround=workaround):
                            assert hasStartCode(nalu), f"frame[{meta['count']+1}] NALU(type={type}) at {pos} without START CODE: {nalu[:8].tobytes()}"
                            if type in (NALU_t.SPS, NALU_t.PPS):
                                if CPD:
                                    # NOTE: some streams could have multiple UNSPECIFIED(0) NALUs within a single packet with SPS/PPS
                                    #assert len(CPD) == 2, f"len(CPD) == {len(CPD)}, not 2 for SPS/PPS"
                                    ordinal = type - NALU_t.SPS
                                    if nalu == CPD[ordinal]:
                                        logging.info(f"frame[{meta['count']+1}] same {NALU_t(type).name}({nalu[:8].tobytes()}) at {pos} as in CPD({CPD[ordinal][:8]})")
                                    else:
                                        # FIXME may expect the CPD to be inserted in the beginning?
                                        logging.warning(f"frame[{meta['count']+1}] inconsistent {NALU_t(type).name}({nalu[:8].tobytes()}) at {pos} with CPD({CPD[ordinal][:8]})")
                                        print(f"CPD {NALU_t(type).name}:", CPD[ordinal])
                                        print(f"NALU {NALU_t(type).name}:", nalu.tobytes())
                                        # XXX bitstream may present invalid CPD => replacement with bitstream SPS/PPS
                                        CPD[ordinal] = nalu
                                else:
                                    NALUs.append(nalu)
                                    logging.info(f"frame[{meta['count']+1}] {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()} ending with {nalu[-1:].tobytes()}")
                            # XXX KVS master is ready to filter out non-VCL NALUs as part of the CPD
                            # elif type in (NALU_t.IDR, NALU_t.NIDR):
                            elif type in (NALU_t.AUD, NALU_t.SEI, NALU_t.IDR, NALU_t.NIDR):
                                NALUs.append(nalu)
                                logging.info(f"frame[{meta['count']+1}] {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()}")
                            else:
                                # FIXME may expect CPD to be inserted in the beginning?
                                logging.warning(f"frame[{meta['count']+1}] skipped unexpected NALU(type={type}) at {pos}: {nalu[:8].tobytes()}")
                        logging.info(f"{pkt.is_keyframe and 'key ' or ''}frame[{meta['count']}] combining CPD({len(CPD)}) and NALUs({len(NALUs)})")
                    else:
                        NALUs.append(memoryview(pkt))
                        logging.info(f"{pkt.is_keyframe and 'key ' or ''}frame[{meta['count']}] prepending CPD({len(CPD)})")
                    packet = av.Packet(bytearray(b''.join(CPD+NALUs)))
                    packet.dts = pkt.dts
                    packet.pts = pkt.pts
                    packet.time_base = pkt.time_base
                    pkt = packet
                    if pkt.pts is None:
                        logging.warning(f"Initial packet dts/pts={pkt.dts}/{pkt.pts}, time_base={pkt.time_base}")
                    elif pkt.pts > 0:
                        logging.warning(f"Reset dts/pts of 1st frame from {pkt.pts} to 0")
                        pkt.pts = pkt.dts = 0
                elif 'dash' in sformat:
                    # TODO In case of out of band CPD: SPS/PPS in AVCC.
                    logging.info(f"DASH AVCC extradata: {codec.extradata}")
                    logging.info(f"pkt[:16]({pkt.is_keyframe}) {memoryview(pkt)[:16].tobytes()}")
            else:
                keyframe = pkt.is_keyframe
                logging.debug(f"packet[{meta['count']}] {keyframe and 'key ' or ''}dts/pts={pkt.dts}/{pkt.pts}, time_base={pkt.time_base}, duration={pkt.duration}")
                if 'hls' in sformat or 'rtsp' in sformat or '264' in sformat:
                    NALUs = []
                    if workaround:
                        for (pos, _, _, type), nalu in NALUParser(memoryview(pkt), workaround=workaround):
                            # assert hasStartCode(nalu), f"frame[{meta['count']+1}] NALU(type={type}) at {pos} without START CODE: {nalu[:8].tobytes()}"
                            # FIXME KVS master is not ready to take AUD/SEI as part of the CPD
                            # if type in (NALU_t.SPS, NALU_t.PPS, NALU_t.IDR, NALU_t.NIDR):
                            if type in (NALU_t.AUD, NALU_t.SEI, NALU_t.SPS, NALU_t.PPS, NALU_t.IDR, NALU_t.NIDR):
                                NALUs.append(nalu)
                                logging.debug(f"frame[{meta['count']+1}] {NALU_t(type).name} at {pos}: {nalu[:8].tobytes()}")
                            else:
                                # FIXME may expect CPD to be inserted?
                                logging.debug(f"frame[{meta['count']+1}] skipped NALU(type={type}) at {pos}: {nalu[:8].tobytes()} ending with {nalu[-1:].tobytes()}")
                    else:
                        NALUs.append(memoryview(pkt))
                    # XXX Assme no SPS/PPS change
                    packet = av.Packet(bytearray(b''.join(NALUs)))
                    packet.dts = pkt.dts
                    packet.pts = pkt.pts
                    packet.time_base = pkt.time_base
                    pkt = packet
                frame = prev
                if session['decoding']:
                    try:
                        frames = codec.decode(prev)
                        if not frames:
                            logging.warning(f"Decoded nothing, continue to read...")
                            meta['prev'] = pkt
                            meta['count'] += 1
                            continue
                    except Exception as e:
                        logging.error(f"Failed to decode video packet of size {prev.size}: {e}")
                        raise e
                    else:
                        # print(prev, frames)
                        frame = frames[0]
                        meta['width'] = frame.width
                        meta['height'] = frame.height
                        if format == 'BGR':
                            frame = frame.to_rgb().to_ndarray()[:,:,::-1]
                        elif format == 'RGB':
                            frame = frame.to_rgb().to_ndarray()
                if session['rt']:
                    '''
                    Live source from network or local camera encoder.
                    Bitstream contains no pts but frame duration.
                    Adaptive frame duration on drift from wall clock:
                        - Faster for long frame buffering
                        - Fall behind for being slower than claimed FPS: resync as now
                    '''
                    if pkt.pts is not None and not meta['drifting']:
                        # Check if drifting
                        if prev.pts is None:
                            prev.dts = prev.pts = 0
                            logging.warning("Reset previous packet dts/pts from None to 0")
                        duration = float((pkt.pts - prev.pts) * pkt.time_base)
                        # assert duration > 0, f"pkt.pts={pkt.pts}, prev.pts={prev.pts}, pkt.time_base={pkt.time_base}, pkt.duration={pkt.duration}, prev.duration={prev.duration}, duration={duration}"
                        if duration <= 0:
                            # FIXME RTSP from Dahua/QB and WiseNet/Ernie
                            pts = prev.pts + (meta['duration'] / pkt.time_base) / 2
                            duration = float((pts - prev.pts) * pkt.time_base)
                            logging.warning(f"Non-increasing pts: pkt.pts={pkt.pts}, prev.pts={prev.pts} => pts={pts}, duration={duration}")
                            pkt.pts = pts
                        
                        timestamp = meta['time'] + duration
                        if meta['adaptive']:
                            # adaptive frame duration only if not KVS
                            diff = abs(timestamp - now)
                            threshold = meta['thresholds']['drifting']
                            if diff > threshold:
                                meta['drifting'] = True
                                logging.warning(f"Drifting video timestamps: abs({timestamp:.3f} - {now:.3f}) = {diff:.3f} > {threshold}s")
                    if pkt.pts is None or meta['drifting']:
                        # Real-time against wall clock
                        duration = now - meta['time']
                        duration = min(1.5 / meta['fps'], duration)
                        duration = max(0.5 / meta['fps'], duration)
                        meta['duration'] = duration
                        yield meta, frame
                        meta['time'] += duration
                    else:
                        meta['duration'] = duration
                        yield meta, frame
                        meta['time'] = timestamp
                else:
                    # TODO: no sleep for being handled by renderer playback
                    # Simulating RT
                    meta['duration'] = 1.0 / meta['fps']
                    slack = (meta['time'] + meta['duration']) - now
                    if slack > 0:
                        logging.debug(f"Sleeping for {slack:.3f}s to simulate RT source")
                        time.sleep(slack)
                    yield meta, frame
                    meta['time'] += meta['duration']
                meta['keyframe'] = keyframe
            if pkt.size == 0:
                logging.warning(f"EOF/EOS on empty packet")
                return None
            else:
                meta['prev'] = pkt
                meta['count'] += 1

    def read(self, session, media='video', format='BGR'):
        if session is None or media not in session:
            logging.error(f"{media} not in session to read")
            return None
        try:
            if media == 'video':
                meta = session[media]
                stream = meta['stream']
                codec = meta['codec']
                meta['framer'] = framer = meta.get('framer', self.read_video(session, format))
                meta, frame = next(framer)
            elif media == 'audio':
                meta = session[media]
                stream = meta['stream']
                meta['framer'] = framer = meta.get('framer', self.read_audio(session))
                meta, frame = next(framer)
        except StopIteration:
            # EOS
            return None
        except Exception as e:
            logging.info(f"Failed to read a frame: {e}")
            raise e
        else:
            return media, meta, frame
    
    def get(self, session, key, media='video'):
        if media == 'video' and media in session:
            video = session[media]
            if key == av.VIDEO_IO_FLAGS.CAP_PROP_FOURCC:
                return video['codec'] and av.avcodec(video['codec'].name)[1] or None
            elif key == av.VIDEO_IO_FLAGS.CAP_PROP_FRAME_WIDTH:
                return video['width']
            elif key == av.VIDEO_IO_FLAGS.CAP_PROP_FRAME_HEIGHT:
                return video['height']
            elif key == av.VIDEO_IO_FLAGS.CAP_PROP_FPS:
                return video['fps']
            elif key == av.VIDEO_IO_FLAGS.CAP_PROP_POS_MSEC:
                return video['time'] * 1000
            elif key == av.VIDEO_IO_FLAGS.CAP_PROP_BUFFERSIZE:
                return 0

        logging.warning(f"Unknown key to get: {key} from {media}")
        return None
 
    def set(self, session, key, value, media='video'):
        if media == 'video' and media in session:
            video = session[media]
            stream = video['stream']
            if not hasattr(stream, 'set'):
                logging.warning(f"Source stream property cannot be changed")
                return False

            if key == av.VIDEO_IO_FLAGS.CAP_PROP_FOURCC:
                fmt, fourcc = av.codec(value)
                if stream.set(av.VIDEO_IO_FLAGS.CAP_PROP_FOURCC, fourcc):
                    video['format'] = fmt
                    return True
                else:
                    logging.warning(f"Failed to set video source CAP_PROP_FOURCC to {fmt}({value})")
                    return False
            elif stream.set(key, value):
                res = stream.get(key)
                if key == av.VIDEO_IO_FLAGS.CAP_PROP_FPS:
                    video['fps'] = int(res)
                    logging.warning(f"Set video source CAP_PROP_FPS to {value}({int(res)})")
                elif key == av.VIDEO_IO_FLAGS.CAP_PROP_FRAME_WIDTH:
                    video['width'] = int(res)
                    video['height'] = int(stream.get(av.CAP_PROP_FRAME_HEIGHT))
                    logging.info(f"Set video source CAP_PROP_FRAME_WIDTH to {value}({int(res)})")
                elif key == av.VIDEO_IO_FLAGS.CAP_PROP_FRAME_HEIGHT:
                    video['height'] = int(res)
                    video['width'] = int(stream.get(av.VIDEO_IO_FLAGS.CAP_PROP_FRAME_WIDTH))
                    logging.info(f"Set video source CAP_PROP_FRAME_HEIGHT to {value}({int(res)})")
                return True

        logging.warning(f"Unsupported {media} property to set")
        return False

'''
def test_program_date_time():
    import requests
    #url = kvs_session_url('farley-4871', timestamp='SERVER')
    url = kvs_session_url('farley-4871', timestamp='PRODUCER')

    response = requests.get(url)
    assert response
    print(response.text)

    # #EXTM3U
    # #EXT-X-VERSION:1
    # #EXT-X-STREAM-INF:CODECS="avc1.4d4029",RESOLUTION=1280x720,FRAME-RATE=15.0,BANDWIDTH=1953544
    # getHLSMediaPlaylist.m3u8?SessionToken=

    headers = response.text.strip().split('\n')

    master = url
    endpoint, sessionToken = master.split('?')
    endpoint = endpoint[:-len('getHLSMasterPlaylist.m3u8')]
    url = f"{endpoint}{headers[-1]}"
    print(url)

    response = requests.get(url)
    assert response
    print(response.text)

def test_throughput(stream='farley-4871', duration=30, timestamp='SERVER'):
    cli = AVSource(timestamp=timestamp)
    cli.open(stream, retry=1)
    total = int(cli.fps * 30)
    start = time.time()
    times = [cli.read(format='raw').time for i in range(total)]
    end = time.time()
    logging.info(f"{timestamp} timestamp throughput: {cli.stats()['fps']:.2f}/{cli.fps}FPS in {end-start:.3f}/{duration}s")

def test_live_sync(stream='farley-4871', timestamp='SERVER'):
    cli = AVSource(timestamp=timestamp)
    before = datetime.now()
    cli.open(stream, retry=1)
    frame = cli.read(format='raw').to_image()    
    after = datetime.now()
    start = datetime.fromtimestamp(cli.start)

    frame.show()
    logging.info(f"{timestamp} timestamp: \nbefore: {before}\n start: {start}\n after: {after}")

if __name__ == '__main__':
    test_live_sync(timestamp='SERVER')
    test_live_sync(timestamp='PRODUCER')
    
    #test_throughput(timestamp='SERVER')
    #test_throughput(timestamp='PRODUCER')
'''