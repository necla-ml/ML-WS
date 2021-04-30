# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import os
import sys
import errno
from ml.time import (
    time,
    sleep,
    HUNDREDS_OF_NANOS_HOUR, 
    HUNDREDS_OF_NANOS_SEC
)
from ml import logging
from ml.av.h264 import NALUParser, NALU_t

from ml.ws.common import timeit

from .._C import ffi, lib

DEFAULT_AWS_REGION              = "us-east-1"
DEFAULT_RETENTION_PERIOD        = 6 * HUNDREDS_OF_NANOS_HOUR
DEFAULT_BUFFER_DURATION         = 120 * HUNDREDS_OF_NANOS_SEC
DEFAULT_KEY_FRAME_INTERVAL      = 15
DEFAULT_FPS_VALUE               = 15


class KVProducer(object):
    def __init__(self, accessKey=None, secretKey=None, region=None):
        from ...ws.aws import utils
        self.sessionToken = ffi.NULL
        self.streamName = ffi.NULL
        self.cacertPath = ffi.NULL

        secret = utils.get_secret()
        self.region = region or secret.get('AWS_DEFAULT_REGION', ffi.NULL)
        self.accessKey = accessKey or secret.get('AWS_ACCESS_KEY_ID', ffi.NULL)
        self.secretKey = secretKey or secret.get('AWS_SECRET_ACCESS_KEY', ffi.NULL)
        self.privateKey = secret.get('PRIVATE_KEY', None)
       
    def connect(self, streamName, region=ffi.NULL, sessionToken=ffi.NULL, cacertPath=ffi.NULL):
        '''Connect to KVS as the streaming sink.
        '''
        self.streamName = streamName
        self.region = region or os.getenv('AWS_DEFAULT_REGION', DEFAULT_AWS_REGION)
        self.sessionToken = sessionToken or os.getenv('AWS_SESSION_TOKEN', ffi.NULL)
        self.cacertPath = cacertPath or os.getenv('AWS_KVS_CACERT_PATH', ffi.NULL)

        # default storage size is 128MB. Use setDeviceInfoStorageSize after create to change storage size.
        ppDeviceInfo = ffi.new('PDeviceInfo[1]')
        ppStreamInfo = ffi.new('PStreamInfo[1]')
        ppClientCallbacks = ffi.new('PClientCallbacks[1]')

        assert lib.createDefaultDeviceInfo(ffi.cast('PDeviceInfo*', ppDeviceInfo)) == 0, f"createDefaultDeviceInfo(...) failed"
        self.pDeviceInfo = ppDeviceInfo[0]
        self.pDeviceInfo.clientInfo.loggerLogLevel = lib.LOG_LEVEL_DEBUG
        
        #print('DeviceInfo:', self.pDeviceInfo.version, ffi.string(self.pDeviceInfo.name), ffi.string(self.pDeviceInfo.clientId))
        storageI = self.pDeviceInfo.storageInfo
        #print('StorageInfo:', storageI.storageType, storageI.storageSize, storageI.spillRatio, ffi.string(storageI.rootDirectory))

        ret = lib.createRealtimeVideoStreamInfoProvider(streamName.encode(), 
                                                        DEFAULT_RETENTION_PERIOD, 
                                                        DEFAULT_BUFFER_DURATION, 
                                                        ffi.cast('PStreamInfo*', ppStreamInfo))
        assert ret == 0, f"createRealtimeVideoStreamInfoProvider(...) failed with ret: {ret}"
        self.pStreamInfo = ppStreamInfo[0]
        print(f"Default streamCaps.bufferDuration={self.pStreamInfo.streamCaps.bufferDuration}")
        print(f"Default streamCaps.frameRate={self.pStreamInfo.streamCaps.frameRate}")
        print(f"Default streamCaps.fragmentAcks={self.pStreamInfo.streamCaps.fragmentAcks}")
        print(f"Default streamCaps.frameTimecodes={self.pStreamInfo.streamCaps.frameTimecodes}")
        print(f"Default streamCaps.timecodeScale={self.pStreamInfo.streamCaps.timecodeScale}")
        print(f"Default streamCaps.absoluteFragmentTimes={self.pStreamInfo.streamCaps.absoluteFragmentTimes}")
        print(f"Default streamCaps.keyFrameFragmentation={self.pStreamInfo.streamCaps.keyFrameFragmentation}")
        print(f"Default streamCaps.nalAdaptationFlags={self.pStreamInfo.streamCaps.nalAdaptationFlags}")
        #self.pStreamInfo.streamCaps.timecodeScale = 10000      # 1ms = 1e-3 * 1e7 by KVS?
        #self.pStreamInfo.streamCaps.bufferDuration = 120        # 120
        #self.pStreamInfo.streamCaps.frameRate = 120             # 120 for stream buffering content view items, not encoder FPS
        #self.pStreamInfo.streamCaps.fragmentAcks = 1           # 1
        #self.pStreamInfo.streamCaps.frameTimecodes = 1         # 1
        #self.pStreamInfo.streamCaps.absoluteFragmentTimes = 1  # 1
        #self.pStreamInfo.streamCaps.keyFrameFragmentation = 1  # 1
        #self.pStreamInfo.streamCaps.nalAdaptationFlags     = 40 # 8+32 [ 0, x8, x16, 32 ]
        
        #NAL_ADAPTATION_FLAG_NONE=0
        #NAL_ADAPTATION_ANNEXB_NALS=8
        #NAL_ADAPTATION_AVCC_NALS=16
        #NAL_ADAPTATION_ANNEXB_CPD_NALS=32
        #self.pStreamInfo.streamCaps.nalAdaptationFlags     = lib.NAL_ADAPTATION_FLAG_NONE
        #self.pStreamInfo.streamCaps.nalAdaptationFlags     = lib.NAL_ADAPTATION_ANNEXB_NALS
        #self.pStreamInfo.streamCaps.nalAdaptationFlags     = lib.NAL_ADAPTATION_AVCC_NALS
        '''
        CPD = b'\x01d\x00\x1f\xff\xe1\x00\x19gd\x00\x1f\xac\xb2\x00\xa0\x0bv\x02 \x00\x00\x03\x00 \x00\x00\x03\x03\xc1\xe3\x06I\x01\x00\x05h\xeb\xcc\xb2,'
        assert self.pStreamInfo.streamCaps.trackInfoList[0].trackId == lib.DEFAULT_VIDEO_TRACK_ID
        from ml import av
        source = av.open('store720p.mp4')
        video0 = source.streams.video[0]
        codec = video0.codec_context
        cpd = codec.extradata
        assert CPD == cpd
        self.pStreamInfo.streamCaps.trackInfoList[0].codecPrivateDataSize = len(CPD)
        self.pStreamInfo.streamCaps.trackInfoList[0].codecPrivateData = ffi.new("UINT8[]", len(CPD))
        ffi.memmove(self.pStreamInfo.streamCaps.trackInfoList[0].codecPrivateData, CPD, len(CPD))
        '''
        print(f"streamCaps.bufferDuration={self.pStreamInfo.streamCaps.bufferDuration}")
        print(f"streamCaps.frameRate={self.pStreamInfo.streamCaps.frameRate}")
        print(f"streamCaps.fragmentAcks={self.pStreamInfo.streamCaps.fragmentAcks}")
        print(f"streamCaps.timecodeScale={self.pStreamInfo.streamCaps.timecodeScale}")
        print(f"streamCaps.frameTimecodes={self.pStreamInfo.streamCaps.frameTimecodes}")
        print(f"streamCaps.absoluteFragmentTimes={self.pStreamInfo.streamCaps.absoluteFragmentTimes}")
        print(f"streamCaps.keyFrameFragmentation={self.pStreamInfo.streamCaps.keyFrameFragmentation}")
        print(f"streamCaps.nalAdaptationFlags={self.pStreamInfo.streamCaps.nalAdaptationFlags}")
        #print(f'connecting with {self.accessKey.encode()}, {self.secretKey.encode()}')
        #print(f'    {self.sessionToken}, {self.region.encode()}, {self.cacertPath}')
        ret = lib.createDefaultCallbacksProviderWithAwsCredentials(self.accessKey and self.accessKey.encode(),
                                                                    self.secretKey and self.secretKey.encode(),
                                                                    self.sessionToken and self.sessionToken.encode(),
                                                                    ffi.integer_const('MAX_UINT64'),
                                                                    self.region and self.region.encode(),
                                                                    self.cacertPath and self.cacertPath.encode(),
                                                                    ffi.NULL,
                                                                    ffi.NULL,
                                                                    ffi.cast('PClientCallbacks*', ppClientCallbacks))
        assert ret == 0, f"createDefaultCallbacksProviderWithAwsCredentials(...) failed with ret: {ret:#04x}"
        self.pClientCallbacks = ppClientCallbacks[0]

        pClientHandle = ffi.new('UINT64[1]')
        pStreamHandle = ffi.new('UINT64[1]')
        ret = lib.createKinesisVideoClient(self.pDeviceInfo, self.pClientCallbacks, ffi.cast('UINT64*', pClientHandle));
        assert ret == 0, f"createKinesisVideoClient(...) failed with ret: {ret:#04x}"
        self.clientHandle = pClientHandle[0]

        ret = lib.createKinesisVideoStreamSync(self.clientHandle, self.pStreamInfo, ffi.cast('UINT64*', pStreamHandle));
        assert ret == 0, f"createKinesisVideoStreamSync(...) failed with ret: {ret:#04x}"
        self.streamHandle = pStreamHandle[0]

    def open(self, *args, **kwargs):
        r"""Open the input source to read.

        Args:
            path(str): local file path or a supported streaming url
        Kwargs:
            loop(bool): video source in loop or opening first time
        """
        from .. import AVSource
        path = args[0]
        loop = kwargs.pop('loop', False)
        if not loop:
            self.src =  AVSource.create(path)
        return [self.src.open(*args[1:], **kwargs, decoding=False)]

    def upload(self, *args, **kwargs):
        r"""Start streaming from some input source.
        Subclass producers should instead override `open()` and `close()` mostly.

        Args:
            path(str): local file path or supported streaming url
        Kwargs:
            duration(int): time to stop streaming in seconds
            loop(bool): whether to restart on EOF/EOS
            env(str): current env (DEV | PROD)
            stream_id(int): stream_id to update stream status from PENDING to STREAMING 
        """
        duration = kwargs.pop('duration', None)
        loop = kwargs.pop('loop', False)
        stop_event = kwargs.pop('stop_event', None)
        
        if stop_event is None:
            # Python signal delivery depends on the main thread not being blocked on system calls
            import signal
            from ml.ws.common import Signaled
            # custom signal object to match the threading.Event() syntax for while loop check
            signaled = Signaled()
            def handler(sig, frame):
                nonlocal signaled
                logging.warning(f"Interrupted or stopped by {signal.Signals(sig).name}")
                signaled.signal = True
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        else:
            # signal for stopping upload is set from the driver thread 
            signaled = stop_event

        # Upload frame to KVS
        pFrame = ffi.new('PFrame')
        pFrame.version = ffi.integer_const('FRAME_CURRENT_VERSION')
        pFrame.trackId = ffi.integer_const('DEFAULT_VIDEO_TRACK_ID')
        retStatus = ffi.integer_const('STATUS_SUCCESS')

        # Open input source to read
        sessions = self.open(*args, **kwargs)
        assert len(sessions) == 1, f"Only one streaming session at a time but got {len(sessions)} sessions"
        session = sessions.pop()

        # Streaming duration if any
        streamStartTime = lib.defaultGetTime()
        streamStopTime = None
        if duration is None:
            logging.info(f"Streaming indefinitely")
        else:
            streamingDuration = duration * HUNDREDS_OF_NANOS_SEC
            streamStopTime = lib.defaultGetTime() + streamingDuration
            logging.info(f"Streaming stops in {streamStopTime / HUNDREDS_OF_NANOS_SEC:.3f}s")

        # start = time()
        while (streamStopTime is None or lib.defaultGetTime() < streamStopTime) and not signaled.is_set():
            # TODO audio streaming
            res = self.src.read(session, media='video')
            now = lib.defaultGetTime()
            if res is None:
                if loop:
                    self.close(session)
                    logging.warning(f"Restarting streaming due to EOS/EOF")
                    session = self.open(*args, loop=True, **kwargs).pop()
                    restart = (pFrame.presentationTs + pFrame.duration) / HUNDREDS_OF_NANOS_SEC
                    session['start'] = session['start'] < restart and restart or session['start']
                    logging.info(f"Streaming restart: {session['start']:.3f}s")
                    res = self.src.read(session, media='video')
                    assert res is not None
                    res[1]['count'] = pFrame.index + 1
                else:
                    logging.error(f"Failed to read a video frame, EOS/EOF?")
                    break

            m, media, frame = res
            keyframe = media['keyframe']
            offset = 0
            # XXX Why SEI is the very 1st NALU can be due to SPS/PPS saved in extradata
            pFrame.flags = ffi.integer_const('FRAME_FLAG_KEY_FRAME') if keyframe else ffi.integer_const('FRAME_FLAG_NONE')
            pFrame.frameData = ffi.cast('void*', frame.buffer_ptr + offset)
            pFrame.size = frame.size - offset
            pFrame.index = media['count']
            pFrame.presentationTs = pFrame.decodingTs = max(int(media['time'] * HUNDREDS_OF_NANOS_SEC), pFrame.decodingTs+pFrame.duration) 
            pFrame.duration = int(media['duration'] * HUNDREDS_OF_NANOS_SEC)
            print(f"Sending {'key ' if keyframe else ''}frame[{media['count']}] of duration {media['duration']:.3f}s to KVS with timestamp {media['time']:.3f}s at {now / HUNDREDS_OF_NANOS_SEC:.3f}s", )
            ret = lib.putKinesisVideoFrame(self.streamHandle, pFrame)
            if ret > 0:
                logging.error(f"Failed to send a frame to KVS with ret={ret:#04x}")
                break
            
        self.close(session)

    def close(self, session):
        self.src.close(session)

    def disconnect(self):
        pStreamHandle = ffi.new('UINT64[1]')
        pClientHandle = ffi.new('UINT64[1]')
        pStreamHandle[0] = self.streamHandle
        pClientHandle[0] = self.clientHandle
        ret = lib.stopKinesisVideoStreamSync(self.streamHandle) == 0;
        ret = ret and lib.freeKinesisVideoStream(ffi.cast('UINT64*', pStreamHandle)) == 0;
        ret = ret and lib.freeKinesisVideoClient(ffi.cast('UINT64*', pClientHandle)) == 0;

        #if not ret:
        #    lib.defaultLogPrint(lib.LOG_LEVEL_ERROR, "", "Failed with status 0x%08x\n", retStatus);

        if self.pDeviceInfo != ffi.NULL:
            ppDeviceInfo = ffi.new('PDeviceInfo[1]')
            ppDeviceInfo[0] = self.pDeviceInfo
            lib.freeDeviceInfo(ffi.cast('PDeviceInfo*', ppDeviceInfo));

        if self.pStreamInfo != ffi.NULL:
            ppStreamInfo = ffi.new('PStreamInfo[1]')
            ppStreamInfo[0] = self.pStreamInfo
            lib.freeStreamInfoProvider(ffi.cast('PStreamInfo*', ppStreamInfo));
        
        if self.streamHandle == ffi.integer_const('INVALID_STREAM_HANDLE_VALUE'):
            lib.freeKinesisVideoStream(ffi.cast('UINT64*', pStreamHandle));

        if self.clientHandle == ffi.integer_const('INVALID_CLIENT_HANDLE_VALUE'):
            lib.freeKinesisVideoClient(ffi.cast('UINT64*', pClientHandle));

        if self.pClientCallbacks != ffi.NULL:
            ppClientCallbacks = ffi.new('PClientCallbacks[1]')
            ppClientCallbacks[0] = self.pClientCallbacks
            lib.freeCallbacksProvider(ffi.cast('PClientCallbacks*', ppClientCallbacks))
