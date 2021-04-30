# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

from time import time

from ml import av, logging
from .avsource import AVSource, openAV

def kvs_session_url(stream, start, end, timestamp, **kwargs):
    '''Get KVS streaming session url by stream name from backend.

    Args:
        stream: kvs name
        start: start streaming timestamp
        end: end streaming timestamp
        # expires: streaming session expiration
        timestamp: [ 'SERVER_TIMESTAMP' | 'PRODUCER_TIMESTAMP' ]
        # mode: [ 'LIVE' | 'LIVE_REPLAY' | 'ON_DEMAND' ]
        # protocol: [ None | 'HLS' | 'DASH' ]
    '''

    import boto3
    kvs = boto3.client("kinesisvideo")

    expires = kwargs.pop('expires', 5 * 60)
    protocol = None if end is None else 'HLS'

    # protocol = 'HLS'
    if protocol is None:
        # KVM with PRODUCER_TIMESTAMP as dts/pts
        assert end is None # No range suported
        dataEndpoint = kvs.get_data_endpoint(StreamName=stream, APIName='GET_MEDIA')['DataEndpoint']
        kvm = boto3.client('kinesis-video-media', endpoint_url=dataEndpoint)
        if start:
            startSelector=dict(StartSelectorType=f"{timestamp.upper()}_TIMESTAMP", StartTimestamp=start)
        else:
            startSelector=dict(StartSelectorType='NOW')
        media = kvm.get_media(StreamName=stream, StartSelector=startSelector)
        contentType = media['ContentType']
        webm = media['Payload']
        logging.info(f"KVM::GET_MEDIA(): streaming {contentType} with {startSelector}")
        return webm
    else:
        # HLS | DASH
        endpoint = kvs.get_data_endpoint(
            APIName=f"GET_{protocol.upper()}_STREAMING_SESSION_URL",
            StreamName=stream
        )['DataEndpoint']
        logging.info(f"DataEndpoint: {endpoint}")

        kvam = boto3.client("kinesis-video-archived-media", endpoint_url=endpoint)
        FragmentSelector = dict(
            FragmentSelectorType=f"{timestamp.upper()}_TIMESTAMP",  # [SERVER_TIMESTAMP | PRODUCER_TIMESTAMP]
        )
        if start:
            if end is None:
                FragmentSelector['TimestampRange'] = dict(StartTimestamp=start)
                mode = 'LIVE_REPLAY'
            else:
                FragmentSelector['TimestampRange'] = dict(
                    StartTimestamp=start,
                    EndTimestamp=end
                )
                mode = 'ON_DEMAND'
        else:
            mode = 'LIVE'

        mode = mode.upper()
        MaxFragmentResults = 3 if mode.startswith('LIVE') else 1000
        if protocol.upper() == 'DASH':
            url = kvam.get_dash_streaming_session_url(
                StreamName=stream,
                PlaybackMode=mode,
                DisplayFragmentNumber='NEVER',                  # [ALWAYS | NEVER]
                DisplayFragmentTimestamp='ALWAYS',              # [ALWAYS | NEVER]
                MaxManifestFragmentResults=MaxFragmentResults,  # 5 | 1000
                Expires=expires,                                # 300 - 43200s
                DASHFragmentSelector=FragmentSelector, 
            )['DASHStreamingSessionURL']
            logging.info(f"DASH streaming session URL: {url}")
        elif protocol.upper() == 'HLS':
            DiscontinuityMode = 'NEVER' if timestamp == 'PRODUCER' else 'ALWAYS'
            DiscontinuityMode = 'ON_DISCONTINUITY'
            url = kvam.get_hls_streaming_session_url(
                StreamName=stream,
                PlaybackMode=mode.upper(),
                DiscontinuityMode=DiscontinuityMode,                # [ ALWAYS() | NEVER(producer) | ON_DISCONTINUITY ]
                DisplayFragmentTimestamp='ALWAYS',                  # [ ALWAYS | NEVER*]
                MaxMediaPlaylistFragmentResults=MaxFragmentResults, # 5 | 1000
                Expires=expires,                                    # 300 - 43200s
                HLSFragmentSelector=FragmentSelector, 
            )['HLSStreamingSessionURL']
            logging.info(f"HLS streaming session URL: {url}")
        else:
            ValueError(f"Unknown protocol: {protocol}")
        return url


class KVSource(AVSource):
    def __init__(self, *args, **kwargs):
        '''Single streaming session at a time.

        Args:
            args[0](str): kvs://stream
        '''
        url = args[0]
        
        # TODO Two stream AV session
        import re
        match = re.match(r'kvs://(.+)', url)
        if not match:
            raise ValueError(f"Unexpected KVS name in the url: {url}")
        
        self.url = url
        self.stream = match.group(1)
        
    def open(self, start=None, end=None, timestamp='PRODUCER', **kwargs):
        """Start streaming from KVS.
        Streaming may be interleaved with media of different formats.
        Kwargs:
            start(float): KVS start timestamp
            start(float): KVS end timestamp
            expires(int): streaming expiry
            timestamp: [ SERVER | PRODUCER ]
                DeepLens allows for either timestamp.
                NUUO works only with PRODUCER timestamp.
        """
        try:
            now = start or time()
            url = kvs_session_url(self.stream, start, end, timestamp, **kwargs)
            session = openAV(url, adaptive=False, **kwargs)
        except Exception as e:
            # TODO Possible causes:
            # - [tcp @ 0x7fde64038000] Connection to tcp://b-604520a7.kinesisvideo.us-east-1.amazonaws.com:443 failed: Connection timed out
            # - [tls @ 0x7f4660073f40] error:00000000:lib(0):func(0):reason(0)
            # - botocore.errorfactory.ResourceNotFoundException: 
            #       An error occurred (ResourceNotFoundException) when calling the GetHLSStreamingSessionURL operation: 
            #       No fragments found in the stream for the streaming request.
            logging.error(f"Failed to open {self.url}: {e}")
            raise e
        else:
            #logging.info(f"KVS session start: requested={now:.3f}s, actual={session['start']:.3f}s")
            return session