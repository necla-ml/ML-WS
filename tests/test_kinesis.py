from time import time, sleep
import pytest
import boto3

from ml.streaming import AVSource
from ml.streaming import kinesis
from ml import av, cv, sys, logging

@pytest.fixture
def stream():
    # Find one available on KVS
    return 'rtsp-15925945649570014' # Cashier
    return 'aws_cam-5302'
    return 'loopback'
    return 'rtsp-15925075098307528'
    return 'nuuo-50341658'
    return 'wyze-farley'
    # return 'nuuo-usa'
    # return 'aws_cam-3349'
    # return 'nuuo-50341658'
    # return 'nuuo-3659174698811392' # convenience store
    # return 'nuuo-3659174698549248' # Medical Entry
    # return 'nuuo-3659174698549248' # Mall Entrance: FPS 25->30
    # return 'nuuo-3659174699335680' # Highway

@pytest.fixture
def total():
    return 15 * 2 * 100

@pytest.fixture
def inf(request):
    return request.config.getoption("--inf")

def url(stream):
    return f"kvs://{stream}"

# @pytest.mark.essential
def test_producer_timestamp_live(stream, total):
    source = AVSource.create(url(stream))
    assert type(source) is kinesis.KVSource
    assert source.stream == stream
    session = source.open(start=time(), decoding=False, fps=30, threshold=10)
    #session = source.open(start=time(), decoding=False, fps=15, timestamp='SERVER')
    assert session is not None
    assert 'video' in session
    assert 'audio' not in session
    logging.info(session)
    for i in range(total):
        m, media, frame = source.read(session)
        assert m == 'video'
        assert media == session[m]
        if hasattr(frame,'shape'):
            assert media['height'] == frame.shape[0]
            assert media['width'] == frame.shape[1]
            assert 3 == frame.shape[2]
            logging.info(f"[{i}] {tuple(frame.shape)} duration={media['duration']:.3f}s, time={media['time']:.3f}s, now={time():.3f}s")
        else:
            logging.info(f"[{i}] duration={media['duration']:.3f}s, time={media['time']:.3f}s, now={time():.3f}s")
    source.close(session)
    assert not session

def test_fragments(stream, total, inf):
    kvs = boto3.client("kinesisvideo")
    info = kvs.describe_stream(StreamName=stream)['StreamInfo']
    assert stream == info['StreamName']
    print(f"{stream} info:")
    for key, value in info.items():
        print(f'\t{key}:', value)

    media, codec = info['MediaType'].split('/')
    assert media == 'video'
    assert codec == 'h264'
    dataEndpoint = kvs.get_data_endpoint(StreamName=stream, APIName='GET_MEDIA')['DataEndpoint']
    print(f"{stream} endpoint: {dataEndpoint}")

    kvm = boto3.client('kinesis-video-media', endpoint_url=dataEndpoint, region_name='us-east-1')
    while True:
        now = elapse = tic = time()
        media = kvm.get_media(
            StreamName=stream,
            StartSelector=dict(StartSelectorType='NOW'),
            #StartSelector=dict(StartSelectorType='PRODUCER_TIMESTAMP',
            #StartSelector=dict(StartSelectorType='SERVER_TIMESTAMP',
            #                   StartTimestamp=time())
            )

        contentType = media['ContentType']
        payload = media['Payload']
        print(f"Received {contentType} stream payload")
        
        webm = av.open(payload)
        video = webm.streams.video[0]
        codec = video.codec_context
        print(webm.format, f"{webm.start_time / 1000:.3f}") # start time in ms
        print(video, video.type, video.time_base, f"{video.start_time / 1000:.3f}s", video.base_rate, video.average_rate, video.guessed_rate) # start time in s
        print(f"{codec.type}/{codec.name}", codec.time_base, codec.ticks_per_frame) # codec type/name, frame duration, FPS
        
        start = video.start_time
        duration_cc = float(codec.time_base * codec.ticks_per_frame)
        fps = 1 / duration_cc
        started = False
        X = sys.x_available()
        print(f"Streaming {codec.type}/{codec.name} since {start/1000:.3f}s(now={now:.3f}s, diff={now-start/1000:.3f}s) at {fps:.2f}FPS with frame duration of {duration_cc:.3f}s")
        try:
            for i, packet in enumerate(webm.demux(video=0), 1):
                frame = packet.decode()[0]
                now = time()
                pts = frame.time
                duration_pkt = float(packet.duration * packet.time_base) # FIXME 0 -> 1/FPS
                frame = frame.to_rgb().to_ndarray()[:,:,::-1]
                print(f"{packet.is_keyframe and 'key ' or ''}frame[{i}]{frame.shape} of {frame.nbytes} bytes with pts={pts:.3f} at {now:.3f}s and duration={duration_pkt:.3f}s({packet.time_base}, {packet.duration})")
                
                elapse += duration_pkt
                slack = elapse - now
                if slack > 0:
                    print(f"Sleep for {slack:.3f}s")
                    sleep(slack)
                
                if X:
                    cv.imshow('LIVE', frame)
                    cv.waitKey(1)
                if i == total:
                    print(f"RT FPS={total/(now-tic)}")
                    break
        except Exception as e:
            print(f"Failed to decode: {e}")
            webm.close()
        if not inf:
            break

def test_save_webm(stream):
    import os
    import time
    import json
    import boto3
    import base64
    import datetime
    import subprocess
    from botocore.exceptions import ClientError

    # init required clients
    s3 = boto3.client('s3')
    kv = boto3.client('kinesisvideo')
    def get_fragments(stream, start, end):
        """
        Get the Fragments from Kinesis Video Stream
        Returns: list of fragment_number
        """
        get_ep = kv.get_data_endpoint(StreamName=stream, APIName='LIST_FRAGMENTS')
        lst_frag_ep = get_ep['DataEndpoint']

        #Get KVS fragment
        kvs = boto3.client('kinesis-video-archived-media', endpoint_url=lst_frag_ep)

        timestamp_range = dict(StartTimestamp = start, EndTimestamp = end)
        fragment_selector_dict = dict(FragmentSelectorType = 'PRODUCER_TIMESTAMP',  # XXX: be consistent with SINet detector
                                    TimestampRange = timestamp_range 
                                    )

        fragment_list = kvs.list_fragments(StreamName=stream, FragmentSelector=fragment_selector_dict)
        fragments = []
        
        # Get fragment numbers from the fragment list
        for fragment in fragment_list['Fragments']:
            fragments.append(fragment['FragmentNumber'])

        return fragments

    def save(base_key, stream, start, end):
        """
        Function to get the Fragments from KVS and save to S3
        """
        # XXX: skip video if too long to save else Lambda might timeout.
        if float(end) - float(start) > float(3000):
            print(f'Action event too long {start} - {end} and stream_id {stream_id}')
            return

        # Ensure video is atleat 6 seconds
        if float(end) - float(start) < float(10):
            print('Action event too short, +- 5 sec to event clip')
            start = float(start) - 5
            end = float(end) + 5

        fragments = get_fragments(stream, start, end)

        get_ep = kv.get_data_endpoint(StreamName=stream, APIName='GET_MEDIA_FOR_FRAGMENT_LIST')
        kvam_ep = get_ep['DataEndpoint']
        kvam = boto3.client('kinesis-video-archived-media', endpoint_url=kvam_ep)
        
        if not fragments:
            # XXX: workaround for when fragments don't exist for the start and end timestamp
            fragments = get_fragments(stream, str(float(start) + 1), str(float(end) + 1))
        
        if fragments:
            # NOTE: The fragments might not be in order when received
            # sort the fragments
            fragments.sort()
            print('\n', '\n'.join(fragments))
            getmedia = kvam.get_media_for_fragment_list(
                StreamName=stream,
                Fragments=fragments)

            # save clip with key_value
            s3_key = base_key 
            body = getmedia['Payload'].read()

            try:
                #cmd = [ffmpeg, '-i', body, '-f', 'image2pipe', '-']
                #output = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                #out, err = output.communicate()
            
                with open(f'{s3_key}.webm', 'wb') as video:
                    video.write(body)

                #s3.put_object(Bucket=BUCKET, Key=s3_key, Body=getmedia['Payload'].read())
                print(f'Detection details and fragment for {stream} with object name: {s3_key} saved')
            except Exception as e:
                print(f'Error occured while saving the clip to s3: {e}')
        else:
            # NOTE: Action video will not play in the frontend from s3 if this happens. 
            # TODO send to dead letter queue to inspect manually
            print(f'Error getting the fragments from KVS for {stream}, startTS: {start} and endTS: {end}')

    save('video', stream, time.time()-50, time.time()-40)