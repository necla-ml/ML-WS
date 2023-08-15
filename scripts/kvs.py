#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory
import argparse

from ml.streaming import KVProducer

def main(cfg):
    print(f'Streaming video at {cfg.path} to KV stream={cfg.stream}')
    stream = f'{cfg.stream}'
    producer = KVProducer()
    producer.connect(stream)
    producer.upload(cfg.path,
                    fps=int(cfg.fps),
                    loop=cfg.loop,
                    start=cfg.start,
                    end=cfg.end,
                    transcode=cfg.transcode,
                    bucket=cfg.bucket,
                    stream_id=cfg.stream_id,
                    env=cfg.env,
                    workaround=True,
                    rtsp_transport=cfg.rtsp_transport
                    )
    producer.disconnect()

if __name__ == '__main__':
    from os import environ as env
    parser = argparse.ArgumentParser('Local bistream producer')
    parser.add_argument('path', help='Local bitstream file path or s3 key if fetching from s3 (e.g s3://key_name)')
    parser.add_argument('--stream', default='loopback', help='Destination AWS KV stream name')
    parser.add_argument('--fps', default=10, help='Video bitstream frames per second')
    parser.add_argument('--loop', action='store_true', help='Playback in loops')
    parser.add_argument('--rtsp_transport', default='tcp', choices=['tcp', 'http'], help='RTSP transport protocol')


    # database 
    parser.add_argument('--env', default='DEV', choices=['DEV', 'PROD'], help='Current env')
    parser.add_argument('--stream_id', default=None, help='Stream ID to update the status when running in ECS Cluster')

    # s3
    parser.add_argument('--transcode', action='store_true', help='Transcode video to h264 before streaming')
    parser.add_argument('--bucket', type=str, default='eigen-stream-videos', help='Name of the bucket to fetch video from')
    # TODO: add start and end timestamp for s3 video

    # youtube 
    parser.add_argument('--start', type=str, help='start time for youtube video (00:00:00)')
    parser.add_argument('--end', type=str, help='end time for youtube video (00:00:00)')

    cfg = parser.parse_args()

    # NOTE: for error codes - https://docs.python.org/3/library/errno.html
    main(cfg)
