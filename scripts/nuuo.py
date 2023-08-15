#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory
import argparse
from os import environ as env

from ml.streaming import NUUOProducer

AREAS_UCB = [
    'First Floor Entry',        # nuuo-50341658: 1920p@8-12FPS
    'First Floor Books',        # nuuo-50341665: 1080p@15FPS
    'First Floor Reg. 1 and 2', # nuuo-50341650: 1080p@15FPS
    'First Floor Reg. 3 and 4', # nuuo-50341660: 1080p@30FPS
    'First Floor Reg. 5 and 6', # nuuo-50341668: 1080p@15FPS
    'First Floor Reg. 7 and 8', # nuuo-50341661: 1080p@15FPS
]

AREAS_CA = [
    # nuuo-B8220_camera:
    'Medical Entry',        # nuuo-3659174698549248    1080p@30FPS     ~2.9s fragment duration
    'convenience store',    # nuuo-3659174698811392    1080p@30FPS     ~8.3s fragment duration
    'Mall Entrance',        # nuuo-3659174699073536    720x576@25FPS    0.5 fragment duration
    'Highway',              # nuuo-3659174699335680	   720p@30FPS      ~8.3s fragment duration
]

AREAS_711_LATHAM = [
    'Cam 1',                # nuuo-                     2560x1920@20FPS
    'Cam 2',                # nuuo-                     1920x1080@30FPS
    'Cam 3',                # nuuo-                     2560x1920@20FPS
    'Cam 4',                # nuuo-                     2560x1920@20FPS
]

# site_id = 3, Cal student store
# site_id = 100, NUUO CA office

def main(cfg):
    stream = f"{cfg.stream}"
    streamer = NUUOProducer(cfg.ip, cfg.port, cfg.user, cfg.passwd, site_id=cfg.site_id, env=cfg.env)
    streamer.connect(stream)
    print(f'Connecting to NUUO area: {cfg.area}')
    streamer.upload(str(cfg.area),
                    fps=int(cfg.fps),
                    profile=cfg.profile,
                    stream_id=cfg.stream_id,
                    env=cfg.env, 
                    timeout=(15, 30),
                    )
    streamer.disconnect()

if __name__ == '__main__':
    # XXX: specifying the type of argument might break reading message from sqs as those msgs are string 
    parser = argparse.ArgumentParser('NUUO Streamer')
    parser.add_argument('--ip', help='NVR IP address')
    parser.add_argument('--port', type=int, help='NVR port')
    parser.add_argument('-u', '--user', help='username')
    parser.add_argument('--passwd', help='password')
    parser.add_argument('--stream', default='nuuo-usa', help='Destination AWS KV stream name')
    parser.add_argument('-a', '--area', help='Camera area query')
    parser.add_argument('--profile', default='Original', help='Video compression quality')
    parser.add_argument('--fps', default=15, help='NVR source FPS')

    # database 
    parser.add_argument('--env', default='DEV', choices=['DEV', 'PROD'], help='Current env')
    parser.add_argument('--stream_id', default=None, help='Stream ID to update the status when running in ECS Cluster')
    parser.add_argument('--site_id', default=100, help='Site id of the stream to fetch NVR creds from database')

    cfg = parser.parse_args()
    main(cfg)
