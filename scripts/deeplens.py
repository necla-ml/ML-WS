#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory


from ml import argparse, sys
from ml.streaming import DeepLensProducer

def main(cfg):
    print(f"Streaming on behalf of user={cfg.user} to stream={cfg.stream}")
    producer = DeepLensProducer()
    producer.connect(f"{cfg.user}-{cfg.stream}")
    producer.upload(resolution=720, fps=cfg.fps)
    producer.disconnect()

if __name__ == '__main__':
    parser = argparse.ArgumentParser('DeepLens Streamer')
    parser.add_argument('-u', '--user', help='username')
    parser.add_argument('--stream', help='destination AWS Kenisis video stream name')
    parser.add_argument('--fps', type=int, default=15, help='encoder FPS')
    
    cfg = parser.parse_args()
    assert cfg.user, f"username unspecified"
    assert cfg.stream, f"destination stream name unspecified"
    main(cfg)