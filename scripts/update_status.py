#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import logging
import argparse

from ml.ws.aws.rds import RDS

logging.getLogger().setLevel('INFO')

def update(cfg):
    rds = RDS(env=cfg.env)
    stream = rds.update_stream_status_tasks(stream_id=cfg.stream_id, status=cfg.status)
    logging.info(stream)
    return stream

if __name__ == '__main__':

    parser = argparse.ArgumentParser('Update stream_status')
    parser.add_argument('--stream_id', help='Stream ID of the stream to update the status')
    parser.add_argument('--env', default='DEV', choices=['DEV', 'PROD'], help='Current env')
    parser.add_argument('--status', default='PENDING', choices=['PENDING', 'STREAMING', 'DETECTING'], help='stream status to set')

    cfg = parser.parse_args()
    update(cfg)
    