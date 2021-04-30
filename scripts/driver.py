#!/usr/bin/env python

# Copyright (c) 2017-present, NEC Laboratories America, Inc. ("NECLA"). 
# All rights reserved.
#
# This source code is licensed under the license found in the LICENSE file in
# the root directory of this source tree. An additional grant of patent rights
# can be found in the PATENTS file in the same directory

import json
import time
import errno
from uuid import uuid4
from threading import Event
from os import environ as env
from ml import argparse, sys, logging

from ml.ws.aws.sqs import SQS
from ml.ws.executor import Executor
from ml.ws.common import task, TASKS
from ml.ws.aws.secrets import get as get_secret
from ml.ws.aws.utils import get_task_id_on_instance
from ml.ws.broker.connector import (
    MessageBroker,
    SUBSCRIPTION_ID,
    MSG_TYPE
)

from queue import Queue
from enum import Enum, auto

logging.getLogger().setLevel('INFO')

STREAMER_JOIN_TIMEOUT = 4 * 5

# fetch ecs_task_id from localhost
ECS_TASK_ID = None
DOCKER_ID = None

def send_msg(sqs, msg):
    try:
        global ECS_TASK_ID
        global DOCKER_ID
        if not ECS_TASK_ID:
            try:
                out = get_task_id_on_instance()
                ECS_TASK_ID = out.get('task_id', 'INVALID')
                DOCKER_ID = out.get('docker_id', 'INVALID')
            except Exception as e:
                logging.error(e)
                ECS_TASK_ID = uuid4().hex
                DOCKER_ID = uuid4().hex

        msg['ecs_task_id'] = ECS_TASK_ID
        msg['ecs_docker_id'] = DOCKER_ID
        msg['publisher'] = 'STREAMING_TASK'
        stream_id = msg['stream_id']
        # NOTE: Messages that belong to the same message group are always processed one by one, 
        # in a strict order relative to the message group 
        # (however, messages that belong to different message groups might be processed out of order). 
        sqs.send_message(
            message_body=json.dumps(msg),
            message_group_id=f'{stream_id}'
        )
    except Exception as e:
        logging.error(f'SQS send message failed: {e}')
        raise e

@task
def nuuo(args, stop_event):
    from ml.streaming import NUUOProducer
    try:
        stream = args.get('stream', 'nuuo-usa')
        area = str(args.get('area'))
        producer = NUUOProducer(
            ip=args.get('ip', None),
            port=args.get('port', None),
            user=args.get('user', None),
            passwd=args.get('passwd', None),
            site_id=args.get('site_id'),
            env=args.get('env', 'PROD')
        )
        producer.connect(stream)
        logging.info(f'Connecting to NUUO area: {area}')
        producer.upload(
            area,
            fps=int(args.get('fps', 10)),
            profile=args.get('profile', 'Original'),
            stream_id=args.get('stream_id', None),
            env=args.get('env', 'PROD'),
            timeout=(15, 30),
            stop_event=stop_event
        )
    except Exception as e:
        raise e
    finally:
        # NOTE: make sure the thread timeout is enough for streamer to disconnect properly
        # XXX: could result in doubled streams 
        producer.disconnect()

@task
def kvs(args, stop_event):
    from ml.streaming import KVProducer
    try:
        path = args.get('path')
        stream = args.get('stream', 'loopback')
        logging.info(f'Streaming video at {path} to KV stream={stream}')
        producer = KVProducer()
        producer.connect(stream)
        producer.upload(
            path,
            fps=int(args.get('fps', 10)),
            loop=args.get('loop', True),
            start=args.get('start', None),
            end=args.get('end', None),
            transcode=args.get('transcode', False),
            bucket=args.get('bucket', 'eigen-stream-videos'),
            stream_id=args.get('stream_id'),
            env=args.get('env', 'PROD'),
            workaround=args.get('workaround', True),
            stop_event=stop_event
        )
    except Exception as e:
        raise e
    finally:
        producer.disconnect()

class Monitor(Executor):
    def __init__(self, sqs_url, env='PROD', debug=False):
        super().__init__(name=self.__class__.__name__, debug=debug)
        self._queue = Queue()
        self.sqs = SQS(queue_url=sqs_url)
        self.env = env

    def put_msg(self, msg):
        self._queue.put(msg)

    def run(self):
        while not self.stop_event.is_set():
            try:
                msg = self._queue.get(block=True)
                if msg is not None:
                    if self.env == 'TEST':
                        logging.info(f'[{self.name}] Published {msg}')
                    else:
                        send_msg(self.sqs, msg)
                    self._queue.task_done()
            except Exception as e:
                logging.error(f'[{self.name}] Monitor Failed: {e}')
                raise e
                
class Streamer(Executor):
    def __init__(self, monitor, max_exceptions=3, debug=False):
        super().__init__(name=self.__class__.__name__, debug=debug)
        self._args = None
        self.max_exceptions = max_exceptions
        self.monitor = monitor
        self.exception_count = 0

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value

    def run(self):
        # loop streaming thread until stop_event is set
        while not self.stop_event.is_set():
            try:
                # get task_type and respective task function
                task_type = self._args.get('stream_type')
                TASKS[task_type](
                    self._args,
                    self.stop_event
                )
            except Exception as e:
                # exception occured:
                # put to monitor queue for updating the database
                logging.error(e)
                if self.exception_count == 0:
                    stream_id = self._args.get('stream_id')
                    msg = dict(
                        timestamp=time.time(),
                        payload=str(e),
                        msg_type=MSG_TYPE.ERROR,
                        stream_id=stream_id
                    )
                    self.monitor.put_msg(msg)

                self.exception_count += 1
                # exponential delay before restarting
                time.sleep(self.exception_count * 10)
                # XXX: make sure the delay does not get too big 
                if self.exception_count == self.max_exceptions:
                    self.exception_count = 1
                logging.info(f'[{self.name}] Restarting streaming producer on error')

class Driver(Executor):
    def __init__(self, broker, streamer, monitor, 
                job_destination='/queue/streaming_jobs',
                admin_destination=None,
                debug=False):
        super().__init__(name=self.__class__.__name__, debug=debug)
        # msg broker
        self.broker = broker 
        self.streamer = streamer
        self.monitor = monitor
        # destination message broker queue/topic to subscribe to
        self.job_destination = job_destination

    def run(self):
        while not self.stop_event.is_set():
            try:
                # blocking operation
                source, headers, message = self.broker.get_msg()
                self.handler(source, headers, message)
                self.broker.task_done()
            except Exception as e:
                logging.error(f'[{self.name}] Driver Failed: {e}')
                raise e

    def handler(self, source, headers, message):
        msg = {}
        if source == SUBSCRIPTION_ID.JOB:
            message = json.loads(message)
            correlation_id = headers.get('correlation-id')
            msg_type = headers.get('type')
            stream_id = message.get('stream_id')

            msg = {
                'stream_id': stream_id,
                'timestamp': time.time()
            }
            if int(correlation_id) == stream_id:
                # Request to turn off streaming received
                if msg_type == MSG_TYPE.OFF:
                    logging.warning(f'[{self.name}] Stopping streaming job for stream_id: {stream_id}')
                    self.streamer.stop(timeout=STREAMER_JOIN_TIMEOUT)
                    self.broker.subscribe(
                        destination=self.job_destination,
                        subscription_id=SUBSCRIPTION_ID.JOB,
                        headers={
                            'selector': 'JMSCorrelationID=1'
                        }
                    )
                    msg['msg_type'] = MSG_TYPE.OFF
                    logging.info(f'[{self.name}] Listening to streaming queue with selector: {self.broker.headers}')
                elif msg_type == MSG_TYPE.RELOAD:
                    # stream attribute changed, update args and restart streaming
                    logging.info(f'[{self.name}] Restarting streaming on changes in attributes')
                    self.streamer.args = message
                    # stop streaming thread
                    self.streamer.stop(timeout=STREAMER_JOIN_TIMEOUT)
                    # start streaming thread with latest args
                    self.streamer.start()
                    self.broker.subscribe(
                        destination=self.job_destination,
                        subscription_id=SUBSCRIPTION_ID.JOB,
                        headers={
                            'selector': f'JMSCorrelationID={stream_id}'
                        }
                    )
                    msg['payload'] = 'Streaming restarted on changes in attributes'
                    msg['msg_type'] = MSG_TYPE.RELOAD
                else:
                    logging.warning(f'[{self.name}] Skipping: Invalid message type')
            else:
                if correlation_id == '1':
                    # new streaming job request
                    # subscribe to individual stream attribute changes e.g fps, profile, etc 
                    # ==> same queue but filter based on correlation id value using selector
                    self.streamer.args = message
                    self.streamer.start()
                    msg['msg_type'] = MSG_TYPE.STREAMING
                    self.broker.subscribe(
                        destination=self.job_destination,
                        subscription_id=SUBSCRIPTION_ID.JOB,
                        headers={
                            'selector': f'JMSCorrelationID={stream_id}'
                        }
                    )
                    logging.info(f'[{self.name}] Listening to streaming queue with selector: {self.broker.headers}')
            
            # put msg to monitor queue ==> event_queue
            self.monitor.put_msg(msg)
        else:
            # SUBSCRIPTION_ID.ADMIN
            logging.warning('Admin Listener not implemented yet')

def main(cfg, creds):
    '''
    ===================> BROKER <===================
    '''
    # init Message broker
    broker = MessageBroker.create(
        credentials=dict(
            server=creds.get('server'),
            user=creds.get('user'),
            passwd=creds.get('passwd'),
            port=int(creds.get('port'))
        ),
        job_destination=cfg.job_destination
    )
    '''
    ===================> MONITOR <===================
    '''
    # monitor stream producer and report events to sqs queue
    monitor = Monitor(sqs_url=creds.get('event_queue'), env=cfg.env) 
    # start monitor
    monitor.start()

    '''
    ===================> STREAMER <===================
    '''
    # stream video frames to kinesis
    streamer = Streamer(monitor) 

    '''
    ===================> DRIVER <===================
    '''
    # manage all the modules including: monitor, streamer, broker
    driver = Driver(
        broker=broker,
        streamer=streamer,
        monitor=monitor,
        job_destination=cfg.job_destination
    )
    # start driver
    driver.start()

    # subscribe and listen for streaming jobs
    # NOTE: selector with correlation id = 1 is reserved for streaming jobs
    broker.subscribe(
        destination=cfg.job_destination,
        subscription_id=SUBSCRIPTION_ID.JOB,
        headers={
            'selector': 'JMSCorrelationID=1'
        }
    )

    # TODO: subscribe and listen for admin jobs
    logging.info('Listening for streaming jobs...')
    
    # listen to SIGTERM and SIGINT
    import signal
    signaled = False
    def handler(sig, frame):
        logging.warning(f"Interrupted or stopped by {signal.Signals(sig).name}")
        nonlocal signaled
        signaled = True
        # XXX: avoid stale TCP connections
        broker.disconnect()
        sys.exit(signal.Signals(sig).value)

    # listen to signals from ecs tasks/user
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    #signal.signal(signal.SIGKILL, handler)

    while not signaled:
        # wait for msgs in the destination queue
        time.sleep(3)

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Driver to manage the broker and streaming')
    parser.add_argument('--secret-name',     default='eigen/prod/message_broker', help='Message broker credentials', type=str)
    parser.add_argument('--job_destination', default='/queue/streaming_jobs',     help='Destination subscription for the broker', type=str)
    parser.add_argument("--env",              default="PROD",                     help="Current env", choices=["TEST", "PROD"])

    cfg = parser.parse_args()
    
    if cfg.env == 'TEST':
        from ml.ws.aws.utils import setup_test
        broker_creds = setup_test()
    else:
        broker_creds = get_secret(cfg.secret_name)

    main(cfg, broker_creds)