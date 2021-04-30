import uuid
import time
import boto3
import atexit
import threading
from queue import Queue

from ml import logging

def encode_data(data, encoding='utf_8'):
    """
    Helper function to encode data
    """
    if isinstance(data, bytes):
        return data
    else:
        return str(data).encode(encoding)
   
class KinesisProducerThread(threading.Thread):
    """Basic Kinesis Producer.
    Parameters
    ----------
    stream_name : string
        Name of the stream to send the records.
    batch_size : int
        Numbers of records to batch before flushing the queue.
    batch_time : int
        Maximum of seconds to wait before flushing the queue.
    max_retries: int
        Maximum number of times to retry the put operation.
    kinesis_client: boto3.client
        Kinesis client.
    Attributes
    ----------
    records : array
        Queue of formated records.
    """
    def __init__(self, stream_name, batch_size=500,
                 batch_time=15, max_retries=5,
                 kinesis_client=None):
        super(KinesisProducerThread, self).__init__()
        self._name = self.__class__.__name__
        self.stream_name = stream_name
        self.queue = Queue()
        self.batch_size = batch_size
        self.batch_time = batch_time
        self.max_retries = max_retries
        if kinesis_client is None:
            kinesis_client = boto3.client('kinesis')
        self.kinesis_client = kinesis_client

        self.last_flush = time.time()
        self.monitor_running = threading.Event()
        self.monitor_running.set()
        from collections import defaultdict
        self.hash_keys = defaultdict(lambda: None)
        self.hash_lst = []
        self.get_hash_keys()

        # start thread
        self.start()

        atexit.register(self.close)

    def unload_hash(self, partition_key):
        hash = self.hash_keys.pop(partition_key, None)
        if hash is not None:
            self.hash_lst.append(hash)

    def load_hash(self, partition_key):
        if partition_key not in self.hash_keys and self.hash_lst:
            self.hash_keys[partition_key] = self.hash_lst.pop(0)

    def get_hash_keys(self):
        try:
            response = self.kinesis_client.describe_stream(
                StreamName=self.stream_name,
                Limit=1000,
            )
            for shard in response['StreamDescription']['Shards']:
                self.hash_lst.append(shard['HashKeyRange']['EndingHashKey'])
        except Exception as e:
            raise e

    def monitor(self):
        """Flushes the queue periodically."""
        while self.monitor_running.is_set():
            if time.time() - self.last_flush > self.batch_time or self.queue.qsize() >= self.batch_size:
                if not self.queue.empty():
                    self.flush_queue()
            time.sleep(self.batch_time / 2)

    def put_records(self, records, partition_key=None):
        """Add a list of data records to the record queue in the proper format.
        Convinience method that calls self.put_record for each element.
        Parameters
        ----------
        records : list
            Lists of records to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.
        """
        for record in records:
            self.put_record(record, partition_key)

    def put_record(self, data, partition_key=None):
        """Add data to the record queue in the proper format.
        Parameters
        ----------
        data : str
            Data to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.
        """
        # Byte encode the data
        data = encode_data(data)

        # Create a random partition key if not provided
        partition_key = str(partition_key) or uuid.uuid4().hex

        # Build the record
        record = {
            'Data': data,
            'PartitionKey': partition_key
        }

        explicit_hash_key = self.hash_keys[partition_key]
        if explicit_hash_key is not None:
            record['ExplicitHashKey'] = str(explicit_hash_key)

        # Append the record
        logging.debug('Putting record "{}"'.format(record['Data'][:100]))
        self.queue.put(record)

    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        logging.info(f'[{self._name}] Closing kinesis producer')
        self.flush_queue()
        self.monitor_running.clear()
        self.join()
        logging.info(f'[{self._name}] Kinesis producer closed')

    def run(self):
        try:
            self.monitor()
        except Exception as e:
            logging.error(e)
            raise e

    def flush_queue(self):
        """Grab all the current records in the queue and send them."""
        records = []

        while not self.queue.empty() and len(records) < self.batch_size:
            records.append(self.queue.get())

        if records:
            self.send_records(records)
            self.last_flush = time.time()

    def send_records(self, records, attempt=0):
        """Send records to the Kinesis stream.
        Falied records are sent again with an exponential backoff decay.
        Parameters
        ----------
        records : array
            Array of formated records to send.
        attempt: int
            Number of times the records have been sent without success.
        """

        # If we already tried more times than we wanted, save to a file
        if attempt > self.max_retries:
            logging.warning(f'[{self._name}] Writing {len(records)} records to file')
            with open('failed_records.dlq', 'ab') as f:
                for r in records:
                    f.write(r.get('Data'))
            return

        # Sleep before retrying
        if attempt:
            time.sleep(2 ** attempt * .1)

        try:
            response = self.kinesis_client.put_records(StreamName=self.stream_name,
                                                    Records=records)
        except Exception as e:
            logging.error(f'[{self._name}]: {e}')
            raise e
        else:
            failed_record_count = response['FailedRecordCount']

            # Grab failed records
            if failed_record_count:
                logging.warning(f'[{self._name}] Retrying failed records')
                failed_records = []
                for i, record in enumerate(response['Records']):
                    if record.get('ErrorCode'):
                        failed_records.append(records[i])

                # Recursive call
                attempt += 1
                self.send_records(failed_records, attempt=attempt)

class KinesisProducerThreadPool:
    """Basic Kinesis Producer.
    Parameters
    ----------
    stream_name : string
        Name of the stream to send the records.
    batch_size : int
        Numbers of records to batch before flushing the queue.
    batch_time : int
        Maximum of seconds to wait before flushing the queue.
    max_retries: int
        Maximum number of times to retry the put operation.
    kinesis_client: boto3.client
        Kinesis client.
    Attributes
    ----------
    records : array
        Queue of formated records.
    pool: concurrent.futures.ThreadPoolExecutor
        Pool of threads handling client I/O.
    """

    def __init__(self, stream_name, batch_size=500,
                 batch_time=5, max_retries=5, threads=10,
                 kinesis_client=None):
        self.stream_name = stream_name
        self.queue = Queue()
        self.batch_size = batch_size
        self.batch_time = batch_time
        self.max_retries = max_retries
        if kinesis_client is None:
            kinesis_client = boto3.client('kinesis')
        self.kinesis_client = kinesis_client

        from concurrent.futures import ThreadPoolExecutor
        self.pool = ThreadPoolExecutor(threads)
        self.last_flush = time.time()
        self.monitor_running = threading.Event()
        self.monitor_running.set()
        self.pool.submit(self.monitor)

        atexit.register(self.close)

    def monitor(self):
        """Flushes the queue periodically."""
        while self.monitor_running.is_set():
            if time.time() - self.last_flush > self.batch_time:
                if not self.queue.empty():
                    logging.info("KinesisProducer: Queue Flush-time without flush exceeded")
                    self.flush_queue()
            time.sleep(self.batch_time)

    def put_records(self, records, partition_key=None):
        """Add a list of data records to the record queue in the proper format.
        Convinience method that calls self.put_record for each element.
        Parameters
        ----------
        records : list
            Lists of records to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.
        """
        for record in records:
            self.put_record(record, partition_key)

    def put_record(self, data, partition_key=None):
        """Add data to the record queue in the proper format.
        Parameters
        ----------
        data : str
            Data to send.
        partition_key: str
            Hash that determines which shard a given data record belongs to.
        """
        # Byte encode the data
        data = encode_data(data)

        # Create a random partition key if not provided
        if not partition_key:
            partition_key = uuid.uuid4().hex

        # Build the record
        record = {
            'Data': data,
            'PartitionKey': partition_key
        }

        # Flush the queue if it reaches the batch size
        if self.queue.qsize() >= self.batch_size:
            #logging.info("Queue Flush: batch size reached")
            self.pool.submit(self.flush_queue)

        # Append the record
        logging.debug('Putting record "{}"'.format(record['Data'][:100]))
        self.queue.put(record)

    def close(self):
        """Flushes the queue and waits for the executor to finish."""
        logging.info('Closing producer')
        self.flush_queue()
        self.monitor_running.clear()
        self.pool.shutdown()
        logging.info('Producer closed')

    def flush_queue(self):
        """Grab all the current records in the queue and send them."""
        records = []

        while not self.queue.empty() and len(records) < self.batch_size:
            records.append(self.queue.get())

        if records:
            self.send_records(records)
            self.last_flush = time.time()

    def send_records(self, records, attempt=0):
        """Send records to the Kinesis stream.
        Falied records are sent again with an exponential backoff decay.
        Parameters
        ----------
        records : array
            Array of formated records to send.
        attempt: int
            Number of times the records have been sent without success.
        """

        # If we already tried more times than we wanted, save to a file
        if attempt > self.max_retries:
            logging.warning('Writing {} records to file'.format(len(records)))
            with open('failed_records.dlq', 'ab') as f:
                for r in records:
                    f.write(r.get('Data'))
            return

        # Sleep before retrying
        if attempt:
            time.sleep(2 ** attempt * .1)

        response = self.kinesis_client.put_records(StreamName=self.stream_name,
                                                   Records=records)
        
        failed_record_count = response['FailedRecordCount']

        # Grab failed records
        if failed_record_count:
            logging.warning('Retrying failed records')
            failed_records = []
            for i, record in enumerate(response['Records']):
                if record.get('ErrorCode'):
                    failed_records.append(records[i])

            # Recursive call
            attempt += 1
            self.send_records(failed_records, attempt=attempt)
