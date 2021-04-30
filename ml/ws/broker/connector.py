import sys
import ssl
import time
from queue import Queue

from ml import logging

# activeMQ broker with stomp protocol
import stomp
from stomp import ConnectionListener

# local modules
from ..common import retry

logging.getLogger().setLevel('INFO')

class MSG_TYPE:
    ERROR='ERROR'
    RELOAD='RELOAD'
    STREAMING='STREAMING'
    OFF='OFF'
    DETECTING='DETECTING'

class SUBSCRIPTION_ID:
    JOB = 0
    ADMIN = 1
        
class Listener(ConnectionListener):
    """Listener with subscription to multiple destinations
    """
    def __init__(self, broker, job_destination, admin_destination=None, name=None):
        super().__init__()
        # msg broker
        self.broker = broker
        # destinations
        self.job_destination = job_destination
        self.admin_destination = admin_destination
        self._name = name or self.__class__.__name__

    @property
    def name(self):
        return self._name

    def on_message(self, headers, message):
        """
        Called by the STOMP connection when a MESSAGE frame is received.
        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param message: the frame's payload - the message body.
        """
        # put message to broker queue 
        source = headers.get('source', SUBSCRIPTION_ID.JOB)
        # unsubscribe to avoid duplicate messages
        # NOTE: client is responsible for resubscription
        if source is not None:
            self.broker.unsubscribe(subscription_id=source)
            self.broker._put_msg((int(source), headers, message))

    def on_error(self, headers, message):
        """
        Called by the STOMP connection when an ERROR frame is received.
        :param dict headers: a dictionary containing all headers sent by the server as key/value pairs.
        :param body: the frame's payload - usually a detailed error description.
        """
        logging.error(f'{self.name}: Error receving the message: {message}: {headers}')

    def reconnect(self):
        logging.info(f'[{self.name}] Broker Reconnecting')
        try:
            # reconnect
            self.broker.connect(self.name)
            # set listener
            self.broker.set_listener(self.name, self)
            # resubscribe
            for id, dest in zip([SUBSCRIPTION_ID.JOB],
                                [self.job_destination]):
                self.broker.subscribe(
                    destination=dest,
                    subscription_id=id
                )
                logging.info(f'[{self.name}] listening with headers: {self.broker.get_headers(id)}')
        except Exception as e:
            raise e

    def on_disconnected(self):
        """
        Called by the STOMP connection when a TCP/IP connection to the
        STOMP server has been lost.  No messages should be sent via
        the connection until it has been reestablished.
        """
        logging.info(f'[{self.name}] Broker disconnected')
        self.reconnect()

    def on_receiver_loop_completed(self, headers, body):
        """
        Called when the connection receiver_loop has finished.
        """
        logging.info(f'[{self.name}] Broker receiver loop has ended')
        self.reconnect()
        
@retry(Exception, tries=-1, delay=3, backoff=2, max_delay=24)
def setup(server, port, user, passwd):
    conn = stomp.Connection(
        host_and_ports=[(server, port)],
        auto_decode=True,
        keepalive=True,
        reconnect_attempts_max=-1
    )  # heartbeats=(4000, 4000)
    conn.set_ssl(for_hosts=[(server, port)], ssl_version=ssl.PROTOCOL_TLS)
    logging.info(f'Connecting to {server}:{port} with user: {user}')
    conn.connect(user, passwd, wait=True)
    return conn

class Broker:
    """Wrapper around the original stomp connection with setup and reconnect with credentials
    """
    def __init__(self, server, user, passwd, port=61614):
        self.port = port
        self.user = user
        self.server = server
        self.passwd = passwd
        self.headers = {}

        self.queue = Queue()
        self.conn = None
    
    def get_headers(self, subscription_id):
        return self.headers[subscription_id]

    def set_headers(self, subscription_id, headers):
        self.headers[subscription_id] = headers

    def get_listener(self, name):
        return self.conn.get_listener(name)

    def set_listener(self, listener_name, listener):
        try:
            self.conn.set_listener(listener_name, listener)
        except Exception as e:
            raise e

    def get_msg(self):
        msg = self.queue.get(block=True)
        return msg

    def task_done(self):
        self.queue.task_done()

    def _put_msg(self, msg):
        self.queue.put(msg)

    def is_connected(self):
        return self.conn is not None and self.conn.is_connected()

    def connect(self, listener_name=None):
        if self.conn is not None or self.is_connected():
            if listener_name is not None:
                # remove listener to avoid on disconnected trigger
                self.conn.remove_listener(listener_name)
            # disconnect to prevent stale TCP connections
            self.conn.disconnect()
        self.conn = setup(self.server, self.port, self.user, self.passwd)

    def disconnect(self, receipt=None, headers=None, **kwargs):
        """
        Disconnect from the server.
        :param str receipt: the receipt to use (once the server acknowledges that receipt, we're
            officially disconnected; optional - if not specified a unique receipt id will
            be generated)
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        #self.conn.remove_listener(listener_name)
        self.conn.disconnect(receipt=None, headers=headers)

    def subscribe(self, destination, subscription_id, headers=None, ack="auto", **kwargs):
        """
        Subscribe to a destination.
        :param str destination: the topic or queue to subscribe to
        :param str id: a unique id to represent the subscription
        :param str ack: acknowledgement mode, either auto, client, or client-individual
            (see http://stomp.github.io/stomp-specification-1.2.html#SUBSCRIBE_ack_Header)
            for more information
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        assert subscription_id is not None, 'subscription_id is required and cannot be None'
        if headers is not None:
            self.headers[subscription_id] = headers
        else:
            headers = self.headers[subscription_id]
        self.conn.subscribe(destination=destination, id=subscription_id, ack=ack, headers=headers)

    def unsubscribe(self, destination=None, subscription_id=None, **kwargs):
        """
        Unsubscribe from a destination by either id or the destination name.
        :param str destination: the name of the topic or queue to unsubscribe from
        :param str id: the unique identifier of the topic or queue to unsubscribe from
        :param dict headers: a map of any additional headers the broker requires
        :param keyword_headers: any additional headers the broker requires
        """
        self.conn.unsubscribe(destination=destination, id=subscription_id)

class MessageBroker:
    @classmethod
    def create(cls, credentials, job_destination, admin_destination=None):
        r"""Return message broker instance with job/admin and reconnection listener.
        """
        broker = Broker(**credentials)
        broker.connect()
        # listener to listen for streaming/detecting jobs 
        listener = Listener(broker=broker, job_destination=job_destination, admin_destination=None)
        broker.set_listener(listener.name, listener)
        return broker
