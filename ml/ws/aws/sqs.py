import boto3
import logging
from botocore.exceptions import ClientError

class SQS:
    def __init__(self, queue_url, client=None):
        self.client = client or boto3.client('sqs')
        self.url = queue_url

    def retrieve_sqs_messages(self, num_msgs=1, wait_time=20, visibility_time=5):
        """Retrieve messages from an SQS queue

        The retrieved messages are not deleted from the queue.

        :param sqs_queue_url: String URL of existing SQS queue
        :param num_msgs: Number of messages to retrieve (1-10)
        :param wait_time: Number of seconds to wait if no messages in queue
        :param visibility_time: Number of seconds to make retrieved messages
            hidden from subsequent retrieval requests
        :return: List of retrieved messages. If no messages are available, returned
            list is empty. If error, returns None.
        """
        # Validate number of messages to retrieve
        if num_msgs < 1:
            num_msgs = 1
        elif num_msgs > 10:
            num_msgs = 10

        # Retrieve messages from the queue
        try:
            msgs = self.client.receive_message(QueueUrl=self.url,
                                               MaxNumberOfMessages=num_msgs,
                                               WaitTimeSeconds=wait_time,
                                               VisibilityTimeout=visibility_time)
        except ClientError as e:
            logging.error(e)
            raise e

        # Return the list of retrieved messages
        return msgs.get('Messages', None)
        
    def delete_sqs_message(self, msg_receipt_handle):
        """Delete a message from an SQS queue

        :param sqs_queue_url: String URL of existing SQS queue
        :param msg_receipt_handle: Receipt handle value of retrieved message
        """
        # Delete the message from the SQS queue
        try:
            self.client.delete_message(QueueUrl=self.url,
                                       ReceiptHandle=msg_receipt_handle)
        except Exception as e:
            raise e
    
    def send_message(self, message_body, delay_seconds=None, message_group_id=None):
        """
        Send message to sqs queue
        
        Params:
            message_body - the body of the message to be sent
        Returns:
            sqs message response if success or None if failed
            Response Syntax
            {
                'MD5OfMessageBody': 'string',
                'MD5OfMessageAttributes': 'string',
                'MD5OfMessageSystemAttributes': 'string',
                'MessageId': 'string',
                'SequenceNumber': 'string'
            }
        """
        response = None
        params = dict(
            QueueUrl=self.url,
            MessageBody=message_body
        )
        if delay_seconds is not None:
            params['DelaySeconds'] = delay_seconds
        if message_group_id is not None:
            params['MessageGroupId'] = str(message_group_id) # required for FIFO queues
        try:
            response = self.client.send_message(**params)
        except Exception as e:
            raise e

        return response