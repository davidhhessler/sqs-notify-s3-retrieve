import boto3
import json


class DequeueMessage(object):
    def __init__(self, queue,
                 messages_per_request=1, wait_time=20):
        # Get the service resource
        sqs = boto3.resource('sqs')

        # Get the queue
        self.queue = sqs.get_queue_by_name(QueueName=queue)

        # Set the event version supported in message
        self.event_version_supported = '2.0'

        # Tuning values for performance

        # Messages per request set to 1 to minimize visibility timeout expiration causing multiple watching
        # instances to repeatedly retrieve the same object. Can be bumped up to max (10) if this
        # proves to not be a problem.
        self.messages_per_request = messages_per_request
        # Wait time set to the SQS max to reduce chattiness during downtime without notifications.
        # This will perform a long-poll operation over the duration, but end immediately on message receipt
        self.wait_time = wait_time

    def receive(self, s3_callback):
        """Identify all messages currently available and call function for each S3 object

        :param s3_callback: function callback that must either process s3 object or throw error
        """

        print 'Beginning long-poll against queue with wait time of %s seconds.' % self.wait_time
        messages = self.queue.receive_messages(MaxNumberOfMessages=self.messages_per_request,
                                               WaitTimeSeconds=self.wait_time)
        while len(messages):
            for message in messages:
                self.extract_s3_object(message, s3_callback)

                # Let the queue know that the message is processed
                message.delete()

            messages = self.queue.receive_messages(MaxNumberOfMessages=self.messages_per_request,
                                                   WaitTimeSeconds=self.wait_time)

    def extract_s3_object(self, message, s3_callback):
        """Extracts an S3 notification object from SQS message body and delivers to callback

        We want to ensure we have the following minimal values before passing S3 object
        on to s3_callback:

        - message.body.Message.Subject == 'Amazon S3 Notification'
        - message.body.Message.Type == 'Notification
        - message.body.Message.Records[x].eventName starts with 'ObjectCreated'
        - message.body.Message.Records[x].eventVersion == '2.0'

        Once the above have been validated we will pass the message on to callback, otherwise
        message will be discarded.

        :param message: raw SQS message
        :param s3_callback: function pointer responsible for processing s3 object
        """

        body = json.loads(message.body)

        if body['Subject'] == 'Amazon S3 Notification' and body['Type'] == 'Notification':
            message = json.loads(body['Message'])

            for record in message['Records']:
                if 'eventName' in record and record['eventName'].startswith('ObjectCreated') and 'eventVersion' in record and record['eventVersion'] == self.event_version_supported:
                    s3_callback(record['s3'])
                else:
                    # Log message that didn't match with valid EventName and EventVersion
                    print 'Unable to process message as it does not match EventName and EventVersion: ' \
                          '%s' % json.dumps(message)
        else:
            print 'Unable to process message as it does not appear to be an S3 notification: ' \
                  '%s' % json.dumps(message)
