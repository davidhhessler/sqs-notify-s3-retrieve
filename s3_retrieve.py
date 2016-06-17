from sqs import dequeue
from s3 import retrieve

import argparse

parser = argparse.ArgumentParser(description='Listen for S3 ObjectCreated notifications on SQS queue'
                                             ' and download files locally.')
parser.add_argument('queue', type=str,
                    help='SQS queue name to retrieve notifications from')
parser.add_argument('--dir', '-d', dest='dir', type=str, default='incoming',
                    help='directory for incoming files')
parser.add_argument('--flatten', '-f', dest='flatten', action='store_true', default=False,
                    help='download all s3 objects directly into root of incoming ignoring s3 folders')
parser.add_argument('--watch', '-w', dest='watch', action='store_true', default=False,
                    help='endlessly poll for notification. interrupt via SIGINT')
# TODO: Add some args for SQS performance parameters

args = parser.parse_args()
print('Launching with args: %s' % args)

messages = dequeue.DequeueMessage(args.queue)
retriever = retrieve.ObjectRetriever(args.dir, args.flatten)

messages.receive(retriever.download_object)
while args.watch:
    messages.receive(retriever.download_object)
