import argparse
import os
import time

import boto3

s3 = boto3.resource('s3')

parser = argparse.ArgumentParser(description='Test script to pump random files into an S3 bucket.')
parser.add_argument('--bucket', '-b', dest='bucket', type=str, default='scale-s3-create-retrieve-test',
                    help='S3 bucket to load into')
args = parser.parse_args()

while True:
    unix_time = time.time()
    file_name = '%s.txt' % unix_time
    try:
        with open(file_name, 'w') as temp_file:
            temp_file.write(str(unix_time))

        if int(unix_time) % 5 > 0:
            target_key = '%s/%s/%s' % (int(unix_time / 60), int(unix_time / 5), file_name)
        else:
            target_key = '%s/%s' % (int(unix_time / 60), file_name)

        s3.Object(args.bucket, target_key).upload_file(file_name)
        print "Uploaded '%s' to '%s'." % (file_name, target_key)
    finally:
        os.remove(file_name)

