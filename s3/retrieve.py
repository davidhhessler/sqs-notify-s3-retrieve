import boto3
import os


class ObjectRetriever(object):
    def __init__(self, target_dir, flatten):
        # Get the service resource
        self.s3 = boto3.client('s3')

        # Set up instance variables
        self.target_dir = target_dir
        self.flatten = flatten

    def download_object(self, s3_dict):
        """Downloads an S3 object to a local directory specified by S3Retrieve instance

        We are going to additionally ignore any object of size 0 as these are generally
        folder create operations.

        :param s3_dict: dictionary object containing s3 bucket and object metadata
        """

        bucket_name = s3_dict['bucket']['name']
        object_key = s3_dict['object']['key']
        object_path, object_name = os.path.split(object_key)

        download_path = os.path.join(self.target_dir, object_path)
        if self.flatten:
            download_path = self.target_dir

        if not os.path.exists(download_path):
            os.makedirs(download_path)

        if s3_dict['object']['size']:
            full_download = os.path.join(download_path, object_name)
            print "Downloading '%s' from bucket '%s' to '%s'..." % \
                  (object_key, bucket_name, full_download)
            self.s3.download_file(bucket_name, object_key, full_download)
            print 'Downloading complete.'
        else:
            print 'Skipping directory or 0 byte file: %s' % object_key
