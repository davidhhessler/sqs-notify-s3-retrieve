import logging
import os
import time

import boto3

from boto3.s3.transfer import TransferConfig, S3Transfer, random_file_extension

MiB = 1024 * 1024


class ObjectRetriever(object):
    def __init__(self, target_dir, flatten, chunk_size, concurrency, temp_suffix):
        # Get the service resource
        client = boto3.client('s3')

        config = TransferConfig(
            multipart_threshold=chunk_size,
            max_concurrency=concurrency
        )
        self.s3 = S3Transfer(client, config)

        # Set up instance variables
        self.target_dir = target_dir
        self.flatten = flatten
        self.temp_suffix = temp_suffix

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

        full_download = os.path.join(download_path, object_name)
        logging.info("Downloading '%s' from bucket '%s' to '%s'..." %
                     (object_key, bucket_name, full_download))

        self._download_file(bucket_name, object_key, full_download)

    def _download_file(self, bucket, key, filename):
        """Download an S3 object to a file.

        This method is a straight port of the S3Transfer download_file method to
        override the random extension behavior with an explicit suffix
        """
        # This method will issue a ``head_object`` request to determine
        # the size of the S3 object.  This is used to determine if the
        # object is downloaded in parallel.
        object_size = self.s3._object_size(bucket, key, {})
        if not object_size:
            logging.warning('Skipping directory or 0 byte file: %s' % key)
            return

        temp_filename = '%s%s%s%s' % (filename, os.extsep, random_file_extension(), self.temp_suffix)
        try:
            start_time = time.time()
            # Download file with in-progress name
            self.s3._download_file(bucket, key, temp_filename, object_size,
                                   {}, None)
            elapsed_time = time.time() - start_time
            # Rename file to final download
            logging.info('Downloading complete. %s MiBs downloaded in %s seconds at %s MiB/s' %
                         (object_size / MiB, elapsed_time, object_size / MiB / elapsed_time))
        except Exception:
            logging.debug("Exception caught in download_file, removing partial "
                         "file: %s", temp_filename, exc_info=True)
            self.s3._osutil.remove_file(temp_filename)
            raise
        else:
            self.s3._osutil.rename_file(temp_filename, filename)
