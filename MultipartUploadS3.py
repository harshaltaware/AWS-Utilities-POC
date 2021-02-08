import threading
import boto3
import os
import sys
from boto3.s3.transfer import TransferConfig
client = boto3.client('s3')
s3 = boto3.resource('s3')

BUCKET_NAME = "<bucket name>"
def multi_part_upload_with_s3():
    # function for Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.path.dirname(__file__) + '/<file_name>'
    file_path = "<local file path>/<file name>"
    key_path = 'multipart_files/<file_name>'
    s3.meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                            ExtraArgs={'ACL': 'bucket-owner-full-control', 'ContentType': 'text/csv'},
                            Config=config,
                            Callback=ProgressPercentage(file_path)
                            )


class ProgressPercentage(object):
    #to know the stats of the progress
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

if __name__ == '__main__':
    multi_part_upload_with_s3()
