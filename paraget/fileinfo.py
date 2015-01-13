import os
import sys
import time
from functools import partial
import errno
import hashlib

from .utils import find_bucket_key, operate, MD5Error, bytes_print


class CreateDirectoryError(Exception):
    pass


def save_file(filename, response_data, last_update, is_stream=False):
    """
    This writes to the file upon downloading.  It reads the data in the
    response.  Makes a new directory if needed and then writes the
    data to the file.  It also modifies the last modified time to that
    of the S3 object.
    """
    body = response_data['Body']
    etag = response_data['ETag'][1:-1]
    sse = response_data.get('ServerSideEncryption', None)
    if not is_stream:
        d = os.path.dirname(filename)
        try:
            if not os.path.exists(d):
                os.makedirs(d)
        except OSError as e:
            if not e.errno == errno.EEXIST:
                raise CreateDirectoryError(
                    "Could not create directory %s: %s" % (d, e))
    md5 = hashlib.md5()
    file_chunks = iter(partial(body.read, 1024 * 1024), b'')
    if is_stream:
        # Need to save the data to be able to check the etag for a stream
        # becuase once the data is written to the stream there is no
        # undoing it.
        payload = write_to_file(None, etag, md5, file_chunks, True)
    else:
        with open(filename, 'wb') as out_file:
            write_to_file(out_file, etag, md5, file_chunks)

    if not _is_multipart_etag(etag) and sse != 'aws:kms':
        if etag != md5.hexdigest():
            if not is_stream:
                os.remove(filename)
            raise MD5Error(filename)

    if not is_stream:
        last_update_tuple = last_update.timetuple()
        mod_timestamp = time.mktime(last_update_tuple)
        os.utime(filename, (int(mod_timestamp), int(mod_timestamp)))
    else:
        # Now write the output to stdout since the md5 is correct.
        bytes_print(payload)
        sys.stdout.flush()


def write_to_file(out_file, etag, md5, file_chunks, is_stream=False):
    """
    Updates the etag for each file chunk.  It will write to the file if it a
    file but if it is a stream it will return a byte string to be later
    written to a stream.
    """
    body = b''
    for chunk in file_chunks:
        if not _is_multipart_etag(etag):
            md5.update(chunk)
        if is_stream:
            body += chunk
        else:
            out_file.write(chunk)
    return body


def _is_multipart_etag(etag):
    return '-' in etag


class FileInfo(object):
    """
    This is a child object of the ``TaskInfo`` object.  It can perform more
    operations such as ``upload``, ``download``, ``copy``, ``delete``,
    ``move``.  Similiarly to
    ``TaskInfo`` objects attributes like ``session`` need to be set in order
    to perform operations.

    :param dest: the destination path
    :type dest: string
    :param compare_key: the name of the file relative to the specified
        directory/prefix.  This variable is used when performing synching
        or if the destination file is adopting the source file's name.
    :type compare_key: string
    :param size: The size of the file in bytes.
    :type size: integer
    :param last_update: the local time of last modification.
    :type last_update: datetime object
    :param dest_type: if the destination is s3 or local.
    :param dest_type: string
    :param parameters: a dictionary of important values this is assigned in
        the ``BasicTask`` object.
    """
    def __init__(self, src, dest=None, compare_key=None, size=None,
                 last_update=None, src_type=None, dest_type=None,
                 operation_name=None, service=None, endpoint=None,
                 parameters=None, source_endpoint=None, is_stream=False):
        self.src = src
        self.src_type = src_type
        self.operation_name = operation_name
        self.service = service
        self.endpoint = endpoint

        self.dest = dest
        self.dest_type = dest_type
        self.compare_key = compare_key
        self.size = size
        self.last_update = last_update
        # Usually inject ``parameters`` from ``BasicTask`` class.
        if parameters is not None:
            self.parameters = parameters
        else:
            self.parameters = {'acl': None,
                               'sse': None}
        self.source_endpoint = source_endpoint
        self.is_stream = is_stream

    def set_size_from_s3(self):
        """
        This runs a ``HeadObject`` on the s3 object and sets the size.
        """
        bucket, key = find_bucket_key(self.src)
        params = {'endpoint': self.endpoint,
                  'bucket': bucket,
                  'key': key}
        response_data, http = operate(self.service, 'HeadObject', params)
        self.size = int(response_data['ContentLength'])

    def download(self):
        """
        Redirects the file to the multipart download function if the file is
        large.  If it is small enough, it gets the file as an object from s3.
        """
        bucket, key = find_bucket_key(self.src)
        params = {'endpoint': self.endpoint, 'bucket': bucket, 'key': key}
        response_data, http = operate(self.service, 'GetObject', params)
        save_file(self.dest, response_data, self.last_update,
                  self.is_stream)
