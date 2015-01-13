import os
import sys
import time
from functools import partial
import errno
import hashlib

from .utils import MD5Error, StreamingBody, bytes_print, date_parser


class CreateDirectoryError(Exception):
    pass


def save_file(filename, response, last_update, is_stream=False):
    """
    This writes to the file upon downloading.  It reads the data in the
    response.  Makes a new directory if needed and then writes the
    data to the file.  It also modifies the last modified time to that
    of the S3 object.
    """
    body = StreamingBody(response)
    etag = response.headers.get('ETag')
    server = response.headers.get('Server')
    if server == 'AmazonS3':
        etag = etag[1:-1]
    sse = response.headers.get('x-amz-server-side-encryption', None)
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

    if not is_stream and last_update:
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
    operation_name = 'download'
    last_update = None
    size = None

    def __init__(self, session, src, dest=None, size=None, is_stream=False):
        self.session = session
        self.src = src
        self.dest = dest
        self.size = size
        self.is_stream = is_stream

    def set_info_from_head(self):
        """
        This runs a ``HeadObject`` on the s3 object and sets the size.
        """
        response = self.session.head(self.src)
        self.size = int(response.headers['content-length'])
        last_update = response.headers.get('Last-Modified')
        if last_update is not None:
            self.last_update = date_parser(last_update)

    def download(self):
        """
        Redirects the file to the multipart download function if the file is
        large.  If it is small enough, it gets the file as an object from s3.
        """
        response = self.session.get(self.src, stream=True)
        last_update = response.headers.get('Last-Modified')
        if last_update is not None:
            last_update = date_parser(last_update)

        save_file(self.dest, response, last_update, self.is_stream)
