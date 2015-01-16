import cgi
import os
import sys
import time
from functools import partial
import binascii
import errno
import hashlib

from .compat import urlparse
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
    server = response.headers.get('Server')
    md5_hex = None
    if server == 'AmazonS3':
        etag = response.headers['ETag'][1:-1]
        sse = response.headers.get('x-amz-server-side-encryption', None)
        if not _is_multipart_etag(etag) and sse != 'aws:kms':
            md5_hex = etag
    else:
        content_md5 = response.headers.get('Content-MD5', None)
        if content_md5:
            md5_hex = binascii.hexlify(binascii.a2b_base64(content_md5))

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
        payload = write_to_file(None, md5_hex, md5, file_chunks, True)
    else:
        with open(filename, 'wb') as out_file:
            write_to_file(out_file, md5_hex, md5, file_chunks)

    if md5_hex:
        if md5_hex != md5.hexdigest():
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


def write_to_file(out_file, md5_hex, md5, file_chunks, is_stream=False):
    """
    Updates the etag for each file chunk.  It will write to the file if it a
    file but if it is a stream it will return a byte string to be later
    written to a stream.
    """
    body = b''
    for chunk in file_chunks:
        if md5_hex:
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
    :param src: the source url
    :type src: string
    :param dest: the destination path
    :type dest: string
    :param size: The size of the file in bytes.
    :type size: integer
    :param last_update: the local time of last modification.
    :type last_update: datetime object
    """
    operation_name = 'download'

    def __init__(self, src, dest=None, size=None, last_update=None,
                 is_stream=False):
        if is_stream:
            assert dest is None
        if dest:
            dest = os.path.abspath(dest)
        self.src = src
        self.dest = dest
        self.size = size
        self.last_update = last_update
        self.is_stream = is_stream

    def set_info_from_head(self, session):
        """
        This runs a ``HeadObject`` on the s3 object and sets the size.
        """
        response = session.head(self.src, allow_redirects=True)
        self.size = int(response.headers['content-length'])
        last_update = response.headers.get('Last-Modified')
        if last_update is not None:
            self.last_update = date_parser(last_update)
        if self.dest is None and not self.is_stream:
            content_disposition = response.headers.get('Content-Disposition')
            if content_disposition:
                type_, kw = cgi.parse_header(content_disposition)
                self.dest = kw.get('filename')
            if self.dest is None:
                self.dest = os.path.basename(urlparse(self.src).path)
            self.dest = os.path.abspath(self.dest)

    def download(self, session):
        """
        Redirects the file to the multipart download function if the file is
        large.  If it is small enough, it gets the file as an object from s3.
        """
        response = session.get(self.src, stream=True)
        last_update = response.headers.get('Last-Modified')
        if last_update is not None:
            self.last_update = date_parser(last_update)

        save_file(self.dest, response, self.last_update, self.is_stream)
