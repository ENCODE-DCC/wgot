# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from datetime import datetime
import mimetypes
import hashlib
import math
import os
import sys
from collections import namedtuple, deque
from functools import partial

from email.utils import mktime_tz, parsedate_tz

from .constants import MAX_PARTS
from .constants import MAX_SINGLE_UPLOAD_SIZE
from .compat import PY3
from .compat import queue


class MD5Error(Exception):
    """
    Exception for md5's that do not match.
    """
    pass


class StablePriorityQueue(queue.Queue):
    """Priority queue that maintains FIFO order for same priority items.

    This class was written to handle the tasks created in
    awscli.customizations.s3.tasks, but it's possible to use this
    class outside of that context.  In order for this to be the case,
    the following conditions should be met:

        * Objects that are queued should have a PRIORITY attribute.
          This should be an integer value not to exceed the max_priority
          value passed into the ``__init__``.  Objects with lower
          priority numbers are retrieved before objects with higher
          priority numbers.
        * A relatively small max_priority should be chosen.  ``get()``
          calls are O(max_priority).

    Any object that does not have a ``PRIORITY`` attribute or whose
    priority exceeds ``max_priority`` will be queued at the highest
    (least important) priority available.

    """
    def __init__(self, maxsize=0, max_priority=20):
        queue.Queue.__init__(self, maxsize=maxsize)
        self.priorities = [deque([]) for i in range(max_priority + 1)]
        self.default_priority = max_priority

    def _qsize(self):
        size = 0
        for bucket in self.priorities:
            size += len(bucket)
        return size

    def _put(self, item):
        priority = min(getattr(item, 'PRIORITY', self.default_priority),
                       self.default_priority)
        self.priorities[priority].append(item)

    def _get(self):
        for bucket in self.priorities:
            if not bucket:
                continue
            return bucket.popleft()


def get_file_stat(path):
    """
    This is a helper function that given a local path return the size of
    the file in bytes and time of last modification.
    """
    try:
        stats = os.stat(path)
        update_time = datetime.fromtimestamp(stats.st_mtime, tzlocal())
    except (ValueError, IOError) as e:
        raise ValueError('Could not retrieve file stat of "%s": %s' % (
            path, e))
    return stats.st_size, update_time


def check_etag(etag, fileobj):
    """
    This fucntion checks the etag and the md5 checksum to ensure no
    data was corrupted upon transfer.
    """
    get_chunk = partial(fileobj.read, 1024 * 1024)
    m = hashlib.md5()
    for chunk in iter(get_chunk, b''):
        m.update(chunk)
    if '-' not in etag:
        if etag != m.hexdigest():
            raise MD5Error


def check_error(response_data):
    """
    A helper function that prints out the error message recieved in the
    response_data and raises an error when there is an error.
    """
    if response_data:
        if 'Error' in response_data:
            error = response_data['Error']
            raise Exception("Error: %s\n" % error['Message'])


def create_warning(path, error_message):
    """
    This creates a ``PrintTask`` for whenever a warning is to be thrown.
    """
    print_string = "warning: "
    print_string = print_string + "Skipping file " + path + ". "
    print_string = print_string + error_message
    warning_message = PrintTask(message=print_string, error=False,
                                warning=True)
    return warning_message


def find_chunksize(size, current_chunksize):
    """
    The purpose of this function is determine a chunksize so that
    the number of parts in a multipart upload is not greater than
    the ``MAX_PARTS``.  If the ``chunksize`` is greater than
    ``MAX_SINGLE_UPLOAD_SIZE`` it returns ``MAX_SINGLE_UPLOAD_SIZE``.
    """
    chunksize = current_chunksize
    num_parts = int(math.ceil(size / float(chunksize)))
    while num_parts > MAX_PARTS:
        chunksize *= 2
        num_parts = int(math.ceil(size / float(chunksize)))
    if chunksize > MAX_SINGLE_UPLOAD_SIZE:
        return MAX_SINGLE_UPLOAD_SIZE
    else:
        return chunksize


class MultiCounter(object):
    """
    This class is used as a way to keep track of how many multipart
    operations are in progress.  It also is used to track how many
    part operations are occuring.
    """
    def __init__(self):
        self.count = 0


def uni_print(statement, out_file=None):
    """
    This function is used to properly write unicode to a file, usually
    stdout or stdderr.  It ensures that the proper encoding is used if the
    statement is not a string type.
    """
    if out_file is None:
        out_file = sys.stdout
    # Check for an encoding on the file.
    encoding = getattr(out_file, 'encoding', None)
    if encoding is not None and not PY3:
        out_file.write(statement.encode(out_file.encoding))
    else:
        try:
            out_file.write(statement)
        except UnicodeEncodeError:
            # Some file like objects like cStringIO will
            # try to decode as ascii.  Interestingly enough
            # this works with a normal StringIO.
            out_file.write(statement.encode('utf-8'))
    out_file.flush()


def bytes_print(statement):
    """
    This function is used to properly write bytes to standard out.
    """
    if PY3:
        if getattr(sys.stdout, 'buffer', None):
            sys.stdout.buffer.write(statement)
        else:
            # If it is not possible to write to the standard out buffer.
            # The next best option is to decode and write to standard out.
            sys.stdout.write(statement.decode('utf-8'))
    else:
        sys.stdout.write(statement)


def guess_content_type(filename):
    """Given a filename, guess it's content type.

    If the type cannot be guessed, a value of None is returned.
    """
    return mimetypes.guess_type(filename)[0]


def relative_path(filename, start=os.path.curdir):
    """Cross platform relative path of a filename.

    If no relative path can be calculated (i.e different
    drives on Windows), then instead of raising a ValueError,
    the absolute path is returned.

    """
    try:
        dirname, basename = os.path.split(filename)
        relative_dir = os.path.relpath(dirname, start)
        return os.path.join(relative_dir, basename)
    except ValueError:
        return os.path.abspath(filename)


def date_parser(date_string):
    return datetime.fromtimestamp(mktime_tz(parsedate_tz(date_string)))


class PrintTask(namedtuple('PrintTask',
                           ['message', 'error', 'total_parts', 'warning'])):
    def __new__(cls, message, error=False, total_parts=None, warning=None):
        """
        :param message: An arbitrary string associated with the entry.   This
            can be used to communicate the result of the task.
        :param error: Boolean indicating a failure.
        :param total_parts: The total number of parts for multipart transfers.
        :param warning: Boolean indicating a warning
        """
        return super(PrintTask, cls).__new__(cls, message, error, total_parts,
                                             warning)


IORequest = namedtuple('IORequest',
                       ['filename', 'offset', 'data', 'is_stream'])
# Used to signal that IO for the filename is finished, and that
# any associated resources may be cleaned up.
IOCloseRequest = namedtuple('IOCloseRequest', ['filename'])


class IncompleteReadError(Exception):
    """HTTP response did not return expected number of bytes."""
    fmt = ('{actual_bytes} read, but total bytes '
           'expected is {expected_bytes}.')

    def __init__(self, **kwargs):
        msg = self.fmt.format(**kwargs)
        Exception.__init__(self, msg)
        self.kwargs = kwargs


class StreamingBody(object):
    """Wrapper class for an http response body.

    This provides a few additional conveniences that do not exist
    in the urllib3 model:

        * Auto validation of content length, if the amount of bytes
          we read does not match the content length, an exception
          is raised.

    """
    def __init__(self, response):
        self._raw_stream = response.raw
        self._content_length = response.headers.get('content-length')
        self._amount_read = 0

    def read(self, amt=None):
        chunk = self._raw_stream.read(amt)
        self._amount_read += len(chunk)
        if not chunk or amt is None:
            # If the server sends empty contents or
            # we ask to read all of the contents, then we know
            # we need to verify the content length.
            self._verify_content_length()
        return chunk

    def _verify_content_length(self):
        if self._content_length is not None and \
                self._amount_read != int(self._content_length):
            raise IncompleteReadError(
                actual_bytes=self._amount_read,
                expected_bytes=int(self._content_length))


def _validate_content_length(expected_content_length, body_length):
    # See: https://github.com/kennethreitz/requests/issues/1855
    # Basically, our http library doesn't do this for us, so we have
    # to do this ourself.
    if expected_content_length is not None:
        if int(expected_content_length) != body_length:
            raise IncompleteReadError(
                actual_bytes=body_length,
                expected_bytes=int(expected_content_length))
