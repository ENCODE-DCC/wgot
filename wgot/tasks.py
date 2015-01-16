import logging
import os
import time
import threading

import requests

from .utils import MD5Error, \
    relative_path, IORequest, IOCloseRequest, \
    IncompleteReadError, StreamingBody, PrintTask


LOGGER = logging.getLogger(__name__)


class UploadCancelledError(Exception):
    pass


class DownloadCancelledError(Exception):
    pass


class RetriesExeededError(Exception):
    pass


def print_operation(filename, failed, dryrun=False):
    """
    Helper function used to print out what an operation did and whether
    it failed.
    """
    print_str = filename.operation_name
    if dryrun:
        print_str = '(dryrun) ' + print_str
    if failed:
        print_str += " failed"
    print_str += ": "
    print_str = print_str + filename.src
    if not filename.is_stream:
        print_str += " to " + relative_path(filename.dest)
    return print_str


class OrderableTask(object):
    PRIORITY = 10


class BasicTask(OrderableTask):
    """
    This class is a wrapper for all ``TaskInfo`` and ``TaskInfo`` objects
    It is practically a thread of execution.  It also injects the necessary
    attributes like ``session`` object in order for the filename to
    perform its designated operation.
    """
    def __init__(self, session, filename, parameters,
                 result_queue):
        self.session = session

        self.filename = filename

        self.parameters = parameters
        self.result_queue = result_queue

    def __call__(self):
        self._execute_task(attempts=3)

    def _execute_task(self, attempts, last_error=''):
        if attempts == 0:
            # We've run out of retries.
            self._queue_print_message(self.filename, failed=True,
                                      dryrun=self.parameters['dryrun'],
                                      error_message=last_error)
            return
        filename = self.filename
        try:
            if not self.parameters['dryrun']:
                filename.download(self.session)
        except requests.ConnectionError as e:
            connect_error = str(e)
            LOGGER.debug("%s %s failure: %s",
                         filename.src, filename.operation_name, connect_error)
            self._execute_task(attempts - 1, last_error=str(e))
        except MD5Error as e:
            LOGGER.debug("%s %s failure: Data was corrupted: %s",
                         filename.src, filename.operation_name, e)
            self._execute_task(attempts - 1, last_error=str(e))
        except Exception as e:
            LOGGER.debug(str(e), exc_info=True)
            self._queue_print_message(filename, failed=True,
                                      dryrun=self.parameters['dryrun'],
                                      error_message=str(e))
        else:
            self._queue_print_message(filename, failed=False,
                                      dryrun=self.parameters['dryrun'])

    def _queue_print_message(self, filename, failed, dryrun,
                             error_message=None):
        try:
            if filename.operation_name != 'list_objects':
                message = print_operation(filename, failed,
                                          self.parameters['dryrun'])
                if error_message is not None:
                    message += ' ' + error_message
                result = {'message': message, 'error': failed}
                self.result_queue.put(PrintTask(**result))
        except Exception as e:
            LOGGER.debug('%s' % str(e))


class CreateLocalFileTask(OrderableTask):
    def __init__(self, context, filename):
        self._context = context
        self._filename = filename

    def __call__(self):
        dirname = os.path.dirname(self._filename.dest)
        try:
            if not os.path.isdir(dirname):
                try:
                    os.makedirs(dirname)
                except OSError:
                    # It's possible that between the if check and the makedirs
                    # check that another thread has come along and created the
                    # directory.  In this case the directory already exists and we
                    # can move on.
                    pass
            # Always create the file.  Even if it exists, we need to
            # wipe out the existing contents.
            with open(self._filename.dest, 'wb'):
                pass
        except Exception as e:
            self._context.cancel()
        else:
            self._context.announce_file_created()


class CompleteDownloadTask(OrderableTask):
    def __init__(self, context, filename, result_queue, params, io_queue):
        self._context = context
        self._filename = filename
        self._result_queue = result_queue
        self._parameters = params
        self._io_queue = io_queue

    def __call__(self):
        # When the file is downloading, we have a few things we need to do:
        # 1) Fix up the last modified time to match s3.
        # 2) Tell the result_queue we're done.
        # 3) Queue an IO request to the IO thread letting it know we're
        #    done with the file.
        self._context.wait_for_completion()
        if self._filename.last_update:
            last_update_tuple = self._filename.last_update.timetuple()
            mod_timestamp = time.mktime(last_update_tuple)
            os.utime(self._filename.dest, (int(mod_timestamp), int(mod_timestamp)))
        message = print_operation(self._filename, False,
                                  self._parameters['dryrun'])
        print_task = {'message': message, 'error': False}
        self._result_queue.put(PrintTask(**print_task))
        self._io_queue.put(IOCloseRequest(self._filename.dest))


class DownloadPartTask(OrderableTask):
    """
    This task downloads and writes a part to a file.  This task pulls
    from a ``part_queue`` which represents the queue for a specific
    multipart download.  This pulling from a ``part_queue`` is necessary
    in order to keep track and complete the multipart download initiated by
    the ``FileInfo`` object.
    """

    # Amount to read from response body at a time.
    ITERATE_CHUNK_SIZE = 1024 * 1024
    CONNECT_TIMEOUT = 10
    READ_TIMEOUT = 60
    TOTAL_ATTEMPTS = 5

    def __init__(self, part_number, chunk_size, result_queue, session,
                 filename, context, io_queue):
        self._part_number = part_number
        self._chunk_size = chunk_size
        self._result_queue = result_queue
        self._filename = filename
        self.session = session
        self._context = context
        self._io_queue = io_queue

    def __call__(self):
        try:
            self._download_part()
        except Exception as e:
            LOGGER.debug(
                'Exception caught downloading byte range: %s',
                e, exc_info=True)
            self._context.cancel()
            raise e

    def _download_part(self):
        total_file_size = self._filename.size
        start_range = self._part_number * self._chunk_size
        if self._part_number == int(total_file_size / self._chunk_size) - 1:
            end_range = ''
        else:
            end_range = start_range + self._chunk_size - 1
        range_param = 'bytes=%s-%s' % (start_range, end_range)
        LOGGER.debug("Downloading bytes range of %s for file %s", range_param,
                     self._filename.dest)
        for i in range(self.TOTAL_ATTEMPTS):
            try:
                LOGGER.debug("Making GetObject requests with byte range: %s",
                             range_param)
                response = self.session.get(
                    self._filename.src,
                    headers={'Range': range_param},
                    stream=True,
                    timeout=(self.CONNECT_TIMEOUT, self.READ_TIMEOUT))
                LOGGER.debug("Response received from GetObject")
                self._filename.set_info_from_headers(response)
                body = StreamingBody(response)
                self._queue_writes(body)
                self._context.announce_completed_part(self._part_number)

                message = print_operation(self._filename, 0)
                total_parts = int(self._filename.size / self._chunk_size)
                result = {'message': message, 'error': False,
                          'total_parts': total_parts}
                self._result_queue.put(PrintTask(**result))
                LOGGER.debug("Task complete: %s", self)
                return
            except requests.Timeout as e:
                LOGGER.debug("Socket timeout caught, retrying request, "
                             "(attempt %s / %s)", i, self.TOTAL_ATTEMPTS,
                             exc_info=True)
                continue
            except IncompleteReadError as e:
                LOGGER.debug("Incomplete read detected: %s, (attempt %s / %s)",
                             e, i, self.TOTAL_ATTEMPTS)
                continue
        raise RetriesExeededError("Maximum number of attempts exceeded: %s" %
                                  self.TOTAL_ATTEMPTS)

    def _queue_writes(self, body):
        self._context.wait_for_file_created()
        LOGGER.debug("Writing part number %s to file: %s",
                     self._part_number, self._filename.dest)
        iterate_chunk_size = self.ITERATE_CHUNK_SIZE
        if self._filename.is_stream:
            self._queue_writes_for_stream(body)
        else:
            self._queue_writes_in_chunks(body, iterate_chunk_size)

    def _queue_writes_for_stream(self, body):
        # We have to handle an output stream differently.  The main reason is
        # that we cannot seek() in the output stream.  This means that we need
        # to queue the writes in order.  If we queue IO writes in smaller than
        # part size chunks, on the case of a retry we'll need to do a range GET
        # for only the remaining parts.  The other alternative, which is what
        # we do here, is to just request the entire chunk size write.
        self._context.wait_for_turn(self._part_number)
        chunk = body.read()
        offset = self._part_number * self._chunk_size
        LOGGER.debug("Submitting IORequest to write queue.")
        self._io_queue.put(
            IORequest(self._filename.dest, offset, chunk,
                      self._filename.is_stream)
        )
        self._context.done_with_turn()

    def _queue_writes_in_chunks(self, body, iterate_chunk_size):
        amount_read = 0
        current = body.read(iterate_chunk_size)
        while current:
            offset = self._part_number * self._chunk_size + amount_read
            LOGGER.debug("Submitting IORequest to write queue.")
            self._io_queue.put(
                IORequest(self._filename.dest, offset, current,
                          self._filename.is_stream)
            )
            LOGGER.debug("Request successfully submitted.")
            amount_read += len(current)
            current = body.read(iterate_chunk_size)
        # Change log message.
        LOGGER.debug("Done queueing writes for part number %s to file: %s",
                     self._part_number, self._filename.dest)


class MultipartDownloadContext(object):

    _STATES = {
        'UNSTARTED': 'UNSTARTED',
        'STARTED': 'STARTED',
        'COMPLETED': 'COMPLETED',
        'CANCELLED': 'CANCELLED'
    }

    def __init__(self, num_parts, lock=None):
        self.num_parts = num_parts

        if lock is None:
            lock = threading.Lock()
        self._lock = lock
        self._created_condition = threading.Condition(self._lock)
        self._submit_write_condition = threading.Condition(self._lock)
        self._completed_condition = threading.Condition(self._lock)
        self._state = self._STATES['UNSTARTED']
        self._finished_parts = set()
        self._current_stream_part_number = 0

    def announce_completed_part(self, part_number):
        with self._completed_condition:
            self._finished_parts.add(part_number)
            if len(self._finished_parts) == self.num_parts:
                self._state = self._STATES['COMPLETED']
                self._completed_condition.notifyAll()

    def announce_file_created(self):
        with self._created_condition:
            self._state = self._STATES['STARTED']
            self._created_condition.notifyAll()

    def wait_for_file_created(self):
        with self._created_condition:
            while not self._state == self._STATES['STARTED']:
                if self._state == self._STATES['CANCELLED']:
                    raise DownloadCancelledError(
                        "Download has been cancelled.")
                self._created_condition.wait(timeout=1)

    def wait_for_completion(self):
        with self._completed_condition:
            while not self._state == self._STATES['COMPLETED']:
                if self._state == self._STATES['CANCELLED']:
                    raise DownloadCancelledError(
                        "Download has been cancelled.")
                self._completed_condition.wait(timeout=1)

    def wait_for_turn(self, part_number):
        with self._submit_write_condition:
            while self._current_stream_part_number != part_number:
                if self._state == self._STATES['CANCELLED']:
                    raise DownloadCancelledError(
                        "Download has been cancelled.")
                self._submit_write_condition.wait(timeout=0.2)

    def done_with_turn(self):
        with self._submit_write_condition:
            self._current_stream_part_number += 1
            self._submit_write_condition.notifyAll()

    def cancel(self):
        with self._lock:
            self._state = self._STATES['CANCELLED']

    def is_cancelled(self):
        with self._lock:
            return self._state == self._STATES['CANCELLED']

    def is_started(self):
        with self._lock:
            return self._state == self._STATES['STARTED']
