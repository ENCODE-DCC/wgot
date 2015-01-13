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
from collections import namedtuple
import logging
import os
import requests
import sys

from .constants import MULTI_THRESHOLD, CHUNKSIZE, \
    NUM_THREADS, MAX_QUEUE_SIZE
from .utils import find_chunksize, PrintTask
from .executor import Executor
from . import tasks
from .compat import six
from .compat import queue


LOGGER = logging.getLogger(__name__)

CommandResult = namedtuple('CommandResult',
                           ['num_tasks_failed', 'num_tasks_warned'])


class S3Handler(object):
    """
    This class sets up the process to perform the tasks sent to it.  It
    sources the ``self.executor`` from which threads inside the
    class pull tasks from to complete.
    """
    MAX_IO_QUEUE_SIZE = 20
    MAX_EXECUTOR_QUEUE_SIZE = MAX_QUEUE_SIZE
    EXECUTOR_NUM_THREADS = NUM_THREADS

    def __init__(self, params=None, session=None, result_queue=None,
                 multi_threshold=MULTI_THRESHOLD, chunksize=CHUNKSIZE):
        if session is None:
            session = requests.Session()
        self.session = session
        # The write_queue has potential for optimizations, so the constant
        # for maxsize is scoped to this class (as opposed to constants.py)
        # so we have the ability to change this value later.
        self.write_queue = queue.Queue(maxsize=self.MAX_IO_QUEUE_SIZE)
        self.result_queue = result_queue
        if not self.result_queue:
            self.result_queue = queue.Queue()
        self.params = {'dryrun': False, 'quiet': False,
                       'only_show_errors': False,
                       'is_stream': False}
        if params:
            self.params.update(params)
        for key in self.params.keys():
            if key in params:
                self.params[key] = params[key]
        self.multi_threshold = multi_threshold
        self.chunksize = chunksize
        self.executor = Executor(
            num_threads=self.EXECUTOR_NUM_THREADS,
            result_queue=self.result_queue,
            quiet=self.params['quiet'],
            only_show_errors=self.params['only_show_errors'],
            max_queue_size=self.MAX_EXECUTOR_QUEUE_SIZE,
            write_queue=self.write_queue
        )
        self._multipart_downloads = []

    def call(self, files):
        """
        This function pulls a ``FileInfo`` or ``TaskInfo`` object from
        a list ``files``.  Each object is then deemed if it will be a
        multipart operation and add the necessary attributes if so.  Each
        object is then wrapped with a ``BasicTask`` object which is
        essentially a thread of execution for a thread to follow.  These
        tasks are then submitted to the main executor.
        """
        try:
            self.executor.start()
            total_files, total_parts = self._enqueue_tasks(files)
            self.executor.print_thread.set_total_files(total_files)
            self.executor.print_thread.set_total_parts(total_parts)
            self.executor.initiate_shutdown()
            self.executor.wait_until_shutdown()
            self._shutdown()
        except Exception as e:
            LOGGER.debug('Exception caught during task execution: %s',
                         str(e), exc_info=True)
            self.result_queue.put(PrintTask(message=str(e), error=True))
            self.executor.initiate_shutdown(
                priority=self.executor.IMMEDIATE_PRIORITY)
            self._shutdown()
            self.executor.wait_until_shutdown()
        except KeyboardInterrupt:
            self.result_queue.put(PrintTask(message=("Cleaning up. "
                                                     "Please wait..."),
                                            error=True))
            self.executor.initiate_shutdown(
                priority=self.executor.IMMEDIATE_PRIORITY)
            self._shutdown()
            self.executor.wait_until_shutdown()

        return CommandResult(self.executor.num_tasks_failed,
                             self.executor.num_tasks_warned)

    def _shutdown(self):
        # The downloads case is easier than the uploads case because we don't
        # need to make any service calls.  To properly cleanup we just need
        # to go through the multipart downloads that were in progress but
        # cancelled and remove the local file.
        for context, local_filename in self._multipart_downloads:
            if (context.is_cancelled() or context.is_started()) and \
                    os.path.exists(local_filename):
                # The file is in an inconsistent state (not all the parts
                # were written to the file) so we should remove the
                # local file rather than leave it in a bad state.  We don't
                # want to remove the files if the download has *not* been
                # started because we haven't touched the file yet, so it's
                # better to leave the old version of the file rather than
                # deleting the file entirely.
                os.remove(local_filename)
            context.cancel()

    def _enqueue_tasks(self, files):
        total_files = 0
        total_parts = 0
        for filename in files:
            num_downloads = 1
            is_multipart_task = self._is_multipart_task(filename)
            if is_multipart_task and not self.params['dryrun']:
                # If we're in dryrun mode, then we don't need the
                # real multipart tasks.  We can just use a BasicTask
                # in the else clause below, which will print out the
                # fact that it's transferring a file rather than
                # the specific part tasks required to perform the
                # transfer.
                num_downloads = self._enqueue_range_download_tasks(filename)
            else:
                task = tasks.BasicTask(
                    session=self.session, filename=filename,
                    parameters=self.params,
                    result_queue=self.result_queue)
                self.executor.submit(task)
            total_files += 1
            total_parts += num_downloads
        return total_files, total_parts

    def _is_multipart_task(self, filename):
        # First we need to determine if it's an operation that even
        # qualifies for multipart download.
        if hasattr(filename, 'size'):
            above_multipart_threshold = filename.size > self.multi_threshold
            if above_multipart_threshold:
                return True
        else:
            return False

    def _enqueue_range_download_tasks(self, filename):
        chunksize = find_chunksize(filename.size, self.chunksize)
        num_downloads = int(filename.size / chunksize)
        context = tasks.MultipartDownloadContext(num_downloads)
        create_file_task = tasks.CreateLocalFileTask(context=context,
                                                     filename=filename)
        self.executor.submit(create_file_task)
        self._do_enqueue_range_download_tasks(
            filename=filename, chunksize=chunksize,
            num_downloads=num_downloads, context=context,
        )
        complete_file_task = tasks.CompleteDownloadTask(
            context=context, filename=filename, result_queue=self.result_queue,
            params=self.params, io_queue=self.write_queue)
        self.executor.submit(complete_file_task)
        self._multipart_downloads.append((context, filename.dest))
        return num_downloads

    def _do_enqueue_range_download_tasks(self, filename, chunksize,
                                         num_downloads, context,
                                         remove_remote_file=False):
        for i in range(num_downloads):
            task = tasks.DownloadPartTask(
                part_number=i, chunk_size=chunksize,
                result_queue=self.result_queue, service=filename.service,
                filename=filename, context=context, io_queue=self.write_queue)
            self.executor.submit(task)


class S3StreamHandler(S3Handler):
    """
    This class is an alternative ``S3Handler`` to be used when the operation
    involves a stream since the logic is different when uploading and
    downloading streams.
    """

    # This ensures that the number of multipart chunks waiting in the
    # executor queue and in the threads is limited.
    MAX_EXECUTOR_QUEUE_SIZE = 2
    EXECUTOR_NUM_THREADS = 6

    def _enqueue_tasks(self, files):
        total_files = 0
        total_parts = 0
        for filename in files:
            num_downloads = 1
            # Set the file size for the ``FileInfo`` object since
            # streams do not use a ``FileGenerator`` that usually
            # determines the size.
            filename.set_info_from_head(self.session)
            is_multipart_task = self._is_multipart_task(filename)
            if is_multipart_task and not self.params['dryrun']:
                # If we're in dryrun mode, then we don't need the
                # real multipart tasks.  We can just use a BasicTask
                # in the else clause below, which will print out the
                # fact that it's transferring a file rather than
                # the specific part tasks required to perform the
                # transfer.
                num_downloads = self._enqueue_range_download_tasks(filename)
            else:
                task = tasks.BasicTask(
                    session=self.session, filename=filename,
                    parameters=self.params,
                    result_queue=self.result_queue)
                self.executor.submit(task)
            total_files += 1
            total_parts += num_downloads
        return total_files, total_parts

    def _pull_from_stream(self, amount_requested):
        """
        This function pulls data from stdin until it hits the amount
        requested or there is no more left to pull in from stdin.  The
        function wraps the data into a ``BytesIO`` object that is returned
        along with a boolean telling whether the amount requested is
        the amount returned.
        """
        stream_filein = sys.stdin
        if six.PY3:
            stream_filein = sys.stdin.buffer
        payload = stream_filein.read(amount_requested)
        payload_file = six.BytesIO(payload)
        return payload_file, len(payload) == amount_requested

    def _enqueue_range_download_tasks(self, filename):

        # Create the context for the multipart download.
        chunksize = find_chunksize(filename.size, self.chunksize)
        num_downloads = int(filename.size / chunksize)
        context = tasks.MultipartDownloadContext(num_downloads)

        # No file is needed for downloading a stream.  So just announce
        # that it has been made since it is required for the context to
        # begin downloading.
        context.announce_file_created()

        # Submit download part tasks to the executor.
        self._do_enqueue_range_download_tasks(
            filename=filename, chunksize=chunksize,
            num_downloads=num_downloads, context=context,
        )
        return num_downloads
