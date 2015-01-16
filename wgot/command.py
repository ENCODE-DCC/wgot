"""
"""
EPILOG = __doc__

import logging
import pkg_resources
import requests
import sys
from .fileinfo import FileInfo
from .handler import Handler, StreamHandler
from .compat import (
    PY3,
    http_client,
    urlparse,
)


def default_user_agent(name='wgot'):
    return '%s/%s %s' % (
        name,
        pkg_resources.get_distribution('wgot').version,
        requests.utils.default_user_agent(),
    )


def enable_debug_logging():
    logging.basicConfig()
    logging.getLogger('wgot').setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    http_client.HTTPConnection.debuglevel = 1
    http_client.HTTPSConnection.debuglevel = 1


def run(debug, input_file, max_redirect, output_document, user, password,
        quiet, urls, user_agent, version):
    if version:
        print(default_user_agent())
    if debug:
        enable_debug_logging()

    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})
    session.max_redirects = max_redirect

    if user is not None:
        assert password is not None
        session.auth = (user, password)

    if input_file:
        if input_file == '-':
            input_file_data = sys.stdin.read()
        elif urlparse(input_file).scheme:
            input_file_data = session.get(input_file).text
        else:
            input_file_data = open(input_file).read()
        urls.extend(
            line.strip() for line in input_file_data.split('\n')
            if line.strip())

    is_stream = False
    if output_document:
        is_stream = True
        if output_document != '-':
            if PY3:
                sys.stdout.buffer = open(output_document, 'wb')
            else:
                sys.stdout = open(output_document, 'wb')

    if is_stream:
        handler = StreamHandler(
            {'quiet': True, 'is_stream': True}, session=session)
    else:
        handler = Handler({'quiet': quiet}, session=session)
    fileinfos = [FileInfo(url, is_stream=is_stream) for url in urls]
    handler.call(fileinfos)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Parallel HTTP ",
        epilog=EPILOG + '\n\nversions:\n  %s\n' % default_user_agent(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'urls', metavar='URL', default=[], nargs='*', help="URLs to download")
    parser.add_argument(
        '-d', '--debug', action='store_true', help="Turn on debug output")
    parser.add_argument(
        '-i', '--input-file',
        help="Read URLs from a local or external file."
        " If '-' is specified as file, URLs are read from the standard input.")
    parser.add_argument(
        '--max-redirect', default=20, type=int,
        help="Specifies the maximum number of redirections to follow for a "
        "resource. The default is 20, which is usually far more than "
        "necessary.")
    parser.add_argument(
        '-O', '--output-document', metavar='file',
        help="The documents will not be written to the appropriate files, "
        "but all will be concatenated together and written to file."
        " If '-'' is used as file, documents will be printed to standard "
        "output.")
    parser.add_argument(
        '-q', '--quiet', action='store_true', help="Turn off output")
    parser.add_argument(
        '-U', '--user-agent', default=default_user_agent(),
        metavar='agent-string',
        help="Identify as agent-string to the HTTP server.")
    parser.add_argument(
        '--user')
    parser.add_argument(
        '--password')
    parser.add_argument(
        '--version', action='store_true', help='Print version and exit')

    args = parser.parse_args()
    return run(**vars(args))


if __name__ == '__main__':
    main()
