"""
"""
EPILOG = __doc__

import logging
import pkg_resources
import requests
from .fileinfo import FileInfo
from .handler import Handler
from .compat import http_client


def default_user_agent(name='paraget'):
    return '%s/%s %s' % (
        name,
        pkg_resources.get_distribution('paraget').version,
        requests.utils.default_user_agent(),
    )


def enable_debug_logging():
    logging.basicConfig()
    logging.getLogger('paraget').setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True
    http_client.HTTPConnection.debuglevel = 1
    http_client.HTTPSConnection.debuglevel = 1


def run(debug, max_redirect, user, password, urls, user_agent):
    if debug:
        enable_debug_logging()

    session = requests.Session()
    session.headers.update({'User-Agent': user_agent})

    max_redirect_adapter = requests.adapters.HTTPAdapter(
        max_retries=max_redirect)
    session.mount('http://', max_redirect_adapter)
    session.mount('https://', max_redirect_adapter)

    if user is not None:
        assert password is not None
        session.auth = (user, password)

    handler = Handler(session=session)
    fileinfos = [FileInfo(url) for url in urls]
    handler.call(fileinfos)


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Parallel HTTP ",
        epilog=EPILOG + '\n\nversions:\n  %s\n' % default_user_agent(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        'urls', metavar='URL', nargs='+', help="URLs to download")
    parser.add_argument(
        '-d', '--debug', action='store_true', help="Turn on debug output")
    parser.add_argument(
        '--max-redirect', default=20, type=int,
        help="Specifies the maximum number of redirections to follow for a resource."
        " The default is 20, which is usually far more than necessary.")
    parser.add_argument(
        '-U', '--user-agent', default=default_user_agent(),
        metavar='agent-string',
        help="Identify as agent-string to the HTTP server.")
    parser.add_argument(
        '--user')
    parser.add_argument(
        '--password')
    args = parser.parse_args()
    return run(**vars(args))


if __name__ == '__main__':
    main()
