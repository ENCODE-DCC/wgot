# Copyright 2012-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at

#     http://aws.amazon.com/apache2.0/

# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import sys

PY3 = sys.version_info[0] == 3

if PY3:
    import queue
    from urllib.parse import urlparse, parse_qsl
    import http.client as http_client
else:
    import Queue as queue
    from urlparse import urlparse, parse_qsl
    import httplib as http_client
