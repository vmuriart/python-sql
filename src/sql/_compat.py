# -*- coding: utf-8 -*-

"""Python 2/3 compatibility.

This module only exists to avoid a dependency on six
for very trivial stuff.
"""

import sys

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    map = map
    range = range
    zip = zip

    text_type = str
    string_types = str,
    integer_types = int,


elif PY2:
    from itertools import izip, imap

    map = imap
    range = xrange
    zip = izip

    text_type = unicode
    string_types = unicode, str
    integer_types = int, long
