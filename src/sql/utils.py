# -*- coding: utf-8 -*-
#
# Copyright (c) 2011-2016, Cédric Krier
# Copyright (c) 2011-2016, B2CK
# Copyright (c) 2016-2016, Victor Uriarte
# and contributors. See AUTHORS for more details.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the copyright holder nor the names of its
#       contributors may be used to endorse or promote products derived from
#       this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from sql._compat import text_type, map


def format2numeric(query, params):
    """Convert format paramstyle query to numeric paramstyle

    >>> format2numeric('SELECT * FROM table WHERE col = %s', ('foo',))
    ('SELECT * FROM table WHERE col = :0', ('foo',))
    >>> format2numeric('SELECT * FROM table WHERE col1 = %s AND col2 = %s',
    ...     ('foo', 'bar'))
    ('SELECT * FROM table WHERE col1 = :0 AND col2 = :1', ('foo', 'bar'))
    """
    return (query % tuple(
        ':{0:d}'.format(i) for i, _ in enumerate(params)), params)


def csv_map(func, text):
    return ', '.join(map(func, text))


def csv_str(text):
    return csv_map(text_type, text)
