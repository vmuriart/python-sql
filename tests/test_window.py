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

import unittest

from sql import Window, Table


class TestWindow(unittest.TestCase):
    def test_window(self):
        t = Table('t')
        window = Window([t.c1, t.c2])

        assert str(window) == 'PARTITION BY "c1", "c2"'
        assert window.params == ()

    def test_window_order(self):
        t = Table('t')
        window = Window([t.c], order_by=t.c)

        assert str(window) == 'PARTITION BY "c" ORDER BY "c"'
        assert window.params == ()

    def test_window_range(self):
        t = Table('t')
        window = Window([t.c], frame='RANGE')

        assert str(window) == 'PARTITION BY "c" RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'
        assert window.params == ()

        window.start = -1
        assert str(window) == 'PARTITION BY "c" RANGE BETWEEN 1 PRECEDING AND CURRENT ROW'
        assert window.params == ()

        window.start = 0
        window.end = 1
        assert str(window) == 'PARTITION BY "c" RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING'
        assert window.params == ()

        window.start = 1
        window.end = None
        assert str(window) == 'PARTITION BY "c" RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING'
        assert window.params == ()

    def test_window_rows(self):
        t = Table('t')
        window = Window([t.c], frame='ROWS')

        assert str(window) == 'PARTITION BY "c" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'
        assert window.params == ()
