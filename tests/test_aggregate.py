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

from sql import Table, Window, AliasManager
from sql.aggregate import Avg


class TestAggregate(unittest.TestCase):
    table = Table('t')

    def test_avg(self):
        avg = Avg(self.table.c)
        assert str(avg) == 'AVG("c")'

        avg = Avg(self.table.a + self.table.b)
        assert str(avg) == 'AVG(("a" + "b"))'

    def test_within(self):
        avg = Avg(self.table.a, within=self.table.b)
        assert str(avg) == 'AVG("a") WITHIN GROUP (ORDER BY "b")'
        assert avg.params == ()

    def test_filter(self):
        avg = Avg(self.table.a, filter_=self.table.a > 0)
        assert str(avg) == 'AVG("a") FILTER (WHERE ("a" > %s))'
        assert avg.params == (0,)

    def test_window(self):
        avg = Avg(self.table.c, window=Window([]))
        with AliasManager():
            assert str(avg) == 'AVG("a"."c") OVER "b"'
        assert avg.params == ()

    def test_distinct(self):
        avg = Avg(self.table.c, distinct=True)
        assert str(avg) == 'AVG(DISTINCT "c")'
        assert avg.params == ()