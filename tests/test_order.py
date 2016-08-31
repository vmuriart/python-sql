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

from sql import Asc, Desc, NullsFirst, NullsLast, Column, Table, Literal
from sql import Flavor


class TestOrder(unittest.TestCase):
    column = Column(Table('t'), 'c')

    def test_asc(self):
        assert str(Asc(self.column)) == '"c" ASC'

    def test_desc(self):
        assert str(Desc(self.column)) == '"c" DESC'

    def test_nulls_first(self):
        assert str(NullsFirst(self.column)) == '"c" NULLS FIRST'
        assert str(NullsFirst(Asc(self.column))) == '"c" ASC NULLS FIRST'

    def test_nulls_last(self):
        assert str(NullsLast(self.column)) == '"c" NULLS LAST'
        assert str(NullsLast(Asc(self.column))) == '"c" ASC NULLS LAST'

    def test_no_null_ordering(self):
        try:
            Flavor.set(Flavor(null_ordering=False))

            exp = NullsFirst(self.column)
            assert str(exp) == ('CASE '
                                'WHEN ("c" IS NULL) THEN %s '
                                'ELSE %s END ASC, "c"')
            assert exp.params == (0, 1)

            exp = NullsFirst(Desc(self.column))
            assert str(exp) == ('CASE '
                                'WHEN ("c" IS NULL) THEN %s '
                                'ELSE %s END ASC, "c" DESC')
            assert exp.params == (0, 1)

            exp = NullsLast(Literal(2))
            assert str(exp) == ('CASE '
                                'WHEN (%s IS NULL) THEN %s '
                                'ELSE %s END ASC, %s')
            assert exp.params == (2, 1, 0, 2)
        finally:
            Flavor.set(Flavor())

    def test_order_query(self):
        table = Table('t')
        column = Column(table, 'c')
        query = table.select(column)
        assert str(Asc(query)) == '(SELECT "a"."c" FROM "t" AS "a") ASC'
        assert str(Desc(query)) == '(SELECT "a"."c" FROM "t" AS "a") DESC'
