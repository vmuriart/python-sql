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

from sql import Case, Coalesce, NullIf, Greatest, Least


def test_case(table):
    case = Case((table.c1, 'foo'),
                (table.c2, 'bar'),
                else_=table.c3)
    assert str(case) == ('CASE '
                         'WHEN "c1" THEN %s '
                         'WHEN "c2" THEN %s '
                         'ELSE "c3" END')
    assert case.params == ('foo', 'bar')


def test_case_no_expression(table):
    case = Case((True, table.c1),
                (table.c2, False),
                else_=False)
    assert str(case) == ('CASE '
                         'WHEN %s THEN "c1" '
                         'WHEN "c2" THEN %s '
                         'ELSE %s END')
    assert case.params == (True, False, False)


def test_coalesce(table):
    coalesce = Coalesce(table.c1, table.c2, 'foo')
    assert str(coalesce) == 'COALESCE("c1", "c2", %s)'
    assert coalesce.params == ('foo',)


def test_nullif(table):
    nullif = NullIf(table.c1, 'foo')
    assert str(nullif) == 'NULLIF("c1", %s)'
    assert nullif.params == ('foo',)


def test_greatest(table):
    greatest = Greatest(table.c1, table.c2, 'foo')
    assert str(greatest) == 'GREATEST("c1", "c2", %s)'
    assert greatest.params == ('foo',)


def test_least(table):
    least = Least(table.c1, table.c2, 'foo')
    assert str(least) == 'LEAST("c1", "c2", %s)'
    assert least.params == ('foo',)
