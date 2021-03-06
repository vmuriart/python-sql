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

from sql import With


def test_delete1(table):
    query = table.delete()
    assert str(query) == 'DELETE FROM "t"'
    assert query.params == ()


def test_delete2(table):
    query = table.delete(where=(table.c == 'foo'))
    assert str(query) == 'DELETE FROM "t" WHERE ("c" = %s)'
    assert query.params == ('foo',)


def test_delete3(t1, t2):
    query = t1.delete(where=(t1.c.in_(t2.select(t2.c))))
    assert str(query) == ('DELETE FROM "t1" WHERE '
                          '("c" IN (SELECT "a"."c" FROM "t2" AS "a"))')
    assert query.params == ()


def test_delete_returning(table):
    query = table.delete(returning=[table.c])
    assert str(query) == 'DELETE FROM "t" RETURNING "c"'
    assert query.params == ()


def test_with(table, t1):
    w = With(query=t1.select(t1.c1))

    query = table.delete(with_=[w],
                         where=table.c2.in_(w.select(w.c3)))
    assert str(query) == ('WITH "a" AS '
                          '(SELECT "b"."c1" FROM "t1" AS "b") '
                          'DELETE FROM "t" WHERE '
                          '("c2" IN (SELECT "a"."c3" FROM "a" AS "a"))')
    assert query.params == ()
