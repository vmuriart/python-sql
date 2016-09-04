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

from copy import deepcopy

from sql import Table, Join, Union, Literal, Flavor, For, With, Window, Select
from sql.aggregate import Min
from sql.functions import Now, Function, Rank, DatePart


def test_select1(table):
    query = table.select()
    assert str(query) == 'SELECT * FROM "t" AS "a"'
    assert query.params == ()


def test_select2(table):
    query = table.select(table.c)
    assert str(query) == 'SELECT "a"."c" FROM "t" AS "a"'
    assert query.params == ()

    query.columns += (table.c2,)
    assert str(query) == 'SELECT "a"."c", "a"."c2" FROM "t" AS "a"'


def test_select3(table):
    query = table.select(where=(table.c == 'foo'))
    assert str(query) == 'SELECT * FROM "t" AS "a" WHERE ("a"."c" = %s)'
    assert query.params == ('foo',)


def test_select_from_list(table):
    t2 = Table('t2')
    t3 = Table('t3')
    query = (table + t2 + t3).select(table.c, getattr(t2, '*'))
    assert str(query) == ('SELECT "a"."c", "b".* '
                          'FROM "t" AS "a", "t2" AS "b", "t3" AS "c"')
    assert query.params == ()


def test_select_union(table):
    query1 = table.select()
    query2 = Table('t2').select()
    union = query1 | query2
    assert str(union) == ('SELECT * FROM "t" AS "a" UNION '
                          'SELECT * FROM "t2" AS "b"')
    union.all_ = True
    assert str(union) == ('SELECT * FROM "t" AS "a" UNION ALL '
                          'SELECT * FROM "t2" AS "b"')
    assert str(union.select()) == ('SELECT * FROM '
                                   '(SELECT * FROM "t" AS "b" UNION ALL '
                                   'SELECT * FROM "t2" AS "c") AS "a"')
    query1.where = table.c == 'foo'
    assert str(union) == ('SELECT * FROM "t" AS "a" '
                          'WHERE ("a"."c" = %s) UNION ALL '
                          'SELECT * FROM "t2" AS "b"')
    assert union.params == ('foo',)

    union = Union(query1)
    assert str(union) == str(query1)
    assert union.params == query1.params


def test_select_union_order(table):
    query1 = table.select()
    query2 = Table('t2').select()
    union = query1 | query2
    union.order_by = Literal(1)
    assert str(union) == ('SELECT * FROM "t" AS "a" UNION '
                          'SELECT * FROM "t2" AS "b" ORDER BY %s')
    assert union.params == (1,)


def test_select_intersect(table):
    query1 = table.select()
    query2 = Table('t2').select()
    intersect = query1 & query2
    assert str(intersect) == ('SELECT * FROM "t" AS "a" INTERSECT '
                              'SELECT * FROM "t2" AS "b"')


def test_select_except(table):
    query1 = table.select()
    query2 = Table('t2').select()
    except_ = query1 - query2
    assert str(except_) == ('SELECT * FROM "t" AS "a" EXCEPT '
                            'SELECT * FROM "t2" AS "b"')


def test_select_join():
    t1 = Table('t1')
    t2 = Table('t2')
    join = Join(t1, t2)

    assert str(join.select()) == ('SELECT * FROM "t1" AS "a" '
                                  'INNER JOIN "t2" AS "b"')
    assert str(join.select(getattr(t1, '*'))) == ('SELECT "a".* '
                                                  'FROM "t1" AS "a" '
                                                  'INNER JOIN "t2" AS "b"')


def test_select_subselect():
    t1 = Table('t1')
    select = t1.select()
    assert str(select.select()) == ('SELECT * FROM '
                                    '(SELECT * FROM "t1" AS "b") AS "a"')
    assert select.params == ()


def test_select_function():
    query = Now().select()
    assert str(query) == 'SELECT * FROM NOW() AS "a"'
    assert query.params == ()


def test_select_function_columns_definitions():
    class Crosstab(Function):
        _function = 'CROSSTAB'

    query = Crosstab('query1', 'query2', columns_definitions=[
        ('c1', 'INT'), ('c2', 'CHAR'), ('c3', 'BOOL')]).select()
    assert str(query) == ('SELECT * FROM CROSSTAB(%s, %s) AS "a" '
                          '("c1" INT, "c2" CHAR, "c3" BOOL)')
    assert query.params == ('query1', 'query2')


def test_select_group_by(table):
    column = table.c
    query = table.select(column, group_by=column)
    assert str(query) == 'SELECT "a"."c" FROM "t" AS "a" GROUP BY "a"."c"'
    assert query.params == ()

    output = column.as_('c1')
    query = table.select(output, group_by=output)
    assert str(query) == ('SELECT "a"."c" AS "c1" '
                          'FROM "t" AS "a" GROUP BY "c1"')
    assert query.params == ()

    query = table.select(Literal('foo'), group_by=Literal('foo'))
    assert str(query) == 'SELECT %s FROM "t" AS "a" GROUP BY %s'
    assert query.params == ('foo', 'foo')


def test_select_having(table):
    col1 = table.col1
    col2 = table.col2
    query = table.select(col1, Min(col2), having=(Min(col2) > 3))
    assert str(query) == ('SELECT "a"."col1", MIN("a"."col2") '
                          'FROM "t" AS "a" HAVING (MIN("a"."col2") > %s)')
    assert query.params == (3,)


def test_select_order(table):
    c = table.c
    query = table.select(c, order_by=Literal(1))
    assert str(query) == 'SELECT "a"."c" FROM "t" AS "a" ORDER BY %s'
    assert query.params == (1,)


def test_select_limit_offset(table):
    try:
        Flavor.set(Flavor(limitstyle='limit'))
        query = table.select(limit=50, offset=10)
        assert str(query) == 'SELECT * FROM "t" AS "a" LIMIT 50 OFFSET 10'
        assert query.params == ()

        query.limit = None
        assert str(query) == 'SELECT * FROM "t" AS "a" OFFSET 10'
        assert query.params == ()

        query.offset = 0
        assert str(query) == 'SELECT * FROM "t" AS "a"'
        assert query.params == ()

        Flavor.set(Flavor(limitstyle='limit', max_limit=-1))

        query.offset = None
        assert str(query) == 'SELECT * FROM "t" AS "a"'
        assert query.params == ()

        query.offset = 0
        assert str(query) == 'SELECT * FROM "t" AS "a"'
        assert query.params == ()

        query.offset = 10
        assert str(query) == 'SELECT * FROM "t" AS "a" LIMIT -1 OFFSET 10'
        assert query.params == ()
    finally:
        Flavor.set(Flavor())


def test_select_offset_fetch(table):
    try:
        Flavor.set(Flavor(limitstyle='fetch'))
        query = table.select(limit=50, offset=10)
        assert str(query) == ('SELECT * FROM "t" AS "a" OFFSET (10) '
                              'ROWS FETCH FIRST (50) ROWS ONLY')
        assert query.params == ()

        query.limit = None
        assert str(query) == 'SELECT * FROM "t" AS "a" OFFSET (10) ROWS'
        assert query.params == ()

        query.offset = 0
        assert str(query) == 'SELECT * FROM "t" AS "a"'
        assert query.params == ()
    finally:
        Flavor.set(Flavor())


def test_select_rownum(table):
    try:
        Flavor.set(Flavor(limitstyle='rownum'))
        query = table.select(limit=50, offset=10)
        assert str(query) == ('SELECT "a".* FROM '
                              '(SELECT "b".*, ROWNUM AS "rnum" FROM '
                              '(SELECT * FROM "t" AS "c") AS "b" '
                              'WHERE (ROWNUM <= %s)) AS "a" '
                              'WHERE ("rnum" > %s)')
        assert query.params == (60, 10)

        query = table.select(table.c1.as_('col1'), table.c2.as_('col2'),
                             limit=50, offset=10)
        assert str(query) == ('SELECT "a"."col1", "a"."col2" FROM '
                              '(SELECT "b"."col1", "b"."col2", '
                              'ROWNUM AS "rnum" FROM '
                              '(SELECT "c"."c1" AS "col1", '
                              '"c"."c2" AS "col2" '
                              'FROM "t" AS "c") AS "b" '
                              'WHERE (ROWNUM <= %s)) AS "a" '
                              'WHERE ("rnum" > %s)')
        assert query.params == (60, 10)

        subquery = query.select(query.col1, query.col2)
        assert str(subquery) == ('SELECT "a"."col1", "a"."col2" FROM '
                                 '(SELECT "b"."col1", "b"."col2" FROM '
                                 '(SELECT "a"."col1", "a"."col2", '
                                 'ROWNUM AS "rnum" FROM '
                                 '(SELECT "c"."c1" AS "col1", '
                                 '"c"."c2" AS "col2" '
                                 'FROM "t" AS "c") AS "a" '
                                 'WHERE (ROWNUM <= %s)) AS "b" '
                                 'WHERE ("rnum" > %s)) AS "a"')
        # XXX alias of query is reused but not a problem
        # as it is hidden in subquery
        assert query.params == (60, 10)

        query = table.select(limit=50, offset=10, order_by=[table.c])
        assert str(query) == ('SELECT "a".* FROM '
                              '(SELECT "b".*, ROWNUM AS "rnum" FROM '
                              '(SELECT * FROM "t" AS "c" '
                              'ORDER BY "c"."c") AS "b" '
                              'WHERE (ROWNUM <= %s)) AS "a" '
                              'WHERE ("rnum" > %s)')
        assert query.params == (60, 10)

        query = table.select(limit=50)
        assert str(query) == ('SELECT "a".* FROM '
                              '(SELECT * FROM "t" AS "b") AS "a" '
                              'WHERE (ROWNUM <= %s)')
        assert query.params == (50,)

        query = table.select(offset=10)
        assert str(query) == ('SELECT "a".* FROM '
                              '(SELECT "b".*, ROWNUM AS "rnum" FROM '
                              '(SELECT * FROM "t" AS "c") AS "b") AS "a" '
                              'WHERE ("rnum" > %s)')
        assert query.params == (10,)

        query = table.select(table.c.as_('col'), where=table.c >= 20,
                             limit=50, offset=10)
        assert str(query) == ('SELECT "a"."col" FROM '
                              '(SELECT "b"."col", ROWNUM AS "rnum" FROM '
                              '(SELECT "c"."c" AS "col" FROM "t" AS "c" '
                              'WHERE ("c"."c" >= %s)) AS "b" '
                              'WHERE (ROWNUM <= %s)) AS "a" '
                              'WHERE ("rnum" > %s)')
        assert query.params == (20, 60, 10)
    finally:
        Flavor.set(Flavor())


def test_select_for(table):
    c = table.c
    query = table.select(c, for_=For('UPDATE'))
    assert str(query) == 'SELECT "a"."c" FROM "t" AS "a" FOR UPDATE'
    assert query.params == ()


def test_copy(table):
    query = table.select()
    copy_query = deepcopy(query)
    assert query != copy_query
    assert str(copy_query) == 'SELECT * FROM "t" AS "a"'
    assert copy_query.params == ()


def test_with(table):
    w = With(query=table.select(table.c1))

    query = w.select(with_=[w])
    assert str(query) == ('WITH "a" AS (SELECT "b"."c1" FROM "t" AS "b") '
                          'SELECT * FROM "a" AS "a"')
    assert query.params == ()


def test_window(table):
    query = table.select(Min(table.c1, window=Window([table.c2])))

    assert str(query) == ('SELECT MIN("a"."c1") OVER "b" '
                          'FROM "t" AS "a" WINDOW "b" '
                          'AS (PARTITION BY "a"."c2")')
    assert query.params == ()

    query = table.select(Rank(window=Window([])))
    assert str(query) == ('SELECT RANK() OVER "b" '
                          'FROM "t" AS "a" WINDOW "b" AS ()')
    assert query.params == ()

    window = Window([table.c1])
    query = table.select(Rank(filter_=table.c1 > 0, window=window),
                         Min(table.c1, window=window))
    assert str(query) == ('SELECT RANK() FILTER '
                          '(WHERE ("a"."c1" > %s)) '
                          'OVER "b", MIN("a"."c1") OVER "b" '
                          'FROM "t" AS "a" WINDOW "b" '
                          'AS (PARTITION BY "a"."c1")')
    assert query.params == (0,)

    window = Window([DatePart('year', table.date_col)])
    query = table.select(Min(table.c1, window=window))
    assert str(query) == ('SELECT MIN("a"."c1") OVER "b" '
                          'FROM "t" AS "a" WINDOW "b" AS '
                          '(PARTITION BY DATE_PART(%s, "a"."date_col"))')
    assert query.params == ('year',)


def test_order_params(table):
    with_ = With(query=table.select(table.c, where=(table.c > 1)))
    w = Window([Literal(8)])
    query = Select([Literal(2), Min(table.c, window=w)],
                   from_=table.select(where=table.c > 3),
                   with_=with_,
                   where=table.c > 4,
                   group_by=[Literal(5)],
                   order_by=[Literal(6)],
                   having=Literal(7))
    assert query.params == (1, 2, 3, 4, 5, 6, 7, 8)


def test_no_as(table):
    query = table.select(table.c)
    try:
        Flavor.set(Flavor(no_as=True))
        assert str(query) == 'SELECT "a"."c" FROM "t" "a"'
        assert query.params == ()
    finally:
        Flavor.set(Flavor())
