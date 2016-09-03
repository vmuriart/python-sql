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
from array import array

from sql import Table, Literal, Null, Flavor
from sql.operators import (
    And, Or, Not, Neg, Pos, Less, Greater, LessEqual, GreaterEqual, Equal,
    NotEqual, Sub, Mul, Div, Mod, Pow, Abs, LShift, RShift, Like, NotLike,
    ILike, NotILike, In, NotIn, Exists)


class TestOperators(unittest.TestCase):
    table = Table('t')

    def test_and(self):
        for and_ in [And((self.table.c1, self.table.c2)),
                     self.table.c1 & self.table.c2]:
            assert str(and_) == '("c1" AND "c2")'
            assert and_.params == ()

        and_ = And((Literal(True), self.table.c2))
        assert str(and_) == '(%s AND "c2")'
        assert and_.params == (True,)

    def test_operator_operators(self):
        and_ = And((Literal(True), self.table.c1))
        and2 = and_ & And((Literal(True), self.table.c2))
        assert str(and2) == '((%s AND "c1") AND %s AND "c2")'
        assert and2.params == (True, True)

        and3 = and_ & Literal(True)
        assert str(and3) == '((%s AND "c1") AND %s)'
        assert and3.params == (True, True)

        or_ = Or((Literal(True), self.table.c1))
        or2 = or_ | Or((Literal(True), self.table.c2))
        assert str(or2) == '((%s OR "c1") OR %s OR "c2")'
        assert or2.params == (True, True)

        or3 = or_ | Literal(True)
        assert str(or3) == '((%s OR "c1") OR %s)'
        assert or3.params == (True, True)

    def test_operator_compat_column(self):
        and_ = And((self.table.c1, self.table.c2))
        assert and_.table == ''
        assert and_.name == ''

    def test_or(self):
        for or_ in [Or((self.table.c1, self.table.c2)),
                    self.table.c1 | self.table.c2]:
            assert str(or_) == '("c1" OR "c2")'
            assert or_.params == ()

    def test_not(self):
        for not_ in [Not(self.table.c), ~self.table.c]:
            assert str(not_) == '(NOT "c")'
            assert not_.params == ()

        not_ = Not(Literal(False))
        assert str(not_) == '(NOT %s)'
        assert not_.params == (False,)

    def test_neg(self):
        for neg in [Neg(self.table.c1), -self.table.c1]:
            assert str(neg) == '(- "c1")'
            assert neg.params == ()

    def test_pos(self):
        for pos in [Pos(self.table.c1), +self.table.c1]:
            assert str(pos) == '(+ "c1")'
            assert pos.params == ()

    def test_less(self):
        for less in [Less(self.table.c1, self.table.c2),
                     self.table.c1 < self.table.c2,
                     ~GreaterEqual(self.table.c1, self.table.c2)]:
            assert str(less) == '("c1" < "c2")'
            assert less.params == ()

        less = Less(Literal(0), self.table.c2)
        assert str(less) == '(%s < "c2")'
        assert less.params == (0,)

    def test_greater(self):
        for greater in [Greater(self.table.c1, self.table.c2),
                        self.table.c1 > self.table.c2,
                        ~LessEqual(self.table.c1, self.table.c2)]:
            assert str(greater) == '("c1" > "c2")'
            assert greater.params == ()

    def test_less_equal(self):
        for less in [LessEqual(self.table.c1, self.table.c2),
                     self.table.c1 <= self.table.c2,
                     ~Greater(self.table.c1, self.table.c2)]:
            assert str(less) == '("c1" <= "c2")'
            assert less.params == ()

    def test_greater_equal(self):
        for greater in [GreaterEqual(self.table.c1, self.table.c2),
                        self.table.c1 >= self.table.c2,
                        ~Less(self.table.c1, self.table.c2)]:
            assert str(greater) == '("c1" >= "c2")'
            assert greater.params == ()

    def test_equal(self):
        for equal in [Equal(self.table.c1, self.table.c2),
                      self.table.c1 == self.table.c2,
                      ~NotEqual(self.table.c1, self.table.c2)]:
            assert str(equal) == '("c1" = "c2")'
            assert equal.params == ()

        equal = Equal(Literal('foo'), Literal('bar'))
        assert str(equal) == '(%s = %s)'
        assert equal.params == ('foo', 'bar')

        equal = Equal(self.table.c1, Null)
        assert str(equal) == '("c1" IS NULL)'
        assert equal.params == ()

        equal = Equal(Literal('test'), Null)
        assert str(equal) == '(%s IS NULL)'
        assert equal.params == ('test',)

        equal = Equal(Null, self.table.c1)
        assert str(equal) == '("c1" IS NULL)'
        assert equal.params == ()

        equal = Equal(Null, Literal('test'))
        assert str(equal) == '(%s IS NULL)'
        assert equal.params == ('test',)

    def test_not_equal(self):
        for equal in [NotEqual(self.table.c1, self.table.c2),
                      self.table.c1 != self.table.c2,
                      ~Equal(self.table.c1, self.table.c2)]:
            assert str(equal) == '("c1" != "c2")'
            assert equal.params == ()

        equal = NotEqual(self.table.c1, Null)
        assert str(equal) == '("c1" IS NOT NULL)'
        assert equal.params == ()

        equal = NotEqual(Null, self.table.c1)
        assert str(equal) == '("c1" IS NOT NULL)'
        assert equal.params == ()

    def test_sub(self):
        for sub in [Sub(self.table.c1, self.table.c2),
                    self.table.c1 - self.table.c2]:
            assert str(sub) == '("c1" - "c2")'
            assert sub.params == ()

    def test_mul(self):
        for mul in [Mul(self.table.c1, self.table.c2),
                    self.table.c1 * self.table.c2]:
            assert str(mul) == '("c1" * "c2")'
            assert mul.params == ()

    def test_div(self):
        for div in [Div(self.table.c1, self.table.c2),
                    self.table.c1 / self.table.c2]:
            assert str(div) == '("c1" / "c2")'
            assert div.params == ()

    def test_mod(self):
        for mod in [Mod(self.table.c1, self.table.c2),
                    self.table.c1 % self.table.c2]:
            assert str(mod) == '("c1" %% "c2")'
            assert mod.params == ()

    def test_mod_paramstyle(self):
        flavor = Flavor(paramstyle='format')
        Flavor.set(flavor)
        try:
            mod = Mod(self.table.c1, self.table.c2)
            assert str(mod) == '("c1" %% "c2")'
            assert mod.params == ()
        finally:
            Flavor.set(Flavor())

        flavor = Flavor(paramstyle='qmark')
        Flavor.set(flavor)
        try:
            mod = Mod(self.table.c1, self.table.c2)
            assert str(mod) == '("c1" % "c2")'
            assert mod.params == ()
        finally:
            Flavor.set(Flavor())

    def test_pow(self):
        for pow_ in [Pow(self.table.c1, self.table.c2),
                     self.table.c1 ** self.table.c2]:
            assert str(pow_) == '("c1" ^ "c2")'
            assert pow_.params == ()

    def test_abs(self):
        for abs_ in [Abs(self.table.c1), abs(self.table.c1)]:
            assert str(abs_) == '(@ "c1")'
            assert abs_.params == ()

    def test_lshift(self):
        for lshift in [LShift(self.table.c1, 2),
                       self.table.c1 << 2]:
            assert str(lshift) == '("c1" << %s)'
            assert lshift.params == (2,)

    def test_rshift(self):
        for rshift in [RShift(self.table.c1, 2),
                       self.table.c1 >> 2]:
            assert str(rshift) == '("c1" >> %s)'
            assert rshift.params == (2,)

    def test_like(self):
        for like in [Like(self.table.c1, 'foo'),
                     self.table.c1.like('foo'),
                     ~NotLike(self.table.c1, 'foo'),
                     ~~Like(self.table.c1, 'foo')]:
            assert str(like) == '("c1" LIKE %s)'
            assert like.params == ('foo',)

    def test_ilike(self):
        flavor = Flavor(ilike=True)
        Flavor.set(flavor)
        try:
            for like in [ILike(self.table.c1, 'foo'),
                         self.table.c1.ilike('foo'),
                         ~NotILike(self.table.c1, 'foo')]:
                assert str(like) == '("c1" ILIKE %s)'
                assert like.params == ('foo',)
        finally:
            Flavor.set(Flavor())

        flavor = Flavor(ilike=False)
        Flavor.set(flavor)
        try:
            like = ILike(self.table.c1, 'foo')
            assert str(like) == '(UPPER("c1") LIKE UPPER(%s))'
            assert like.params == ('foo',)
        finally:
            Flavor.set(Flavor())

    def test_not_ilike(self):
        flavor = Flavor(ilike=True)
        Flavor.set(flavor)
        try:
            for like in [NotILike(self.table.c1, 'foo'),
                         ~self.table.c1.ilike('foo')]:
                assert str(like) == '("c1" NOT ILIKE %s)'
                assert like.params == ('foo',)
        finally:
            Flavor.set(Flavor())

        flavor = Flavor(ilike=False)
        Flavor.set(flavor)
        try:
            like = NotILike(self.table.c1, 'foo')
            assert str(like) == '(UPPER("c1") NOT LIKE UPPER(%s))'
            assert like.params == ('foo',)
        finally:
            Flavor.set(Flavor())

    def test_in(self):
        for in_ in [In(self.table.c1, [self.table.c2, 1, Null]),
                    ~NotIn(self.table.c1, [self.table.c2, 1, Null]),
                    ~~In(self.table.c1, [self.table.c2, 1, Null])]:
            assert str(in_) == '("c1" IN ("c2", %s, %s))'
            assert in_.params == (1, None)

        t2 = Table('t2')
        in_ = In(self.table.c1, t2.select(t2.c2))
        assert str(in_) == '("c1" IN (SELECT "a"."c2" FROM "t2" AS "a"))'
        assert in_.params == ()

        in_ = In(self.table.c1, t2.select(t2.c2) | t2.select(t2.c3))
        assert str(in_) == ('("c1" IN '
                            '(SELECT "a"."c2" FROM "t2" AS "a" UNION '
                            'SELECT "a"."c3" FROM "t2" AS "a"))')
        assert in_.params == ()

        in_ = In(self.table.c1, array('l', range(10)))
        assert str(in_) == '("c1" IN (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s))'
        assert in_.params == tuple(range(10))

    def test_exists(self):
        exists = Exists(self.table.select(self.table.c1,
                                          where=self.table.c1 == 1))
        assert str(exists) == ('(EXISTS (SELECT "a"."c1" '
                               'FROM "t" AS "a" '
                               'WHERE ("a"."c1" = %s)))')
        assert exists.params == (1,)