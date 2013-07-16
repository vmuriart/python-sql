#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Join, Table, AliasManager
from sql.functions import Now


class TestJoin(unittest.TestCase):
    def test_join(self):
        t1 = Table('t1')
        t2 = Table('t2')
        join = Join(t1, t2)
        with AliasManager():
            self.assertEqual(str(join), '"t1" AS "a" INNER JOIN "t2" AS "b"')
            self.assertEqual(join.params, ())

        join.condition = t1.c == t2.c
        with AliasManager():
            self.assertEqual(str(join),
                '"t1" AS "a" INNER JOIN "t2" AS "b" ON ("a"."c" = "b"."c")')

    def test_join_subselect(self):
        t1 = Table('t1')
        t2 = Table('t2')
        select = t2.select()
        join = Join(t1, select)
        join.condition = t1.c == select.c
        with AliasManager():
            self.assertEqual(str(join),
                '"t1" AS "a" INNER JOIN (SELECT * FROM "t2" AS "c") AS "b" '
                'ON ("a"."c" = "b"."c")')
            self.assertEqual(join.params, ())

    def test_join_function(self):
        t1 = Table('t1')
        join = Join(t1, Now())
        with AliasManager():
            self.assertEqual(str(join), '"t1" AS "a" INNER JOIN NOW() AS "b"')
            self.assertEqual(join.params, ())