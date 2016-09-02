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

from __future__ import unicode_literals

import string
from threading import local, currentThread
from collections import defaultdict

from sql._compat import text_type, map, zip

__version__ = '0.9.0-dev0'
__all__ = ('Flavor', 'Table', 'Values', 'Literal', 'Column', 'Join',
           'Asc', 'Desc', 'NullsFirst', 'NullsLast', 'format2numeric')


def alias(i, letters=string.ascii_lowercase):
    """Generate a unique alias based on integer

    >>> [alias(n) for n in range(6)]
    ['a', 'b', 'c', 'd', 'e', 'f']
    >>> [alias(n) for n in range(26, 30)]
    ['ba', 'bb', 'bc', 'bd']
    >>> [alias(26**n) for n in range(5)]
    ['b', 'ba', 'baa', 'baaa', 'baaaa']
    """
    s = ''
    length = len(letters)
    while True:
        r = i % length
        s = letters[r] + s
        i //= length
        if i == 0:
            break
    return s


class Flavor(object):
    """Contains the flavor of SQL

    Contains:
        limitstyle - state the type of pagination
        max_limit - limit to use if there is no limit but an offset
        paramstyle - state the type of parameter marker formatting
        ilike - support ilike extension
        no_as - doesn't support AS keyword for column and table
        null_ordering - support NULL ordering
        function_mapping - dictionary with Function to replace
    """

    def __init__(self, limitstyle='limit', max_limit=None, paramstyle='format',
                 ilike=False, no_as=False, no_boolean=False,
                 null_ordering=True, function_mapping=None):
        self.limitstyle = limitstyle
        self.max_limit = max_limit
        self.paramstyle = paramstyle
        self.ilike = ilike
        self.no_as = no_as
        self.no_boolean = no_boolean
        self.null_ordering = null_ordering
        self.function_mapping = function_mapping or {}

    @property
    def param(self):
        if self.paramstyle == 'format':
            return '%s'
        elif self.paramstyle == 'qmark':
            return '?'

    @staticmethod
    def set(flavor):
        """Set this thread's flavor to flavor.
        """
        currentThread().__sql_flavor__ = flavor

    @staticmethod
    def get():
        """Return this thread's flavor.

        If this thread does not yet have a flavor, returns a new flavor and
        sets this thread's flavor.
        """
        try:
            return currentThread().__sql_flavor__
        except AttributeError:
            flavor = Flavor()
            currentThread().__sql_flavor__ = flavor
            return flavor


class AliasManager(object):
    """Context Manager for unique alias generation
    """
    __slots__ = ()

    local = local()
    local.alias = None
    local.nested = 0
    local.exclude = None

    def __init__(self, exclude=None):
        if exclude:
            if getattr(self.local, 'exclude', None) is None:
                self.local.exclude = []
            self.local.exclude.extend(exclude)

    @classmethod
    def __enter__(cls):
        if getattr(cls.local, 'alias', None) is None:
            cls.local.alias = defaultdict(cls.alias_factory)
            cls.local.nested = 0
        if getattr(cls.local, 'exclude', None) is None:
            cls.local.exclude = []
        cls.local.nested += 1

    @classmethod
    def __exit__(cls, type_, value, traceback):
        cls.local.nested -= 1
        if not cls.local.nested:
            cls.local.alias = None
            cls.local.exclude = None

    @classmethod
    def get(cls, from_):
        if getattr(cls.local, 'alias', None) is None:
            return ''
        if from_ in cls.local.exclude:
            return ''
        return cls.local.alias[id(from_)]

    @classmethod
    def set(cls, from_, alias_):
        cls.local.alias[id(from_)] = alias_

    @classmethod
    def alias_factory(cls):
        i = len(cls.local.alias)
        return alias(i)


def format2numeric(query, params):
    """Convert format paramstyle query to numeric paramstyle

    >>> format2numeric('SELECT * FROM table WHERE col = %s', ('foo',))
    ('SELECT * FROM table WHERE col = :0', ('foo',))
    >>> format2numeric('SELECT * FROM table WHERE col1 = %s AND col2 = %s',
    ...     ('foo', 'bar'))
    ('SELECT * FROM table WHERE col1 = :0 AND col2 = :1', ('foo', 'bar'))
    """
    return (query % tuple(':{0:d}'.format(i)
                          for i, _ in enumerate(params)), params)


class Query(object):
    __slots__ = ()

    @property
    def params(self):
        return ()

    def __iter__(self):
        yield text_type(self)
        yield self.params

    def __or__(self, other):
        return Union(self, other)

    def __and__(self, other):
        return Intersect(self, other)

    def __sub__(self, other):
        return Except(self, other)


class WithQuery(Query):
    __slots__ = ('_with',)

    def __init__(self, **kwargs):
        self._with = None
        self.with_ = kwargs.get('with_')
        super(Query, self).__init__()

    @property
    def with_(self):
        return self._with

    @with_.setter
    def with_(self, value):
        if isinstance(value, With):
            value = [value]
        self._with = value

    def _with_str(self):
        if not self.with_:
            return ''
        recursive = (' RECURSIVE' if any(w.recursive for w in self.with_)
                     else '')
        with_ = 'WITH{0} '.format(recursive) + ', '.join(
            w.statement() for w in self.with_) + ' '
        return with_

    def _with_params(self):
        if not self.with_:
            return ()
        params = []
        for w in self.with_:
            params.extend(w.statement_params())
        return tuple(params)


class FromItem(object):
    __slots__ = ()

    @property
    def alias(self):
        return AliasManager.get(self)

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError
        return Column(self, name)

    def __add__(self, other):
        return From((self, other))

    def select(self, *args, **kwargs):
        return From((self,)).select(*args, **kwargs)

    def join(self, right, type_='INNER', condition=None):
        return Join(self, right, type_=type_, condition=condition)

    def lateral(self):
        return Lateral(self)


class Lateral(FromItem):
    __slots__ = ('_from_item',)

    def __init__(self, from_item):
        self._from_item = from_item

    def __str__(self):
        template = '%s'
        if isinstance(self._from_item, Query):
            template = '(%s)'
        return 'LATERAL ' + template % self._from_item

    def __getattr__(self, name):
        return getattr(self._from_item, name)


class With(FromItem):
    __slots__ = ('columns', 'query', 'recursive')

    def __init__(self, *args, **kwargs):
        self.columns = args
        self.recursive = kwargs.get('recursive')
        self.query = kwargs.get('query')
        super(With, self).__init__()

    def statement(self):
        columns = ' ({0})'.format(
            ', '.join('"{0}"'.format(c) for c in self.columns)
        ) if self.columns else ''
        return '"{0}"{1} AS ({2})'.format(self.alias, columns, self.query)

    def statement_params(self):
        return self.query.params

    def __str__(self):
        return '"{0}"'.format(self.alias)

    @property
    def params(self):
        return tuple()


class SelectQuery(WithQuery):
    __slots__ = ('_order_by', '_limit', '_offset')

    def __init__(self, **kwargs):
        self._order_by = None
        self._limit = None
        self._offset = None
        self.order_by = kwargs.get('order_by')
        self.limit = kwargs.get('limit')
        self.offset = kwargs.get('offset')
        super(SelectQuery, self).__init__(**kwargs)

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        if isinstance(value, Expression):
            value = [value]
        self._order_by = value

    @property
    def _order_by_str(self):
        order_by = ''
        if self.order_by:
            order_by = ' ORDER BY ' + ', '.join(map(text_type, self.order_by))
        return order_by

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, value):
        self._limit = value

    @property
    def offset(self):
        return self._offset

    @offset.setter
    def offset(self, value):
        self._offset = value

    @property
    def _limit_offset_str(self):
        if Flavor.get().limitstyle == 'limit':
            offset = ''
            if self.offset:
                offset = ' OFFSET {0}'.format(self.offset)
            limit = ''
            if self.limit is not None:
                limit = ' LIMIT {0}'.format(self.limit)
            elif self.offset:
                max_limit = Flavor.get().max_limit
                if max_limit:
                    limit = ' LIMIT {0}'.format(max_limit)
            return limit + offset
        else:
            offset = ''
            if self.offset:
                offset = ' OFFSET ({0}) ROWS'.format(self.offset)
            fetch = ''
            if self.limit is not None:
                fetch = ' FETCH FIRST ({0}) ROWS ONLY'.format(self.limit)
            return offset + fetch


class Select(FromItem, SelectQuery):
    __slots__ = ('_columns', 'where', '_group_by', 'having', '_for_', 'from_')

    def __init__(self, columns, from_=None, where=None, group_by=None,
                 having=None, for_=None, **kwargs):
        self._columns = None
        self._group_by = None
        self._for_ = None
        super(Select, self).__init__(**kwargs)
        # TODO ALL|DISTINCT
        self.columns = columns
        self.from_ = from_
        self.where = where
        self.group_by = group_by
        self.having = having
        self.for_ = for_

    @property
    def columns(self):
        return self._columns

    @columns.setter
    def columns(self, value):
        self._columns = tuple(value)

    @property
    def group_by(self):
        return self._group_by

    @group_by.setter
    def group_by(self, value):
        if isinstance(value, Expression):
            value = [value]
        self._group_by = value

    @property
    def for_(self):
        return self._for_

    @for_.setter
    def for_(self, value):
        if isinstance(value, For):
            value = [value]
        self._for_ = value

    @staticmethod
    def _format_column(column):
        if isinstance(column, As):
            if Flavor.get().no_as:
                return '{0} {1}'.format(column.expression, column)
            else:
                return '{0} AS {1}'.format(column.expression, column)
        else:
            return text_type(column)

    def _window_functions(self):
        from sql.functions import WindowFunction
        from sql.aggregate import Aggregate
        windows = set()
        for column in self.columns:
            window_function = None
            if isinstance(column, (WindowFunction, Aggregate)):
                window_function = column
            elif isinstance(column, As) and isinstance(column.expression,
                                                       (WindowFunction,
                                                        Aggregate)):
                window_function = column.expression
            if window_function and window_function.window and (
                    window_function.window not in windows):
                windows.add(window_function.window)
                yield window_function

    def _rownum(self, func):
        aliases = [c.output_name if isinstance(c, As) else None
                   for c in self.columns]

        def columns(table):
            if aliases and all(aliases):
                return [Column(table, alias_) for alias_ in aliases]
            else:
                return [Column(table, '*')]

        limitselect = self.select(*columns(self))
        if self.limit is not None:
            max_row = self.limit
            if self.offset is not None:
                max_row += self.offset
            limitselect.where = _rownum <= max_row
        if self.offset is not None:
            rnum = _rownum.as_('rnum')
            limitselect.columns += (rnum,)
            offsetselect = limitselect.select(
                *columns(limitselect), where=rnum > self.offset)
            query = offsetselect
        else:
            query = limitselect

        self.limit, limit = None, self.limit
        self.offset, offset = None, self.offset
        query.for_, self.for_ = self.for_, None

        try:
            value = func(query)
        finally:
            self.limit = limit
            self.offset = offset
            self.for_ = query.for_
        return value

    def __str__(self):
        if (Flavor.get().limitstyle == 'rownum' and
                (self.limit is not None or self.offset is not None)):
            return self._rownum(text_type)

        with AliasManager():
            from_ = text_type(self.from_)
            if self.columns:
                columns = ', '.join(map(self._format_column, self.columns))
            else:
                columns = '*'
            where = ''
            if self.where:
                where = ' WHERE ' + text_type(self.where)
            group_by = ''
            if self.group_by:
                group_by = ' GROUP BY ' + ', '.join(
                    map(text_type, self.group_by))
            having = ''
            if self.having:
                having = ' HAVING ' + text_type(self.having)
            window = ''
            windows = [f.window for f in self._window_functions()]
            if windows:
                window = ' WINDOW ' + ', '.join(
                    '"{0}" AS ({1})'.format(w.alias, w) for w in windows)
            for_ = ''
            if self.for_ is not None:
                for_ = ' ' + ' '.join(map(text_type, self.for_))
            return (self._with_str() +
                    'SELECT {0} FROM {1}'.format(columns, from_) +
                    where + group_by + having + window + self._order_by_str +
                    self._limit_offset_str + for_)

    @property
    def params(self):
        if (Flavor.get().limitstyle == 'rownum' and
                (self.limit is not None or self.offset is not None)):
            return self._rownum(lambda q: q.params)
        p = []
        p.extend(self._with_params())
        for column in self.columns:
            if isinstance(column, As):
                p.extend(column.expression.params)
            p.extend(column.params)
        p.extend(self.from_.params)
        if self.where:
            p.extend(self.where.params)
        if self.group_by:
            for expression in self.group_by:
                p.extend(expression.params)
        if self.order_by:
            for expression in self.order_by:
                p.extend(expression.params)
        if self.having:
            p.extend(self.having.params)
        for window_function in self._window_functions():
            p.extend(window_function.window.params)
        return tuple(p)


class Insert(WithQuery):
    __slots__ = ('table', 'columns', '_values', 'returning')

    def __init__(self, table, columns=None, values=None, returning=None,
                 **kwargs):
        self._values = None
        self.values = values

        self.table = table
        self.columns = columns
        self.returning = returning
        super(Insert, self).__init__(**kwargs)

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        if isinstance(value, list):
            value = Values(value)
        self._values = value

    @staticmethod
    def _format(value, param=None):
        if param is None:
            param = Flavor.get().param
        if isinstance(value, Expression):
            return text_type(value)
        elif isinstance(value, Select):
            return '({0})'.format(value)
        else:
            return param

    def __str__(self):
        returning = values = columns = ''

        if self.columns:
            columns = ' (' + ', '.join(map(text_type, self.columns)) + ')'

        if isinstance(self.values, Query):
            values = ' {0}'.format(text_type(self.values))
            # TODO manage DEFAULT
        elif self.values is None:
            values = ' DEFAULT VALUES'

        if self.returning:
            returning = ' RETURNING ' + ', '.join(
                map(text_type, self.returning))
        with AliasManager():
            return (self._with_str() + 'INSERT INTO {0}'.format(self.table) +
                    columns + values + returning)

    @property
    def params(self):
        p = []
        p.extend(self._with_params())
        if isinstance(self.values, Query):
            p.extend(self.values.params)
        if self.returning:
            for exp in self.returning:
                p.extend(exp.params)
        return tuple(p)


class Update(Insert):
    __slots__ = ('where', '_values', 'from_')

    def __init__(self, table, columns, values, from_=None, where=None,
                 returning=None, **kwargs):
        super(Update, self).__init__(table, columns=columns, values=values,
                                     returning=returning, **kwargs)
        self.from_ = From(from_) if from_ else None
        self.where = where

    @property
    def values(self):
        return self._values

    @values.setter
    def values(self, value):
        if isinstance(value, Select):
            value = [value]
        self._values = value

    def __str__(self):
        # Get columns without alias
        columns = list(map(text_type, self.columns))

        with AliasManager():
            from_ = ''
            if self.from_:
                table = From([self.table])
                from_ = ' FROM {0}'.format(text_type(self.from_))
            else:
                table = self.table
                AliasManager.set(table, text_type(table)[1:-1])
            values = ', '.join('{0} = {1}'.format(c, self._format(v))
                               for c, v in zip(columns, self.values))
            where = ''
            if self.where:
                where = ' WHERE ' + text_type(self.where)
            returning = ''
            if self.returning:
                returning = ' RETURNING ' + ', '.join(
                    map(text_type, self.returning))
            return (self._with_str() + 'UPDATE {0} SET '.format(table) +
                    values + from_ + where + returning)

    @property
    def params(self):
        p = []
        p.extend(self._with_params())
        for value in self.values:
            if isinstance(value, (Expression, Select)):
                p.extend(value.params)
            else:
                p.append(value)
        if self.from_:
            p.extend(self.from_.params)
        if self.where:
            p.extend(self.where.params)
        if self.returning:
            for exp in self.returning:
                p.extend(exp.params)
        return tuple(p)


class Delete(WithQuery):
    __slots__ = ('table', 'where', 'returning', 'only')

    def __init__(self, table, only=False, where=None, returning=None,
                 **kwargs):
        # TODO using (not standard)
        self.table = table
        self.only = only
        self.where = where
        self.returning = returning
        super(Delete, self).__init__(**kwargs)

    def __str__(self):
        with AliasManager(exclude=[self.table]):
            only = ' ONLY' if self.only else ''
            where = ''
            if self.where:
                where = ' WHERE ' + text_type(self.where)
            returning = ''
            if self.returning:
                returning = ' RETURNING ' + ', '.join(
                    map(text_type, self.returning))
            return self._with_str() + 'DELETE FROM{0} {1}'.format(
                only, self.table) + where + returning

    @property
    def params(self):
        p = []
        p.extend(self._with_params())
        if self.where:
            p.extend(self.where.params)
        if self.returning:
            for exp in self.returning:
                p.extend(exp.params)
        return tuple(p)


class CombiningQuery(FromItem, SelectQuery):
    __slots__ = ('queries', 'all_')
    _operator = ''

    def __init__(self, *queries, **kwargs):
        self.queries = queries
        self.all_ = kwargs.get('all_')
        super(CombiningQuery, self).__init__(**kwargs)

    def __str__(self):
        with AliasManager():
            operator = ' {0} {1}'.format(
                self._operator, 'ALL ' if self.all_ else '')
            return (operator.join(map(text_type, self.queries)) +
                    self._order_by_str + self._limit_offset_str)

    @property
    def params(self):
        p = []
        for q in self.queries:
            p.extend(q.params)
        if self.order_by:
            for expression in self.order_by:
                p.extend(expression.params)
        return tuple(p)


class Union(CombiningQuery):
    __slots__ = ()
    _operator = 'UNION'


class Intersect(CombiningQuery):
    __slots__ = ()
    _operator = 'INTERSECT'


class Except(CombiningQuery):
    __slots__ = ()
    _operator = 'EXCEPT'


class Table(FromItem):
    __slots__ = ('_name', '_schema', '_database')

    def __init__(self, name, schema=None, database=None):
        super(Table, self).__init__()
        self._name = name
        self._schema = schema
        self._database = database

    def __str__(self):
        if self._database:
            return '"{0}"."{1}"."{2}"'.format(
                self._database, self._schema, self._name)
        elif self._schema:
            return '"{0}"."{1}"'.format(self._schema, self._name)
        else:
            return '"{0}"'.format(self._name)

    @property
    def params(self):
        return ()

    def insert(self, columns=None, values=None, returning=None, with_=None):
        return Insert(self, columns=columns, values=values,
                      returning=returning, with_=with_)

    def update(self, columns, values, from_=None, where=None, returning=None,
               with_=None):
        return Update(self, columns=columns, values=values, from_=from_,
                      where=where, returning=returning, with_=with_)

    def delete(self, only=False, using=None, where=None, returning=None,
               with_=None):
        return Delete(self, only=only, using=using, where=where,
                      returning=returning, with_=with_)


class Join(FromItem):
    __slots__ = ('left', 'right', 'condition', '_type_')

    def __init__(self, left, right, type_='INNER', condition=None):
        super(Join, self).__init__()
        self.left = left
        self.right = right
        self.condition = condition

        self._type_ = None
        self.type_ = type_

    @property
    def type_(self):
        return self._type_

    @type_.setter
    def type_(self, value):
        value = value.upper()
        self._type_ = value

    def __str__(self):
        join = '{0} {1} JOIN {2}'.format(
            From([self.left]), self.type_, From([self.right]))
        if self.condition:
            condition = ' ON {0}'.format(self.condition)
        else:
            condition = ''
        return join + condition

    @property
    def params(self):
        p = []
        for item in (self.left, self.right):
            if hasattr(item, 'params'):
                p.extend(item.params)
        if hasattr(self.condition, 'params'):
            p.extend(self.condition.params)
        return tuple(p)

    @property
    def alias(self):
        raise AttributeError

    def __getattr__(self, name):
        raise AttributeError

    def select(self, *args, **kwargs):
        return super(Join, self).select(*args, **kwargs)


class From(list):
    __slots__ = ()

    def select(self, *args, **kwargs):
        return Select(args, from_=self, **kwargs)

    def __str__(self):
        def format_(from_):
            template = '%s'
            if isinstance(from_, Query):
                template = '(%s)'
            alias_ = getattr(from_, 'alias', None)
            # TODO column_alias
            columns_definitions = getattr(from_, 'columns_definitions', None)
            if Flavor.get().no_as:
                alias_template = ' "%s"'
            else:
                alias_template = ' AS "%s"'
            # XXX find a better test for __getattr__ which returns Column
            if (alias_ and columns_definitions and
                    not isinstance(columns_definitions, Column)):
                return (template + alias_template + ' (%s)') % (
                    from_, alias_, columns_definitions)
            elif alias_:
                return (template + alias_template) % (from_, alias_)
            else:
                return template % from_

        return ', '.join(map(format_, self))

    @property
    def params(self):
        p = []
        for from_ in self:
            p.extend(from_.params)
        return tuple(p)

    def __add__(self, other):
        return From(super(From, self).__add__([other]))


class Values(list, Query, FromItem):
    __slots__ = ()

    # TODO order, fetch

    def __str__(self):
        param = Flavor.get().param

        def format_(value):
            if isinstance(value, Expression):
                return text_type(value)
            else:
                return param

        return 'VALUES ' + ', '.join(
            '({0})'.format(', '.join(map(format_, v))) for v in self)

    @property
    def params(self):
        p = []
        for values in self:
            for value in values:
                if isinstance(value, Expression):
                    p.extend(value.params)
                else:
                    p.append(value)
        return tuple(p)


class Expression(object):
    __slots__ = ()

    def __str__(self):
        raise NotImplementedError

    @property
    def params(self):
        raise NotImplementedError

    def __and__(self, other):
        from sql.operators import And
        return And((self, other))

    def __or__(self, other):
        from sql.operators import Or
        return Or((self, other))

    def __invert__(self):
        from sql.operators import Not
        return Not(self)

    def __add__(self, other):
        from sql.operators import Add
        return Add(self, other)

    def __sub__(self, other):
        from sql.operators import Sub
        return Sub(self, other)

    def __mul__(self, other):
        from sql.operators import Mul
        return Mul(self, other)

    def __div__(self, other):
        from sql.operators import Div
        return Div(self, other)

    __truediv__ = __div__

    def __floordiv__(self, other):
        from sql.functions import Div
        return Div(self, other)

    def __mod__(self, other):
        from sql.operators import Mod
        return Mod(self, other)

    def __pow__(self, other):
        from sql.operators import Pow
        return Pow(self, other)

    def __neg__(self):
        from sql.operators import Neg
        return Neg(self)

    def __pos__(self):
        from sql.operators import Pos
        return Pos(self)

    def __abs__(self):
        from sql.operators import Abs
        return Abs(self)

    def __lshift__(self, other):
        from sql.operators import LShift
        return LShift(self, other)

    def __rshift__(self, other):
        from sql.operators import RShift
        return RShift(self, other)

    def __lt__(self, other):
        from sql.operators import Less
        return Less(self, other)

    def __le__(self, other):
        from sql.operators import LessEqual
        return LessEqual(self, other)

    def __eq__(self, other):
        from sql.operators import Equal
        return Equal(self, other)

    # When overriding __eq__, __hash__ is implicitly set to None
    __hash__ = object.__hash__

    def __ne__(self, other):
        from sql.operators import NotEqual
        return NotEqual(self, other)

    def __gt__(self, other):
        from sql.operators import Greater
        return Greater(self, other)

    def __ge__(self, other):
        from sql.operators import GreaterEqual
        return GreaterEqual(self, other)

    def in_(self, values):
        from sql.operators import In
        return In(self, values)

    def like(self, test):
        from sql.operators import Like
        return Like(self, test)

    def ilike(self, test):
        from sql.operators import ILike
        return ILike(self, test)

    def as_(self, output_name):
        return As(self, output_name)

    def cast(self, typename):
        return Cast(self, typename)

    @property
    def asc(self):
        return Asc(self)

    @property
    def desc(self):
        return Desc(self)

    @property
    def nulls_first(self):
        return NullsFirst(self)

    @property
    def nulls_last(self):
        return NullsLast(self)


class Literal(Expression):
    __slots__ = ('value',)

    def __init__(self, value):
        super(Literal, self).__init__()
        self.value = value

    def __str__(self):
        flavor = Flavor.get()
        if flavor.no_boolean:
            if self.value is True:
                return '(1 = 1)'
            elif self.value is False:
                return '(1 != 1)'
        return flavor.param

    @property
    def params(self):
        if Flavor.get().no_boolean:
            if self.value is True or self.value is False:
                return ()
        return self.value,


class _Rownum(Expression):
    def __str__(self):
        return 'ROWNUM'

    @property
    def params(self):
        return ()


class Column(Expression):
    __slots__ = ('_from', 'name')

    def __init__(self, from_, name):
        super(Column, self).__init__()
        self._from = from_
        self.name = name

    @property
    def table(self):
        return self._from

    def __str__(self):
        if self.name == '*':
            t = '%s'
        else:
            t = '"%s"'
        alias_ = self._from.alias
        if alias_:
            t = '"%s".' + t
            return t % (alias_, self.name)
        else:
            return t % self.name

    @property
    def params(self):
        return ()


class As(Expression):
    __slots__ = ('expression', 'output_name')

    def __init__(self, expression, output_name):
        super(As, self).__init__()
        self.expression = expression
        self.output_name = output_name

    def __str__(self):
        return '"{0}"'.format(self.output_name)

    @property
    def params(self):
        return ()


class Cast(Expression):
    __slots__ = ('expression', 'typename')

    def __init__(self, expression, typename):
        super(Expression, self).__init__()
        self.expression = expression
        self.typename = typename

    def __str__(self):
        if isinstance(self.expression, Expression):
            value = self.expression
        else:
            value = Flavor.get().param
        return 'CAST({0} AS {1})'.format(value, self.typename)

    @property
    def params(self):
        if isinstance(self.expression, Expression):
            return self.expression.params
        else:
            return self.expression,


class Window(object):
    __slots__ = ('_order_by', 'partition', 'frame', 'start', 'end')

    def __init__(self, partition, order_by=None,
                 frame=None, start=None, end=0):
        super(Window, self).__init__()
        self._order_by = None
        self.partition = partition
        self.order_by = order_by
        self.frame = frame
        self.start = start
        self.end = end

    @property
    def order_by(self):
        return self._order_by

    @order_by.setter
    def order_by(self, value):
        if isinstance(value, Expression):
            value = [value]
        self._order_by = value

    @property
    def alias(self):
        return AliasManager.get(self)

    def __str__(self):
        partition = ''
        if self.partition:
            partition = 'PARTITION BY ' + ', '.join(
                map(text_type, self.partition))
        order_by = ''
        if self.order_by:
            order_by = ' ORDER BY ' + ', '.join(map(text_type, self.order_by))

        def format_(frame_, direction):
            if frame_ is None:
                return 'UNBOUNDED {0}'.format(direction)
            elif not frame_:
                return 'CURRENT ROW'
            elif frame_ < 0:
                return '{0} PRECEDING'.format(-frame_)
            elif frame_ > 0:
                return '{0} FOLLOWING'.format(frame_)

        frame = ''
        if self.frame:
            start = format_(self.start, 'PRECEDING')
            end = format_(self.end, 'FOLLOWING')
            frame = ' {0} BETWEEN {1} AND {2}'.format(
                self.frame, start, end)
        return partition + order_by + frame

    @property
    def params(self):
        p = []
        if self.partition:
            for expression in self.partition:
                p.extend(expression.params)
        if self.order_by:
            for expression in self.order_by:
                p.extend(expression.params)
        return tuple(p)


class Order(Expression):
    __slots__ = ('expression',)
    _sql = ''

    def __init__(self, expression):
        super(Order, self).__init__()
        self.expression = expression
        # TODO USING

    def __str__(self):
        if isinstance(self.expression, SelectQuery):
            return '({0}) {1}'.format(self.expression, self._sql)
        return '{0} {1}'.format(self.expression, self._sql)

    @property
    def params(self):
        return self.expression.params


class Asc(Order):
    __slots__ = ()
    _sql = 'ASC'


class Desc(Order):
    __slots__ = ()
    _sql = 'DESC'


class NullOrder(Expression):
    __slots__ = ('expression',)
    _sql = ''

    def __init__(self, expression):
        super(NullOrder, self).__init__()
        self.expression = expression

    def __str__(self):
        if not Flavor.get().null_ordering:
            return '{0}, {1}'.format(self._case, self.expression)
        return '{0} NULLS {1}'.format(self.expression, self._sql)

    @property
    def params(self):
        p = []
        if not Flavor.get().null_ordering:
            p.extend(self.expression.params)
            p.extend(self._case_values())
        p.extend(self.expression.params)
        return tuple(p)

    @property
    def _case(self):
        from .conditionals import Case
        values = self._case_values()
        if isinstance(self.expression, Order):
            expression = self.expression.expression
        else:
            expression = self.expression
        return Asc(Case((expression == Null, values[0]), else_=values[1]))

    def _case_values(self):
        raise NotImplementedError


class NullsFirst(NullOrder):
    __slots__ = ()
    _sql = 'FIRST'

    def _case_values(self):
        return 0, 1


class NullsLast(NullOrder):
    __slots__ = ()
    _sql = 'LAST'

    def _case_values(self):
        return 1, 0


class For(object):
    __slots__ = ('_tables', '_type_', 'nowait')

    def __init__(self, type_, *tables, **kwargs):
        self._tables = None
        self._type_ = None
        self.tables = list(tables)
        self.type_ = type_
        self.nowait = kwargs.get('nowait')

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, value):
        if not isinstance(value, list):
            value = [value]
        self._tables = value

    @property
    def type_(self):
        return self._type_

    @type_.setter
    def type_(self, value):
        value = value.upper()
        self._type_ = value

    def __str__(self):
        tables = ''
        if self.tables:
            tables = ' OF ' + ', '.join(map(text_type, self.tables))
        nowait = ''
        if self.nowait:
            nowait = ' NOWAIT'
        return 'FOR {0}'.format(self.type_) + tables + nowait


Null = None
_rownum = _Rownum()
