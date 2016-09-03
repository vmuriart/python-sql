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

from sql.core import Expression
from sql.utils import csv_str

__all__ = ('Avg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr', 'Count', 'Every',
           'Max', 'Min', 'Stddev', 'Sum', 'Variance')


class Aggregate(Expression):
    __slots__ = ('expression', 'distinct', '_within', 'filter_', 'window')
    _sql = ''

    def __init__(self, expression, distinct=False, within=None, filter_=None,
                 window=None):
        # TODO order_by
        super(Aggregate, self).__init__()
        self.expression = expression

        self._within = None
        self.within = within

        self.distinct = distinct
        self.filter_ = filter_
        self.window = window

    @property
    def within(self):
        return self._within

    @within.setter
    def within(self, value):
        self._within = [value] if isinstance(value, Expression) else value

    def __str__(self):
        quantifier = 'DISTINCT ' if self.distinct else ''
        aggregate = '{}({}{})'.format(self._sql, quantifier, self.expression)
        within = ''
        if self.within:
            within = ' WITHIN GROUP (ORDER BY {})'.format(csv_str(self.within))
        filter_ = ''
        if self.filter_:
            filter_ = ' FILTER (WHERE {})'.format(self.filter_)
        window = ''
        if self.window:
            window = ' OVER "{}"'.format(self.window.alias)
        return aggregate + within + filter_ + window

    @property
    def params(self):
        p = list(self.expression.params)
        if self.within:
            for expression in self.within:
                p.extend(expression.params)
        if self.filter_:
            p.extend(self.filter_.params)
        return tuple(p)


class Avg(Aggregate):
    __slots__ = ()
    _sql = 'AVG'


class BitAnd(Aggregate):
    __slots__ = ()
    _sql = 'BIT_AND'


class BitOr(Aggregate):
    __slots__ = ()
    _sql = 'BIT_OR'


class BoolAnd(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_AND'


class BoolOr(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_OR'


class Count(Aggregate):
    __slots__ = ()
    _sql = 'COUNT'


class Every(Aggregate):
    __slots__ = ()
    _sql = 'EVERY'


class Max(Aggregate):
    __slots__ = ()
    _sql = 'MAX'


class Min(Aggregate):
    __slots__ = ()
    _sql = 'MIN'


class Stddev(Aggregate):
    __slots__ = ()
    _sql = 'Stddev'


class Sum(Aggregate):
    __slots__ = ()
    _sql = 'SUM'


class Variance(Aggregate):
    __slots__ = ()
    _sql = 'VARIANCE'
