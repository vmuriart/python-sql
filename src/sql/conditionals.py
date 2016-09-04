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

from sql.core import Flavor, Expression
from sql.utils import csv_map
from sql._compat import text_type

__all__ = ('Case', 'Coalesce', 'NullIf', 'Greatest', 'Least')


class Conditional(Expression):
    __slots__ = ()
    _sql = ''
    table = ''
    name = ''

    @staticmethod
    def _format(value):
        if isinstance(value, Expression):
            return text_type(value)
        else:
            return Flavor().get().param


class Case(Conditional):
    __slots__ = ('whens', 'else_')

    def __init__(self, *args, **kwargs):
        self.whens = args
        self.else_ = kwargs.get('else_')

    def __str__(self):
        when_tpl = 'WHEN {} THEN {} '.format
        else_tpl = 'ELSE {} '.format
        case = 'CASE '
        for cond, result in self.whens:
            case += when_tpl(self._format(cond), self._format(result))
        if self.else_ is not None:
            case += else_tpl(self._format(self.else_))
        case += 'END'
        return case

    @property
    def params(self):
        p = []
        for cond, result in self.whens:
            if isinstance(cond, Expression):
                p.extend(cond.params)
            else:
                p.append(cond)
            if isinstance(result, Expression):
                p.extend(result.params)
            else:
                p.append(result)
        if self.else_ is not None:
            if isinstance(self.else_, Expression):
                p.extend(self.else_.params)
            else:
                p.append(self.else_)
        return tuple(p)


class Coalesce(Conditional):
    __slots__ = ('values',)
    _conditional = 'COALESCE'

    def __init__(self, *args):
        self.values = args

    def __str__(self):
        par = '({})'.format
        return self._conditional + par(csv_map(self._format, self.values))

    @property
    def params(self):
        p = []
        for value in self.values:
            if isinstance(value, Expression):
                p.extend(value.params)
            else:
                p.append(value)
        return tuple(p)


class NullIf(Coalesce):
    __slots__ = ()
    _conditional = 'NULLIF'


class Greatest(Coalesce):
    __slots__ = ()
    _conditional = 'GREATEST'


class Least(Coalesce):
    __slots__ = ()
    _conditional = 'LEAST'
