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

from array import array

from sql.core import Flavor, Select, CombiningQuery, Expression
from sql.functions import Upper
from sql._compat import text_type, map

__all__ = ('And', 'Or', 'Not', 'Less', 'Greater', 'LessEqual', 'GreaterEqual',
           'Equal', 'NotEqual', 'Add', 'Sub', 'Mul', 'Div', 'Mod', 'Pow',
           'SquareRoot', 'CubeRoot', 'Factorial', 'Abs', 'BAnd', 'BOr', 'BXor',
           'BNot', 'LShift', 'RShift', 'Concat', 'Like', 'NotLike', 'ILike',
           'NotILike', 'In', 'NotIn', 'Exists', 'Any', 'Some', 'All',)


class Operator(Expression):
    __slots__ = ()

    @property
    def table(self):
        return ''

    @property
    def name(self):
        return ''

    @property
    def _operands(self):
        return ()

    @property
    def params(self):

        def convert(operands):
            params = []
            for operand in operands:
                if isinstance(operand, (Expression, Select, CombiningQuery)):
                    params.extend(operand.params)
                elif isinstance(operand, (list, tuple)):
                    params.extend(convert(operand))
                elif isinstance(operand, array):
                    params.extend(operand)
                else:
                    params.append(operand)
            return params

        return tuple(convert(self._operands))

    def _format(self, operand, param=None):
        if param is None:
            param = Flavor.get().param
        if isinstance(operand, Expression):
            return text_type(operand)
        elif isinstance(operand, (Select, CombiningQuery)):
            return '({})'.format(operand)
        elif isinstance(operand, (list, tuple)):
            return '(' + ', '.join(self._format(o, param)
                                   for o in operand) + ')'
        elif isinstance(operand, array):
            return '(' + ', '.join((param,) * len(operand)) + ')'
        else:
            return param

    def __str__(self):
        raise NotImplemented

    def __and__(self, other):
        if isinstance(other, And):
            return And([self] + other)
        else:
            return And((self, other))

    def __or__(self, other):
        if isinstance(other, Or):
            return Or([self] + other)
        else:
            return Or((self, other))


class UnaryOperator(Operator):
    __slots__ = ('operand',)
    _operator = ''

    def __init__(self, operand):
        self.operand = operand

    @property
    def _operands(self):
        return self.operand,

    def __str__(self):
        return '({} {})'.format(self._operator, self._format(self.operand))


class BinaryOperator(Operator):
    __slots__ = ('left', 'right')
    _operator = ''

    def __init__(self, left, right):
        self.left = left
        self.right = right

    @property
    def _operands(self):
        return self.left, self.right

    def __str__(self):
        left, right = self._operands
        return '({} {} {})'.format(
            self._format(left), self._operator, self._format(right))

    def __invert__(self):
        return _INVERT[type(self)](self.left, self.right)


class NaryOperator(list, Operator):
    __slots__ = ()
    _operator = ''

    @property
    def _operands(self):
        return self

    def __str__(self):
        return '(' + (' {} '.format(self._operator)).join(
            map(text_type, self)) + ')'


class And(NaryOperator):
    __slots__ = ()
    _operator = 'AND'


class Or(NaryOperator):
    __slots__ = ()
    _operator = 'OR'


class Not(UnaryOperator):
    __slots__ = ()
    _operator = 'NOT'


class Neg(UnaryOperator):
    __slots__ = ()
    _operator = '-'


class Pos(UnaryOperator):
    __slots__ = ()
    _operator = '+'


class Less(BinaryOperator):
    __slots__ = ()
    _operator = '<'


class Greater(BinaryOperator):
    __slots__ = ()
    _operator = '>'


class LessEqual(BinaryOperator):
    __slots__ = ()
    _operator = '<='


class GreaterEqual(BinaryOperator):
    __slots__ = ()
    _operator = '>='


class Equal(BinaryOperator):
    __slots__ = ()
    _operator = '='

    @property
    def _operands(self):
        if self.left is None:
            return self.right,
        elif self.right is None:
            return self.left,
        return super(Equal, self)._operands

    def __str__(self):
        if self.left is None:
            return '({} IS NULL)'.format(self.right)
        elif self.right is None:
            return '({} IS NULL)'.format(self.left)
        return super(Equal, self).__str__()


class NotEqual(Equal):
    __slots__ = ()
    _operator = '!='

    def __str__(self):
        if self.left is None:
            return '({} IS NOT NULL)'.format(self.right)
        elif self.right is None:
            return '({} IS NOT NULL)'.format(self.left)
        return super(Equal, self).__str__()


class Add(BinaryOperator):
    __slots__ = ()
    _operator = '+'


class Sub(BinaryOperator):
    __slots__ = ()
    _operator = '-'


class Mul(BinaryOperator):
    __slots__ = ()
    _operator = '*'


class Div(BinaryOperator):
    __slots__ = ()
    _operator = '/'


class Mod(BinaryOperator):
    __slots__ = ()

    @property
    def _operator(self):
        # '%' must be escaped with format paramstyle
        return '%%' if Flavor.get().paramstyle == 'format' else '%'


class Pow(BinaryOperator):
    __slots__ = ()
    _operator = '^'


class SquareRoot(UnaryOperator):
    __slots__ = ()
    _operator = '|/'


class CubeRoot(UnaryOperator):
    __slots__ = ()
    _operator = '||/'


class Factorial(UnaryOperator):
    __slots__ = ()
    _operator = '!!'


class Abs(UnaryOperator):
    __slots__ = ()
    _operator = '@'


class BAnd(BinaryOperator):
    __slots__ = ()
    _operator = '&'


class BOr(BinaryOperator):
    __slots__ = ()
    _operator = '|'


class BXor(BinaryOperator):
    __slots__ = ()
    _operator = '#'


class BNot(UnaryOperator):
    __slots__ = ()
    _operator = '~'


class LShift(BinaryOperator):
    __slots__ = ()
    _operator = '<<'


class RShift(BinaryOperator):
    __slots__ = ()
    _operator = '>>'


class Concat(BinaryOperator):
    __slots__ = ()
    _operator = '||'


class Like(BinaryOperator):
    __slots__ = ()
    _operator = 'LIKE'


class NotLike(BinaryOperator):
    __slots__ = ()
    _operator = 'NOT LIKE'


class ILike(BinaryOperator):
    __slots__ = ()

    @property
    def _operator(self):
        return 'ILIKE' if Flavor.get().ilike else 'LIKE'

    @property
    def _operands(self):
        operands = super(ILike, self)._operands
        if not Flavor.get().ilike:
            operands = tuple(Upper(o) for o in operands)
        return operands


class NotILike(ILike):
    __slots__ = ()

    @property
    def _operator(self):
        return 'NOT ILIKE' if Flavor.get().ilike else 'NOT LIKE'


# TODO SIMILAR
class In(BinaryOperator):
    __slots__ = ()
    _operator = 'IN'


class NotIn(BinaryOperator):
    __slots__ = ()
    _operator = 'NOT IN'


class Exists(UnaryOperator):
    __slots__ = ()
    _operator = 'EXISTS'


class All(UnaryOperator):
    __slots__ = ()
    _operator = 'ALL'


class Any(UnaryOperator):
    __slots__ = ()
    _operator = 'ANY'


Some = Any

_INVERT = {
    Less: GreaterEqual,
    Greater: LessEqual,
    LessEqual: Greater,
    GreaterEqual: Less,
    Equal: NotEqual,
    NotEqual: Equal,
    Like: NotLike,
    NotLike: Like,
    ILike: NotILike,
    NotILike: ILike,
    In: NotIn,
    NotIn: In,
}
