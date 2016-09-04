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

from sql.aggregate import (
    Avg, BitAnd, BitOr, BoolAnd, BoolOr, Count, Every, Max, Min, Stddev, Sum,
    Variance)

from sql.conditionals import (
    Case, Coalesce, NullIf, Greatest, Least)

from sql.core import (
    Flavor, AliasManager, Query, WithQuery, FromItem, With,
    SelectQuery, Select, CombiningQuery, Union,
    Intersect, Except, Table, Join, From, Expression, Literal, _Rownum,
    Column, As, Cast, Window, Order, Asc, Desc, NullOrder, NullsFirst,
    NullsLast, Null, _rownum)

from sql.functions import (
    Abs, Cbrt, Ceil, Degrees, Div, Exp, Floor, Ln,
    Log, Mod, Pi, Power, Radians, Random, Round,
    SetSeed, Sign, Sqrt, Trunc, WidthBucket,

    Acos, Asin, Atan, Atan2, Cos, Cot, Sin, Tan,
    BitLength, CharLength, Overlay, Position, Substring,
    Trim, Upper,

    ToChar, ToDate, ToNumber, ToTimestamp,

    Age, ClockTimestamp, CurrentDate, CurrentTime,
    CurrentTimestamp, DatePart, DateTrunc, Extract, Isfinite,
    JustifyDays, JustifyHours, JustifyInterval, Localtime,
    Localtimestamp, Now, StatementTimestamp, Timeofday,
    TransactionTimestamp, AtTimeZone,

    RowNumber, Rank, DenseRank, PercentRank, CumeDist,
    Ntile, Lag, Lead, FirstValue, LastValue, NthValue, )

from sql.operators import (
    And, Or, Not, Less, Greater, LessEqual, GreaterEqual,
    Equal, NotEqual, Add, Sub, Mul, Pow,
    SquareRoot, CubeRoot, Factorial, BAnd, BOr, BXor,
    BNot, LShift, RShift, Concat, Like, NotLike, ILike,
    NotILike, In, NotIn, Exists, Any, Some, All, )

__version__ = '0.9.0-dev0'
__all__ = (
    # Core
    'Flavor', 'AliasManager', 'Query', 'WithQuery', 'FromItem',
    'With', 'SelectQuery', 'Select',
    'CombiningQuery', 'Union', 'Intersect', 'Except', 'Table', 'Join', 'From',
    'Expression', 'Literal', '_Rownum', 'Column', 'As', 'Cast',
    'Window', 'Order', 'Asc', 'Desc', 'NullOrder', 'NullsFirst', 'NullsLast',
    'Null', '_rownum',

    # Aggregate
    'Avg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr', 'Count', 'Every',
    'Max', 'Min', 'Stddev', 'Sum', 'Variance',

    # Conditionals
    'Case', 'Coalesce', 'NullIf', 'Greatest', 'Least',

    # Functions
    'Abs', 'Cbrt', 'Ceil', 'Degrees', 'Div', 'Exp', 'Floor', 'Ln',
    'Log', 'Mod', 'Pi', 'Power', 'Radians', 'Random', 'Round',
    'SetSeed', 'Sign', 'Sqrt', 'Trunc', 'WidthBucket',

    'Acos', 'Asin', 'Atan', 'Atan2', 'Cos', 'Cot', 'Sin', 'Tan',
    'BitLength', 'CharLength', 'Overlay', 'Position', 'Substring',
    'Trim', 'Upper',

    'ToChar', 'ToDate', 'ToNumber', 'ToTimestamp',

    'Age', 'ClockTimestamp', 'CurrentDate', 'CurrentTime',
    'CurrentTimestamp', 'DatePart', 'DateTrunc', 'Extract', 'Isfinite',
    'JustifyDays', 'JustifyHours', 'JustifyInterval', 'Localtime',
    'Localtimestamp', 'Now', 'StatementTimestamp', 'Timeofday',
    'TransactionTimestamp', 'AtTimeZone',

    'RowNumber', 'Rank', 'DenseRank', 'PercentRank', 'CumeDist',
    'Ntile', 'Lag', 'Lead', 'FirstValue', 'LastValue', 'NthValue',

    # Operators
    'And', 'Or', 'Not', 'Less', 'Greater', 'LessEqual', 'GreaterEqual',
    'Equal', 'NotEqual', 'Add', 'Sub', 'Mul', 'Pow', 'SquareRoot', 'CubeRoot',
    'Factorial', 'BAnd', 'BOr', 'BXor', 'BNot', 'LShift', 'RShift', 'Concat',
    'Like', 'NotLike', 'ILike', 'NotILike', 'In', 'NotIn', 'Exists', 'Any',
    'Some', 'All',
)
