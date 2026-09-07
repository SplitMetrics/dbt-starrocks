#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass, field
from typing import Optional, Type
from dbt.adapters.base.relation import BaseRelation, Policy
from dbt_common.dataclass_schema import StrEnum


class classproperty(object):
    def __init__(self, func):
        self.func = func

    def __get__(self, obj, objtype):
        return self.func(objtype)


@dataclass
class StarRocksQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class StarRocksIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


class StarRocksRelationType(StrEnum):
    Table = "table"
    View = "view"
    MaterializedView = "materialized_view"
    SystemView = "system_view"
    CTE = "cte"
    Unknown = "unknown"


type_map = {}


@dataclass(frozen=True, eq=False, repr=False)
class StarRocksRelation(BaseRelation):
    type: Optional[StarRocksRelationType] = None  # type: ignore
    include_policy: StarRocksIncludePolicy = field(
        default_factory=lambda: StarRocksIncludePolicy())
    quote_policy: StarRocksQuotePolicy = field(
        default_factory=lambda: StarRocksQuotePolicy())
    quote_character: str = "`"

    def quoted(self, identifier):
        return "{quote_char}{identifier}{quote_char}".format(
            quote_char=self.quote_character,
            identifier=identifier,
        )

    @property
    def is_materialized_view(self) -> bool:
        return self.type == StarRocksRelationType.MaterializedView

    def init_type_map(self, desc_table):
        for row in desc_table:
            # name -> type
            type_map[row[0]] = row[1]

    def get_type_by_desc(self, row):
        new_row = list(row)
        # type
        new_row[1] = type_map[row[0]]
        return new_row

    @classproperty
    def get_relation_type(cls) -> Type[StarRocksRelationType]:
        return StarRocksRelationType
