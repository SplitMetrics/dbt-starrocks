/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{# Schema creation is intentionally left to dbt's default `create_schema`, which
   renders `create schema if not exists <db>` — and, for a relation carrying a
   catalog, `<catalog>.<db>`. `drop_schema` below mirrors that catalog-qualified
   form. StarRocks accepts the catalog-qualified `CREATE/DROP DATABASE
   <catalog>.<db>` for external (e.g. Iceberg) catalogs since v3.2 — the engine
   parser and AST route it to the named catalog — even though the SQL reference
   docs only show the unqualified name plus a `SET CATALOG` workflow. #}
{% macro starrocks__drop_schema(relation) -%}
  {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier() }}
  {% endcall %}
{%- endmacro %}
