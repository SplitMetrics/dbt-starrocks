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

{% macro starrocks__olap_table(is_create_table_as) -%}

  {%- set is_create_table = is_create_table_as is none or not is_create_table_as -%}

  {%- set table_type = config.get('table_type', 'DUPLICATE') -%}
  {%- set keys = config.get('keys') -%}
  {%- set partition_by = config.get('partition_by') -%}
  {%- set partition_by_init = config.get('partition_by_init') -%}
  {%- set order_by = config.get('order_by') -%}
  {%- set partition_type = config.get('partition_type', 'RANGE') -%}
  {%- set buckets = config.get('buckets') -%}
  {%- set distributed_by = config.get('distributed_by') -%}
  {%- set properties = config.get('properties') -%}
  {%- set materialized = config.get('materialized', none) -%}
  {%- set unique_key = config.get('unique_key', none) -%}

  {%- if materialized == 'incremental' and unique_key is not none -%}
    {%- set table_type = 'PRIMARY' -%}
    {%- set keys = unique_key if unique_key is sequence and unique_key is not mapping and unique_key is not string else [unique_key] -%}
  {%- endif -%}

  {# 1. SET ENGINE #}
  {%- if is_create_table %} ENGINE = OLAP {% endif -%}

  {# 2. SET KEYS #}
  {%- if keys is not none -%}
    {%- if table_type == "DUPLICATE" %}
      DUPLICATE KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- elif table_type == "PRIMARY" %}
      PRIMARY KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- elif table_type == "UNIQUE" %}
      UNIQUE KEY (
        {%- for item in keys -%}
          {{ item }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
      )
    {%- else -%}
      {%- set msg -%}
        "{{ table_type }}" is not support
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- else -%}
    {%- if table_type != "DUPLICATE" -%}
      {%- set msg -%}
        "{{ table_type }}" is must set "keys"
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {% endif -%}

  {# 3. SET PARTITION #}
  {% if partition_by is not none -%}
    {{ starrocks__partition_by(partition_type, partition_by, partition_by_init) }}
  {%- endif %}

  {# 4. SET DISTRIBUTED #}
  {%- if distributed_by is not none %}
    DISTRIBUTED BY HASH (
      {%- for item in distributed_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
    {%- if buckets is not none -%}
      BUCKETS {{ buckets }}
    {%- elif adapter.is_before_version("2.5.7") -%}
      {%- set msg -%}
        [buckets] must set before version 2.5.7, current version is {{ adapter.current_version() }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- elif adapter.is_before_version("3.1.0") -%}
    {%- set msg -%}
      [distributed_by] must set before version 3.1, current version is {{ adapter.current_version() }}
    {%- endset -%}
    {{ exceptions.raise_compiler_error(msg) }}
  {% endif -%}

  {# 5. SET ORDER BY #}
  {% if order_by is not none %}
    ORDER BY (
      {%- for item in order_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
  {% endif %}

  {# 6. SET PROPERTIES #}
  {% if properties is not none %}
    PROPERTIES (
      {% for key, value in properties.items() -%}
        "{{ key }}" = "{{ value }}"
        {%- if not loop.last -%},
        {% endif -%}
      {%- endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro starrocks__other_table() -%}
  {% set engine = config.get('engine') %}
  {% set properties = config.get('properties') %}

  ENGINE = {{ engine }}
  {% if properties is not none %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% endif %}
{%- endmacro %}

{%- macro starrocks__partition_by(p_type, cols, init) -%}

  {%- if p_type == 'Expr' %}
    {%- if cols | length != 1 -%}
        {%- set msg -%}
          The number of partition_by parameters for expression partition should be 1
        {%- endset -%}
    {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
    PARTITION BY {{ cols[0] }}
  {%- else -%}
      {%- if cols is not none %}
        PARTITION BY {{ p_type }} (
          {%- for col in cols -%}
            {{ col }} {%- if not loop.last -%}, {%- endif -%}
          {%- endfor -%}
        )(
          {%- if init is not none -%}
            {%- for row in init -%}
              {{ row }} {%- if not loop.last -%}, {%- endif -%}
            {%- endfor -%}
          {%- endif -%}
        )
      {% endif -%}
  {% endif -%}
{%- endmacro -%}

{# Resolve the storage location for a new external (Iceberg) table: an explicit
   base via the external_table_location_base var / DBT_EXTERNAL_TABLE_LOCATION_BASE
   env, else derived from the target schema's `show create database` location. #}
{% macro starrocks__external_table_location(relation) -%}
  {%- set configured_base = var('external_table_location_base', env_var('DBT_EXTERNAL_TABLE_LOCATION_BASE', '')) -%}

  {%- if configured_base -%}
    {{ return(configured_base.rstrip('/') ~ '/' ~ relation.identifier) }}
  {%- endif -%}

  {%- if not execute -%}
    {{ return(none) }}
  {%- endif -%}

  {# SHOW CREATE DATABASE resolves against the session catalog and cannot be
     catalog-qualified, so switch into the external catalog to read and switch
     back afterwards to leave the session catalog unchanged. #}
  {%- set switched_catalog = relation.database and relation.database != 'default_catalog' -%}
  {%- if switched_catalog -%}
    {%- do run_query('set catalog ' ~ relation.quoted(relation.database)) -%}
  {%- endif -%}
  {%- set database_location_result = run_query('show create database ' ~ relation.quoted(relation.schema)) -%}
  {%- if switched_catalog -%}
    {%- do run_query('set catalog ' ~ relation.quoted(target.catalog)) -%}
  {%- endif -%}
  {%- if database_location_result is none or database_location_result.rows | length == 0 -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set database_ddl = database_location_result.rows[0][1] -%}
  {%- set location_match = modules.re.search('"location"\\s*=\\s*"([^"]+)"', database_ddl) -%}
  {%- if location_match is none -%}
    {{ return(none) }}
  {%- endif -%}

  {{ return(location_match.group(1).rstrip('/') ~ '/' ~ relation.identifier) }}
{%- endmacro %}

{# With a relation: render external-catalog CTAS options — a plain parenthesized
   partition column list (the Iceberg transform syntax; the OLAP Expr/Range forms
   below don't apply to external tables) and PROPERTIES with a derived location
   when none is configured. Without a relation: legacy rendering, unchanged. #}
{% macro starrocks__external_table(relation=none) -%}
  {% set properties = config.get('properties') %}
  {% set partition_by = config.get('partition_by') %}
  {%- set partition_type = config.get('partition_type', 'Expr') -%}
  {%- set partition_by_init = config.get('partition_by_init') -%}

  {%- if relation is none -%}
  {% if partition_by is not none %}
    {{ starrocks__partition_by(partition_type, partition_by, partition_by_init) }}
  {% endif %}

  {% if properties is not none %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% endif %}
  {%- else -%}
  {% if partition_by is not none %}
    PARTITION BY (
      {%- for col in partition_by -%}
        {{ col }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%}
    )
  {% endif %}

  {%- if properties is none -%}
    {%- set properties = {} -%}
  {%- endif -%}
  {%- if properties.get('location') is none -%}
    {%- set table_location = starrocks__external_table_location(relation) -%}
    {%- if table_location is not none -%}
      {%- do properties.update({'location': table_location}) -%}
    {%- endif -%}
  {%- endif -%}

  {% if properties %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% endif %}
  {%- endif -%}
{%- endmacro %}
