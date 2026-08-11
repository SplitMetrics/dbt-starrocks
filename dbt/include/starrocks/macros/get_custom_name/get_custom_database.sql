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

{# The relation database slot carries the StarRocks catalog. An explicit
   model-level catalog always flows through (a run whose session sits in an
   external catalog needs default_catalog spelled out); a profile catalog
   flows through when it is not the default, so single-catalog setups keep
   rendering two-part exactly as before. #}
{% macro starrocks__generate_database_name(custom_database_name=none, node=none) -%}
  {%- if custom_database_name is not none -%}
    {{ return(custom_database_name | trim) }}
  {%- endif -%}
  {%- set configured_catalog = none -%}
  {%- if node is not none and node.config is defined -%}
    {%- set configured_catalog = node.config.get('catalog') -%}
  {%- endif -%}
  {%- if configured_catalog -%}
    {{ return(configured_catalog) }}
  {%- endif -%}
  {%- if target.catalog is defined and target.catalog and target.catalog != 'default_catalog' -%}
    {{ return(target.catalog) }}
  {%- endif -%}
  {{ return(None) }}
{%- endmacro %}
