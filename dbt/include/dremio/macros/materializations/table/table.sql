/*Copyright (C) 2022 Dremio Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*/

{% materialization table, adapter = 'dremio', supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}
  {%- set identifier = model['alias'] -%}
  {%- set branch = config.get('branch', validator=validation.any[string]) -%}
  {%- set format = config.get('format', validator=validation.any[basestring]) or 'iceberg' -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {% set grant_config = config.get('grants') %}

  {{ run_hooks(pre_hooks) }}

  {%- if language == 'python' %}

    {%- if old_relation is not none -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- endif -%}

    -- Python model: execute client-side and write to Iceberg via REST catalog.
    -- The DataFrame is written via pandas.to_iceberg() using the catalog
    -- configured in the profile (iceberg_catalog_uri).
    -- refresh_metadata and apply_twin_strategy are skipped because the table
    -- is created directly by PyIceberg outside of Dremio's SQL engine.
    {% call statement('main', language='python') -%}
      {{ compiled_code }}
    {%- endcall %}

    {% do persist_docs(target_relation, model) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {%- else %}

    -- SQL path (unchanged)
    -- create branch first if needed
    {% if branch is not none %}
      {{ create_branch_statement(target_relation, branch) }}
    {% endif %}

    -- setup: if the target relation already exists, drop it
    {% if branch is not none %}
      {%- set branch_relation_exists = get_relation_at_branch(database, schema, identifier, branch) -%}
      {% if branch_relation_exists %}
        {{ drop_relation_with_branch(target_relation, branch) }}
      {% endif %}
    {% elif old_relation is not none -%}
      {{ adapter.drop_relation(old_relation) }}
    {%- endif %}

    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, external_query(sql)) }}
    {%- endcall %}

    {{ refresh_metadata(target_relation, format) }}
    {{ apply_twin_strategy(target_relation) }}
    {% do persist_docs(target_relation, model) %}
    {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {%- endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]})}}

{% endmaterialization %}
