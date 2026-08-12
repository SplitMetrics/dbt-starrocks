import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined

from dbt.adapters.starrocks.relation import (
    StarRocksIncludePolicy,
    StarRocksQuotePolicy,
    StarRocksRelation,
)

MACROS_DIR = (
    Path(__file__).resolve().parents[2] / "dbt" / "include" / "starrocks" / "macros"
)


class TestRenderMatrix:
    def test_three_part_render(self):
        relation = StarRocksRelation.create(
            database="iceberg", schema="mmp_silver", identifier="events", type="table"
        )
        assert relation.render() == "`iceberg`.`mmp_silver`.`events`"

    def test_two_part_render_without_catalog(self):
        relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events", type="table"
        )
        assert relation.render() == "`mmp_silver`.`events`"

    def test_schema_level_render_with_catalog(self):
        relation = StarRocksRelation.create(
            database="iceberg", schema="mmp_silver", identifier="events", type="table"
        )
        assert relation.without_identifier().render() == "`iceberg`.`mmp_silver`"

    def test_schema_level_render_without_catalog(self):
        relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events", type="table"
        )
        assert relation.without_identifier().render() == "`mmp_silver`"

    def test_no_render_guard_exception_for_catalogless_relation(self):
        relation = StarRocksRelation.create(schema="s", identifier="t", type="table")
        assert relation.render() == "`s`.`t`"

    def test_policies_include_database(self):
        assert StarRocksIncludePolicy().database is True
        assert StarRocksQuotePolicy().database is True


class TestCacheKeysAcrossCatalogs:
    def _cache(self):
        from dbt.adapters.cache import RelationsCache

        return RelationsCache()

    def _relation(self, catalog, schema="s", identifier="t"):
        return StarRocksRelation.create(
            database=catalog, schema=schema, identifier=identifier, type="table"
        )

    def test_same_name_in_two_catalogs_does_not_collapse(self):
        cache = self._cache()
        cache.add_schema("default_catalog", "s")
        cache.add_schema("iceberg", "s")
        cache.add(self._relation("default_catalog"))
        cache.add(self._relation("iceberg"))

        assert len(cache.get_relations("iceberg", "s")) == 1
        assert len(cache.get_relations("default_catalog", "s")) == 1

    def test_catalogless_entries_keep_none_key(self):
        cache = self._cache()
        cache.add_schema(None, "s")
        cache.add(self._relation(None))

        assert len(cache.get_relations(None, "s")) == 1


class TestListRelationsCarriesCatalog:
    def test_relations_inherit_schema_relation_catalog(self):
        from unittest.mock import MagicMock

        from dbt.adapters.starrocks.impl import StarRocksAdapter

        schema_relation = StarRocksRelation.create(
            database="iceberg", schema="mmp_silver", identifier=None
        )
        rows = [("ignored", "events", "mmp_silver", "table")]

        adapter = MagicMock(spec=StarRocksAdapter)
        adapter.Relation = StarRocksRelation
        adapter.execute_macro = MagicMock(return_value=rows)
        relations = StarRocksAdapter.list_relations_without_caching(
            adapter, schema_relation
        )

        assert len(relations) == 1
        assert relations[0].database == "iceberg"
        assert relations[0].schema == "mmp_silver"
        assert relations[0].identifier == "events"

    def test_catalogless_schema_relation_yields_none_database(self):
        from unittest.mock import MagicMock

        from dbt.adapters.starrocks.impl import StarRocksAdapter

        schema_relation = StarRocksRelation.create(
            database=None, schema="db", identifier=None
        )
        rows = [("ignored", "t", "db", "table")]

        adapter = MagicMock(spec=StarRocksAdapter)
        adapter.Relation = StarRocksRelation
        adapter.execute_macro = MagicMock(return_value=rows)
        relations = StarRocksAdapter.list_relations_without_caching(
            adapter, schema_relation
        )

        assert relations[0].database is None


class TestColumnsLookup:
    def _render(self, relation):
        statements = []

        def statement(name, fetch_result=False, caller=None):
            statements.append((name, " ".join(caller().split())))
            return ""

        def fail_if_converted(*args):
            raise AssertionError("empty metadata must not be converted")

        environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )
        environment.globals.update(
            statement=statement,
            load_result=lambda name: SimpleNamespace(table=SimpleNamespace(rows=[])),
            starrocks__sql_convert_columns_in_relation=fail_if_converted,
        )
        environment.globals["return"] = lambda value: value
        template = environment.from_string(
            (MACROS_DIR / "adapters" / "columns.sql").read_text()
            + "\n{{ starrocks__get_columns_in_relation(relation) }}"
        )
        template.render(relation=relation)
        return statements

    def test_lookup_is_catalog_qualified_and_skips_desc_on_empty(self):
        relation = StarRocksRelation.create(
            database="glue_iceberg_catalog",
            schema="mmp_silver",
            identifier="events",
            type="table",
        )
        assert self._render(relation) == [
            (
                "get_columns_in_relation",
                "select column_name, data_type, character_maximum_length, "
                "numeric_precision, numeric_scale from "
                "`glue_iceberg_catalog`.INFORMATION_SCHEMA.columns "
                "where table_name = 'events' "
                "and table_schema = 'mmp_silver' order by ordinal_position",
            )
        ]

    def test_lookup_without_catalog_matches_upstream(self):
        relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events", type="table"
        )
        statements = self._render(relation)
        assert len(statements) == 1
        assert "INFORMATION_SCHEMA.columns" in statements[0][1]
        assert "`mmp_silver`" not in statements[0][1].split("where")[0]


class TestExternalTableLocationRestoresCatalog:
    def _run_query_sequence(self, relation, target_catalog="default_catalog"):
        calls = []

        def run_query(sql):
            calls.append(" ".join(sql.split()))
            if sql.strip().lower().startswith("show create database"):
                return SimpleNamespace(
                    rows=[
                        (
                            "mmp_silver",
                            'CREATE DATABASE `mmp_silver` PROPERTIES '
                            '("location" = "s3://bucket/warehouse/mmp_silver/")',
                        )
                    ]
                )
            return None

        environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )
        environment.globals.update(
            run_query=run_query,
            var=lambda name, default="": default,
            env_var=lambda name, default="": default,
            execute=True,
            target=SimpleNamespace(catalog=target_catalog),
            modules=SimpleNamespace(re=re),
        )
        environment.globals["return"] = lambda value: value
        template = environment.from_string(
            (MACROS_DIR / "adapters" / "relation_helpers.sql").read_text()
            + "\n{{ starrocks__external_table_location(relation) }}"
        )
        template.render(relation=relation)
        return calls

    def test_session_catalog_restored_after_external_read(self):
        relation = StarRocksRelation.create(
            database="glue_iceberg_catalog",
            schema="mmp_silver",
            identifier="events",
            type="table",
        )
        assert self._run_query_sequence(relation) == [
            "set catalog `glue_iceberg_catalog`",
            "show create database `mmp_silver`",
            "set catalog `default_catalog`",
        ]

    def test_no_catalog_switch_for_internal_relation(self):
        relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events", type="table"
        )
        assert self._run_query_sequence(relation) == [
            "show create database `mmp_silver`"
        ]


class TestGenerateDatabaseNameCatalogDerivation:
    class _Return(Exception):
        def __init__(self, value):
            self.value = value

    def _render(self, custom_database_name, node=None, target_catalog=None):
        environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )

        def do_return(value):
            # dbt's return() short-circuits the macro; mirror that so only the
            # first branch's value is observed.
            raise TestGenerateDatabaseNameCatalogDerivation._Return(value)

        environment.globals["return"] = do_return
        environment.globals["target"] = SimpleNamespace(catalog=target_catalog)
        template = environment.from_string(
            (MACROS_DIR / "get_custom_name" / "get_custom_database.sql").read_text()
            + "\n{{ starrocks__generate_database_name(custom_database_name, node) }}"
        )
        try:
            template.render(custom_database_name=custom_database_name, node=node)
        except TestGenerateDatabaseNameCatalogDerivation._Return as ret:
            return ret.value
        return None

    def test_database_config_is_not_treated_as_catalog(self):
        # A pre-existing +database value must not render as a StarRocks catalog;
        # it falls through to the catalog logic and yields no catalog here.
        assert self._render("analytics") is None

    def test_config_catalog_still_flows_through_with_database_set(self):
        node = SimpleNamespace(config={"catalog": "iceberg"})
        assert self._render("analytics", node=node) == "iceberg"

    def test_non_default_target_catalog_flows_through(self):
        assert self._render(None, target_catalog="iceberg") == "iceberg"


class TestRenameViewGuardsExternalCatalog:
    class CompilerError(Exception):
        pass

    def _rename(self, from_relation, to_relation, view_def="select 1"):
        run_queries = []

        def statement(name, fetch_result=False, auto_begin=True, caller=None):
            caller()
            return ""

        def raise_compiler_error(msg):
            raise TestRenameViewGuardsExternalCatalog.CompilerError(msg)

        def run_query(sql):
            run_queries.append(" ".join(sql.split()))
            return [{"sql": view_def}]

        environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )
        environment.globals.update(
            statement=statement,
            run_query=run_query,
            exceptions=SimpleNamespace(raise_compiler_error=raise_compiler_error),
        )
        environment.globals["return"] = lambda value: value
        template = environment.from_string(
            (MACROS_DIR / "adapters" / "relation.sql").read_text()
            + "\n{{ starrocks__rename_relation(from_relation, to_relation) }}"
        )
        template.render(from_relation=from_relation, to_relation=to_relation)
        return run_queries

    def test_external_view_rename_raises_clear_error(self):
        from_relation = StarRocksRelation.create(
            database="glue_iceberg_catalog",
            schema="mmp_silver",
            identifier="events",
            type="view",
        )
        to_relation = StarRocksRelation.create(
            database="glue_iceberg_catalog",
            schema="mmp_silver",
            identifier="events_new",
            type="view",
        )
        with pytest.raises(self.CompilerError) as excinfo:
            self._rename(from_relation, to_relation)
        assert "glue_iceberg_catalog" in str(excinfo.value)

    def test_internal_view_rename_reads_information_schema(self):
        from_relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events", type="view"
        )
        to_relation = StarRocksRelation.create(
            schema="mmp_silver", identifier="events_new", type="view"
        )
        run_queries = self._rename(from_relation, to_relation)
        assert any("information_schema.views" in q for q in run_queries)


class TestMetadataIdentifierEscaping:
    def _render(self, call, schema_relation=None, database=None):
        statements = []

        def statement(name, fetch_result=False, auto_begin=True, caller=None):
            statements.append(" ".join(caller().split()))
            return ""

        environment = Environment(
            undefined=StrictUndefined, extensions=["jinja2.ext.do"]
        )
        environment.globals.update(
            statement=statement,
            load_result=lambda name: SimpleNamespace(table=None),
        )
        environment.globals["return"] = lambda value: value
        template = environment.from_string(
            (MACROS_DIR / "adapters" / "metadata.sql").read_text() + "\n" + call
        )
        template.render(schema_relation=schema_relation, database=database)
        return statements

    def test_list_schemas_escapes_backticks_in_raw_database(self):
        # check_schema_exists passes the raw component
        statements = self._render(
            "{{ starrocks__list_schemas(database) }}", database="ice`berg"
        )
        assert len(statements) == 1
        assert "`ice``berg`.information_schema.schemata" in statements[0]

    def test_list_schemas_passes_through_prerendered_database(self):
        # dbt-core's create_schemas passes str(relation), already quoted
        statements = self._render(
            "{{ starrocks__list_schemas(database) }}", database="`iceberg`"
        )
        assert len(statements) == 1
        assert "`iceberg`.information_schema.schemata" in statements[0]
        assert "``iceberg``" not in statements[0]

    def test_list_relations_escapes_quotes_in_catalog_literal(self):
        schema_relation = StarRocksRelation.create(
            database="o'catalog", schema="mmp_silver", identifier=None
        )
        statements = self._render(
            "{{ starrocks__list_relations_without_caching(schema_relation) }}",
            schema_relation=schema_relation,
        )
        assert len(statements) == 1
        assert "'o''catalog' as \"database\"" in statements[0]
