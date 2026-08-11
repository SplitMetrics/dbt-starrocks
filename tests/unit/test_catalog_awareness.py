import pytest

from dbt.adapters.starrocks.relation import (
    StarRocksIncludePolicy,
    StarRocksQuotePolicy,
    StarRocksRelation,
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
