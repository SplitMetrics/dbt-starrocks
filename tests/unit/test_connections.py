from types import SimpleNamespace
from unittest import mock

import dbt_common.exceptions
import pytest

from mysql.connector.constants import FieldType

from dbt.adapters.starrocks.connections import (
    StarRocksConnectionManager,
    StarRocksCredentials,
)


def test_data_type_code_to_name_maps_mysql_connector_type_codes():
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DECIMAL) == "decimal"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DATE) == "date"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.DATETIME) == "datetime"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.TIMESTAMP) == "datetime"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.LONGLONG) == "bigint"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.NEWDECIMAL) == "decimal"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.TIME) == "varchar"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.YEAR) == "smallint"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.VAR_STRING) == "varchar"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.BLOB) == "varbinary"
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.BIT) == "tinyint"


def test_data_type_code_to_name_normalizes_string_type_codes():
    assert StarRocksConnectionManager.data_type_code_to_name("VARCHAR") == "varchar"


def test_data_type_code_to_name_falls_back_to_mysql_connector_name():
    assert StarRocksConnectionManager.data_type_code_to_name(FieldType.GEOMETRY) == "geometry"


def test_data_type_code_to_name_logs_unknown_numeric_code():
    assert StarRocksConnectionManager.data_type_code_to_name(99999) == "99999"


def _open_and_capture_executes(catalog):
    executed = []
    cursor = SimpleNamespace(
        execute=lambda sql: executed.append(sql), close=lambda: None
    )
    handle = SimpleNamespace(cursor=lambda: cursor)
    # version set so open() skips the current_version() probe cursor
    credentials = StarRocksCredentials(catalog=catalog, version="3.0.0")
    connection = SimpleNamespace(
        state="init", credentials=credentials, handle=None
    )
    with mock.patch(
        "mysql.connector.connect", return_value=handle
    ):
        StarRocksConnectionManager.open(connection)
    return executed


def test_open_default_catalog_skips_set_catalog():
    # 'default_catalog' is the class default and is already the session catalog,
    # so SET CATALOG must be skipped (older StarRocks servers reject it).
    executed = _open_and_capture_executes("default_catalog")
    assert not any("SET CATALOG" in sql for sql in executed)


def test_open_omitted_catalog_skips_set_catalog():
    executed = _open_and_capture_executes(StarRocksCredentials().catalog)
    assert not any("SET CATALOG" in sql for sql in executed)


def test_open_non_default_catalog_runs_set_catalog():
    executed = _open_and_capture_executes("iceberg")
    assert "SET CATALOG `iceberg`" in executed


def _open_and_capture_connect_kwargs(**cred_kwargs):
    captured = {}
    cursor = SimpleNamespace(execute=lambda sql: None, close=lambda: None)
    handle = SimpleNamespace(cursor=lambda: cursor)
    # version set so open() skips the current_version() probe cursor
    credentials = StarRocksCredentials(version="3.0.0", **cred_kwargs)
    connection = SimpleNamespace(state="init", credentials=credentials, handle=None)

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return handle

    with mock.patch("mysql.connector.connect", side_effect=fake_connect):
        StarRocksConnectionManager.open(connection)
    return captured


def test_open_default_credentials_impose_no_read_write_timeout():
    # Default credentials must not pass read/write timeouts, so recv() stays
    # unlimited and long server-side queries are not capped as on upstream.
    captured = _open_and_capture_connect_kwargs()
    assert "read_timeout" not in captured
    assert "write_timeout" not in captured


def test_open_passes_configured_read_write_timeout():
    captured = _open_and_capture_connect_kwargs(read_timeout=60, write_timeout=90)
    assert captured["read_timeout"] == 60
    assert captured["write_timeout"] == 90


def test_credentials_database_equal_to_schema_is_normalized_to_none():
    # dbt-core defaults a source's database to credentials.database; leaving the
    # schema string there would render it as a catalog in three-part names.
    credentials = StarRocksCredentials(schema="mmp_x", database="mmp_x")
    assert credentials.database is None


def test_credentials_database_mismatch_raises():
    with pytest.raises(dbt_common.exceptions.DbtRuntimeError):
        StarRocksCredentials(schema="mmp_x", database="other")


def test_credentials_omitted_database_stays_none():
    assert StarRocksCredentials(schema="mmp_x").database is None
