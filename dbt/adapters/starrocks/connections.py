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

from contextlib import contextmanager
from threading import Lock, local

import mysql.connector
from mysql.connector.constants import FieldType

import dbt.exceptions
import dbt_common.exceptions
from dataclasses import dataclass

from dbt.adapters.contracts.connection import (
    Credentials,
    AdapterResponse,
    Connection
)
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.events.logging import AdapterLogger
from typing import Optional, Union

from dbt.adapters.starrocks.insert_overwrite_response_recovery import (
    RecoveryObserver,
    overwrite_insert_match,
    mark_overwrite_insert,
)

logger = AdapterLogger("starrocks")


@dataclass
class StarRocksCredentials(Credentials):
    host: Optional[str] = None
    port: Optional[int] = None
    catalog: Optional[str] = 'default_catalog'
    database: Optional[str] = None
    schema: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    charset: Optional[str] = None
    version: Optional[str] = None
    use_pure: Optional[str] = None
    is_async: Optional[bool] = False
    async_query_timeout: Optional[int] = 300
    connection_timeout: Optional[int] = 10
    read_timeout: Optional[int] = 1800
    write_timeout: Optional[int] = 1800
    poll_interval: Optional[int] = 1
    poll_max_delay: Optional[int] = 600
    poll_factor: Optional[float] = 2.0
    auth_plugin: Optional[str] = ''
    
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __post_init__(self):
        # starrocks classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On StarRocks, database must be omitted or have the same value as"
                f" schema."
            )

    @property
    def type(self):
        return 'starrocks'

    @property
    def unique_field(self):
        return self.schema

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "host",
            "port",
            "schema",
            "catalog",
            "username",
            "use_pure",
            "is_async",
            "async_query_timeout",
            "connection_timeout",
            "read_timeout",
            "write_timeout",
            "poll_interval",
            "poll_max_delay",
            "poll_factor",
            "auth_plugin",
        )


def _parse_version(result):
    default_version = (999, 999, 999)
    first_part = None

    if '-' in result:
        first_part = result.split('-')[0]
    if ' ' in result:
        first_part = result.split(' ')[0]

    if first_part and len(first_part.split('.')) == 3:
        return int(first_part[0]), int(first_part[2]), int(first_part[4])

    return default_version


class StarRocksConnectionManager(SQLConnectionManager):
    TYPE = 'starrocks'
    _recovery_local = local()
    _recoveries_lock = Lock()
    _recoveries = {}
    TYPE_CODE_TO_NAME = {
        FieldType.DECIMAL: "decimal",
        FieldType.NEWDECIMAL: "decimal",
        FieldType.TINY: "tinyint",
        FieldType.SHORT: "smallint",
        FieldType.LONG: "int",
        FieldType.INT24: "int",
        FieldType.LONGLONG: "bigint",
        FieldType.FLOAT: "float",
        FieldType.DOUBLE: "double",
        FieldType.DATE: "date",
        FieldType.DATETIME: "datetime",
        FieldType.TIMESTAMP: "datetime",
        FieldType.TIME: "varchar",
        FieldType.YEAR: "smallint",
        FieldType.VARCHAR: "varchar",
        FieldType.VAR_STRING: "varchar",
        FieldType.STRING: "varchar",
        FieldType.BLOB: "varbinary",
        FieldType.BIT: "tinyint",
        FieldType.JSON: "json",
    }

    @classmethod
    def data_type_code_to_name(cls, type_code: Union[int, str]) -> str:
        if isinstance(type_code, str):
            return type_code.lower()

        data_type = cls.TYPE_CODE_TO_NAME.get(type_code)
        if data_type:
            return data_type

        mysql_type_name = FieldType.get_info(type_code)
        if mysql_type_name:
            return mysql_type_name.lower()

        logger.warning("Unknown StarRocks data type code: %s", type_code)
        return str(type_code)

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {"host": credentials.host, "username": credentials.username,
                  "password": credentials.password, "auth_plugin": credentials.auth_plugin}

        kwargs["buffered"] = True

        if credentials.port:
            kwargs["port"] = credentials.port

        for timeout_key in ("connection_timeout", "read_timeout", "write_timeout"):
            timeout_value = getattr(credentials, timeout_key, None)
            if timeout_value is not None:
                kwargs[timeout_key] = timeout_value

        if credentials.use_pure in ["true", "True"]:
            kwargs["use_pure"] = True

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'

            if credentials.catalog:
                cursor = connection.handle.cursor()
                escaped_catalog = credentials.catalog.replace("`", "``")
                cursor.execute("SET CATALOG `{}`".format(escaped_catalog))
                cursor.close()
        except mysql.connector.Error as e:
            logger.debug("Got an error when attempting to open a StarRocks "
                         "connection: '{}'".format(e))

            connection.handle = None
            connection.state = 'fail'

            raise dbt_common.exceptions.ConnectionError(str(e))

        if credentials.version is None:
            cursor = connection.handle.cursor()
            try:
                cursor.execute("select current_version()")
                connection.handle.server_version = _parse_version(
                    cursor.fetchone()[0])
            except Exception as e:
                logger.debug(
                    "Got an error when obtain StarRocks version exception: '{}'".format(e))
        else:
            version = credentials.version.strip().split('.')
            if len(version) == 3:
                connection.handle.server_version = (
                    int(version[0]), int(version[1]), int(version[2]))
            elif len(version) == 2:
                connection.handle.server_version = (
                    int(version[0]), int(version[1]), 0)
            else:
                logger.debug("Config version '{}' is invalid".format(version))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        handle = connection.handle
        with self._recoveries_lock:
            attempt = self._recoveries.get(id(handle))
        if attempt is not None:
            attempt.done.set()
            handle.shutdown()
        else:
            handle.close()

    def _replace_recovered_connection(self, connection):
        connection.handle = None
        connection.state = 'init'
        connection.transaction_open = False
        self.open(connection)

    def add_query(self, sql, auto_begin=True, bindings=None, abridge_sql_log=False,
                  retryable_exceptions=tuple(), retry_limit=1):
        match = overwrite_insert_match(sql)
        if match is None:
            return super().add_query(sql, auto_begin, bindings, abridge_sql_log,
                                     retryable_exceptions, retry_limit)
        if bindings is not None:
            raise dbt_common.exceptions.DbtRuntimeError(
                "INSERT OVERWRITE response recovery does not support query bindings"
            )

        connection = self.get_thread_connection()
        credentials = self.get_credentials(connection.credentials)
        if credentials.is_async:
            raise dbt_common.exceptions.DbtRuntimeError(
                "INSERT OVERWRITE response recovery is incompatible with is_async=true"
            )
        if credentials.use_pure not in ("true", "True"):
            raise dbt_common.exceptions.DbtRuntimeError(
                "INSERT OVERWRITE response recovery requires use_pure=true"
            )
        if retryable_exceptions or retry_limit != 1:
            raise dbt_common.exceptions.DbtRuntimeError(
                "INSERT OVERWRITE response recovery cannot retry the INSERT"
            )

        attempt = RecoveryObserver(credentials, connection.handle)
        try:
            marked_sql = mark_overwrite_insert(sql, match, attempt.attempt_id)
        except ValueError as exc:
            raise dbt_common.exceptions.DbtRuntimeError(str(exc)) from exc
        self._recovery_local.attempt = attempt
        logger.info(f"Starting INSERT OVERWRITE response recovery attempt {attempt.attempt_id}")
        with self._recoveries_lock:
            self._recoveries[id(attempt.old_handle)] = attempt
        stopped = False
        try:
            attempt.start()
            try:
                result = super().add_query(marked_sql, auto_begin, bindings,
                                           abridge_sql_log, retryable_exceptions,
                                           retry_limit)
            except _RecoveredOverwriteInsert:
                attempt.stop()
                stopped = True
                self._replace_recovered_connection(connection)
                logger.info(
                    f"Recovered INSERT OVERWRITE attempt {attempt.attempt_id} "
                    f"with profile {attempt.finished_query_id}"
                )
                return connection, _RecoveredCursor(attempt.finished_query_id)
            attempt.stop()
            stopped = True
            if attempt.interrupted.is_set():
                # The normal response can race with the 30-second grace timer.
                # Never hand the next model a handle the observer shut down.
                self._replace_recovered_connection(connection)
            return result
        finally:
            try:
                if not stopped:
                    attempt.stop()
            finally:
                self._recovery_local.attempt = None
                with self._recoveries_lock:
                    self._recoveries.pop(id(attempt.old_handle), None)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except (mysql.connector.InterfaceError, mysql.connector.OperationalError) as e:
            attempt = getattr(self._recovery_local, 'attempt', None)
            is_transport_error = isinstance(e, mysql.connector.InterfaceError) or e.errno in (2006, 2013, 2055, 3024)
            if attempt is not None:
                if is_transport_error and attempt.interrupted.is_set() and attempt.finished_query_id:
                    raise _RecoveredOverwriteInsert() from e
                # Any OperationalError can mean a broken transport. Never try
                # rollback or QUIT on this handle; only known transport errors
                # with a confirmed Finished profile may become success.
                attempt.old_handle.shutdown()
                connection = self.get_thread_connection()
                connection.handle = None
                connection.state = 'fail'
                connection.transaction_open = False
                raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e
            logger.debug('StarRocks transport error: {}'.format(str(e)))
            self.rollback_if_open()
            raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e

        except mysql.connector.DatabaseError as e:
            logger.debug('StarRocks error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(str(e)) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        if isinstance(cursor, _RecoveredCursor):
            return AdapterResponse(
                _message=f"SUCCESS recovered query_id={cursor.query_id} rows=unknown",
                rows_affected=None,
                code="SUCCESS",
                query_id=cursor.query_id,
            )
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            _message="{} {}".format(code, num_rows),
            rows_affected=num_rows,
            code=code
        )

    def add_begin_query(self):
        return self.add_query("", auto_begin=False)


class _RecoveredOverwriteInsert(Exception):
    pass


class _RecoveredCursor:
    rowcount = None

    def __init__(self, query_id):
        self.query_id = query_id

    def close(self):
        pass
