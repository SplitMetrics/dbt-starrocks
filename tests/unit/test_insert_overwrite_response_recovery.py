"""Fail-closed tests for catalog-qualified INSERT OVERWRITE recovery."""

import importlib.util
import sys
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import mysql.connector
import mysql.connector.errors

import dbt_common.exceptions
from dbt.adapters.sql import SQLConnectionManager


ADAPTER_DIR = Path(__file__).resolve().parents[2] / "dbt/adapters/starrocks"
EXTRACTED_INSERT = (
    "insert /*+SET_VAR(dynamic_overwrite = TRUE, "
    "new_planner_optimize_timeout = 30000, query_timeout = 3600, "
    "insert_timeout = 5400, query_mem_limit = 12884901888)*/ "
    "overwrite `glue`.`silver`.`rawevents_extracted` select 1"
)
OBT_INSERT = EXTRACTED_INSERT.replace("rawevents_extracted", "rawevents_obt")
GOLD_INSERT = EXTRACTED_INSERT.replace(
    "`glue`.`silver`.`rawevents_extracted`", "`default_catalog`.`mmp`.`campaign_data`"
)


def _load_adapter_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ADAPTER_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# These names collide with the real package modules. Loading them here would
# otherwise leak a second, distinct StarRocksConnectionManager class into
# sys.modules for every test module collected afterward. Snapshot whatever
# was there before (an already-imported real module, or nothing) and restore
# it once this file's tests are done.
_ORIGINAL_MODULES = {
    name: sys.modules.get(name)
    for name in (
        "dbt.adapters.starrocks.insert_overwrite_response_recovery",
        "dbt.adapters.starrocks.connections",
    )
}


def tearDownModule():
    for name, original in _ORIGINAL_MODULES.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


recovery = _load_adapter_module(
    "dbt.adapters.starrocks.insert_overwrite_response_recovery",
    "insert_overwrite_response_recovery.py",
)
connections = _load_adapter_module(
    "dbt.adapters.starrocks.connections", "connections.py"
)


class FakeObserver:
    instances = []

    def __init__(self, credentials, old_handle):
        self.credentials = credentials
        self.old_handle = old_handle
        self.attempt_id = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa"
        self.done = threading.Event()
        self.interrupted = threading.Event()
        self.finished_query_id = None
        self.start = mock.Mock()
        self.stop = mock.Mock()
        self.instances.append(self)


class RecoveryConnectionTest(unittest.TestCase):
    def setUp(self):
        FakeObserver.instances = []
        self.old_handle = mock.Mock()
        self.credentials = SimpleNamespace(use_pure="true", is_async=False)
        self.connection = SimpleNamespace(
            name="model.silver_extracted",
            credentials=self.credentials,
            handle=self.old_handle,
            state="open",
            transaction_open=False,
        )
        self.manager = object.__new__(connections.StarRocksConnectionManager)
        self.manager.get_thread_connection = mock.Mock(return_value=self.connection)
        self.manager.rollback_if_open = mock.Mock()
        self.manager.open = mock.Mock(side_effect=self._open_new_connection)

    def _open_new_connection(self, connection):
        connection.handle = mock.Mock()
        connection.state = "open"
        return connection

    def test_unrelated_sql_uses_original_path(self):
        cursor = SimpleNamespace(rowcount=1)
        with mock.patch.object(SQLConnectionManager, "add_query", return_value=(self.connection, cursor)) as parent:
            result = self.manager.add_query("select 1", auto_begin=False)
        self.assertIs(result[1], cursor)
        self.assertEqual("select 1", parent.call_args.args[0])
        self.manager.open.assert_not_called()

    def test_normal_extracted_response_never_reconnects(self):
        cursor = SimpleNamespace(rowcount=8)
        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", return_value=(self.connection, cursor)
        ) as parent:
            result = self.manager.add_query(
                EXTRACTED_INSERT, auto_begin=False
            )
        self.assertIs(result[1], cursor)
        self.assertTrue(parent.call_args.args[0].startswith("/* mmp_overwrite_attempt:"))
        self.assertIn("dynamic_overwrite = TRUE", parent.call_args.args[0])
        self.assertIn("enable_profile = TRUE", parent.call_args.args[0])
        FakeObserver.instances[0].start.assert_called_once()
        FakeObserver.instances[0].stop.assert_called_once()
        self.manager.open.assert_not_called()

    def test_finished_and_interrupted_reconnects_without_second_insert(self):
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
        calls = []

        def blocked_insert(_manager, sql, *args):
            calls.append(sql)
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = query_id
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.InterfaceError(errno=2013, msg="lost response")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", blocked_insert
        ):
            _, cursor = self.manager.add_query(
                EXTRACTED_INSERT, auto_begin=False
            )

        self.assertEqual(1, len(calls))
        self.assertEqual(query_id, cursor.query_id)
        response = self.manager.get_response(cursor)
        self.assertEqual("SUCCESS", response.code)
        self.assertIsNone(response.rows_affected)
        self.assertEqual(query_id, response.query_id)
        self.manager.rollback_if_open.assert_not_called()
        self.manager.open.assert_called_once_with(self.connection)
        self.assertEqual("open", self.connection.state)
        next_cursor = SimpleNamespace(rowcount=1)
        with mock.patch.object(
            SQLConnectionManager, "add_query", return_value=(self.connection, next_cursor)
        ) as parent:
            self.assertIs(
                next_cursor,
                self.manager.add_query("select 1", auto_begin=False)[1],
            )
        self.assertEqual("select 1", parent.call_args.args[0])

    def test_real_dbt_add_query_path_continues_to_next_query(self):
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
        sent_sql = []
        old_cursor = mock.Mock()

        def lose_response(sql, bindings):
            sent_sql.append(sql)
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = query_id
            attempt.interrupted.set()
            raise mysql.connector.InterfaceError(errno=2013, msg="lost response")

        old_cursor.execute.side_effect = lose_response
        self.old_handle.cursor.return_value = old_cursor
        with mock.patch.object(connections, "RecoveryObserver", FakeObserver):
            _, recovered_cursor = self.manager.add_query(EXTRACTED_INSERT, auto_begin=False)
        self.assertEqual(query_id, recovered_cursor.query_id)
        self.assertEqual(1, len(sent_sql))
        self.assertTrue(sent_sql[0].startswith("/* mmp_overwrite_attempt:"))
        self.assertIn("dynamic_overwrite = TRUE", sent_sql[0])
        self.assertIn("enable_profile = TRUE", sent_sql[0])
        self.manager.rollback_if_open.assert_not_called()

        next_cursor = mock.Mock()
        next_cursor.rowcount = 1
        self.connection.handle.cursor.return_value = next_cursor
        _, returned_cursor = self.manager.add_query("select 1", auto_begin=False)
        self.assertIs(next_cursor, returned_cursor)
        next_cursor.execute.assert_called_once_with("select 1", None)

    def test_obt_finished_and_interrupted_recovers_without_second_insert(self):
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
        sent_sql = []

        def lose_response(_manager, sql, *args):
            sent_sql.append(sql)
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = query_id
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.InterfaceError(errno=2013, msg="lost OBT response")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", lose_response
        ):
            _, cursor = self.manager.add_query(OBT_INSERT, auto_begin=False)

        self.assertEqual(1, len(sent_sql))
        self.assertIn("`rawevents_obt`", sent_sql[0])
        self.assertEqual(query_id, cursor.query_id)
        self.assertIsNone(self.manager.get_response(cursor).rows_affected)
        self.manager.open.assert_called_once_with(self.connection)
        self.manager.rollback_if_open.assert_not_called()

    def test_normal_response_racing_shutdown_reconnects(self):
        cursor = SimpleNamespace(rowcount=8)

        def successful_insert(_manager, sql, *args):
            FakeObserver.instances[0].interrupted.set()
            return self.connection, cursor

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", successful_insert
        ):
            result = self.manager.add_query(
                EXTRACTED_INSERT, auto_begin=False
            )
        self.assertIs(cursor, result[1])
        self.manager.open.assert_called_once_with(self.connection)

    def test_cancellation_stops_observer_and_shuts_down_old_handle(self):
        def canceled_insert(_manager, sql, *args):
            self.manager.cancel(self.connection)
            with self.manager.exception_handler(sql):
                raise mysql.connector.InterfaceError(errno=2013, msg="canceled")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", canceled_insert
        ):
            with self.assertRaisesRegex(Exception, "canceled"):
                self.manager.add_query(
                    EXTRACTED_INSERT, auto_begin=False
                )
        self.assertTrue(FakeObserver.instances[0].done.is_set())
        self.old_handle.shutdown.assert_called()
        self.manager.open.assert_not_called()

    def test_transport_error_without_finished_profile_fails_without_rollback(self):
        def broken_insert(_manager, sql, *args):
            with self.manager.exception_handler(sql):
                raise mysql.connector.InterfaceError(errno=2013, msg="lost response")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", broken_insert
        ):
            with self.assertRaisesRegex(Exception, "lost response"):
                self.manager.add_query(
                    EXTRACTED_INSERT, auto_begin=False
                )

        self.assertEqual("fail", self.connection.state)
        self.assertIsNone(self.connection.handle)
        self.old_handle.shutdown.assert_called_once()
        self.manager.rollback_if_open.assert_not_called()
        self.manager.open.assert_not_called()

    def test_operational_error_2006_without_profile_skips_rollback(self):
        def broken_insert(_manager, sql, *args):
            with self.manager.exception_handler(sql):
                raise mysql.connector.OperationalError(errno=2006, msg="server has gone away")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", broken_insert
        ):
            with self.assertRaisesRegex(Exception, "server has gone away"):
                self.manager.add_query(EXTRACTED_INSERT, auto_begin=False)

        self.old_handle.shutdown.assert_called_once()
        self.manager.rollback_if_open.assert_not_called()
        self.assertEqual("fail", self.connection.state)
        self.assertIsNone(self.connection.handle)

    def test_operational_error_2006_after_confirmed_shutdown_recovers(self):
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"

        def interrupted_insert(_manager, sql, *args):
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = query_id
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.OperationalError(errno=2006, msg="server has gone away")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", interrupted_insert
        ):
            _, cursor = self.manager.add_query(EXTRACTED_INSERT, auto_begin=False)

        self.assertEqual(query_id, cursor.query_id)
        self.manager.rollback_if_open.assert_not_called()
        self.manager.open.assert_called_once_with(self.connection)

    def test_other_operational_error_never_becomes_recovered_success(self):
        def failed_insert(_manager, sql, *args):
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.OperationalError(errno=1040, msg="too many connections")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", failed_insert
        ):
            with self.assertRaisesRegex(Exception, "too many connections"):
                self.manager.add_query(EXTRACTED_INSERT, auto_begin=False)

        # errno 1040 is a genuine server-side error on a healthy socket, not
        # a transport failure: the connection must not be torn down for it.
        self.old_handle.shutdown.assert_not_called()
        self.manager.rollback_if_open.assert_called_once()
        self.manager.open.assert_not_called()
        self.assertEqual("open", self.connection.state)

    def test_read_timeout_error_with_finished_profile_recovers(self):
        # ReadTimeoutError (errno 3024) is the client's own read-timeout on a
        # wedged socket. It subclasses Error directly, not
        # InterfaceError/OperationalError, so it needs its own explicit match.
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"

        def blocked_insert(_manager, sql, *args):
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = query_id
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.errors.ReadTimeoutError(
                    errno=3024, msg="Timed out reading from socket"
                )

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", blocked_insert
        ):
            _, cursor = self.manager.add_query(EXTRACTED_INSERT, auto_begin=False)

        self.assertEqual(query_id, cursor.query_id)
        self.manager.rollback_if_open.assert_not_called()
        self.manager.open.assert_called_once_with(self.connection)
        self.assertEqual("open", self.connection.state)

    def test_operational_error_without_active_attempt_stays_database_error(self):
        # Outside a recovery attempt, OperationalError must keep its normal
        # DbtDatabaseError classification for every other model in the
        # adapter, not fall into the recovery-only DbtRuntimeError path.
        with self.assertRaises(dbt_common.exceptions.DbtDatabaseError):
            with self.manager.exception_handler("select 1"):
                raise mysql.connector.OperationalError(
                    errno=1205, msg="lock wait timeout exceeded"
                )
        self.manager.rollback_if_open.assert_called_once()

    def test_server_error_is_not_replaced_by_finished_profile(self):
        def failed_insert(_manager, sql, *args):
            attempt = FakeObserver.instances[0]
            attempt.finished_query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
            attempt.interrupted.set()
            with self.manager.exception_handler(sql):
                raise mysql.connector.DatabaseError(errno=1064, msg="server rejected INSERT")

        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", failed_insert
        ):
            with self.assertRaisesRegex(Exception, "server rejected INSERT"):
                self.manager.add_query(
                    EXTRACTED_INSERT, auto_begin=False
                )
        self.manager.open.assert_not_called()
        self.assertEqual("open", self.connection.state)

    def test_async_and_non_pure_modes_are_rejected_before_insert(self):
        for field, value in (("is_async", True), ("use_pure", "false")):
            with self.subTest(field=field):
                setattr(self.credentials, field, value)
                with self.assertRaises(Exception):
                    self.manager.add_query(EXTRACTED_INSERT)
                setattr(self.credentials, field, False if field == "is_async" else "true")

    def test_all_catalog_qualified_overwrite_targets_are_observed(self):
        cursor = SimpleNamespace(rowcount=1)
        with mock.patch.object(connections, "RecoveryObserver", FakeObserver), mock.patch.object(
            SQLConnectionManager, "add_query", return_value=(self.connection, cursor)
        ) as parent:
            for statement in (EXTRACTED_INSERT, OBT_INSERT, GOLD_INSERT):
                with self.subTest(statement=statement):
                    self.assertIs(cursor, self.manager.add_query(statement, auto_begin=False)[1])
                    self.assertIn("enable_profile = TRUE", parent.call_args.args[0])
        self.assertEqual(3, parent.call_count)
        self.assertEqual(3, len(FakeObserver.instances))

    def test_other_sql_shapes_use_original_path(self):
        cursor = SimpleNamespace(rowcount=1)
        statements = (
            "insert overwrite `glue`.`silver`.`rawevents_extracted` select 1",
            EXTRACTED_INSERT.replace("`glue`.`silver`.`rawevents_extracted`", "`silver`.`rawevents_extracted`"),
            EXTRACTED_INSERT.replace("overwrite", "into"),
            EXTRACTED_INSERT.replace("query_mem_limit = 12884901888", "other_setting = 1"),
            "create table `glue`.`silver`.`new_table` as select 1",
            "select '" + EXTRACTED_INSERT + "'",
        )
        with mock.patch.object(SQLConnectionManager, "add_query", return_value=(self.connection, cursor)) as parent:
            for statement in statements:
                with self.subTest(statement=statement):
                    self.assertIs(cursor, self.manager.add_query(statement, auto_begin=False)[1])
                    self.assertEqual(statement, parent.call_args.args[0])
        self.assertEqual(len(statements), parent.call_count)

    def test_dbt_query_comment_before_insert_is_observed(self):
        sql = '/* {"app": "dbt"} */\n' + OBT_INSERT
        match = recovery.overwrite_insert_match(sql)
        self.assertIsNotNone(match)
        marked = recovery.mark_overwrite_insert(sql, match, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")
        self.assertTrue(marked.startswith("/* mmp_overwrite_attempt:"))
        self.assertIn("enable_profile = TRUE", marked)

    def test_existing_profile_setting_is_not_duplicated(self):
        sql = EXTRACTED_INSERT.replace("dynamic_overwrite = TRUE", "dynamic_overwrite = TRUE, enable_profile = TRUE")
        match = recovery.overwrite_insert_match(sql)
        marked = recovery.mark_overwrite_insert(sql, match, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")
        self.assertEqual(1, marked.count("enable_profile = TRUE"))

    def test_conflicting_profile_setting_fails_before_insert(self):
        sql = EXTRACTED_INSERT.replace("dynamic_overwrite = TRUE", "dynamic_overwrite = TRUE, enable_profile = FALSE")
        with mock.patch.object(SQLConnectionManager, "add_query") as parent:
            with self.assertRaisesRegex(Exception, "requires enable_profile"):
                self.manager.add_query(sql, auto_begin=False)
        parent.assert_not_called()


class ProfileIdentificationTest(unittest.TestCase):
    def test_missing_and_foreign_profiles_do_not_match(self):
        observer = recovery.RecoveryObserver(SimpleNamespace(), mock.Mock())
        cursor = mock.Mock()
        connection = mock.Mock()
        connection.cursor.return_value = cursor
        cursor.fetchall.return_value = []
        self.assertIsNone(observer._matching_profile(connection))
        cursor.fetchall.return_value = [{
            "QueryId": "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb",
            "Statement": "insert overwrite unrelated select 1",
        }]
        self.assertIsNone(observer._matching_profile(connection))
        self.assertEqual(2, connection.cursor.call_count)

    def test_running_profile_does_not_fetch_full_profile(self):
        observer = recovery.RecoveryObserver(SimpleNamespace(), mock.Mock())
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
        cursor = mock.Mock()
        cursor.fetchall.return_value = [{
            "QueryId": query_id,
            "State": "Running",
            "Statement": f"/* {observer.marker} */ insert overwrite t select 1",
        }]
        connection = mock.Mock()
        connection.cursor.return_value = cursor
        self.assertEqual((query_id, "Running"), observer._matching_profile(connection))
        connection.cursor.assert_called_once_with(dictionary=True)

    def test_error_profile_never_interrupts_insert(self):
        old_handle = mock.Mock()
        observer = recovery.RecoveryObserver(SimpleNamespace(), old_handle)
        observer._connect = mock.Mock(return_value=mock.Mock())
        observer._matching_profile = mock.Mock(return_value=(
            "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb", "Error"
        ))
        observer.start()
        observer.thread.join(1)
        self.assertFalse(observer.thread.is_alive())
        self.assertIsNone(observer.finished_query_id)
        old_handle.shutdown.assert_not_called()

    def test_observer_connection_failure_never_interrupts_insert(self):
        old_handle = mock.Mock()
        observer = recovery.RecoveryObserver(SimpleNamespace(), old_handle)
        observer._connect = mock.Mock(side_effect=ConnectionError("unavailable"))
        observer.start()
        observer.thread.join(1)
        self.assertFalse(observer.thread.is_alive())
        self.assertIsInstance(observer.error, ConnectionError)
        self.assertIsNone(observer.finished_query_id)
        old_handle.shutdown.assert_not_called()

    def test_finished_profile_interrupts_only_after_grace(self):
        old_handle = mock.Mock()
        observer = recovery.RecoveryObserver(SimpleNamespace(), old_handle)
        observer._connect = mock.Mock(return_value=mock.Mock())
        observer._matching_profile = mock.Mock(return_value=(
            "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb", "Finished"
        ))
        with mock.patch.object(recovery, "RESPONSE_GRACE_SECONDS", 0.02):
            observer.start()
            observer.thread.join(1)
        self.assertFalse(observer.thread.is_alive())
        self.assertTrue(observer.interrupted.is_set())
        old_handle.shutdown.assert_called_once()

    def test_transient_connection_error_is_retried_not_fatal(self):
        old_handle = mock.Mock()
        observer = recovery.RecoveryObserver(SimpleNamespace(), old_handle)
        observer._connect = mock.Mock(
            side_effect=[
                mysql.connector.OperationalError(errno=2003, msg="cannot connect"),
                mock.Mock(),
            ]
        )
        observer._matching_profile = mock.Mock(return_value=(
            "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb", "Finished"
        ))
        with mock.patch.object(recovery, "RESPONSE_GRACE_SECONDS", 0.02), \
                mock.patch.object(recovery, "POLL_SECONDS", 0.01):
            observer.start()
            observer.thread.join(1)
        self.assertFalse(observer.thread.is_alive())
        # A blip on the observer's own connection is retried, not fatal to
        # observation of the real INSERT.
        self.assertEqual(2, observer._connect.call_count)
        self.assertIsInstance(observer.error, mysql.connector.OperationalError)
        self.assertTrue(observer.interrupted.is_set())
        old_handle.shutdown.assert_called_once()

    def test_normal_completion_stops_observer_before_shutdown(self):
        old_handle = mock.Mock()
        observer = recovery.RecoveryObserver(SimpleNamespace(), old_handle)
        observer._connect = mock.Mock(return_value=mock.Mock())
        observer._matching_profile = mock.Mock(return_value=(
            "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb", "Finished"
        ))
        with mock.patch.object(recovery, "RESPONSE_GRACE_SECONDS", 1):
            observer.start()
            deadline = time.monotonic() + 1
            while observer.finished_query_id is None and time.monotonic() < deadline:
                time.sleep(0.001)
            self.assertIsNotNone(observer.finished_query_id)
            observer.stop()
        old_handle.shutdown.assert_not_called()

    def test_requires_matching_statement_and_profile_id(self):
        observer = recovery.RecoveryObserver(SimpleNamespace(), mock.Mock())
        query_id = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbbb"
        list_cursor = mock.Mock()
        list_cursor.fetchall.return_value = [{
            "QueryId": query_id,
            "Statement": f"/* {observer.marker} */ insert overwrite t select 1",
        }]
        profile_cursor = mock.Mock()
        profile_cursor.fetchone.return_value = (
            "Query:\n  Summary:\n"
            f"    - Query ID: {query_id}\n"
            "    - Query State: Finished\n"
            f"    - Sql Statement: /* {observer.marker} */ insert overwrite t select 1\n",
        )
        connection = mock.Mock()
        connection.cursor.side_effect = [list_cursor, profile_cursor]
        self.assertEqual((query_id, "Finished"), observer._matching_profile(connection))

        profile_cursor.fetchone.return_value = (
            "Query:\n  Summary:\n"
            f"    - Query ID: {query_id}\n"
            "    - Query State: Finished\n"
            "    - Sql Statement: insert overwrite t select 1\n",
        )
        connection.cursor.side_effect = [list_cursor, profile_cursor]
        with self.assertRaisesRegex(RuntimeError, "attempt marker"):
            observer._matching_profile(connection)

    def test_stop_never_raises_when_thread_outlives_join_budget(self):
        # stop() runs from add_query's finally block: it must never raise,
        # or it would shadow a real in-flight exception, or fail a model
        # whose INSERT actually succeeded, just because the observer thread
        # is still finishing a slow network call.
        observer = recovery.RecoveryObserver(SimpleNamespace(), mock.Mock())
        observer.thread = SimpleNamespace(join=mock.Mock(), is_alive=mock.Mock(return_value=True))
        observer.stop()  # must not raise


if __name__ == "__main__":
    unittest.main()
