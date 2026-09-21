"""Observe one marked Iceberg INSERT without sending it a second time."""

import re
import threading
import time
import uuid

import mysql.connector

from dbt.adapters.events.logging import AdapterLogger


logger = AdapterLogger("starrocks")

ATTEMPT_PREFIX = "mmp_iceberg_attempt:"
POLL_SECONDS = 15
RESPONSE_GRACE_SECONDS = 30
OBSERVER_TIMEOUT_SECONDS = 10
MAX_OBSERVE_SECONDS = 7200
EXTRACTED_INSERT = re.compile(
    r"(?is)\binsert\s*/\*\+\s*set_var\((?P<set_vars>[^)]*)\)\s*\*/"
    r"\s*overwrite\s+(?P<target>[^\s(]+)"
)
PROFILE_SETTING = re.compile(r"(?i)(?:^|,)\s*enable_profile\s*=\s*([^,\s]+)")


def extracted_insert_match(sql):
    """Match only the existing rawevents_extracted INSERT OVERWRITE shape."""
    match = EXTRACTED_INSERT.search(sql)
    if match is None:
        return None
    target_parts = match.group("target").replace("`", "").split(".")
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in target_parts):
        return None
    if target_parts[-1].lower() != "rawevents_extracted":
        return None
    return match


def mark_extracted_insert(sql, match, attempt_id):
    """Enable this INSERT's profile and prepend a unique attempt marker."""
    set_vars = match.group("set_vars")
    profile_setting = PROFILE_SETTING.search(set_vars)
    if profile_setting is not None:
        if profile_setting.group(1).lower() != "true":
            raise ValueError("rawevents_extracted recovery requires enable_profile = TRUE")
    else:
        separator = ", " if set_vars.strip() else ""
        sql = (
            sql[:match.end("set_vars")]
            + separator + "enable_profile = TRUE"
            + sql[match.end("set_vars"):]
        )
    return f"/* {ATTEMPT_PREFIX}{attempt_id} */\n{sql}"


class RecoveryObserver:
    def __init__(self, credentials, old_handle):
        self.credentials = credentials
        self.old_handle = old_handle
        self.attempt_id = str(uuid.uuid4())
        self.marker = f"{ATTEMPT_PREFIX}{self.attempt_id}"
        self.done = threading.Event()
        self.interrupted = threading.Event()
        self.finished_query_id = None
        self.observed_state = None
        self.error = None
        self.thread = threading.Thread(
            target=self._watch,
            name=f"starrocks-iceberg-recovery-{self.attempt_id}",
            daemon=True,
        )

    def start(self):
        self.thread.start()

    def stop(self):
        self.done.set()
        self.thread.join(2 * OBSERVER_TIMEOUT_SECONDS + 5)
        if self.thread.is_alive():
            raise RuntimeError("Iceberg recovery observer did not stop")

    def _connect(self):
        kwargs = {
            "host": self.credentials.host,
            "username": self.credentials.username,
            "password": self.credentials.password,
            "auth_plugin": self.credentials.auth_plugin,
            "buffered": True,
            "use_pure": True,
            "connection_timeout": OBSERVER_TIMEOUT_SECONDS,
            "read_timeout": OBSERVER_TIMEOUT_SECONDS,
            "write_timeout": OBSERVER_TIMEOUT_SECONDS,
        }
        if self.credentials.port:
            kwargs["port"] = self.credentials.port
        return mysql.connector.connect(**kwargs)

    def _matching_profile(self, connection):
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute("SHOW PROFILELIST")
            rows = cursor.fetchall()
        finally:
            cursor.close()

        matches = [
            row for row in rows
            if self.marker in (row.get("Statement") or "")
        ]
        if len(matches) > 1:
            raise RuntimeError("Multiple profiles matched the Iceberg attempt")
        if not matches:
            return None

        query_id = str(matches[0]["QueryId"])
        uuid.UUID(query_id)
        list_state = matches[0].get("State")
        if list_state in ("Running", "Error"):
            return query_id, list_state
        cursor = connection.cursor()
        try:
            cursor.execute("select get_query_profile(%s)", (query_id,))
            row = cursor.fetchone()
        finally:
            cursor.close()
        profile = row[0] if row else None
        if not profile:
            return None

        profile_id = re.search(r"(?m)^\s*- Query ID: ([0-9a-f-]+)\s*$", profile)
        state = re.search(r"(?m)^\s*- Query State: (Finished|Running|Error)\s*$", profile)
        statement = re.search(r"(?m)^\s*- Sql Statement: (.*)$", profile)
        if not profile_id or profile_id.group(1) != query_id:
            raise RuntimeError("Iceberg profile Query ID did not match PROFILELIST")
        if not statement or self.marker not in statement.group(1):
            raise RuntimeError("Iceberg profile did not contain the attempt marker")
        if not state:
            raise RuntimeError("Iceberg profile has no recognized Query State")
        return query_id, state.group(1)

    def _watch(self):
        connection = None
        started = time.monotonic()
        try:
            connection = self._connect()
            while not self.done.is_set() and time.monotonic() - started < MAX_OBSERVE_SECONDS:
                found = self._matching_profile(connection)
                if found:
                    query_id, state = found
                    self.observed_state = state
                    logger.info(
                        f"Iceberg attempt {self.attempt_id} profile {query_id} state {state}"
                    )
                    if state == "Finished":
                        self.finished_query_id = query_id
                        if not self.done.wait(RESPONSE_GRACE_SECONDS):
                            self.interrupted.set()
                            self.old_handle.shutdown()
                        return
                    if state == "Error":
                        return
                self.done.wait(POLL_SECONDS)
        except Exception as exc:
            self.error = exc
            logger.warning(f"Iceberg attempt {self.attempt_id} observer failed: {exc}")
        finally:
            if connection is not None:
                connection.shutdown()
