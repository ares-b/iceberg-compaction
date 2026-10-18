import json
import logging

from iceberg_compaction.log import LoggingMixin


class Job(LoggingMixin, compaction_phase="plan"):
    pass


def test_logs_one_json_event(caplog):
    with caplog.at_level(logging.INFO):
        Job.log.info("Planned", table="db.events", files=3)

    event = json.loads(caplog.messages[0])
    del event["timestamp"]
    assert event == {"level": "INFO", "logger": "test_log.Job", "compaction_phase": "plan", "table": "db.events", "message": "Planned", "files": 3}


def test_exception_keeps_the_traceback_in_the_json(caplog):
    try:
        raise ValueError("test")
    except ValueError:
        Job.log.exception("Failed", table="db.events")

    assert "ValueError: test" in json.loads(caplog.messages[0])["exception"]
