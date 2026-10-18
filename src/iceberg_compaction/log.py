import json
import logging
import logging.config
import traceback
from datetime import UTC, datetime
from typing import Any


class Logger:
    def __init__(self, logger_name: str, compaction_phase: str):
        self.logger_name = logger_name
        self.compaction_phase = compaction_phase
        self._logger = logging.getLogger(logger_name)

    def debug(self, message: str, *, table: str, **fields: Any) -> None:
        self._log(logging.DEBUG, message, table, fields)

    def info(self, message: str, *, table: str, **fields: Any) -> None:
        self._log(logging.INFO, message, table, fields)

    def warning(self, message: str, *, table: str, **fields: Any) -> None:
        self._log(logging.WARNING, message, table, fields)

    def error(self, message: str, *, table: str, **fields: Any) -> None:
        self._log(logging.ERROR, message, table, fields)

    def exception(self, message: str, *, table: str, **fields: Any) -> None:
        """Log at ERROR with the traceback of the exception being handled."""
        self._log(logging.ERROR, message, table, fields, exc_info=True)

    def _log(self, level: int, message: str, table: str, fields: dict[str, Any], exc_info: bool = False) -> None:
        if not self._logger.isEnabledFor(level):
            return
        event = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": logging.getLevelName(level),
            "logger": self.logger_name,
            "compaction_phase": self.compaction_phase,
            "table": table,
            "message": message,
            **fields,
        }
        if exc_info:
            event["exception"] = traceback.format_exc()
        self._logger.log(level, json.dumps(event, default=str))


class LoggingMixin:

    log: Logger

    def __init_subclass__(cls, compaction_phase: str, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.log = Logger(f"{cls.__module__}.{cls.__qualname__}", compaction_phase)


DEFAULT_LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"format": "%(message)s"}},  # Logger already writes the whole JSON event
    "handlers": {"stderr": {"class": "logging.StreamHandler", "formatter": "json"}},
    "loggers": {
        "iceberg_compaction": {
            "handlers": ["stderr"],
            "level": "INFO",
            "propagate": False,
        },
    },
}


def configure_logging(config: dict[str, Any] = DEFAULT_LOGGING_CONFIG) -> None:
    logging.config.dictConfig(config)


configure_logging()
