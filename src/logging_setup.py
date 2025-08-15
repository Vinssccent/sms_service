# src/logging_setup.py
# -*- coding: utf-8 -*-
import logging
from typing import Any, Mapping, MutableMapping, Optional

from rich.logging import RichHandler

try:
    # опционально читаем настройки, чтобы знать LOG_SQL
    from src.settings import settings
    _LOG_SQL = bool(getattr(settings, "LOG_SQL", False))
except Exception:
    _LOG_SQL = False


class ConnAdapter(logging.LoggerAdapter):
    """
    Добавляет в лог компактный контекст соединения (ip:port).
    Пример: [127.0.0.1:54321] OUR: submit_sm ...
    """
    def process(self, msg: str, kwargs: MutableMapping[str, Any]):
        extra: MutableMapping[str, Any] = kwargs.setdefault("extra", {})
        ctx_parts = []
        conn = self.extra.get("conn")
        if conn:
            ctx_parts.append(f"[dim][{conn}][/]")
        if ctx_parts:
            msg = f"{' '.join(ctx_parts)} {msg}"
        return msg, kwargs


def _silence_third_party(log_sql: bool = False) -> None:
    """
    Глушим «простыни» от SQLAlchemy и uvicorn.
    SQL-запросы показываем только если log_sql=True.
    """
    # SQLAlchemy огромные INFO/DEBUG про мапперы/пулы
    for name in ("sqlalchemy", "sqlalchemy.orm", "sqlalchemy.orm.mapper", "sqlalchemy.pool"):
        logging.getLogger(name).setLevel(logging.WARNING)

    # сами SQL-запросы
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if log_sql else logging.WARNING
    )

    # uvicorn шумит в access/error — приглушим
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.WARNING)


def setup_logging(level: int = logging.INFO) -> None:
    """
    Единая настройка логирования для всего проекта.
    """
    # Базовый формат — только сообщение; дату/уровень/цвет даёт RichHandler
    fmt = "%(message)s"
    datefmt = "[%m/%d/%y %H:%M:%S]"

    # Очищаем корневые хендлеры, если они уже были выставлены
    root = logging.getLogger()
    if root.handlers:
        for h in list(root.handlers):
            root.removeHandler(h)

    handler = RichHandler(
        rich_tracebacks=True,
        tracebacks_show_locals=False,
        show_path=False,
        markup=True,     # поддержка [green]...[/] в сообщениях
    )
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt=datefmt,
        handlers=[handler],
    )

    _silence_third_party(_LOG_SQL)
