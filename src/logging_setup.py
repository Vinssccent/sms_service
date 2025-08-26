# src/logging_setup.py
# -*- coding: utf-8 -*-
import logging
import sys
import os
import re

__all__ = ["setup_logging", "ConnAdapter"]

# Удаляем разметку [green]...[/] когда пишем в plain
_MARKUP_RE = re.compile(r"\[(\/)?[a-zA-Z0-9#,+\-\.: ]+\]")

def _strip_markup(s: str) -> str:
    return _MARKUP_RE.sub("", s)

class ConnAdapter(logging.LoggerAdapter):
    """Adapter для добавления информации о соединении (conn=id) в extra."""
    def process(self, msg, kwargs):
        extra = kwargs.get("extra", {}) or {}
        extra.setdefault("conn", self.extra.get("conn", "-"))
        kwargs["extra"] = extra
        return msg, kwargs

class EnsureConnFilter(logging.Filter):
    """Гарантирует наличие record.conn для любых логов (uvicorn, sqlalchemy, etc.)."""
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "conn"):
            setattr(record, "conn", "-")
        return True

class OneLineFormatter(logging.Formatter):
    """Однострочный форматтер + безопасное удаление разметки и переводов строк."""
    def __init__(self, fmt: str, datefmt: str, strip_markup: bool):
        super().__init__(fmt=fmt, datefmt=datefmt)
        self.strip_markup = strip_markup
    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "conn"):
            setattr(record, "conn", "-")
        if self.strip_markup and isinstance(record.msg, str):
            record.msg = _strip_markup(record.msg)
        out = super().format(record)
        return out.replace("\r", " ").replace("\n", " | ")

class _SQLFilter(logging.Filter):
    """Фильтруем лишнее от SQLAlchemy. sql_mode: 0/off | short | full."""
    def __init__(self, mode: str):
        super().__init__()
        self.mode = (mode or "").lower()
    def filter(self, record: logging.LogRecord) -> bool:
        name = record.name or ""
        if not name.startswith("sqlalchemy."):
            return True
        if self.mode in ("0", "false", "off", ""):
            return False
        if self.mode in ("full", "1", "true", "yes"):
            return True
        msg = record.getMessage().strip()
        if not msg:
            return False
        if msg.startswith("{") or msg.startswith("[cached") or msg.startswith("[generated"):
            return False
        if msg.startswith("BEGIN") or msg.startswith("COMMIT") or msg.startswith("ROLLBACK"):
            return False
        return True

def _dedupe_handlers(names):
    """Снять хендлеры у перечисленных логгеров и включить propagate к root."""
    for name in names:
        lg = logging.getLogger(name)
        for h in list(lg.handlers):
            lg.removeHandler(h)
        lg.propagate = True

def _reset_root_handlers():
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

def _load_env_for_logging():
    """Подхватить LOG_* из .env до чтения os.getenv."""
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=os.getenv("ENV_FILE", ".env"), override=False)
        return
    except Exception:
        pass  # нет python-dotenv — парсим минимально сами

    path = os.getenv("ENV_FILE", ".env")
    try:
        with open(path, "r", encoding="utf-8") as f:
            wanted = {"LOG_STYLE", "LOG_LEVEL", "LOG_SQL"}
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k in wanted and k not in os.environ:
                    os.environ[k] = v
    except FileNotFoundError:
        pass

def _setup_pretty(level: int, sql_mode: str):
    try:
        from rich.logging import RichHandler
        from rich.console import Console
    except Exception:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
        h.addFilter(EnsureConnFilter())
        h.addFilter(_SQLFilter(sql_mode))
        root = logging.getLogger()
        root.addHandler(h)
        root.setLevel(level)
        return
    console = Console(force_terminal=True, markup=True, soft_wrap=True)
    h = RichHandler(
        console=console,
        markup=True,
        show_time=True,
        show_level=True,
        show_path=False,
        rich_tracebacks=True,
        tracebacks_show_locals=False,
    )
    h.setFormatter(logging.Formatter("%(message)s"))  # префиксы рисует Rich
    h.addFilter(EnsureConnFilter())
    h.addFilter(_SQLFilter(sql_mode))
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

def _setup_plain(level: int, sql_mode: str):
    fmt = "%(asctime)s | %(levelname).1s | %(name)s | %(conn)s | %(message)s"
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(OneLineFormatter(fmt, "%H:%M:%S", strip_markup=True))
    h.addFilter(EnsureConnFilter())
    h.addFilter(_SQLFilter(sql_mode))
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(level)

def setup_logging(level: str | None = None) -> None:
    """
    Читает LOG_STYLE/LOG_LEVEL/LOG_SQL из окружения ИЛИ .env.
    Убирает дубли хендлеров SQLAlchemy/uvicorn и красит логи (Rich).
    """
    _load_env_for_logging()

    level_name = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    lvl = getattr(logging, level_name, logging.INFO)
    style = (os.getenv("LOG_STYLE") or "plain").lower()
    sql_mode = (os.getenv("LOG_SQL") or "0").lower()

    _reset_root_handlers()
    _dedupe_handlers([
        "sqlalchemy", "sqlalchemy.engine", "sqlalchemy.pool", "sqlalchemy.orm", "sqlalchemy.dialects",
        "uvicorn", "uvicorn.error", "uvicorn.access",   # ← чтобы их строки тоже шли через наш хендлер
    ])

    if style in ("pretty", "dev"):
        _setup_pretty(lvl, sql_mode)
    else:
        _setup_plain(lvl, sql_mode)

    # уровни (шум приглушён)
    logging.getLogger("uvicorn.error").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.INFO)
