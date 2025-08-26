"""Cleanup duplicate indexes; add composite index for orphan report

Revision ID: aa11bb22cc33
Revises: d05bb43732c4
Create Date: 2025-08-19 00:00:00
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "aa11bb22cc33"
down_revision: Union[str, Sequence[str], None] = "d05bb43732c4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # --- sms_messages: убираем дубли индексов ---
    op.execute("DROP INDEX IF EXISTS idx_sms_received_at")
    op.execute("DROP INDEX IF EXISTS idx_sms_session_id")
    # Оставляем ix_sms_messages_code из прежней миграции, убираем дубль из модели:
    op.execute("DROP INDEX IF EXISTS ix_sms_code")

    # --- phone_numbers: убираем дубли ---
    op.execute("DROP INDEX IF EXISTS idx_pn_number")
    op.execute("DROP INDEX IF EXISTS idx_pn_country")
    op.execute("DROP INDEX IF EXISTS idx_pn_operator")
    op.execute("DROP INDEX IF EXISTS idx_pn_provider")

    # --- orphan_sms: чистка дублей и «кривого» индекса группировки ---
    op.execute("DROP INDEX IF EXISTS idx_orphan_country")
    op.execute("DROP INDEX IF EXISTS idx_orphan_operator")
    op.execute("DROP INDEX IF EXISTS idx_orphan_provider")
    op.execute("DROP INDEX IF EXISTS idx_orphan_received_at")
    op.execute("DROP INDEX IF EXISTS idx_orphan_source")
    op.execute("DROP INDEX IF EXISTS idx_orphan_phone")
    op.execute("DROP INDEX IF EXISTS ix_orphan_country")
    op.execute("DROP INDEX IF EXISTS ix_orphan_operator")
    op.execute("DROP INDEX IF EXISTS ix_orphan_provider")
    op.execute("DROP INDEX IF EXISTS ix_orphan_phone")           # на всякий случай
    op.execute("DROP INDEX IF EXISTS ix_orphan_sms_phone")       # дублирует phone_number_str
    op.execute("DROP INDEX IF EXISTS idx_orphan_group")

    # Полезный составной индекс для отчёта:
    # Поставщик • Сендер • Страна • Оператор
    op.create_index(
        "ix_orphan_report_grp",
        "orphan_sms",
        ["provider_id", "source_addr", "country_id", "operator_id"],
        unique=False,
    )
    # Индекс ix_orphan_src_date (source_addr, received_at) уже существует — оставляем.

def downgrade() -> None:
    op.drop_index("ix_orphan_report_grp", table_name="orphan_sms")
    # Удалённые дубли обратно не создаём (намеренная чистка).
