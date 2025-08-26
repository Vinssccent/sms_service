"""Add orphan dedupe unique index (columns-based, no backfill)"""

from alembic import op
import sqlalchemy as sa

revision = "210605f58518"
down_revision = "73f5893de66b"
branch_labels = None
depends_on = None

def upgrade() -> None:
    # 1) Добавляем колонки (без NOT NULL и без DEFAULT) — лёгко и быстро
    op.add_column("orphan_sms", sa.Column("dedupe_minute_bucket", sa.BigInteger(), nullable=True))
    op.add_column("orphan_sms", sa.Column("text_md5", sa.String(length=32), nullable=True))

    # 2) Уникальный индекс ТОЛЬКО на новые строки (где поля уже заполнены кодом)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_orphan_dedupe
        ON orphan_sms (provider_id, source_addr, phone_number_str, dedupe_minute_bucket, text_md5)
        WHERE dedupe_minute_bucket IS NOT NULL AND text_md5 IS NOT NULL
    """)

def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_orphan_dedupe")
    op.drop_column("orphan_sms", "text_md5")
    op.drop_column("orphan_sms", "dedupe_minute_bucket")
