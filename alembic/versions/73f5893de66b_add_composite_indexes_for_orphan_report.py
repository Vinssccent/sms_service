"""Add composite indexes for orphan report"""

from alembic import op
import sqlalchemy as sa

# Идентификаторы ревизий — ЖЁСТКИЕ константы,
# их НЕЛЬЗЯ вычислять через op.get_context() и т.п.
revision = "73f5893de66b"            # ← как в имени файла
down_revision = "461a4b63e33e"       # ← текущий head до этой ревизии
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Составной индекс для отчётов: провайдер • сендер • страна • оператор
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_orphan_report_grp
        ON orphan_sms (provider_id, source_addr, country_id, operator_id)
    """)
    # Ускоряем детализацию/хронологию по конкретному sender
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_orphan_src_date
        ON orphan_sms (source_addr, received_at)
    """)

def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_orphan_src_date")
    op.execute("DROP INDEX IF EXISTS ix_orphan_report_grp")
