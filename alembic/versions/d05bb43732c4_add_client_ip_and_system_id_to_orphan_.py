"""Add client_ip and system_id to orphan_sms

Revision ID: d05bb43732c4
Revises: efde51d8c034
Create Date: 2025-08-14 00:00:00.000000
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "d05bb43732c4"
down_revision: Union[str, Sequence[str], None] = "efde51d8c034"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    op.add_column("orphan_sms", sa.Column("client_ip", sa.String(length=45), nullable=True))
    op.add_column("orphan_sms", sa.Column("system_id", sa.String(length=64), nullable=True))
    op.create_index("ix_orphan_sms_client_ip", "orphan_sms", ["client_ip"], unique=False)
    op.create_index("ix_orphan_sms_system_id", "orphan_sms", ["system_id"], unique=False)

def downgrade() -> None:
    op.drop_index("ix_orphan_sms_system_id", table_name="orphan_sms")
    op.drop_index("ix_orphan_sms_client_ip", table_name="orphan_sms")
    op.drop_column("orphan_sms", "system_id")
    op.drop_column("orphan_sms", "client_ip")
