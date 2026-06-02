"""add job category and cancel support

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-02
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("category", sa.String(length=64), nullable=True))
    op.create_index("ix_jobs_category", "jobs", ["category"])

    # Backfill from the previous frontend convention: description = '[category] text'.
    op.execute(
        r"""
        UPDATE jobs
        SET
          category = substring(description from '^\[([^\]]+)\]'),
          description = regexp_replace(description, '^\[[^\]]+\]\s*', '')
        WHERE description ~ '^\[[^\]]+\]'
        """
    )


def downgrade() -> None:
    op.drop_index("ix_jobs_category", table_name="jobs")
    op.drop_column("jobs", "category")
