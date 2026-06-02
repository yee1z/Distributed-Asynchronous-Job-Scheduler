"""add job ownership

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("jobs", sa.Column("owner_user_id", sa.BigInteger(), nullable=True))
    op.create_index("ix_jobs_owner_user_id", "jobs", ["owner_user_id"])
    op.create_foreign_key(
        "fk_jobs_owner_user_id_users",
        "jobs",
        "users",
        ["owner_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.drop_constraint("jobs_name_key", "jobs", type_="unique")
    op.create_unique_constraint("uq_jobs_owner_name", "jobs", ["owner_user_id", "name"])

    # Legacy jobs created before auth have no owner, so do not let them keep
    # running invisibly after API access becomes user-scoped. Recreate them
    # after logging in if they are still needed.
    op.execute("UPDATE jobs SET enabled = false WHERE owner_user_id IS NULL")


def downgrade() -> None:
    op.drop_constraint("uq_jobs_owner_name", "jobs", type_="unique")
    op.create_unique_constraint("jobs_name_key", "jobs", ["name"])
    op.drop_constraint("fk_jobs_owner_user_id_users", "jobs", type_="foreignkey")
    op.drop_index("ix_jobs_owner_user_id", table_name="jobs")
    op.drop_column("jobs", "owner_user_id")
