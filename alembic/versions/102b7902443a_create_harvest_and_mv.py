"""create harvest and mv

Revision ID: 102b7902443a
Revises: "102b7902443a" 
Create Date: 2026-02-07 20:54:22.467509

"""
from alembic import op

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "102b7902443a"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1) harvest table
    op.execute("""
    CREATE TABLE IF NOT EXISTS harvest (
        company     text        NOT NULL,
        crop        text        NOT NULL,
        month       text        NOT NULL,
        total_kg    double precision NOT NULL,
        measured_at  timestamptz NOT NULL DEFAULT now(),
        measure_no  integer     NOT NULL DEFAULT 0,
        PRIMARY KEY (company, crop, month)
    );
    """)

    #2) materialized view
    op.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_harvest_monthly AS
    SELECT
      company,
      crop,
      month,
      SUM(total_kg) AS total_kg
    FROM harvest
    GROUP BY company, crop, month;
    """)

    #3) unique index for concurrently refresh
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mv_harvest_monthly
    ON mv_harvest_monthly (company, crop, month);
    """)

def downgrade():
    op.execute("DROP MATERIALIZED VIEW IF EXISTS mv_harvest_monthly;")
    op.execute("DROP TABLE IF EXISTS harvest;")
