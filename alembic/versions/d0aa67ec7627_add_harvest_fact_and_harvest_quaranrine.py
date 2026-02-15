"""add harvest_fact and harvest_quaranrine

Revision ID: <auto_generated>
Revises: 102b7902443a
Create Date: <auto_generated>
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "<auto_generated>"
down_revision: Union[str, Sequence[str], None] = '102b7902443a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1) harvets_fact (daily fact)
    """Upgrade schema."""
    op.execute("""
    CREATE TABLE IF NOT EXISTS harvest_fact (
        id              bigserial PRIMARY KEY,
        harvest_date    date        NOT NULL,
        company         text        NOT NULL,
        crop            text        NOT NULL,
        house           text        NOT NULL,   -- 最終契約：NULL禁止
        qty_g           integer     NOT NULL,

        batch_no        integer     NULL,       -- 人が確定（未確定はNULL）
        source_file     text        NULL,
        source_row_num  integer     NULL,
        row_hash        text        NOT NULL,

        cerated_at      timestamptz  NOT NULL DEFAULT now()
    );
    """)

    # 未確定(batch_no IS NULL)は自然キーで一意（重複の流入をDBでも止める）
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_harvest_fact_natural_batch_null
    ON harvest_fact (harvest_date, company, crop, house)
    WHERE batch_no IS NULL;
    """)

    # batch_noがある場合は（自然キー + batch_no）で一意
    op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_harvest_fact_natural_plus_batch
    ON harvest_fact (harvest_date, company, crop, house, batch_no)
    WHERE batch_no IS NOT NULL;
    """)

    op.execute("""CREATE INDEX IF NOT EXISTS ix_harvest_fact_date ON harvest_fact (harvest_date);""")
    op.execute("""CREATE INDEX IF NOT EXISTS ix_harvest_fact_date ON harvest_fact (company);""")
    op.execute("""CREATE INDEX IF NOT EXISTS ix_harvest_fact_date ON harvest_fact (crop);""")

    # 2) harvest_quarantine (do not drop; keep evidence)
    op.execute("""
    CREATE TABLE IF NOT EXISTS harvest_quarantine (
        id                  bigserial   PRIMARY KEY,

        harvest_date        date        NULL,
        company             text        NULL,
        crop                text        NULL,
        house               text        NULL,   -- 未解決を許す 
        qty_g               integer     NULL,

        reason              text        NOT NULL,   --  contract_violation / duplicatte / house_unkown etc
        details             jsonb       NULL,       --  エラー文字列、検出情報、候補など
        raw_payload         jsonb       NULL,       --  元行（捨てない）
        row_hash            text        NULL,

        source_file         text        NULL,
        source_row_num      integer     NULL,

        resoluved           boolean     NOT NULL DEFAULT false,
        assigned_batch_no   integer     NULL, --  人が記入
        resolved_at         timestamptz NULL,

        created_at          timestamptz NOT NULL DEFAULT now()
    );
    """)

    op.execute("""CREATE INDEX IF NOT EXISTS ix_hq_reason ON harvest_quarantine (reason);""")
    op.execute("""CREATE INDEX IF NOT EXISTS ix_hq_reason ON harvest_quarantine (resolved_at);""")
    op.execute("""CREATE INDEX IF NOT EXISTS ix_hq_reason ON harvest_quarantine (created_at);""")

def downgrade() -> None:
    """Downgrade schema."""
    op.execute("DROP TABLE IF EXISTS harvest_quarantine;")
    op.execute("DROP TABLE IF EXISTS harvest_fact;")
