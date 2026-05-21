"""Add workspace_id to approvals table for workspace isolation.

Revision ID: 0018
Revises: 0017
Create Date: 2026-05-10

Changes:
  • approvals.workspace_id — INTEGER, FK to workspaces(id), nullable
    Allows list_approvals API to scope queries per workspace.
    Back-populated from runs.task_id join on first migration.
"""
from alembic import op

revision    = "0018"
down_revision = "0017"
branch_labels = None
depends_on  = None


def upgrade():
    op.execute("""
        ALTER TABLE approvals
        ADD COLUMN IF NOT EXISTS workspace_id INTEGER
        REFERENCES workspaces(id) ON DELETE SET NULL ON UPDATE CASCADE
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_approvals_workspace_id ON approvals (workspace_id)")


def downgrade():
    op.execute("ALTER TABLE approvals DROP COLUMN IF EXISTS workspace_id")
