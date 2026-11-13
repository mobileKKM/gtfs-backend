"""core.feed_version.retired_at: when a version was retired, so its archived zip can be removed some days later.

A retired version's own last_service_date can be years ahead (newer versions took its days over), so it can't tell
how long ago the version stopped serving. Versions retired before this migration count from now.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-13
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE core.feed_version ADD COLUMN retired_at timestamptz")
    op.execute("UPDATE core.feed_version SET retired_at = now() WHERE status = 'retired'")


def downgrade() -> None:
    op.execute("ALTER TABLE core.feed_version DROP COLUMN retired_at")
