"""index the time-series fact dates

Revision ID: a3c5e7b9d1f5
Revises: f7b9d1c3e5a7
Create Date: 2026-08-02

Both facts are keyed (gameid, date), so a window read filtering on the
date alone cannot use that index and scans the table. game_event is the
oldest and largest of them — daily observations per appid back to 2019
— and the title league reads a rolling window over both on every panel
render.
"""
from alembic import op


revision = 'a3c5e7b9d1f5'
down_revision = 'f7b9d1c3e5a7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index('ix_game_event_eventdate', 'game_event',
                    ['eventdate'], schema='games')
    op.create_index('ix_community_snapshot_date', 'community_snapshot',
                    ['snapshot_date'], schema='games')


def downgrade():
    op.drop_index('ix_community_snapshot_date',
                  table_name='community_snapshot', schema='games')
    op.drop_index('ix_game_event_eventdate', table_name='game_event',
                  schema='games')
