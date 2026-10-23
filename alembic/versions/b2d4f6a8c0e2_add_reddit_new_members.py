"""Reddit's attention signal becomes weekly new subreddit members; the
active-user columns it replaces are marked retired, not dropped, so a
worker still on the old code keeps reading them through a deploy.

Revision ID: b2d4f6a8c0e2
Revises: f3a5c7e9b1d3
"""
import sqlalchemy as sa
from alembic import op


revision = 'b2d4f6a8c0e2'
down_revision = 'f3a5c7e9b1d3'
branch_labels = None
depends_on = None

RETIRED = ("Retired: Reddit's about answer no longer carries "
           'active_user_count, so nothing writes this. Drop once no '
           'deployed code reads it.')
SNAPSHOT_COMMENT = ('Subreddit active_user_count at snapshot time '
                    '(about.json) — concurrent-scale, unlike the '
                    'cumulative reddit_members.')
ATTENTION_COMMENT = 'Weekly mean of the nightly samples.'
COLUMNS = (('community_snapshot', SNAPSHOT_COMMENT),
           ('attention_share', ATTENTION_COMMENT))


def upgrade():
    op.add_column(
        'attention_share',
        sa.Column('reddit_new_members', sa.Numeric(),
                  comment='Subreddit subscribers gained since the '
                          'previous reading, scaled to seven days and '
                          'clipped at zero.'),
        schema='games')
    for table, comment in COLUMNS:
        op.alter_column(table, 'reddit_active_users', schema='games',
                        existing_type=sa.Numeric(), comment=RETIRED,
                        existing_comment=comment)


def downgrade():
    for table, comment in COLUMNS:
        op.alter_column(table, 'reddit_active_users', schema='games',
                        existing_type=sa.Numeric(), comment=comment,
                        existing_comment=RETIRED)
    op.drop_column('attention_share', 'reddit_new_members', schema='games')
