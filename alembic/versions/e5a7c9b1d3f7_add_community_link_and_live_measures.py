"""add community_link + live snapshot measures

Revision ID: e5a7c9b1d3f7
Revises: d9f1b3c5a7e9
Create Date: 2026-07-28

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e5a7c9b1d3f7'
down_revision = 'd9f1b3c5a7e9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'community_link',
        sa.Column('communitylinkid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('platform', sa.Text(), nullable=False,
                  comment="'twitch' | 'reddit' | 'youtube'."),
        sa.Column('handle', sa.Text(), nullable=True,
                  comment='Twitch category name / subreddit without '
                          'the r/ prefix / YouTube channel title.'),
        sa.Column('external_id', sa.Text(), nullable=True,
                  comment='Twitch category id / YouTube channel id; '
                          'NULL for Reddit (the handle is the id).'),
        sa.Column('source', sa.Text(), nullable=False,
                  comment="'curated' (registry sheet) or 'derived' "
                          '(auto-search); derived never overwrites '
                          'curated.'),
        sa.Column('derived_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; when auto-derivation last '
                          'confirmed this handle.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last touch by any writer.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('communitylinkid'),
        sa.UniqueConstraint('gameid', 'platform',
                            name='uq_community_link_game_platform'),
        schema='games',
        comment='Per-platform community handle per game; curated rows '
                'always beat derived ones.',
    )
    op.add_column(
        'community_snapshot',
        sa.Column('twitch_viewers', sa.Numeric(), nullable=True,
                  comment="Concurrent viewers on the title's Twitch "
                          'category (Helix Get Streams, top pages '
                          'summed) — a point-in-time sample at the '
                          'nightly run hour, not a peak.'),
        schema='games')
    op.add_column(
        'community_snapshot',
        sa.Column('reddit_active_users', sa.Numeric(), nullable=True,
                  comment='Subreddit active_user_count at snapshot '
                          'time (about.json) — concurrent-scale, '
                          'unlike the cumulative reddit_members.'),
        schema='games')


def downgrade():
    op.drop_column('community_snapshot', 'reddit_active_users',
                   schema='games')
    op.drop_column('community_snapshot', 'twitch_viewers',
                   schema='games')
    op.drop_table('community_link', schema='games')
