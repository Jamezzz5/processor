"""add search_interest + attention_share facts and youtube view columns

Revision ID: b7d9f1a3c5e7
Revises: a7c9e1b3d5f7
Create Date: 2026-08-12

"""
import sqlalchemy as sa
from alembic import op

revision = 'b7d9f1a3c5e7'
down_revision = 'a7c9e1b3d5f7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'search_interest',
        sa.Column('searchinterestid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game '
                          'dim; the raw title is retained.'),
        sa.Column('title', sa.Text(), nullable=False,
                  comment='Tracked title as queried.'),
        sa.Column('week_start', sa.Date(), nullable=False,
                  comment='ISO week Monday (Trends weekly grain).'),
        sa.Column('geo', sa.Text(), nullable=False,
                  server_default='GLOBAL',
                  comment="Trends geo cut; 'GLOBAL' = worldwide."),
        sa.Column('interest', sa.Numeric(), nullable=True,
                  comment='Anchor-rescaled reading (raw_interest x 100 '
                          '/ anchor_value) - the only column comparable '
                          'across batches. NULL when the anchor read '
                          'too low to scale by. The rolling 12-month '
                          'request window re-normalises history, so '
                          'values shift slightly between runs; the '
                          'upsert overwrite is intended.'),
        sa.Column('raw_interest', sa.Numeric(), nullable=True,
                  comment="The batch's own 0-100 reading, kept for "
                          'audit; never compare across batches.'),
        sa.Column('anchor', sa.Text(), nullable=True,
                  comment='Anchor title the batch was scaled by.'),
        sa.Column('anchor_value', sa.Numeric(), nullable=True,
                  comment="The anchor's raw reading in this batch."),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last sweep that refreshed this '
                          'row - the lane recency column, since '
                          'week_start ages by construction.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('searchinterestid'),
        sa.UniqueConstraint('title', 'week_start', 'geo',
                            name='uq_search_interest_obs'),
        schema='games',
        comment='Google Trends weekly search interest per title. '
                'Readings are indexed 0-100 within one request, so '
                'interest is rescaled by the shared anchor term; '
                'compare interest across rows, never raw_interest.',
    )
    op.create_index('ix_search_interest_week', 'search_interest',
                    ['week_start'], schema='games')
    op.create_index('ix_search_interest_gameid', 'search_interest',
                    ['gameid'], schema='games')
    op.create_table(
        'attention_share',
        sa.Column('attentionshareid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game '
                          'dim; the raw title is retained.'),
        sa.Column('title', sa.Text(), nullable=False,
                  comment='Raw tracked title.'),
        sa.Column('week_start', sa.Date(), nullable=False,
                  comment='ISO week Monday.'),
        sa.Column('search_interest', sa.Numeric(), nullable=True,
                  comment='Anchor-rescaled weekly Trends reading.'),
        sa.Column('youtube_views', sa.Numeric(), nullable=True,
                  comment='Week-over-week delta of trailing-90d '
                          'official-channel video views, clipped at '
                          'zero.'),
        sa.Column('twitch_viewers', sa.Numeric(), nullable=True,
                  comment='Weekly mean of the nightly '
                          'concurrent-viewer samples.'),
        sa.Column('reddit_active_users', sa.Numeric(), nullable=True,
                  comment='Weekly mean of the nightly samples.'),
        sa.Column('steam_ccu', sa.Numeric(), nullable=True,
                  comment='Weekly mean of daily concurrent players.'),
        sa.Column('attention_share', sa.Numeric(), nullable=True,
                  comment="0-1 fraction of the tracked set's combined "
                          'weekly attention.'),
        sa.Column('signals_present', sa.Integer(), nullable=True,
                  comment='How many of the five signals carried data '
                          'for this title this week.'),
        sa.Column('set_size', sa.Integer(), nullable=True,
                  comment='Titles scored that week. Shares are only '
                          'comparable at equal set_size - re-base '
                          'within a chosen cohort to compare across '
                          'weeks.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last derive that refreshed this '
                          'row - the recency column, since week_start '
                          'ages by construction.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('attentionshareid'),
        sa.UniqueConstraint('week_start', 'title',
                            name='uq_attention_share_week'),
        schema='games',
        comment='Weekly attention share per tracked title: each '
                "signal's share of the tracked set's weekly volume, "
                'combined on fixed weights that renormalise over the '
                'signals live that week. A share is a position within '
                "that week's field (see set_size), not an absolute "
                'rating.',
    )
    op.create_index('ix_attention_share_week', 'attention_share',
                    ['week_start'], schema='games')
    op.create_index('ix_attention_share_gameid', 'attention_share',
                    ['gameid'], schema='games')
    op.add_column(
        'community_snapshot',
        sa.Column('youtube_recent_views', sa.Numeric(), nullable=True,
                  comment='Total views on the linked official '
                          "channel's uploads published in the "
                          'trailing 90 days - a level, not a rate. '
                          'Week-over-week deltas are the view '
                          'velocity; videos aging out of the window '
                          'can dip the level, so deltas clip at '
                          'zero.'),
        schema='games')
    op.add_column(
        'community_snapshot',
        sa.Column('youtube_video_count', sa.Numeric(), nullable=True,
                  comment='Uploads on the linked official channel in '
                          'the trailing 90 days.'),
        schema='games')


def downgrade():
    op.drop_column('community_snapshot', 'youtube_video_count',
                   schema='games')
    op.drop_column('community_snapshot', 'youtube_recent_views',
                   schema='games')
    op.drop_index('ix_attention_share_gameid',
                  table_name='attention_share', schema='games')
    op.drop_index('ix_attention_share_week',
                  table_name='attention_share', schema='games')
    op.drop_table('attention_share', schema='games')
    op.drop_index('ix_search_interest_gameid',
                  table_name='search_interest', schema='games')
    op.drop_index('ix_search_interest_week',
                  table_name='search_interest', schema='games')
    op.drop_table('search_interest', schema='games')
