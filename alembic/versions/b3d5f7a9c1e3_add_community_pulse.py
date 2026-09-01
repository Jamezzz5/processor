"""add community_pulse intraday fact and stream_flag evidence table

Revision ID: b3d5f7a9c1e3
Revises: a9c1e3b5d7f9
Create Date: 2026-08-31

"""
import sqlalchemy as sa
from alembic import op

revision = 'b3d5f7a9c1e3'
down_revision = 'a9c1e3b5d7f9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'community_pulse',
        sa.Column('communitypulseid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('sampled_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC slot start - the sample time '
                          'truncated to the pulse interval (4h at '
                          'ship).'),
        sa.Column('twitch_viewers', sa.Numeric(), nullable=True,
                  comment="Concurrent viewers on the title's Twitch "
                          'category (Helix Get Streams, top pages '
                          'summed) at sample time.'),
        sa.Column('twitch_channels', sa.Numeric(), nullable=True,
                  comment='Live channels observed across the pages '
                          'read - a floor when pagination stops '
                          'early, not a census.'),
        sa.Column('sponsored_streams', sa.Numeric(), nullable=True,
                  comment='Of the observed channels, those whose '
                          'stream title carries a sponsorship token '
                          '("sponsored" without "not sponsored", '
                          '#ad, "paid partnership", ...). The token '
                          'rule lives in the twitch_pulse lane.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('communitypulseid'),
        sa.UniqueConstraint('gameid', 'sampled_at',
                            name='uq_community_pulse_slot'),
        schema='games',
        comment='Intraday Twitch viewership samples per game. Sparse '
                'by design: a budgeted slice of the tracked pool per '
                'slot, so absence of a row is "not sampled", never '
                '"zero viewers".',
    )
    op.create_index('ix_community_pulse_sampled', 'community_pulse',
                    ['sampled_at'], schema='games')
    op.create_table(
        'stream_flag',
        sa.Column('streamflagid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('sampled_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC slot start - same truncation as '
                          'community_pulse.sampled_at.'),
        sa.Column('channel', sa.Text(), nullable=False,
                  comment='Streamer login (user_login).'),
        sa.Column('title', sa.Text(), nullable=True,
                  comment='Stream title as observed.'),
        sa.Column('token', sa.Text(), nullable=True,
                  comment='The sponsorship token that matched.'),
        sa.Column('viewer_count', sa.Numeric(), nullable=True,
                  comment='Viewers on the flagged stream at sample '
                          'time.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('streamflagid'),
        sa.UniqueConstraint('gameid', 'sampled_at', 'channel',
                            name='uq_stream_flag_slot'),
        schema='games',
        comment='Streams whose title matched a sponsorship token '
                'during a community_pulse sample. Capped per title '
                'per slot - evidence, not a census; counts live on '
                'community_pulse.',
    )
    op.create_index('ix_stream_flag_gameid', 'stream_flag',
                    ['gameid'], schema='games')
    op.create_index('ix_stream_flag_sampled', 'stream_flag',
                    ['sampled_at'], schema='games')


def downgrade():
    op.drop_index('ix_stream_flag_sampled', table_name='stream_flag',
                  schema='games')
    op.drop_index('ix_stream_flag_gameid', table_name='stream_flag',
                  schema='games')
    op.drop_table('stream_flag', schema='games')
    op.drop_index('ix_community_pulse_sampled',
                  table_name='community_pulse', schema='games')
    op.drop_table('community_pulse', schema='games')
