"""Launch coverage: the biggest streams on a title at each Twitch
sample, the category's place in Twitch's top games, and the pull quote
on an OpenCritic review.

Revision ID: c4e6a8b0d2f4
Revises: b2d4f6a8c0e2
"""
import sqlalchemy as sa
from alembic import op


revision = 'c4e6a8b0d2f4'
down_revision = 'b2d4f6a8c0e2'
branch_labels = None
depends_on = None


def upgrade():
    """One slot table and two nullable columns, so a worker still on
    the old code keeps writing through a deploy."""
    op.add_column(
        'critic_review',
        sa.Column('snippet', sa.Text(),
                  comment="The review's pull quote as OpenCritic gives "
                          'it; NULL when absent.'),
        schema='games')
    op.add_column(
        'community_pulse',
        sa.Column('twitch_rank', sa.Integer(),
                  comment="The category's position in Twitch's top "
                          'games at the sample (Helix Get Top Games, '
                          'first 100). NULL outside the list read.'),
        schema='games')
    op.create_table(
        'stream_top',
        sa.Column('streamtopid', sa.BigInteger(), primary_key=True),
        sa.Column('gameid', sa.BigInteger(),
                  sa.ForeignKey('games.game.gameid'), nullable=False),
        sa.Column('sampled_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC slot start - same truncation as '
                          'community_pulse.sampled_at.'),
        sa.Column('channel', sa.Text(), nullable=False,
                  comment='Streamer login (user_login).'),
        sa.Column('title', sa.Text(),
                  comment='Stream title as observed.'),
        sa.Column('viewer_count', sa.Numeric(),
                  comment='Viewers on the stream at sample time.'),
        sa.Column('rank', sa.Integer(),
                  comment="The stream's position by viewers on the "
                          'category at sample time, 1 the biggest.'),
        sa.UniqueConstraint('gameid', 'sampled_at', 'channel',
                            name='uq_stream_top_slot'),
        schema='games',
        comment="The most-watched streams on a title's Twitch "
                'category during a community_pulse sample. Capped '
                'per title per slot - the head of the category, '
                'not a census; totals live on community_pulse.',
    )
    op.create_index('ix_stream_top_sampled', 'stream_top',
                    ['sampled_at'], schema='games')


def downgrade():
    op.drop_index('ix_stream_top_sampled', table_name='stream_top',
                  schema='games')
    op.drop_table('stream_top', schema='games')
    op.drop_column('community_pulse', 'twitch_rank', schema='games')
    op.drop_column('critic_review', 'snippet', schema='games')
