"""add youtube_video dim and youtube_video_stat daily fact

Revision ID: a9c1e3b5d7f9
Revises: f5b7d9a1c3e5
Create Date: 2026-08-26

"""
import sqlalchemy as sa
from alembic import op

revision = 'a9c1e3b5d7f9'
down_revision = 'f5b7d9a1c3e5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'youtube_video',
        sa.Column('youtubevideoid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('video_id', sa.Text(), nullable=False,
                  comment='YouTube video id.'),
        sa.Column('channel_id', sa.Text(), nullable=True),
        sa.Column('channel_title', sa.Text(), nullable=True),
        sa.Column('channel_subscribers', sa.Numeric(), nullable=True,
                  comment='Latest subscriber count of the channel, '
                          'refreshed with the stats; NULL when hidden. '
                          'A size, not a history.'),
        sa.Column('title', sa.Text(), nullable=True),
        sa.Column('published_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC upload time.'),
        sa.Column('kind', sa.Text(), nullable=False,
                  comment="'official' (IGDB-listed trailer or the "
                          "linked channel's upload) | 'creator'."),
        sa.Column('label', sa.Text(), nullable=True,
                  comment='IGDB video name, e.g. "Launch Trailer"; '
                          'NULL when not catalogued.'),
        sa.Column('sponsored', sa.Boolean(), nullable=True,
                  comment='True when the title/description says '
                          '"sponsored" without "not sponsored" - the '
                          "old influencer tool's rule; NULL until the "
                          'snippet has been read.'),
        sa.Column('source', sa.Text(), nullable=False,
                  comment="'igdb' | 'search' | 'import'."),
        sa.Column('first_seen_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last stats request.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('youtubevideoid'),
        sa.UniqueConstraint('video_id', name='uq_youtube_video_id'),
        schema='games',
        comment='YouTube videos per game: official trailers and '
                'uploads plus creator coverage; daily view counts live '
                'in youtube_video_stat.',
    )
    op.create_index('ix_youtube_video_gameid', 'youtube_video',
                    ['gameid'], schema='games')
    op.create_index('ix_youtube_video_published', 'youtube_video',
                    ['published_at'], schema='games')
    op.create_table(
        'youtube_video_stat',
        sa.Column('youtubevideostatid', sa.BigInteger(), nullable=False),
        sa.Column('youtubevideoid', sa.BigInteger(), nullable=False),
        sa.Column('stat_date', sa.Date(), nullable=False,
                  comment='UTC ingest date.'),
        sa.Column('views', sa.Numeric(), nullable=True),
        sa.Column('likes', sa.Numeric(), nullable=True),
        sa.Column('comments', sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(['youtubevideoid'],
                                ['games.youtube_video.youtubevideoid']),
        sa.PrimaryKeyConstraint('youtubevideostatid'),
        sa.UniqueConstraint('youtubevideoid', 'stat_date',
                            name='uq_youtube_video_stat_day'),
        schema='games',
        comment='Daily YouTube statistics per video.',
    )
    op.create_index('ix_youtube_video_stat_date', 'youtube_video_stat',
                    ['stat_date'], schema='games')


def downgrade():
    op.drop_index('ix_youtube_video_stat_date',
                  table_name='youtube_video_stat', schema='games')
    op.drop_table('youtube_video_stat', schema='games')
    op.drop_index('ix_youtube_video_published',
                  table_name='youtube_video', schema='games')
    op.drop_index('ix_youtube_video_gameid',
                  table_name='youtube_video', schema='games')
    op.drop_table('youtube_video', schema='games')
