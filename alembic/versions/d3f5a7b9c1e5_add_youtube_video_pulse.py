"""add youtube_video_pulse intraday per-video view samples

Revision ID: d3f5a7b9c1e5
Revises: f1d3b5a7c9e3
Create Date: 2026-09-08

"""
import sqlalchemy as sa
from alembic import op

revision = 'd3f5a7b9c1e5'
down_revision = 'f1d3b5a7c9e3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'youtube_video_pulse',
        sa.Column('youtubevideopulseid', sa.BigInteger(), nullable=False),
        sa.Column('youtubevideoid', sa.BigInteger(), nullable=False),
        sa.Column('sampled_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC slot start - the sample time '
                          'truncated to the pulse interval (4h at '
                          'ship).'),
        sa.Column('views', sa.Numeric(), nullable=True,
                  comment='Cumulative view count at sample time (the '
                          'API total, never a delta).'),
        sa.Column('likes', sa.Numeric(), nullable=True),
        sa.Column('comments', sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(['youtubevideoid'],
                                ['games.youtube_video.youtubevideoid']),
        sa.PrimaryKeyConstraint('youtubevideopulseid'),
        sa.UniqueConstraint('youtubevideoid', 'sampled_at',
                            name='uq_youtube_video_pulse_slot'),
        schema='games',
        comment='Intraday YouTube view samples per video. Sparse by '
                'design: sampled only while an industry event window '
                'is live and only for a budgeted slice of its '
                'trailers, so absence of a row is "not sampled", '
                'never "no views"; idle between events.',
    )
    op.create_index('ix_youtube_video_pulse_sampled',
                    'youtube_video_pulse', ['sampled_at'],
                    schema='games')


def downgrade():
    op.drop_index('ix_youtube_video_pulse_sampled',
                  table_name='youtube_video_pulse', schema='games')
    op.drop_table('youtube_video_pulse', schema='games')
