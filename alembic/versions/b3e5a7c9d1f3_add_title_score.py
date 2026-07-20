"""add title_score daily competitive snapshot fact

Revision ID: b3e5a7c9d1f3
Revises: a1c3e5b7d9f1
Create Date: 2026-07-17

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b3e5a7c9d1f3'
down_revision = 'a1c3e5b7d9f1'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'title_score',
        sa.Column('titlescoreid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game '
                          'dim; the raw title is retained.'),
        sa.Column('score_date', sa.Date(), nullable=False,
                  comment='UTC snapshot date.'),
        sa.Column('title', sa.Text(), nullable=False,
                  comment='Raw tracked title (Newzoo productname).'),
        sa.Column('primary_period', sa.Text(), nullable=True,
                  comment='Scored month, YYYY-MM.'),
        sa.Column('comparison_period', sa.Text(), nullable=True,
                  comment='Comparison month, YYYY-MM.'),
        sa.Column('influence', sa.Numeric(), nullable=True),
        sa.Column('engagement', sa.Numeric(), nullable=True),
        sa.Column('momentum', sa.Numeric(), nullable=True),
        sa.Column('composite', sa.Numeric(), nullable=True,
                  comment='Sum of the dimension z-scores.'),
        sa.Column('headline_metric', sa.Text(), nullable=True,
                  comment='Metric the current/prior/share columns '
                          'read on.'),
        sa.Column('current', sa.Numeric(), nullable=True),
        sa.Column('prior', sa.Numeric(), nullable=True),
        sa.Column('share', sa.Numeric(), nullable=True,
                  comment='Share of voice across tracked set.'),
        sa.Column('share_delta', sa.Numeric(), nullable=True),
        sa.Column('movement', sa.Text(), nullable=True,
                  comment='Label Surging..Falling.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('titlescoreid'),
        sa.UniqueConstraint('score_date', 'title',
                            name='uq_title_score_day'),
        schema='games',
        comment='Daily competitive snapshot; brandtracker weighted '
                'z-scores + share-of-voice league per tracked title. '
                'Z-scores are relative to the tracked set on that day, '
                'not the whole market.',
    )
    op.create_index('ix_title_score_gameid', 'title_score', ['gameid'],
                    schema='games')


def downgrade():
    op.drop_index('ix_title_score_gameid', table_name='title_score',
                  schema='games')
    op.drop_table('title_score', schema='games')
