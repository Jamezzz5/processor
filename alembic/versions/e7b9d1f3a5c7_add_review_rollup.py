"""add review_rollup monthly steam fact

Revision ID: e7b9d1f3a5c7
Revises: d5f7a9c1e3b5
Create Date: 2026-08-04

"""
import sqlalchemy as sa
from alembic import op

revision = 'e7b9d1f3a5c7'
down_revision = 'd5f7a9c1e3b5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'review_rollup',
        sa.Column('reviewrollupid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('month_start', sa.Date(), nullable=False,
                  comment='UTC month start of the rollup.'),
        sa.Column('positive', sa.Numeric(), nullable=True,
                  comment='Reviews recommending the title that month '
                          '(recommendations_up).'),
        sa.Column('negative', sa.Numeric(), nullable=True,
                  comment='Reviews not recommending the title that '
                          'month (recommendations_down).'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last sweep that refreshed this '
                          'row - the lane recency column, since '
                          'month_start ages by construction.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('reviewrollupid'),
        sa.UniqueConstraint('gameid', 'month_start',
                            name='uq_review_rollup_month'),
        schema='games',
        comment='Steam monthly review rollup; positive/negative review '
                'counts per game per month from the store '
                'appreviewhistogram endpoint.',
    )
    op.create_index('ix_review_rollup_month', 'review_rollup',
                    ['month_start'], schema='games')


def downgrade():
    op.drop_index('ix_review_rollup_month', table_name='review_rollup',
                  schema='games')
    op.drop_table('review_rollup', schema='games')
