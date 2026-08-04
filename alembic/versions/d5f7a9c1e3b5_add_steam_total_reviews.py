"""add steam_total_reviews to community_snapshot

Revision ID: d5f7a9c1e3b5
Revises: c1e3a5b7d9f1
Create Date: 2026-08-03

"""
from alembic import op
import sqlalchemy as sa


revision = 'd5f7a9c1e3b5'
down_revision = 'c1e3a5b7d9f1'
branch_labels = None
depends_on = None

_COMMENT = ('Lifetime Steam review count (appreviews summary) - the '
            'review-stats lane\'s own liveness column; '
            'steam_positive_pct is shared with the curated registry '
            'sheet.')


def upgrade():
    op.add_column(
        'community_snapshot',
        sa.Column('steam_total_reviews', sa.Numeric(), nullable=True,
                  comment=_COMMENT),
        schema='games')


def downgrade():
    op.drop_column('community_snapshot', 'steam_total_reviews',
                   schema='games')
