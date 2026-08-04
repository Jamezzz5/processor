"""add genre to title_score

Revision ID: c1e3a5b7d9f1
Revises: a3c5e7b9d1f5
Create Date: 2026-08-03

"""
from alembic import op
import sqlalchemy as sa


revision = 'c1e3a5b7d9f1'
down_revision = 'a3c5e7b9d1f5'
branch_labels = None
depends_on = None

_COMMENT = ('Genre of the tracked title, denormalised at snapshot '
            'time: latest Newzoo drop (best rank wins) else the game '
            'dim. NULL = no genre known. Pre-column history is '
            'backfilled fill-only from the then-current taxonomy.')


def upgrade():
    op.add_column(
        'title_score',
        sa.Column('genre', sa.Text(), nullable=True, comment=_COMMENT),
        schema='games')


def downgrade():
    op.drop_column('title_score', 'genre', schema='games')
