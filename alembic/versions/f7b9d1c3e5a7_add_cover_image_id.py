"""add cover_image_id to game + game_release

Revision ID: f7b9d1c3e5a7
Revises: e5a7c9b1d3f7
Create Date: 2026-07-29

"""
from alembic import op
import sqlalchemy as sa


revision = 'f7b9d1c3e5a7'
down_revision = 'e5a7c9b1d3f7'
branch_labels = None
depends_on = None

_COMMENT = ('IGDB cover image id; render via images.igdb.com'
            '/igdb/image/upload/t_cover_big/{id}.jpg.')


def upgrade():
    op.add_column(
        'game',
        sa.Column('cover_image_id', sa.Text(), nullable=True,
                  comment=_COMMENT),
        schema='games')
    op.add_column(
        'game_release',
        sa.Column('cover_image_id', sa.Text(), nullable=True,
                  comment=_COMMENT),
        schema='games')


def downgrade():
    op.drop_column('game_release', 'cover_image_id', schema='games')
    op.drop_column('game', 'cover_image_id', schema='games')
