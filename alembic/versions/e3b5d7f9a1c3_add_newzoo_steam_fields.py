"""add newzoo_engagement steam wishlist and follower fields

Revision ID: e3b5d7f9a1c3
Revises: d1f3b5a7c9e1
Create Date: 2026-08-19

"""
import sqlalchemy as sa
from alembic import op

revision = 'e3b5d7f9a1c3'
down_revision = 'd1f3b5a7c9e1'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'newzoo_engagement',
        sa.Column('steam_wishlists', sa.Numeric(), nullable=True,
                  comment='Steam wishlist count carried through the '
                          'Newzoo MAU export - the only automated '
                          'wishlist source.'),
        schema='games')
    op.add_column(
        'newzoo_engagement',
        sa.Column('steam_followers', sa.Numeric(), nullable=True,
                  comment='Steam follower count carried through the '
                          'Newzoo MAU export; the daily sheet-fed '
                          'count lives on community_snapshot.'),
        schema='games')


def downgrade():
    op.drop_column('newzoo_engagement', 'steam_followers',
                   schema='games')
    op.drop_column('newzoo_engagement', 'steam_wishlists',
                   schema='games')
