"""add store_asset storefront asset digest fact

Revision ID: d7f9b1c3e5a7
Revises: c5e7a9b1d3f5
Create Date: 2026-09-01

"""
import sqlalchemy as sa
from alembic import op

revision = 'd7f9b1c3e5a7'
down_revision = 'c5e7a9b1d3f5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'store_asset',
        sa.Column('storeassetid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('checked_at', sa.Date(), nullable=False,
                  comment='UTC ingest date.'),
        sa.Column('asset_kind', sa.Text(), nullable=False,
                  comment='header | screenshots | movies | '
                          'description.'),
        sa.Column('digest', sa.Text(), nullable=False,
                  comment='sha1 over the normalised asset content '
                          '(URLs with cache-busters stripped, movie '
                          'id:name pairs, or whitespace-collapsed '
                          'description text).'),
        sa.Column('item_count', sa.Integer(), nullable=True,
                  comment='Items behind the digest (screenshots or '
                          'movies in the set; 1 for header and '
                          'description).'),
        sa.Column('sample', sa.Text(), nullable=True,
                  comment='First URL, first movie name, or the '
                          'opening characters of the description - '
                          'evidence for the change row, not the full '
                          'asset.'),
        sa.Column('changed', sa.Boolean(), nullable=True,
                  comment='True when the digest differs from this '
                          "game's previous stored digest for the "
                          'same kind; False on the first '
                          'observation.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('storeassetid'),
        sa.UniqueConstraint('gameid', 'checked_at', 'asset_kind',
                            name='uq_store_asset_day'),
        schema='games',
        comment='Storefront asset digests per game per checked day '
                'from Steam appdetails (header image, screenshots, '
                'movies, short description). Sparse: a row lands when '
                'the appdetails budget reaches the title, so gaps mean '
                '"not checked", never "unchanged". changed compares '
                'against the previous stored digest for the same '
                'kind; a first observation is never a change.',
    )
    op.create_index('ix_store_asset_date', 'store_asset',
                    ['checked_at'], schema='games')


def downgrade():
    op.drop_index('ix_store_asset_date', table_name='store_asset',
                  schema='games')
    op.drop_table('store_asset', schema='games')
