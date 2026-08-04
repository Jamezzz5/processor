"""add ad_spend pathmatics fact

Revision ID: f9b1d3c5e7a9
Revises: e7b9d1f3a5c7
Create Date: 2026-08-04

"""
import sqlalchemy as sa
from alembic import op

revision = 'f9b1d3c5e7a9'
down_revision = 'e7b9d1f3a5c7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'ad_spend',
        sa.Column('adspendid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = brand not yet matched to the game '
                          'dim; the raw brand columns are retained.'),
        sa.Column('spend_date', sa.Date(), nullable=False,
                  comment='Spend day as exported.'),
        sa.Column('brand', sa.Text(), nullable=False,
                  comment='Pathmatics Brand (Leaf) - the title-level '
                          'brand.'),
        sa.Column('advertiser', sa.Text(), nullable=True),
        sa.Column('brand_root', sa.Text(), nullable=True,
                  comment='Pathmatics Brand Root (publisher label / '
                          'franchise).'),
        sa.Column('channel', sa.Text(), nullable=False,
                  server_default='ALL',
                  comment='Export channel cut (Facebook, YouTube, '
                          '...); ALL = not cut by channel.'),
        sa.Column('country', sa.Text(), nullable=False,
                  server_default='ALL',
                  comment='Export country cut (US, ...); ALL = not '
                          'cut by country.'),
        sa.Column('buy_type', sa.Text(), nullable=False,
                  server_default='ALL',
                  comment='Direct | Indirect | House Ads; ALL when '
                          'the export lacked the column.'),
        sa.Column('spend', sa.Numeric(), nullable=True,
                  comment='USD.'),
        sa.Column('impressions', sa.Numeric(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last export sweep that '
                          'refreshed this row.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('adspendid'),
        sa.UniqueConstraint('spend_date', 'brand', 'channel',
                            'country', 'buy_type',
                            name='uq_ad_spend_cell'),
        schema='games',
        comment='Pathmatics daily ad spend; one row per brand per '
                'day per export cut (channel/country/buy type). ALL '
                'marks a dimension the export was not cut by - never '
                'sum ALL rows with cut rows.',
    )
    op.create_index('ix_ad_spend_date', 'ad_spend', ['spend_date'],
                    schema='games')
    op.create_index('ix_ad_spend_gameid', 'ad_spend', ['gameid'],
                    schema='games')


def downgrade():
    op.drop_index('ix_ad_spend_gameid', table_name='ad_spend',
                  schema='games')
    op.drop_index('ix_ad_spend_date', table_name='ad_spend',
                  schema='games')
    op.drop_table('ad_spend', schema='games')
