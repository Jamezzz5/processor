"""add price_snapshot daily Steam price/discount fact

Revision ID: c5e7a9b1d3f5
Revises: b3d5f7a9c1e3
Create Date: 2026-08-31

"""
import sqlalchemy as sa
from alembic import op

revision = 'c5e7a9b1d3f5'
down_revision = 'b3d5f7a9c1e3'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'price_snapshot',
        sa.Column('pricesnapshotid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('price_date', sa.Date(), nullable=False,
                  comment='UTC ingest date.'),
        sa.Column('currency', sa.Text(), nullable=True,
                  comment='ISO currency code as returned (request is '
                          'cc=us, so USD in practice).'),
        sa.Column('base_price', sa.Numeric(), nullable=True,
                  comment='List price before discount '
                          '(price_overview.initial / 100).'),
        sa.Column('final_price', sa.Numeric(), nullable=True,
                  comment='Price after the current discount '
                          '(price_overview.final / 100).'),
        sa.Column('discount_pct', sa.Numeric(), nullable=True,
                  comment="Steam's own discount_percent (0 when not "
                          'on sale).'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('pricesnapshotid'),
        sa.UniqueConstraint('gameid', 'price_date',
                            name='uq_price_snapshot_day'),
        schema='games',
        comment='Daily Steam price/discount state per game from '
                'appdetails price_overview, single region (US at '
                'ship). Sparse: a row lands when the appdetails '
                'budget reaches the title, so gaps mean "not '
                'checked", never "no price". Free titles carry no '
                'row (appdetails omits price_overview).',
    )
    op.create_index('ix_price_snapshot_date', 'price_snapshot',
                    ['price_date'], schema='games')


def downgrade():
    op.drop_index('ix_price_snapshot_date',
                  table_name='price_snapshot', schema='games')
    op.drop_table('price_snapshot', schema='games')
