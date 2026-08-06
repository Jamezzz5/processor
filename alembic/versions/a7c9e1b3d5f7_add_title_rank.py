"""add title_rank fact and title_score.set_size

Revision ID: a7c9e1b3d5f7
Revises: f9b1d3c5e7a9
Create Date: 2026-08-05

"""
import sqlalchemy as sa
from alembic import op

revision = 'a7c9e1b3d5f7'
down_revision = 'f9b1d3c5e7a9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'title_rank',
        sa.Column('titlerankid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet promoted into the game '
                          'dim; the raw title is retained.'),
        sa.Column('source', sa.Text(), nullable=False,
                  comment="Ranking source. The IGDB popularity types "
                          "('igdb:visits', 'igdb:want-to-play', "
                          "'igdb:playing', 'igdb:played') are derived "
                          "from /v4/popularity_types at runtime, so a "
                          "type IGDB adds later arrives without a code "
                          "change. Also 'league' (our own derived title "
                          "league), 'igdb-release', 'brandtracker', "
                          "'registry' and 'newzoo'."),
        sa.Column('rank_date', sa.Date(), nullable=False,
                  comment='UTC date the reading was taken.'),
        sa.Column('title', sa.Text(), nullable=False,
                  comment='Raw title as this source spells it.'),
        sa.Column('slug', sa.Text(), nullable=True),
        sa.Column('rank', sa.Numeric(), nullable=False,
                  comment='1-based ordinal within (source, rank_date).'),
        sa.Column('value', sa.Numeric(), nullable=True,
                  comment="The source's own magnitude behind the rank "
                          '(IGDB popularity value, league score, MAU). '
                          'NULL when the source publishes an order '
                          'only.'),
        sa.Column('value_label', sa.Text(), nullable=True,
                  comment="Unit of value — 'igdb_popularity', "
                          "'league_score', 'mau'."),
        sa.Column('igdb_id', sa.BigInteger(), nullable=True,
                  comment='Identity the promoter prefers over a name '
                          'match, when the source carries one.'),
        sa.Column('steam_appid', sa.BigInteger(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last run that saw this row.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('titlerankid'),
        sa.UniqueConstraint('source', 'rank_date', 'title',
                            name='uq_title_rank_obs'),
        schema='games',
        comment='Per-source daily global title ranking — the input the '
                'automated title universe is promoted from. rank is '
                'ordinal within (source, rank_date); value is that '
                "source's own magnitude and value_label names its unit.",
    )
    op.create_index('ix_title_rank_date', 'title_rank', ['rank_date'],
                    schema='games')
    op.create_index('ix_title_rank_gameid', 'title_rank', ['gameid'],
                    schema='games')
    op.add_column(
        'title_score',
        sa.Column('set_size', sa.Integer(), nullable=True,
                  comment="How many titles that day's z-scores were "
                          'taken against. The composite is a position '
                          'within that field, not an absolute rating, '
                          'so rows carrying different set_size values '
                          'are not directly comparable — the field '
                          'widened when the automated title universe '
                          'landed.'),
        schema='games')


def downgrade():
    op.drop_column('title_score', 'set_size', schema='games')
    op.drop_index('ix_title_rank_gameid', table_name='title_rank',
                  schema='games')
    op.drop_index('ix_title_rank_date', table_name='title_rank',
                  schema='games')
    op.drop_table('title_rank', schema='games')
