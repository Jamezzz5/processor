"""add game_release calendar fact + igdb_id game identity

Revision ID: c7a9e1d3b5f7
Revises: b3e5a7c9d1f3
Create Date: 2026-07-20

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c7a9e1d3b5f7'
down_revision = 'b3e5a7c9d1f3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('game', sa.Column('igdb_id', sa.BigInteger(),
                                    nullable=True), schema='games')
    op.create_unique_constraint('uq_game_igdb_id', 'game', ['igdb_id'],
                                schema='games')
    op.create_table(
        'game_release',
        sa.Column('gamereleaseid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game '
                          'dim; the raw title is retained.'),
        sa.Column('igdb_id', sa.BigInteger(), nullable=False),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('slug', sa.Text(), nullable=True),
        sa.Column('release_date', sa.Date(), nullable=True,
                  comment='Current expected date parsed from IGDB '
                          'first_release_date.'),
        sa.Column('hypes', sa.Numeric(), nullable=True,
                  comment='IGDB hype count (pre-release follows).'),
        sa.Column('genres', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB genre names.'),
        sa.Column('platforms', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB platform names.'),
        sa.Column('url', sa.Text(), nullable=True),
        sa.Column('first_seen_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; when the row was created.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last sweep that saw it.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('gamereleaseid'),
        sa.UniqueConstraint('igdb_id', name='uq_game_release_igdb'),
        schema='games',
        comment='IGDB release calendar; one row per title from the '
                'nightly windowed sweep, release_date is the current '
                'expectation and self-corrects on rerun.',
    )
    op.create_index('ix_game_release_gameid', 'game_release', ['gameid'],
                    schema='games')
    op.create_index('ix_game_release_date', 'game_release',
                    ['release_date'], schema='games')


def downgrade():
    op.drop_index('ix_game_release_date', table_name='game_release',
                  schema='games')
    op.drop_index('ix_game_release_gameid', table_name='game_release',
                  schema='games')
    op.drop_table('game_release', schema='games')
    op.drop_constraint('uq_game_igdb_id', 'game', schema='games',
                       type_='unique')
    op.drop_column('game', 'igdb_id', schema='games')
