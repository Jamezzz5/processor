"""add game_descriptor, game_neighbour and game_alias

Revision ID: a5c7e9b1d3f5
Revises: d3f5a7b9c1e5
Create Date: 2026-09-09

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = 'a5c7e9b1d3f5'
down_revision = 'd3f5a7b9c1e5'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'game_descriptor',
        sa.Column('gamedescriptorid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('igdb_id', sa.BigInteger(), nullable=False),
        sa.Column('genres', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB genre names.'),
        sa.Column('themes', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB theme names.'),
        sa.Column('keywords', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB keywords, the first 20 in '
                          'IGDB order.'),
        sa.Column('game_modes', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB game_modes names.'),
        sa.Column('player_perspectives', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB player_perspectives names.'),
        sa.Column('platforms', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB platform abbreviations '
                          '(name when none).'),
        sa.Column('similar_igdb_ids', sa.Text(), nullable=True,
                  comment='Comma-joined IGDB ids from similar_games; the '
                          'competitor model reads it as a direct edge.'),
        sa.Column('summary', sa.Text(), nullable=True,
                  comment='IGDB summary text; stored for reading, never '
                          'scored.'),
        sa.Column('first_release_date', sa.Date(), nullable=True,
                  comment='IGDB first_release_date as a date; NULL when '
                          'IGDB has none.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last refresh - the lane rotation '
                          'column.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('gamedescriptorid'),
        sa.UniqueConstraint('gameid', name='uq_game_descriptor_game'),
        schema='games',
        comment='IGDB multi-valued descriptors per game (genres, themes, '
                'keywords, modes, perspectives, platforms, similar-game '
                'ids, summary). Comma-joined text like game_release; one '
                'row per game, refreshed in place by the nightly '
                'descriptor lane.',
    )
    op.create_index('ix_game_descriptor_updated', 'game_descriptor',
                    ['updated_at'], schema='games')
    op.create_table(
        'game_neighbour',
        sa.Column('gameneighbourid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('neighbour_gameid', sa.BigInteger(), nullable=False),
        sa.Column('rank', sa.Integer(), nullable=False,
                  comment="1-based position among the subject's "
                          'neighbours.'),
        sa.Column('score', sa.Numeric(), nullable=False,
                  comment='0-1 weighted similarity over the present '
                          'components.'),
        sa.Column('evidence_weight', sa.Numeric(), nullable=True,
                  comment='Sum of the weights of the components both '
                          'titles carried (1.0 = fully evidenced).'),
        sa.Column('components', postgresql.JSONB(astext_type=sa.Text()),
                  nullable=True,
                  comment='{component: {s, w, shared, label}} for every '
                          'PRESENT component; absent ones are omitted, '
                          'never zeroed.'),
        sa.Column('computed_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC; the derive that wrote it.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.ForeignKeyConstraint(['neighbour_gameid'],
                                ['games.game.gameid']),
        sa.PrimaryKeyConstraint('gameneighbourid'),
        sa.UniqueConstraint('gameid', 'neighbour_gameid',
                            name='uq_game_neighbour_pair'),
        schema='games',
        comment='Nightly top-N similarity neighbours per game. score is '
                'a weighted mean over the components both titles carried '
                '(weights renormalise over the present ones); components '
                'names every fact behind it. Rebuilt whole each night - '
                'absence means "not built", never "no neighbours".',
    )
    op.create_index('ix_game_neighbour_neighbour', 'game_neighbour',
                    ['neighbour_gameid'], schema='games')
    op.create_index('ix_game_neighbour_computed', 'game_neighbour',
                    ['computed_at'], schema='games')
    op.create_table(
        'game_alias',
        sa.Column('gamealiasid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('alias', sa.Text(), nullable=False,
                  comment='The name as written.'),
        sa.Column('alias_key', sa.Text(), nullable=False,
                  comment='Casefolded, stripped alias - the match key.'),
        sa.Column('source', sa.Text(), nullable=True,
                  comment='Who curated it: registry sheet, competitor '
                          'band, pathmatics brand.'),
        sa.Column('created_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('gamealiasid'),
        sa.UniqueConstraint('alias_key', name='uq_game_alias_key'),
        schema='games',
        comment='Curated alternate names per game (Pathmatics brands, '
                'advertiser labels, store spellings). alias_key is the '
                'casefolded alias and is unique: one spelling names one '
                'game. Matching is exact, never fuzzy.',
    )
    op.create_index('ix_game_alias_gameid', 'game_alias', ['gameid'],
                    schema='games')


def downgrade():
    op.drop_index('ix_game_alias_gameid', table_name='game_alias',
                  schema='games')
    op.drop_table('game_alias', schema='games')
    op.drop_index('ix_game_neighbour_computed', table_name='game_neighbour',
                  schema='games')
    op.drop_index('ix_game_neighbour_neighbour',
                  table_name='game_neighbour', schema='games')
    op.drop_table('game_neighbour', schema='games')
    op.drop_index('ix_game_descriptor_updated',
                  table_name='game_descriptor', schema='games')
    op.drop_table('game_descriptor', schema='games')
