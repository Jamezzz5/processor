"""widen gwi_affinity from top over-indexers to per-cohort crosstab facts

Revision ID: d9f1b3c5a7e9
Revises: c7a9e1d3b5f7
Create Date: 2026-07-23

The table stored one row per (market, gender, base, category, name)
carrying that item's PEAK index across cohorts. That shape answers
"what over-indexes?" and nothing else: penetration was never stored,
sub-115 items were filtered out before the write, and the peak was a
max taken across overlapping age bands (16-24, 25-34 and 16-34 all
exist), which is not a coherent ranking.

Cohort joins the natural key and the measures become per-cohort, so a
reading is always attributable to one band.

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd9f1b3c5a7e9'
down_revision = 'c7a9e1d3b5f7'
branch_labels = None
depends_on = None


def upgrade():
    # Existing rows are peak-across-cohorts with no cohort attribution
    # and no penetration — there is nothing to carry forward, and
    # keeping them would collapse onto cohort='' under the new key.
    # The nightly GWI ingest rebuilds the table from the Master Sheets.
    op.execute('DELETE FROM games.gwi_affinity')
    op.drop_constraint('uq_gwi_affinity_item', 'gwi_affinity',
                       schema='games', type_='unique')
    op.add_column('gwi_affinity',
                  sa.Column('cohort', sa.Text(), nullable=False,
                            server_default='',
                            comment="Age/gender band, e.g. 'Male 16-34'. "
                                    'Bands overlap (16-24, 25-34 and '
                                    '16-34 all exist) — never rank across '
                                    'cohorts, and never sum them.'),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('index_value', sa.Numeric(), nullable=True,
                            comment='GWI Index vs the base average; '
                                    '100 = average.'),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('pct', sa.Numeric(), nullable=True,
                            comment='Column % — penetration within the '
                                    'cohort (0-1).'),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('responses', sa.Numeric(), nullable=True,
                            comment='Unweighted sample behind the cell.'),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('universe', sa.Numeric(), nullable=True,
                            comment='People in the cohort who answered '
                                    'this item.'),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('base_universe', sa.Numeric(), nullable=True,
                            comment="Cohort population from the "
                                    "crosstab's Totals row — the "
                                    'audience-size denominator.'),
                  schema='games')
    op.alter_column('gwi_affinity', 'base',
                    comment="Audience definition, e.g. 'PC Gamer'. An "
                            "'All Internet Users' base is the sizing "
                            'reference, not an audience.',
                    existing_type=sa.Text(), existing_nullable=False,
                    schema='games')
    op.drop_column('gwi_affinity', 'peak_index', schema='games')
    op.drop_column('gwi_affinity', 'peak_cohort', schema='games')
    op.create_unique_constraint(
        'uq_gwi_affinity_item', 'gwi_affinity',
        ['market', 'gender', 'base', 'cohort', 'category', 'name'],
        schema='games')
    op.create_index('ix_gwi_affinity_scope', 'gwi_affinity',
                    ['market', 'cohort', 'category'], schema='games')
    op.create_index('ix_gwi_affinity_name', 'gwi_affinity', ['name'],
                    schema='games')
    op.execute(
        "COMMENT ON TABLE games.gwi_affinity IS "
        "'GWI crosstab cell per audience cohort; audience-keyed, not "
        "game-keyed — no gameid by design. One row per item per cohort.'")


def downgrade():
    # Symmetric with upgrade: per-cohort rows cannot be folded back
    # into one peak row without re-deriving it, so the rebuild owns it.
    op.execute('DELETE FROM games.gwi_affinity')
    op.drop_index('ix_gwi_affinity_name', 'gwi_affinity', schema='games')
    op.drop_index('ix_gwi_affinity_scope', 'gwi_affinity', schema='games')
    op.drop_constraint('uq_gwi_affinity_item', 'gwi_affinity',
                       schema='games', type_='unique')
    op.add_column('gwi_affinity',
                  sa.Column('peak_cohort', sa.Text(), nullable=True),
                  schema='games')
    op.add_column('gwi_affinity',
                  sa.Column('peak_index', sa.Numeric(), nullable=True),
                  schema='games')
    for col in ('base_universe', 'universe', 'responses', 'pct',
                'index_value', 'cohort'):
        op.drop_column('gwi_affinity', col, schema='games')
    op.create_unique_constraint(
        'uq_gwi_affinity_item', 'gwi_affinity',
        ['market', 'gender', 'base', 'category', 'name'], schema='games')
