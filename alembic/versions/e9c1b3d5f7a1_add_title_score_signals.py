"""add title_score signals count

Revision ID: e9c1b3d5f7a1
Revises: d7f9b1c3e5a7
Create Date: 2026-09-04

"""
import sqlalchemy as sa
from alembic import op

revision = 'e9c1b3d5f7a1'
down_revision = 'd7f9b1c3e5a7'
branch_labels = None
depends_on = None

COMPOSITE_COMMENT = ('Mean of the composite dimensions the title '
                     'carries, each the weighted mean of its signals\' '
                     'rank-normal scores, on a ±100 scale.')
LEGACY_COMPOSITE_COMMENT = 'Sum of the dimension z-scores.'
INPUTS_COMMENT = ('Every metric behind the row\'s scores: {metric: '
                  '{value, prior, z, weight, dimension}} - the month '
                  'mean, the comparison month\'s mean, its rank-normal '
                  'score against that day\'s field, the weight it '
                  'carried and the dimension it rode. Roughly 19 '
                  'metrics per title per day (1-2 MB/day over the '
                  'tracked field). NULL on rows scored before the '
                  'column.')
LEGACY_INPUTS_COMMENT = ('Every metric behind the row\'s scores: '
                         '{metric: {value, prior, z, weight, '
                         'dimension}} - the month mean, the comparison '
                         'month\'s mean, its z-score against that '
                         'day\'s field, the weight it carried and the '
                         'dimension it rode. Roughly 19 metrics per '
                         'title per day (1-2 MB/day over the tracked '
                         'field). NULL on rows scored before the '
                         'column.')


def upgrade():
    op.add_column(
        'title_score',
        sa.Column('signals', sa.Integer(), nullable=True,
                  comment='How many metrics the composite dimensions '
                          'actually read - the evidence the score '
                          'stands on. Under the kernel\'s minimum the '
                          'title is thin: it keeps its number, ranks '
                          'behind every fully scored title and sits '
                          'the daily board out. NULL on rows scored '
                          'before the column; Rescore Title Scores '
                          'fills them.'),
        schema='games')
    op.alter_column('title_score', 'composite', schema='games',
                    existing_type=sa.Numeric(),
                    comment=COMPOSITE_COMMENT,
                    existing_comment=LEGACY_COMPOSITE_COMMENT)
    op.alter_column('title_score', 'inputs', schema='games',
                    existing_type=sa.JSON(),
                    comment=INPUTS_COMMENT,
                    existing_comment=LEGACY_INPUTS_COMMENT)


def downgrade():
    op.alter_column('title_score', 'inputs', schema='games',
                    existing_type=sa.JSON(),
                    comment=LEGACY_INPUTS_COMMENT,
                    existing_comment=INPUTS_COMMENT)
    op.alter_column('title_score', 'composite', schema='games',
                    existing_type=sa.Numeric(),
                    comment=LEGACY_COMPOSITE_COMMENT,
                    existing_comment=COMPOSITE_COMMENT)
    op.drop_column('title_score', 'signals', schema='games')
