"""add title_score inputs and satisfaction

Revision ID: f5b7d9a1c3e5
Revises: e3b5d7f9a1c3
Create Date: 2026-08-25

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = 'f5b7d9a1c3e5'
down_revision = 'e3b5d7f9a1c3'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'title_score',
        sa.Column('satisfaction', sa.Numeric(), nullable=True,
                  comment='Weighted z-score of the reception signals '
                          '(Steam positive review share). Reported '
                          'beside the composite, never summed into it. '
                          'NULL on rows scored before the column.'),
        schema='games')
    op.add_column(
        'title_score',
        sa.Column('inputs', postgresql.JSONB(), nullable=True,
                  comment='Every metric behind the row\'s scores: '
                          '{metric: {value, prior, z, weight, '
                          'dimension}} - the month mean, the '
                          'comparison month\'s mean, its z-score '
                          'against that day\'s field, the weight it '
                          'carried and the dimension it rode. Roughly '
                          '19 metrics per title per day (1-2 MB/day '
                          'over the tracked field). NULL on rows '
                          'scored before the column.'),
        schema='games')


def downgrade():
    op.drop_column('title_score', 'inputs', schema='games')
    op.drop_column('title_score', 'satisfaction', schema='games')
