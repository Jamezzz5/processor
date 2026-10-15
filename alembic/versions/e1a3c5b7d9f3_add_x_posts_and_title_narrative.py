"""X post counts on the community snapshot and the weekly title
narratives table.

Revision ID: e1a3c5b7d9f3
Revises: d7f1a3c5e9b1
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = 'e1a3c5b7d9f3'
down_revision = 'd7f1a3c5e9b1'
branch_labels = None
depends_on = None


def upgrade():
    """One snapshot column and one derived table."""
    op.add_column(
        'community_snapshot',
        sa.Column('x_posts', sa.Numeric(),
                  comment="Posts on X that UTC day matching the title's "
                          "names and hashtags, reposts excluded - X's "
                          'own count from counts/recent. A shared name '
                          'is qualified with game words, so it can '
                          'undercount. Context, never scored.'),
        schema='games')
    op.create_table(
        'title_narrative',
        sa.Column('titlenarrativeid', sa.BigInteger(), primary_key=True),
        sa.Column('gameid', sa.BigInteger(),
                  sa.ForeignKey('games.game.gameid'), nullable=False),
        sa.Column('week_start', sa.Date(), nullable=False,
                  comment='ISO Monday of the week read.'),
        sa.Column('narrative_key', sa.Text(), nullable=False,
                  comment='Identity carried across weeks: the slug of '
                          'the name the week it was born.'),
        sa.Column('name', sa.Text(), nullable=False,
                  comment='Model-named the week it was born, kept while '
                          'continued.'),
        sa.Column('item_count', sa.Numeric(),
                  comment='Sampled items assigned.'),
        sa.Column('sample_size', sa.Numeric(),
                  comment='Items shown to the model.'),
        sa.Column('pool_size', sa.Numeric(),
                  comment='New items that week before sampling.'),
        sa.Column('share', sa.Numeric(),
                  comment='item_count / sample_size (0-1).'),
        sa.Column('source_mix',
                  sa.JSON().with_variant(postgresql.JSONB(), 'postgresql'),
                  comment='{review|press|news|youtube: items}.'),
        sa.Column('positive', sa.Numeric(),
                  comment="Items positive by their source's own flag "
                          '(voted_up, press tone, comment sentiment).'),
        sa.Column('negative', sa.Numeric()),
        sa.Column('neutral', sa.Numeric(),
                  comment='Neutral, unjudged and developer posts.'),
        sa.Column('example_ids',
                  sa.JSON().with_variant(postgresql.JSONB(), 'postgresql'),
                  comment='{source: [ids]}, up to two each; text is '
                          'read live and comments age out with the '
                          'purge.'),
        sa.Column('status', sa.Text(),
                  comment="'new' | 'rising' | 'steady' | 'fading' "
                          'against the prior week.'),
        sa.Column('prior_share', sa.Numeric(),
                  comment="The continued narrative's share the week "
                          'before; NULL when new.'),
        sa.Column('prompt_version', sa.Integer(), nullable=False),
        sa.Column('model', sa.Text(), comment='Classifier model id.'),
        sa.Column('computed_at', sa.DateTime(),
                  comment='Naive UTC; the recency column.'),
        sa.UniqueConstraint('gameid', 'week_start', 'narrative_key',
                            name='uq_title_narrative_cell'),
        schema='games',
        comment="Narratives named over one week's sampled reviews, "
                'press, developer posts and trailer comments per title. '
                'Names are model-authored; counts, shares and status '
                'are arithmetic. Never scored.',
    )
    op.create_index('ix_title_narrative_week', 'title_narrative',
                    ['week_start'], schema='games')


def downgrade():
    """Drop only what this revision added."""
    op.drop_index('ix_title_narrative_week', table_name='title_narrative',
                  schema='games')
    op.drop_table('title_narrative', schema='games')
    op.drop_column('community_snapshot', 'x_posts', schema='games')
