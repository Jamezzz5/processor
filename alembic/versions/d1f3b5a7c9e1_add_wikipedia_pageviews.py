"""add wikipedia_pageview fact and attention wikipedia_views

Revision ID: d1f3b5a7c9e1
Revises: c9e1b3d5f7a9
Create Date: 2026-08-18

"""
import sqlalchemy as sa
from alembic import op

revision = 'd1f3b5a7c9e1'
down_revision = 'c9e1b3d5f7a9'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'wikipedia_pageview',
        sa.Column('wikipediapageviewid', sa.BigInteger(),
                  nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game '
                          'dim; the raw title is retained.'),
        sa.Column('title', sa.Text(), nullable=False,
                  comment='Tracked title as queried.'),
        sa.Column('week_start', sa.Date(), nullable=False,
                  comment='ISO week Monday.'),
        sa.Column('views', sa.Numeric(), nullable=True,
                  comment="Sum of the ISO week's daily user (non-bot) "
                          'pageviews for the linked article.'),
        sa.Column('article', sa.Text(), nullable=True,
                  comment='Wikipedia article title fetched, kept for '
                          'audit as the community_link mapping '
                          'evolves.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last sweep that refreshed this '
                          'row - the lane recency column, since '
                          'week_start ages by construction.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('wikipediapageviewid'),
        sa.UniqueConstraint('week_start', 'title',
                            name='uq_wikipedia_pageview_week'),
        schema='games',
        comment='Weekly Wikipedia article pageviews per tracked '
                'title, user agent only. The API serves history back '
                'to July 2015, so this is the one attention signal '
                'with a deep backfill.',
    )
    op.create_index('ix_wikipedia_pageview_week', 'wikipedia_pageview',
                    ['week_start'], schema='games')
    op.create_index('ix_wikipedia_pageview_gameid',
                    'wikipedia_pageview', ['gameid'], schema='games')
    op.add_column(
        'attention_share',
        sa.Column('wikipedia_views', sa.Numeric(), nullable=True,
                  comment='Weekly sum of daily Wikipedia article '
                          'pageviews, user agent only.'),
        schema='games')


def downgrade():
    op.drop_column('attention_share', 'wikipedia_views',
                   schema='games')
    op.drop_index('ix_wikipedia_pageview_gameid',
                  table_name='wikipedia_pageview', schema='games')
    op.drop_index('ix_wikipedia_pageview_week',
                  table_name='wikipedia_pageview', schema='games')
    op.drop_table('wikipedia_pageview', schema='games')
