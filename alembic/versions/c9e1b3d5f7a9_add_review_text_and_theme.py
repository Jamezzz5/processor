"""add review_text corpus and review_theme aggregates

Revision ID: c9e1b3d5f7a9
Revises: b7d9f1a3c5e7
Create Date: 2026-08-17

"""
import sqlalchemy as sa
from alembic import op

revision = 'c9e1b3d5f7a9'
down_revision = 'b7d9f1a3c5e7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'review_text',
        sa.Column('reviewtextid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('recommendationid', sa.BigInteger(), nullable=False,
                  comment="Steam's own review id - the idempotency "
                          'key.'),
        sa.Column('language', sa.Text(), nullable=False,
                  server_default='english'),
        sa.Column('review_text', sa.Text(), nullable=True,
                  comment='The review body as fetched.'),
        sa.Column('voted_up', sa.Integer(), nullable=True,
                  comment='1 = recommends the title, 0 = does not.'),
        sa.Column('playtime_at_review', sa.Numeric(), nullable=True,
                  comment='Author minutes played when the review was '
                          'written.'),
        sa.Column('votes_up', sa.Numeric(), nullable=True,
                  comment='Helpful votes from other users.'),
        sa.Column('weighted_vote_score', sa.Numeric(), nullable=True,
                  comment="Steam's 0-1 helpfulness weighting; how "
                          'exemplar reviews are picked.'),
        sa.Column('created_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC review creation time - the '
                          'cap-trim order column.'),
        sa.Column('updated_review_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last edit by the author.'),
        sa.Column('fetched_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last sweep that touched this '
                          'row - the lane recency column, since '
                          'created_at ages by construction.'),
        sa.Column('themes', sa.Text(), nullable=True,
                  comment='JSON array of taxonomy keys the classifier '
                          'assigned; [] means classified with none '
                          'applying, NULL means not yet classified.'),
        sa.Column('classified_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; when the classifier stamped '
                          'themes.'),
        sa.Column('taxonomy_version', sa.Integer(), nullable=True,
                  comment='Taxonomy the themes were assigned under; '
                          'rows re-enter the eligible pool when the '
                          'current version moves past it.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('reviewtextid'),
        sa.UniqueConstraint('recommendationid',
                            name='uq_review_text_rec'),
        schema='games',
        comment='Most recent N English Steam review bodies per title '
                '(N = STEAM_REVIEW_TEXT_CAP, writer-enforced) - the '
                'reclassifiable corpus behind review_theme, not a '
                'complete history.',
    )
    op.create_index('ix_review_text_game_created', 'review_text',
                    ['gameid', 'created_at'], schema='games')
    op.create_table(
        'review_theme',
        sa.Column('reviewthemeid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('theme', sa.Text(), nullable=False,
                  comment='Taxonomy key.'),
        sa.Column('mentions', sa.Numeric(), nullable=True,
                  comment='Classified reviews carrying the theme.'),
        sa.Column('positive_mentions', sa.Numeric(), nullable=True,
                  comment='Of mentions, reviews whose author '
                          "recommends the title (Steam's own voted_up "
                          'flag, not model-judged sentiment).'),
        sa.Column('negative_mentions', sa.Numeric(), nullable=True,
                  comment='Of mentions, reviews whose author does not '
                          'recommend the title.'),
        sa.Column('share', sa.Numeric(), nullable=True,
                  comment='mentions / sample_size (0-1) - a '
                          'composition of the classified sample, not '
                          'of all reviews.'),
        sa.Column('sample_size', sa.Numeric(), nullable=True,
                  comment='Classified reviews behind every row for '
                          'this title.'),
        sa.Column('corpus_size', sa.Numeric(), nullable=True,
                  comment='All stored review_text rows for the title '
                          'at derive time.'),
        sa.Column('classified_share', sa.Numeric(), nullable=True,
                  comment='sample_size / corpus_size (0-1).'),
        sa.Column('window_start', sa.Date(), nullable=True,
                  comment='Earliest review creation date in the '
                          'classified sample.'),
        sa.Column('window_end', sa.Date(), nullable=True,
                  comment='Latest review creation date in the '
                          'classified sample.'),
        sa.Column('example_ids', sa.Text(), nullable=True,
                  comment='Comma-joined recommendationids of the '
                          'top-voted examples; best-effort audit - '
                          'ids may age past the stored cap.'),
        sa.Column('taxonomy_version', sa.Integer(), nullable=False,
                  comment='Taxonomy the aggregates were derived '
                          'under.'),
        sa.Column('model', sa.Text(), nullable=True,
                  comment='Classifier model id, for provenance.'),
        sa.Column('computed_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; when derive wrote this row - '
                          'the recency column.'),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('reviewthemeid'),
        sa.UniqueConstraint('gameid', 'theme',
                            name='uq_review_theme_cell'),
        schema='games',
        comment='Per-title review-theme aggregates over the '
                'classified sample. Speaks only to theme composition; '
                'positive share comes from the pull totals on '
                'community_snapshot (steam_total_reviews / '
                'steam_positive_pct), never from this sample.',
    )
    op.create_index('ix_review_theme_gameid', 'review_theme',
                    ['gameid'], schema='games')


def downgrade():
    op.drop_index('ix_review_theme_gameid', table_name='review_theme',
                  schema='games')
    op.drop_table('review_theme', schema='games')
    op.drop_index('ix_review_text_game_created',
                  table_name='review_text', schema='games')
    op.drop_table('review_text', schema='games')
