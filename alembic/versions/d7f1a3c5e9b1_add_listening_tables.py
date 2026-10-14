"""Listening lanes' storage: Steam news, YouTube trailer comments and
reactions, and the off-topic review count.

Revision ID: d7f1a3c5e9b1
Revises: 9a6e4207c8d1
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql


revision = 'd7f1a3c5e9b1'
down_revision = '9a6e4207c8d1'
branch_labels = None
depends_on = None


def upgrade():
    """Three insert-only text tables and one snapshot column."""
    op.create_table(
        'steam_news',
        sa.Column('steamnewsid', sa.BigInteger(), primary_key=True),
        sa.Column('gameid', sa.BigInteger(),
                  sa.ForeignKey('games.game.gameid'), nullable=False),
        sa.Column('gid', sa.BigInteger(), nullable=False,
                  comment="Steam's own post id - the idempotency key."),
        sa.Column('title', sa.Text()),
        sa.Column('url', sa.Text()),
        sa.Column('author', sa.Text(),
                  comment='The posting account as Steam shows it.'),
        sa.Column('feedname', sa.Text()),
        sa.Column('feedlabel', sa.Text()),
        sa.Column('tags', sa.Text(),
                  comment='Comma-joined Steam tags; patchnotes marks '
                          'patch notes.'),
        sa.Column('body', sa.Text(),
                  comment='Post body, clipped to 4,000 characters.'),
        sa.Column('posted_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC.'),
        sa.Column('first_seen', sa.DateTime(),
                  comment='Naive UTC; when the lane first stored it - '
                          'the recency column.'),
        sa.UniqueConstraint('gid', name='uq_steam_news_gid'),
        schema='games',
        comment='Steam community announcements per title (ISteamNews '
                'GetNewsForApp, feed steam_community_announcements '
                'only). A patchnotes tag marks patch notes. Context for '
                'other readings; never scored.',
    )
    op.create_index('ix_steam_news_game_posted', 'steam_news',
                    ['gameid', 'posted_at'], schema='games')
    op.create_table(
        'youtube_comment',
        sa.Column('youtubecommentid', sa.BigInteger(), primary_key=True),
        sa.Column('youtubevideoid', sa.BigInteger(),
                  sa.ForeignKey('games.youtube_video.youtubevideoid'),
                  nullable=False),
        sa.Column('comment_id', sa.Text(), nullable=False,
                  comment="YouTube's comment id - the idempotency key."),
        sa.Column('text', sa.Text(),
                  comment='Comment text, clipped to 1,000 characters.'),
        sa.Column('like_count', sa.Numeric()),
        sa.Column('published_at', sa.DateTime(), comment='Naive UTC.'),
        sa.Column('fetched_at', sa.DateTime(),
                  comment='Naive UTC; last fetch that returned it - the '
                          'purge and recency column.'),
        sa.Column('sentiment', sa.Text(),
                  comment="'positive' | 'negative' | 'neutral' as the "
                          'classifier read it; NULL until classified.'),
        sa.Column('themes', sa.Text(),
                  comment='JSON array of taxonomy keys; [] means none '
                          'applied, NULL not yet classified.'),
        sa.Column('classified_at', sa.DateTime(), comment='Naive UTC.'),
        sa.Column('taxonomy_version', sa.Integer()),
        sa.UniqueConstraint('comment_id', name='uq_youtube_comment_id'),
        schema='games',
        comment='Relevance-ordered top-level comments on official '
                'trailers. Purged 30 days after fetch, so never a '
                'history; no author name or channel is stored.',
    )
    op.create_index('ix_youtube_comment_video', 'youtube_comment',
                    ['youtubevideoid'], schema='games')
    op.create_index('ix_youtube_comment_fetched', 'youtube_comment',
                    ['fetched_at'], schema='games')
    op.create_table(
        'youtube_reaction',
        sa.Column('youtubereactionid', sa.BigInteger(),
                  primary_key=True),
        sa.Column('youtubevideoid', sa.BigInteger(),
                  sa.ForeignKey('games.youtube_video.youtubevideoid'),
                  nullable=False),
        sa.Column('sample_size', sa.Numeric(),
                  comment='Classified comments.'),
        sa.Column('positive', sa.Numeric()),
        sa.Column('negative', sa.Numeric()),
        sa.Column('neutral', sa.Numeric()),
        sa.Column('theme_counts', sa.JSON().with_variant(
            postgresql.JSONB(), 'postgresql'),
                  comment='{theme key: comments carrying it}.'),
        sa.Column('example_ids', sa.Text(),
                  comment='Comma-joined comment ids of the most-liked '
                          'examples; they age out with the purge.'),
        sa.Column('taxonomy_version', sa.Integer(), nullable=False),
        sa.Column('model', sa.Text(), comment='Classifier model id.'),
        sa.Column('computed_at', sa.DateTime(),
                  comment='Naive UTC; the recency column.'),
        sa.UniqueConstraint('youtubevideoid',
                            name='uq_youtube_reaction_video'),
        schema='games',
        comment='Sentiment and theme counts over a classified sample '
                'of comments per official trailer. Our read of a '
                'sample, not a YouTube metric.',
    )
    op.add_column(
        'community_snapshot',
        sa.Column('steam_offtopic_reviews', sa.Numeric(),
                  comment='Reviews Steam hides as off-topic activity: '
                          'the filter_offtopic_activity=0 total minus '
                          'the default total. A rising count is a '
                          'review-bomb signal, never a sentiment '
                          'input.'),
        schema='games')


def downgrade():
    """Drop only what this revision added."""
    op.drop_column('community_snapshot', 'steam_offtopic_reviews',
                   schema='games')
    op.drop_table('youtube_reaction', schema='games')
    op.drop_index('ix_youtube_comment_fetched', table_name='youtube_comment',
                  schema='games')
    op.drop_index('ix_youtube_comment_video', table_name='youtube_comment',
                  schema='games')
    op.drop_table('youtube_comment', schema='games')
    op.drop_index('ix_steam_news_game_posted', table_name='steam_news',
                  schema='games')
    op.drop_table('steam_news', schema='games')
