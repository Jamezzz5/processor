"""Conversation text lanes: Reddit posts and comments, stored Bluesky
posts and GDELT news headlines; trailer comments widen to creator
videos.

Revision ID: f3a5c7e9b1d3
Revises: e1a3c5b7d9f3
"""
import sqlalchemy as sa
from alembic import op


revision = 'f3a5c7e9b1d3'
down_revision = 'e1a3c5b7d9f3'
branch_labels = None
depends_on = None

OFFICIAL_ONLY = ('Relevance-ordered top-level comments on official '
                 'trailers. Purged 30 days after fetch, so never a '
                 'history; no author name or channel is stored.')
WITH_CREATORS = ('Relevance-ordered top-level comments on official '
                 "trailers and on each title's most-viewed creator "
                 'videos (youtube_video.kind says which). Refreshed '
                 'while the lane reads the video and deleted 30 days '
                 'after the last fetch that returned it, so never a '
                 'history; no author name or channel is stored.')


def _game_column():
    return sa.Column('gameid', sa.BigInteger(),
                     sa.ForeignKey('games.game.gameid'), nullable=False)


def _fetched_column(comment):
    return sa.Column('fetched_at', sa.DateTime(), comment=comment)


def upgrade():
    """Three insert-or-refresh text tables and one table comment."""
    op.create_table(
        'reddit_item',
        sa.Column('reddititemid', sa.BigInteger(), primary_key=True),
        _game_column(),
        sa.Column('thing_id', sa.Text(), nullable=False,
                  comment="Reddit's fullname (t3_ a post, t1_ a comment) "
                          '- the idempotency key.'),
        sa.Column('subreddit', sa.Text(),
                  comment='The subreddit read, without r/.'),
        sa.Column('kind', sa.Text(), nullable=False,
                  comment="'post' | 'comment'."),
        sa.Column('title', sa.Text(),
                  comment="The post's title; NULL on a comment."),
        sa.Column('body', sa.Text(),
                  comment='Self-text or comment body, clipped to 4,000 '
                          'characters.'),
        sa.Column('permalink', sa.Text(),
                  comment='Path on reddit.com, from /r/.'),
        sa.Column('score', sa.Numeric(),
                  comment="Reddit's score (upvotes less downvotes) at "
                          'the last fetch.'),
        sa.Column('num_comments', sa.Numeric(),
                  comment="A post's comment count at the last fetch; "
                          'NULL on a comment.'),
        sa.Column('created_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC.'),
        _fetched_column('Naive UTC; last fetch that returned it - the '
                        'purge and recency column.'),
        sa.UniqueConstraint('thing_id', name='uq_reddit_item_thing'),
        schema='games',
        comment="Posts and top-level comments from each title's "
                'subreddit. Purged 30 days after fetch, so never a '
                'history; no author name is stored.',
    )
    op.create_index('ix_reddit_item_game_created', 'reddit_item',
                    ['gameid', 'created_at'], schema='games')
    op.create_index('ix_reddit_item_fetched', 'reddit_item',
                    ['fetched_at'], schema='games')
    op.create_table(
        'bluesky_post',
        sa.Column('blueskypostid', sa.BigInteger(), primary_key=True),
        _game_column(),
        sa.Column('uri', sa.Text(), nullable=False,
                  comment='at:// URI of the post - with gameid, the '
                          'idempotency key.'),
        sa.Column('text', sa.Text(),
                  comment='Post text, clipped to 4,000 characters.'),
        sa.Column('language', sa.Text(),
                  comment="'english', the first other language tag, or "
                          "'unknown'."),
        sa.Column('like_count', sa.Numeric(),
                  comment='Likes at the last fetch.'),
        sa.Column('repost_count', sa.Numeric()),
        sa.Column('reply_count', sa.Numeric()),
        sa.Column('created_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC.'),
        _fetched_column('Naive UTC; last fetch that returned it - the '
                        'purge and recency column.'),
        sa.UniqueConstraint('gameid', 'uri', name='uq_bluesky_post_game_uri'),
        schema='games',
        comment='Public Bluesky posts naming each tracked title, one row '
                'per title named. Purged 30 days after fetch, so never a '
                'history; no profile is stored.',
    )
    op.create_index('ix_bluesky_post_game_created', 'bluesky_post',
                    ['gameid', 'created_at'], schema='games')
    op.create_index('ix_bluesky_post_fetched', 'bluesky_post',
                    ['fetched_at'], schema='games')
    op.create_table(
        'news_article',
        sa.Column('newsarticleid', sa.BigInteger(), primary_key=True),
        _game_column(),
        sa.Column('url', sa.Text(), nullable=False,
                  comment='Article link - with gameid, the idempotency '
                          'key.'),
        sa.Column('title', sa.Text(),
                  comment='The headline as GDELT saw it.'),
        sa.Column('domain', sa.Text(),
                  comment='The outlet host, e.g. ign.com.'),
        sa.Column('language', sa.Text(),
                  comment="'english', another language in lower case, or "
                          "'unknown'."),
        sa.Column('source_country', sa.Text(),
                  comment="The outlet's country as GDELT names it."),
        sa.Column('seen_at', sa.DateTime(), nullable=False,
                  comment="Naive UTC; when GDELT first saw the article - "
                          "the article's date."),
        _fetched_column('Naive UTC; when the lane first stored it - the '
                        'recency column.'),
        sa.UniqueConstraint('gameid', 'url', name='uq_news_article_game_url'),
        schema='games',
        comment='Online news whose text names each tracked title, from '
                'the GDELT DOC API article list: headline, link, outlet, '
                'language and country, one row per title named. The body '
                'is never stored.',
    )
    op.create_index('ix_news_article_game_seen', 'news_article',
                    ['gameid', 'seen_at'], schema='games')
    op.create_table_comment('youtube_comment', WITH_CREATORS,
                            existing_comment=OFFICIAL_ONLY, schema='games')


def downgrade():
    """Drop only what this revision added."""
    op.create_table_comment('youtube_comment', OFFICIAL_ONLY,
                            existing_comment=WITH_CREATORS, schema='games')
    op.drop_index('ix_news_article_game_seen', table_name='news_article',
                  schema='games')
    op.drop_table('news_article', schema='games')
    for table in ('bluesky_post', 'reddit_item'):
        for index in ('fetched', 'game_created'):
            op.drop_index(f'ix_{table}_{index}', table_name=table,
                          schema='games')
        op.drop_table(table, schema='games')
