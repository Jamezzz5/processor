"""create games schema and initial tables

Revision ID: a1c3e5b7d9f1
Revises:
Create Date: 2026-07-07

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1c3e5b7d9f1'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute('CREATE SCHEMA IF NOT EXISTS games')
    op.create_table(
        'game',
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('canonical_name', sa.Text(), nullable=False),
        sa.Column('franchise', sa.Text(), nullable=True),
        sa.Column('publisher', sa.Text(), nullable=True),
        sa.Column('developer', sa.Text(), nullable=True),
        sa.Column('primary_genre', sa.Text(), nullable=True),
        sa.Column('release_date', sa.Text(), nullable=True,
                  comment='Raw source display string ("Q4 2026", '
                          '"Coming soon"), not a parseable date.'),
        sa.Column('steam_appid', sa.BigInteger(), nullable=True),
        sa.Column('registry_slug', sa.Text(), nullable=True),
        sa.Column('opencritic_id', sa.BigInteger(), nullable=True),
        sa.Column('newzoo_title', sa.Text(), nullable=True,
                  comment='Raw Newzoo Top-500 title this game matched; '
                          'a match hint, not an identity.'),
        sa.Column('first_seen_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; when the row was created.'),
        sa.Column('updated_at', sa.DateTime(), nullable=True,
                  comment='Naive UTC; last touch by any source.'),
        sa.PrimaryKeyConstraint('gameid'),
        sa.UniqueConstraint('registry_slug', name='uq_game_registry_slug'),
        sa.UniqueConstraint('steam_appid', name='uq_game_steam_appid'),
        sa.UniqueConstraint('opencritic_id', name='uq_game_opencritic_id'),
        schema='games',
        comment='Canonical game dimension; one identity column per '
                'source, filled in as sources are matched.',
    )
    op.create_table(
        'game_event',
        sa.Column('gameeventid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('eventdate', sa.DateTime(), nullable=False,
                  comment='Naive UTC run timestamp.'),
        sa.Column('player_count', sa.Numeric(), nullable=True),
        sa.Column('owners_in_sample', sa.Numeric(), nullable=True),
        sa.Column('wishlists_in_sample', sa.Numeric(), nullable=True),
        sa.Column('avg_achievement_pct', sa.Numeric(), nullable=True),
        sa.Column('review_score', sa.Numeric(), nullable=True),
        sa.Column('review_score_desc', sa.Text(), nullable=True),
        sa.Column('total_positive', sa.Numeric(), nullable=True),
        sa.Column('total_negative', sa.Numeric(), nullable=True),
        sa.Column('total_reviews', sa.Numeric(), nullable=True),
        sa.Column('price', sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('gameeventid'),
        sa.UniqueConstraint('gameid', 'eventdate',
                            name='uq_game_event_obs'),
        schema='games',
        comment='Steam time-series fact; one observation per game per '
                'steapi run. Sample measures come from a random '
                'ownership sample, not full population.',
    )
    op.create_table(
        'newzoo_engagement',
        sa.Column('newzooengagementid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=True,
                  comment='NULL = title not yet matched to the game dim; '
                          'the raw title columns are retained.'),
        sa.Column('title', sa.Text(), nullable=False),
        sa.Column('publisher', sa.Text(), nullable=True),
        sa.Column('franchise', sa.Text(), nullable=True),
        sa.Column('genre', sa.Text(), nullable=True),
        sa.Column('subgenre', sa.Text(), nullable=True),
        sa.Column('release_date', sa.Text(), nullable=True),
        sa.Column('family', sa.Text(), nullable=False),
        sa.Column('market', sa.Text(), nullable=False),
        sa.Column('period', sa.Text(), nullable=False,
                  comment='Month of the drop, YYYY-MM.'),
        sa.Column('rank', sa.Numeric(), nullable=True),
        sa.Column('player_share', sa.Numeric(), nullable=True),
        sa.Column('mau', sa.Numeric(), nullable=True),
        sa.Column('mau_growth', sa.Numeric(), nullable=True),
        sa.Column('stickiness', sa.Numeric(), nullable=True),
        sa.Column('avg_monthly_playtime', sa.Numeric(), nullable=True),
        sa.Column('churn_pct', sa.Numeric(), nullable=True),
        sa.Column('acquisition_pct', sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('newzooengagementid'),
        sa.UniqueConstraint('family', 'market', 'period', 'title',
                            name='uq_newzoo_engagement_drop'),
        schema='games',
        comment='Newzoo Top-500 fact; one row per title per '
                '(family, market, month) drop.',
    )
    # The natural key doesn't lead with gameid, so per-game landscape
    # joins need their own index (Postgres doesn't index FKs).
    op.create_index('ix_newzoo_engagement_gameid', 'newzoo_engagement',
                    ['gameid'], schema='games')
    op.create_table(
        'community_snapshot',
        sa.Column('communitysnapshotid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('snapshot_date', sa.Date(), nullable=False,
                  comment='UTC ingest date.'),
        sa.Column('rank', sa.Numeric(), nullable=True),
        sa.Column('tier', sa.Text(), nullable=True),
        sa.Column('global_mau', sa.Numeric(), nullable=True),
        sa.Column('mau_growth', sa.Numeric(), nullable=True),
        sa.Column('avg_stickiness', sa.Numeric(), nullable=True),
        sa.Column('lifetime_players', sa.Numeric(), nullable=True),
        sa.Column('steam_reviews', sa.Numeric(), nullable=True),
        sa.Column('steam_positive_pct', sa.Numeric(), nullable=True),
        sa.Column('steam_followers', sa.Numeric(), nullable=True),
        sa.Column('gamalytic_wishlists', sa.Numeric(), nullable=True),
        sa.Column('gamalytic_players', sa.Numeric(), nullable=True),
        sa.Column('gamalytic_revenue', sa.Numeric(), nullable=True),
        sa.Column('gamalytic_playtime', sa.Numeric(), nullable=True),
        sa.Column('discord_members', sa.Numeric(), nullable=True),
        sa.Column('reddit_members', sa.Numeric(), nullable=True),
        sa.Column('youtube_subscribers', sa.Numeric(), nullable=True),
        sa.Column('open_critic_score', sa.Numeric(), nullable=True),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('communitysnapshotid'),
        sa.UniqueConstraint('gameid', 'snapshot_date',
                            name='uq_community_snapshot_day'),
        schema='games',
    )
    op.create_table(
        'critic_score',
        sa.Column('criticscoreid', sa.BigInteger(), nullable=False),
        sa.Column('gameid', sa.BigInteger(), nullable=False),
        sa.Column('opencritic_id', sa.BigInteger(), nullable=False),
        sa.Column('checked_at', sa.DateTime(), nullable=False,
                  comment='Naive UTC API-check timestamp.'),
        sa.Column('top_critic_score', sa.Numeric(), nullable=True),
        sa.Column('percent_recommended', sa.Numeric(), nullable=True),
        sa.Column('num_reviews', sa.Numeric(), nullable=True),
        sa.Column('num_top_critic_reviews', sa.Numeric(), nullable=True),
        sa.Column('median_score', sa.Numeric(), nullable=True),
        sa.Column('percentile', sa.Numeric(), nullable=True),
        sa.Column('tier', sa.Text(), nullable=True),
        sa.Column('url', sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(['gameid'], ['games.game.gameid']),
        sa.PrimaryKeyConstraint('criticscoreid'),
        sa.UniqueConstraint('gameid', 'checked_at',
                            name='uq_critic_score_check'),
        schema='games',
    )
    op.create_table(
        'gwi_affinity',
        sa.Column('gwiaffinityid', sa.BigInteger(), nullable=False),
        sa.Column('market', sa.Text(), nullable=False),
        sa.Column('gender', sa.Text(), nullable=False),
        sa.Column('base', sa.Text(), nullable=False),
        # '' (not NULL): NULLs are distinct in unique constraints, so a
        # nullable column in the natural key would admit duplicates.
        sa.Column('category', sa.Text(), nullable=False,
                  server_default=''),
        sa.Column('name', sa.Text(), nullable=False),
        sa.Column('peak_index', sa.Numeric(), nullable=True),
        sa.Column('peak_cohort', sa.Text(), nullable=True),
        sa.Column('waves', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('gwiaffinityid'),
        sa.UniqueConstraint('market', 'gender', 'base', 'category', 'name',
                            name='uq_gwi_affinity_item'),
        schema='games',
        comment='GWI over-indexing fact; audience-keyed, not game-keyed '
                '— no gameid by design.',
    )


def downgrade():
    op.drop_table('gwi_affinity', schema='games')
    op.drop_table('critic_score', schema='games')
    op.drop_table('community_snapshot', schema='games')
    op.drop_table('newzoo_engagement', schema='games')
    op.drop_table('game_event', schema='games')
    op.drop_table('game', schema='games')
    # The schema is NOT dropped: games.alembic_version lives in it and
    # still exists at this point (alembic clears the version row only
    # after the script body), so a DROP SCHEMA here can never succeed.
