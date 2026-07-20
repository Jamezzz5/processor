# coding: utf-8
"""Games-database models — the ``games`` schema on the steam/games DB.

A canonical ``game`` dimension carrying one identity column per source
(Steam appid, community-registry slug, OpenCritic id, Newzoo title)
plus per-source fact tables, so the entire audience/gaming landscape
joins relationally. Facts keep raw source titles alongside the nullable
``gameid`` FK — unmatched rows are stored, never dropped, and matching
improves over time. All ``DateTime``/``Date`` stamps are naive UTC —
writers strip tzinfo from aware UTC datetimes. Schema changes go through
the dedicated alembic environment at the repo root (``alembic.ini`` /
``alembic/``), not ``create_all``. Column ``comment``s land as Postgres
``COMMENT ON`` — they are what an LLM/text-to-SQL reader inspecting the
information schema actually sees, so they carry the non-obvious
semantics.
"""
from sqlalchemy import BigInteger, Column, Date, DateTime, ForeignKey,\
    Index, Integer, Numeric, Text, UniqueConstraint
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()
metadata = Base.metadata
# BIGSERIAL on Postgres; plain INTEGER on sqlite (the only PK type it
# autoincrements), so the same models back unit tests.
BigIntPk = BigInteger().with_variant(Integer, 'sqlite')


class Game(Base):
    __tablename__ = 'game'
    __table_args__ = (
        UniqueConstraint('registry_slug', name='uq_game_registry_slug'),
        UniqueConstraint('steam_appid', name='uq_game_steam_appid'),
        UniqueConstraint('opencritic_id', name='uq_game_opencritic_id'),
        {'schema': 'games',
         'comment': 'Canonical game dimension; one identity column per '
                    'source, filled in as sources are matched.'},
    )

    gameid = Column(BigIntPk, primary_key=True)
    canonical_name = Column(Text, nullable=False)
    franchise = Column(Text)
    publisher = Column(Text)
    developer = Column(Text)
    primary_genre = Column(Text)
    release_date = Column(
        Text, comment='Raw source display string ("Q4 2026", "Coming '
                      'soon"), not a parseable date.')
    steam_appid = Column(BigInteger)
    registry_slug = Column(Text)
    opencritic_id = Column(BigInteger)
    newzoo_title = Column(
        Text, comment='Raw Newzoo Top-500 title this game matched; a '
                      'match hint, not an identity.')
    first_seen_at = Column(DateTime,
                           comment='Naive UTC; when the row was created.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last touch by any source.')
    gameevents = relationship('GameEvent', backref='game', lazy='dynamic')


class GameEvent(Base):
    """Steam time-series fact — one observation per appid per steapi
    run (successor of the old steam project's ``gameevents``)."""
    __tablename__ = 'game_event'
    __table_args__ = (
        UniqueConstraint('gameid', 'eventdate', name='uq_game_event_obs'),
        {'schema': 'games',
         'comment': 'Steam time-series fact; one observation per game '
                    'per steapi run. Sample measures come from a random '
                    'ownership sample, not full population.'},
    )

    gameeventid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    eventdate = Column(DateTime, nullable=False,
                       comment='Naive UTC run timestamp.')
    player_count = Column(Numeric)
    owners_in_sample = Column(Numeric)
    wishlists_in_sample = Column(Numeric)
    avg_achievement_pct = Column(Numeric)
    review_score = Column(Numeric)
    review_score_desc = Column(Text)
    total_positive = Column(Numeric)
    total_negative = Column(Numeric)
    total_reviews = Column(Numeric)
    price = Column(Numeric)


class NewzooEngagement(Base):
    """Newzoo Top-500 fact — one row per title per (family, market,
    month) drop; raw title columns survive unmatched joins."""
    __tablename__ = 'newzoo_engagement'
    __table_args__ = (
        UniqueConstraint('family', 'market', 'period', 'title',
                         name='uq_newzoo_engagement_drop'),
        Index('ix_newzoo_engagement_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Newzoo Top-500 fact; one row per title per '
                    '(family, market, month) drop.'},
    )

    newzooengagementid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title columns are retained.')
    title = Column(Text, nullable=False)
    publisher = Column(Text)
    franchise = Column(Text)
    genre = Column(Text)
    subgenre = Column(Text)
    release_date = Column(Text)
    family = Column(Text, nullable=False)
    market = Column(Text, nullable=False)
    period = Column(Text, nullable=False,
                    comment='Month of the drop, YYYY-MM.')
    rank = Column(Numeric)
    player_share = Column(Numeric)
    mau = Column(Numeric)
    mau_growth = Column(Numeric)
    stickiness = Column(Numeric)
    avg_monthly_playtime = Column(Numeric)
    churn_pct = Column(Numeric)
    acquisition_pct = Column(Numeric)


class CommunitySnapshot(Base):
    """Community-registry fact — one row per game per ingest day."""
    __tablename__ = 'community_snapshot'
    __table_args__ = (
        UniqueConstraint('gameid', 'snapshot_date',
                         name='uq_community_snapshot_day'),
        {'schema': 'games'},
    )

    communitysnapshotid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    snapshot_date = Column(Date, nullable=False,
                           comment='UTC ingest date.')
    rank = Column(Numeric)
    tier = Column(Text)
    global_mau = Column(Numeric)
    mau_growth = Column(Numeric)
    avg_stickiness = Column(Numeric)
    lifetime_players = Column(Numeric)
    steam_reviews = Column(Numeric)
    steam_positive_pct = Column(Numeric)
    steam_followers = Column(Numeric)
    gamalytic_wishlists = Column(Numeric)
    gamalytic_players = Column(Numeric)
    gamalytic_revenue = Column(Numeric)
    gamalytic_playtime = Column(Numeric)
    discord_members = Column(Numeric)
    reddit_members = Column(Numeric)
    youtube_subscribers = Column(Numeric)
    open_critic_score = Column(Numeric)


class CriticScore(Base):
    """OpenCritic fact — one row per matched game per API check."""
    __tablename__ = 'critic_score'
    __table_args__ = (
        UniqueConstraint('gameid', 'checked_at',
                         name='uq_critic_score_check'),
        {'schema': 'games'},
    )

    criticscoreid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    opencritic_id = Column(BigInteger, nullable=False)
    checked_at = Column(DateTime, nullable=False,
                        comment='Naive UTC API-check timestamp.')
    top_critic_score = Column(Numeric)
    percent_recommended = Column(Numeric)
    num_reviews = Column(Numeric)
    num_top_critic_reviews = Column(Numeric)
    median_score = Column(Numeric)
    percentile = Column(Numeric)
    tier = Column(Text)
    url = Column(Text)


class GwiAffinity(Base):
    """GWI over-indexing fact — audience-keyed, not game-keyed; mirrors
    ``extract_affinities`` output per (market, gender, base) crosstab."""
    __tablename__ = 'gwi_affinity'
    __table_args__ = (
        UniqueConstraint('market', 'gender', 'base', 'category', 'name',
                         name='uq_gwi_affinity_item'),
        {'schema': 'games',
         'comment': 'GWI over-indexing fact; audience-keyed, not '
                    'game-keyed — no gameid by design.'},
    )

    gwiaffinityid = Column(BigIntPk, primary_key=True)
    market = Column(Text, nullable=False)
    # '' (not NULL) for the all-genders/uncategorized cases: NULLs are
    # distinct in Postgres unique constraints, so a NULL here would let
    # the natural key admit duplicates.
    gender = Column(Text, nullable=False)
    base = Column(Text, nullable=False)
    category = Column(Text, nullable=False, server_default='')
    name = Column(Text, nullable=False)
    peak_index = Column(Numeric)
    peak_cohort = Column(Text)
    waves = Column(Text)
