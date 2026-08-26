# coding: utf-8
"""Games-database models — the ``games`` schema on the steam/games DB.

A canonical ``game`` dimension carrying one identity column per source
(Steam appid, community-registry slug, OpenCritic id, IGDB id, Newzoo
title) plus per-source fact tables, so the entire audience/gaming
landscape joins relationally. Facts keep raw source titles alongside the nullable
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
        UniqueConstraint('igdb_id', name='uq_game_igdb_id'),
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
    igdb_id = Column(BigInteger)
    cover_image_id = Column(
        Text, comment='IGDB cover image id; render via images.igdb.com'
                      '/igdb/image/upload/t_cover_big/{id}.jpg.')
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
        Index('ix_game_event_eventdate', 'eventdate'),
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


class ReviewRollup(Base):
    """Steam monthly positive/negative review volumes."""
    __tablename__ = 'review_rollup'
    __table_args__ = (
        UniqueConstraint('gameid', 'month_start',
                         name='uq_review_rollup_month'),
        Index('ix_review_rollup_month', 'month_start'),
        {'schema': 'games',
         'comment': 'Steam monthly review rollup; positive/negative '
                    'review counts per game per month from the store '
                    'appreviewhistogram endpoint.'},
    )

    reviewrollupid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    month_start = Column(Date, nullable=False,
                         comment='UTC month start of the rollup.')
    positive = Column(Numeric,
                      comment='Reviews recommending the title that '
                              'month (recommendations_up).')
    negative = Column(Numeric,
                      comment='Reviews not recommending the title that '
                              'month (recommendations_down).')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last sweep that refreshed '
                                'this row - the lane recency column, '
                                'since month_start ages by '
                                'construction.')


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
    steam_wishlists = Column(
        Numeric,
        comment='Steam wishlist count carried through the Newzoo MAU '
                'export - the only automated wishlist source.')
    steam_followers = Column(
        Numeric,
        comment='Steam follower count carried through the Newzoo MAU '
                'export; the daily sheet-fed count lives on '
                'community_snapshot.')


class CommunitySnapshot(Base):
    """Community-registry fact — one row per game per ingest day."""
    __tablename__ = 'community_snapshot'
    __table_args__ = (
        UniqueConstraint('gameid', 'snapshot_date',
                         name='uq_community_snapshot_day'),
        Index('ix_community_snapshot_date', 'snapshot_date'),
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
    twitch_viewers = Column(
        Numeric, comment='Concurrent viewers on the title\'s Twitch '
                         'category (Helix Get Streams, top pages '
                         'summed) — a point-in-time sample at the '
                         'nightly run hour, not a peak.')
    reddit_active_users = Column(
        Numeric, comment='Subreddit active_user_count at snapshot time '
                         '(about.json) — concurrent-scale, unlike the '
                         'cumulative reddit_members.')
    steam_total_reviews = Column(
        Numeric, comment='Lifetime Steam review count (appreviews '
                         'summary) - the review-stats lane\'s own '
                         'liveness column; steam_positive_pct is '
                         'shared with the curated registry sheet.')
    youtube_recent_views = Column(
        Numeric, comment='Total views on the linked official '
                         'channel\'s uploads published in the '
                         'trailing 90 days - a level, not a rate. '
                         'Week-over-week deltas are the view '
                         'velocity; videos aging out of the window '
                         'can dip the level, so deltas clip at zero.')
    youtube_video_count = Column(
        Numeric, comment='Uploads on the linked official channel in '
                         'the trailing 90 days.')


class CommunityLink(Base):
    """Per-platform community handle for a game — where the live
    lanes look. ``source`` is the provenance rule: ``curated`` rows
    (registry sheet) always win; ``derived`` rows (auto-search) may
    never overwrite them."""
    __tablename__ = 'community_link'
    __table_args__ = (
        UniqueConstraint('gameid', 'platform',
                         name='uq_community_link_game_platform'),
        {'schema': 'games',
         'comment': 'Per-platform community handle per game; curated '
                    'rows always beat derived ones.'},
    )

    communitylinkid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    platform = Column(Text, nullable=False,
                      comment="'twitch' | 'reddit' | 'youtube'.")
    handle = Column(
        Text, comment='Twitch category name / subreddit without the '
                      'r/ prefix / YouTube channel title.')
    external_id = Column(
        Text, comment='Twitch category id / YouTube channel id; NULL '
                      'for Reddit (the handle is the id).')
    source = Column(
        Text, nullable=False,
        comment="'curated' (registry sheet) or 'derived' "
                '(auto-search); derived never overwrites curated.')
    derived_at = Column(DateTime,
                        comment='Naive UTC; when auto-derivation last '
                                'confirmed this handle.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last touch by any writer.')


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


class GameRelease(Base):
    """IGDB release-calendar row — one per title from the nightly
    windowed sweep of upcoming/recent releases; the natural key is the
    IGDB id, so a rerun updates the expected date in place (slips
    self-correct; no date history is kept)."""
    __tablename__ = 'game_release'
    __table_args__ = (
        UniqueConstraint('igdb_id', name='uq_game_release_igdb'),
        Index('ix_game_release_gameid', 'gameid'),
        Index('ix_game_release_date', 'release_date'),
        {'schema': 'games',
         'comment': 'IGDB release calendar; one row per title from the '
                    'nightly windowed sweep, release_date is the '
                    'current expectation and self-corrects on rerun.'},
    )

    gamereleaseid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title is retained.')
    igdb_id = Column(BigInteger, nullable=False)
    title = Column(Text, nullable=False)
    slug = Column(Text)
    release_date = Column(Date,
                          comment='Current expected date parsed from '
                                  'IGDB first_release_date.')
    hypes = Column(Numeric,
                   comment='IGDB hype count (pre-release follows).')
    genres = Column(Text, comment='Comma-joined IGDB genre names.')
    platforms = Column(Text,
                       comment='Comma-joined IGDB platform names.')
    url = Column(Text)
    cover_image_id = Column(
        Text, comment='IGDB cover image id; render via images.igdb.com'
                      '/igdb/image/upload/t_cover_big/{id}.jpg.')
    first_seen_at = Column(DateTime,
                           comment='Naive UTC; when the row was created.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last sweep that saw it.')


class TitleRank(Base):
    """One source's global ranking of a title on one day — the fact the
    automated title universe is built from.

    Every ranking source lands here in its own units (``value`` /
    ``value_label``) beside the ordinal ``rank`` that makes sources
    comparable. Rows keep the raw source title next to a nullable
    ``gameid`` so a title can be ranked *before* it exists in the dim —
    which is the point: promotion reads this table to decide what to
    create.
    """
    __tablename__ = 'title_rank'
    __table_args__ = (
        UniqueConstraint('source', 'rank_date', 'title',
                         name='uq_title_rank_obs'),
        Index('ix_title_rank_date', 'rank_date'),
        Index('ix_title_rank_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Per-source daily global title ranking — the input '
                    'the automated title universe is promoted from. '
                    'rank is ordinal within (source, rank_date); value '
                    'is that source\'s own magnitude and value_label '
                    'names its unit.'},
    )

    titlerankid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet promoted into the game '
                            'dim; the raw title is retained.')
    source = Column(
        Text, nullable=False,
        comment="Ranking source. The IGDB popularity types "
                "('igdb:visits', 'igdb:want-to-play', 'igdb:playing', "
                "'igdb:played') are derived from /v4/popularity_types at "
                "runtime, so a type IGDB adds later arrives without a "
                "code change. Also 'league' (our own derived title "
                "league), 'igdb-release', 'brandtracker', 'registry' and "
                "'newzoo'.")
    rank_date = Column(Date, nullable=False,
                       comment='UTC date the reading was taken.')
    title = Column(Text, nullable=False,
                   comment='Raw title as this source spells it.')
    slug = Column(Text)
    rank = Column(Numeric, nullable=False,
                  comment='1-based ordinal within (source, rank_date).')
    value = Column(Numeric,
                   comment="The source's own magnitude behind the rank "
                           '(IGDB popularity value, league score, MAU). '
                           'NULL when the source publishes an order '
                           'only.')
    value_label = Column(Text,
                         comment="Unit of value — 'igdb_popularity', "
                                 "'league_score', 'mau'.")
    igdb_id = Column(BigInteger,
                     comment='Identity the promoter prefers over a name '
                             'match, when the source carries one.')
    steam_appid = Column(BigInteger)
    updated_at = Column(DateTime,
                        comment='Naive UTC; last run that saw this row.')


class TitleScore(Base):
    """Daily competitive-score snapshot — one row per tracked title per
    day, persisting the brandtracker weighted z-scores and the
    competitive league so trend history exists without a human opening
    the tab."""
    __tablename__ = 'title_score'
    __table_args__ = (
        UniqueConstraint('score_date', 'title',
                         name='uq_title_score_day'),
        Index('ix_title_score_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Daily competitive snapshot; brandtracker weighted '
                    'z-scores + share-of-voice league per tracked '
                    'title. Z-scores are relative to the tracked set '
                    'on that day, not the whole market.'},
    )

    titlescoreid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title is retained.')
    score_date = Column(Date, nullable=False, comment='UTC snapshot date.')
    title = Column(Text, nullable=False,
                   comment='Raw tracked title (Newzoo productname).')
    primary_period = Column(Text, comment='Scored month, YYYY-MM.')
    comparison_period = Column(Text, comment='Comparison month, YYYY-MM.')
    genre = Column(Text, comment='Genre of the tracked title, '
                                 'denormalised at snapshot time: '
                                 'latest Newzoo drop (best rank wins) '
                                 'else the game dim. NULL = no genre '
                                 'known. Pre-column history is '
                                 'backfilled fill-only from the '
                                 'then-current taxonomy.')
    influence = Column(Numeric)
    engagement = Column(Numeric)
    momentum = Column(Numeric)
    composite = Column(Numeric, comment='Sum of the dimension z-scores.')
    headline_metric = Column(
        Text, comment='Metric the current/prior/share columns read on.')
    current = Column(Numeric)
    prior = Column(Numeric)
    share = Column(Numeric, comment='Share of voice across tracked set.')
    share_delta = Column(Numeric)
    movement = Column(Text, comment='Label Surging..Falling.')
    set_size = Column(
        Integer, comment='How many titles that day\'s z-scores were '
                         'taken against. The composite is a position '
                         'within that field, not an absolute rating, so '
                         'rows carrying different set_size values are '
                         'not directly comparable — the field widened '
                         'when the automated title universe landed.')


class GwiAffinity(Base):
    """One GWI crosstab cell — an item read for one audience cohort.
    Audience-keyed, not game-keyed; mirrors ``extract_facts`` output.

    One row per (market, gender, base, cohort, category, name): the
    whole crosstab, not just the over-indexers. Storing only the top
    affinities answers "what is unusual about them?" and nothing else
    — penetration questions and the categories that index ~100 by
    construction (Game Playing Frequency, Session Length) need every
    cell. ``base`` is the crosstab's audience definition, so an 'All
    Internet Users' base is the sizing reference, NOT an audience:
    its denominator is far broader and it outranks every real base if
    pooled with them.
    """
    __tablename__ = 'gwi_affinity'
    __table_args__ = (
        UniqueConstraint('market', 'gender', 'base', 'cohort', 'category',
                         'name', name='uq_gwi_affinity_item'),
        Index('ix_gwi_affinity_scope', 'market', 'cohort', 'category'),
        Index('ix_gwi_affinity_name', 'name'),
        {'schema': 'games',
         'comment': 'GWI crosstab cell per audience cohort; '
                    'audience-keyed, not game-keyed — no gameid by '
                    'design. One row per item per cohort.'},
    )

    gwiaffinityid = Column(BigIntPk, primary_key=True)
    market = Column(Text, nullable=False)
    gender = Column(Text, nullable=False)
    base = Column(
        Text, nullable=False,
        comment="Audience definition, e.g. 'PC Gamer'. An 'All Internet "
                "Users' base is the sizing reference, not an audience.")
    cohort = Column(
        Text, nullable=False, server_default='',
        comment="Age/gender band, e.g. 'Male 16-34'. Bands overlap "
                "(16-24, 25-34 and 16-34 all exist) — never rank across "
                "cohorts, and never sum them.")
    category = Column(Text, nullable=False, server_default='')
    name = Column(Text, nullable=False)
    index_value = Column(
        Numeric, comment='GWI Index vs the base average; 100 = average.')
    pct = Column(
        Numeric,
        comment='Column % — penetration within the cohort (0-1).')
    responses = Column(
        Numeric, comment='Unweighted sample behind the cell.')
    universe = Column(
        Numeric, comment='People in the cohort who answered this item.')
    base_universe = Column(
        Numeric, comment="Cohort population from the crosstab's Totals "
                         'row — the audience-size denominator.')
    waves = Column(Text)


class AdSpend(Base):
    """Pathmatics daily brand spend by export cut."""
    __tablename__ = 'ad_spend'
    __table_args__ = (
        UniqueConstraint('spend_date', 'brand', 'channel', 'country',
                         'buy_type', name='uq_ad_spend_cell'),
        Index('ix_ad_spend_date', 'spend_date'),
        Index('ix_ad_spend_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Pathmatics daily ad spend; one row per brand per '
                    'day per export cut (channel/country/buy type). '
                    'ALL marks a dimension the export was not cut by '
                    '- never sum ALL rows with cut rows.'},
    )

    adspendid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = brand not yet matched to the game '
                            'dim; the raw brand columns are retained.')
    spend_date = Column(Date, nullable=False,
                        comment='Spend day as exported.')
    brand = Column(Text, nullable=False,
                   comment='Pathmatics Brand (Leaf) - the title-level '
                           'brand.')
    advertiser = Column(Text)
    brand_root = Column(Text,
                        comment='Pathmatics Brand Root (publisher '
                                'label / franchise).')
    channel = Column(Text, nullable=False, server_default='ALL',
                     comment='Export channel cut (Facebook, YouTube, '
                             '...); ALL = not cut by channel.')
    country = Column(Text, nullable=False, server_default='ALL',
                     comment='Export country cut (US, ...); ALL = not '
                             'cut by country.')
    buy_type = Column(Text, nullable=False, server_default='ALL',
                      comment='Direct | Indirect | House Ads; ALL when '
                              'the export lacked the column.')
    spend = Column(Numeric, comment='USD.')
    impressions = Column(Numeric)
    updated_at = Column(DateTime,
                        comment='Naive UTC; last export sweep that '
                                'refreshed this row.')


class SearchInterest(Base):
    """Google Trends weekly search-interest fact — one row per title
    per ISO week per geo. Trends readings are indexed 0-100 within one
    request, so each batch shares an anchor term and ``interest`` is
    the anchor-rescaled value; only ``interest`` is comparable across
    rows."""
    __tablename__ = 'search_interest'
    __table_args__ = (
        UniqueConstraint('title', 'week_start', 'geo',
                         name='uq_search_interest_obs'),
        Index('ix_search_interest_week', 'week_start'),
        Index('ix_search_interest_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Google Trends weekly search interest per title. '
                    'Readings are indexed 0-100 within one request, so '
                    'interest is rescaled by the shared anchor term; '
                    'compare interest across rows, never raw_interest.'},
    )

    searchinterestid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title is retained.')
    title = Column(Text, nullable=False,
                   comment='Tracked title as queried.')
    week_start = Column(Date, nullable=False,
                        comment='ISO week Monday (Trends weekly '
                                'grain).')
    geo = Column(Text, nullable=False, server_default='GLOBAL',
                 comment="Trends geo cut; 'GLOBAL' = worldwide.")
    interest = Column(
        Numeric, comment='Anchor-rescaled reading (raw_interest x 100 '
                         '/ anchor_value) - the only column comparable '
                         'across batches. NULL when the anchor read '
                         'too low to scale by. The rolling 12-month '
                         'request window re-normalises history, so '
                         'values shift slightly between runs; the '
                         'upsert overwrite is intended.')
    raw_interest = Column(Numeric,
                          comment="The batch's own 0-100 reading, "
                                  'kept for audit; never compare '
                                  'across batches.')
    anchor = Column(Text,
                    comment='Anchor title the batch was scaled by.')
    anchor_value = Column(Numeric,
                          comment="The anchor's raw reading in this "
                                  'batch.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last sweep that refreshed '
                                'this row - the lane recency column, '
                                'since week_start ages by '
                                'construction.')


class WikipediaPageview(Base):
    """Wikipedia weekly pageview fact — one row per tracked title per
    ISO week, the sum of the linked article's daily user (non-bot)
    pageviews. The Wikimedia API serves history back to July 2015, so
    unlike every other signal lane this one can backfill years."""
    __tablename__ = 'wikipedia_pageview'
    __table_args__ = (
        UniqueConstraint('week_start', 'title',
                         name='uq_wikipedia_pageview_week'),
        Index('ix_wikipedia_pageview_week', 'week_start'),
        Index('ix_wikipedia_pageview_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Weekly Wikipedia article pageviews per tracked '
                    'title, user agent only. The API serves history '
                    'back to July 2015, so this is the one attention '
                    'signal with a deep backfill.'},
    )

    wikipediapageviewid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title is retained.')
    title = Column(Text, nullable=False,
                   comment='Tracked title as queried.')
    week_start = Column(Date, nullable=False,
                        comment='ISO week Monday.')
    views = Column(
        Numeric, comment="Sum of the ISO week's daily user (non-bot) "
                         'pageviews for the linked article.')
    article = Column(Text,
                     comment='Wikipedia article title fetched, kept '
                             'for audit as the community_link mapping '
                             'evolves.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last sweep that refreshed '
                                'this row - the lane recency column, '
                                'since week_start ages by '
                                'construction.')


class AttentionShare(Base):
    """Weekly attention-share snapshot — one row per tracked title per
    ISO week; the volume-share companion to the z-scored
    ``TitleScore``. Each signal's share of the tracked set's weekly
    volume is combined on fixed weights that renormalise over the
    signals live that week."""
    __tablename__ = 'attention_share'
    __table_args__ = (
        UniqueConstraint('week_start', 'title',
                         name='uq_attention_share_week'),
        Index('ix_attention_share_week', 'week_start'),
        Index('ix_attention_share_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Weekly attention share per tracked title: each '
                    "signal's share of the tracked set's weekly "
                    'volume, combined on fixed weights that '
                    'renormalise over the signals live that week. A '
                    "share is a position within that week's field "
                    '(see set_size), not an absolute rating.'},
    )

    attentionshareid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    comment='NULL = title not yet matched to the game '
                            'dim; the raw title is retained.')
    title = Column(Text, nullable=False, comment='Raw tracked title.')
    week_start = Column(Date, nullable=False,
                        comment='ISO week Monday.')
    search_interest = Column(
        Numeric, comment='Anchor-rescaled weekly Trends reading.')
    youtube_views = Column(
        Numeric, comment='Week-over-week delta of trailing-90d '
                         'official-channel video views, clipped at '
                         'zero.')
    twitch_viewers = Column(
        Numeric, comment='Weekly mean of the nightly '
                         'concurrent-viewer samples.')
    reddit_active_users = Column(
        Numeric, comment='Weekly mean of the nightly samples.')
    steam_ccu = Column(
        Numeric, comment='Weekly mean of daily concurrent players.')
    wikipedia_views = Column(
        Numeric, comment='Weekly sum of daily Wikipedia article '
                         'pageviews, user agent only.')
    attention_share = Column(
        Numeric, comment="0-1 fraction of the tracked set's combined "
                         'weekly attention.')
    signals_present = Column(
        Integer, comment='How many of the signals carried data '
                         'for this title this week.')
    set_size = Column(
        Integer, comment='Titles scored that week. Shares are only '
                         'comparable at equal set_size - re-base '
                         'within a chosen cohort to compare across '
                         'weeks.')
    updated_at = Column(DateTime,
                        comment='Naive UTC; last derive that refreshed '
                                'this row - the recency column, since '
                                'week_start ages by construction.')


class ReviewText(Base):
    """Capped per-title corpus of recent Steam review bodies in every
    language — the review-mix sample, and in its English slice the
    reclassifiable input behind ``ReviewTheme``. The writer enforces a
    most-recent-N cap per title (N is lqapp's STEAM_REVIEW_TEXT_CAP),
    so the table is a rolling sample, never a complete history. The
    classification columns start NULL and lapse back into the eligible
    pool whenever the taxonomy version moves; a non-English row never
    enters that pool at all."""
    __tablename__ = 'review_text'
    __table_args__ = (
        UniqueConstraint('recommendationid', name='uq_review_text_rec'),
        Index('ix_review_text_game_created', 'gameid', 'created_at'),
        {'schema': 'games',
         'comment': 'Most recent N Steam review bodies per title, in '
                    'every language (N = STEAM_REVIEW_TEXT_CAP, '
                    'writer-enforced) - the review-mix sample, and in '
                    'its English slice the reclassifiable corpus '
                    'behind review_theme. Not a complete history.'},
    )

    reviewtextid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    recommendationid = Column(
        BigInteger, nullable=False,
        comment="Steam's own review id - the idempotency key.")
    language = Column(Text, nullable=False, server_default='english')
    review_text = Column(Text, comment='The review body as fetched.')
    voted_up = Column(Integer,
                      comment='1 = recommends the title, 0 = does not.')
    playtime_at_review = Column(
        Numeric, comment='Author minutes played when the review was '
                         'written.')
    votes_up = Column(Numeric,
                      comment='Helpful votes from other users.')
    weighted_vote_score = Column(
        Numeric, comment="Steam's 0-1 helpfulness weighting; how "
                         'exemplar reviews are picked.')
    created_at = Column(
        DateTime, nullable=False,
        comment='Naive UTC review creation time - the cap-trim order '
                'column.')
    updated_review_at = Column(
        DateTime, comment='Naive UTC; last edit by the author.')
    fetched_at = Column(
        DateTime, comment='Naive UTC; last sweep that touched this '
                          'row - the lane recency column, since '
                          'created_at ages by construction.')
    themes = Column(
        Text, comment='JSON array of taxonomy keys the classifier '
                      'assigned; [] means classified with none '
                      'applying, NULL means not yet classified.')
    classified_at = Column(
        DateTime, comment='Naive UTC; when the classifier stamped '
                          'themes.')
    taxonomy_version = Column(
        Integer, comment='Taxonomy the themes were assigned under; '
                         'rows re-enter the eligible pool when the '
                         'current version moves past it.')


class ReviewTheme(Base):
    """Per-title theme aggregates over the classified ``ReviewText``
    sample — derived arithmetic only, no model call. The writer
    replaces a title's whole row set in one transaction, so stale
    themes and stale taxonomy versions never linger."""
    __tablename__ = 'review_theme'
    __table_args__ = (
        UniqueConstraint('gameid', 'theme', name='uq_review_theme_cell'),
        Index('ix_review_theme_gameid', 'gameid'),
        {'schema': 'games',
         'comment': 'Per-title review-theme aggregates over the '
                    'classified sample. Speaks only to theme '
                    'composition; positive share comes from the pull '
                    'totals on community_snapshot '
                    '(steam_total_reviews / steam_positive_pct), '
                    'never from this sample.'},
    )

    reviewthemeid = Column(BigIntPk, primary_key=True)
    gameid = Column(BigInteger, ForeignKey('games.game.gameid'),
                    nullable=False)
    theme = Column(Text, nullable=False, comment='Taxonomy key.')
    mentions = Column(Numeric,
                      comment='Classified reviews carrying the theme.')
    positive_mentions = Column(
        Numeric, comment='Of mentions, reviews whose author '
                         "recommends the title (Steam's own voted_up "
                         'flag, not model-judged sentiment).')
    negative_mentions = Column(
        Numeric, comment='Of mentions, reviews whose author does not '
                         'recommend the title.')
    share = Column(
        Numeric, comment='mentions / sample_size (0-1) - a '
                         'composition of the classified sample, not '
                         'of all reviews.')
    sample_size = Column(
        Numeric, comment='Classified reviews behind every row for '
                         'this title.')
    corpus_size = Column(
        Numeric, comment='All stored review_text rows for the title '
                         'at derive time.')
    classified_share = Column(
        Numeric, comment='sample_size / corpus_size (0-1).')
    window_start = Column(
        Date, comment='Earliest review creation date in the '
                      'classified sample.')
    window_end = Column(
        Date, comment='Latest review creation date in the classified '
                      'sample.')
    example_ids = Column(
        Text, comment='Comma-joined recommendationids of the '
                      'top-voted examples; best-effort audit - ids '
                      'may age past the stored cap.')
    taxonomy_version = Column(
        Integer, nullable=False,
        comment='Taxonomy the aggregates were derived under.')
    model = Column(Text,
                   comment='Classifier model id, for provenance.')
    computed_at = Column(
        DateTime, comment='Naive UTC; when derive wrote this row - '
                          'the recency column.')
