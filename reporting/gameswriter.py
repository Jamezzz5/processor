"""Normalizes the Steam wide pull into the games schema.

``steapi.get_data`` builds one wide DataFrame per run (ownership
sample, wishlists, player counts, achievements, review summaries, app
details, keyed by appid). This module upserts the ``game`` dimension by
``steam_appid`` and one ``game_event`` observation per appid per run.
Fail-soft by design: any games-DB problem logs and returns 0 so the
raw-CSV pull output is never endangered.
"""
import os
import math
import logging
import reporting.utils as utl
import reporting.gamesdb as gdb
import reporting.gamesmodels as gmdl

EVENT_MEASURES = (
    'player_count', 'owners_in_sample', 'wishlists_in_sample',
    'avg_achievement_pct', 'review_score', 'total_positive',
    'total_negative', 'total_reviews')


def games_db_available(config='steamdbconfig.json'):
    return os.path.isfile(os.path.join(utl.config_path, config))


def clean_val(value):
    """A scalar cell -> value or None (NaN/empty-safe)."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def first_of(value, key=None):
    """First entry of an appdetails list col ('developers') or the
    ``key`` of its first dict ('genres' -> description)."""
    if not isinstance(value, (list, tuple)) or not value:
        return None
    item = value[0]
    if key is not None:
        return item.get(key) if isinstance(item, dict) else None
    return item if isinstance(item, str) else None


def release_date_of(value):
    """appdetails ``release_date`` dict -> its display string."""
    if isinstance(value, dict):
        return clean_val(value.get('date'))
    return None


def price_of(value):
    """appdetails ``price_overview`` dict -> final price in units."""
    if isinstance(value, dict) and value.get('final') is not None:
        return value['final'] / 100
    return None


def game_fields(row):
    """The ``game`` dim fields present in one wide-df row."""
    return {
        'steam_appid': int(row['appid']),
        'publisher': first_of(row.get('publishers')),
        'developer': first_of(row.get('developers')),
        'primary_genre': first_of(row.get('genres'), 'description'),
        'release_date': release_date_of(row.get('release_date')),
    }


def event_fields(row):
    """The ``game_event`` measures present in one wide-df row."""
    fields = {m: clean_val(row.get(m)) for m in EVENT_MEASURES}
    fields['review_score_desc'] = clean_val(row.get('review_score_desc'))
    fields['price'] = price_of(row.get('price_overview'))
    return fields


def write_steam_events(df, config='steamdbconfig.json'):
    """Upsert ``game`` dims + one ``game_event`` per appid for one
    steapi run. Returns rows written (0 on any games-DB problem)."""
    if df is None or df.empty or 'appid' not in df.columns:
        return 0
    if not games_db_available(config):
        logging.info('Games DB config %s not present - skipping games '
                     'schema write.', config)
        return 0
    try:
        session = gdb.GamesDB(config).get_session()
    except Exception as e:
        logging.warning('Games DB unavailable - skipping write: %s', e)
        return 0
    written = 0
    for _, row in df.iterrows():
        if clean_val(row.get('appid')) is None:
            continue
        name = (clean_val(row.get('app_detail_name'))
                or 'Steam app {}'.format(int(row['appid'])))
        game = gdb.upsert_game(session, name, match_name=True,
                               **game_fields(row))
        eventdate = row.get('gameeventdate')
        if hasattr(eventdate, 'to_pydatetime'):
            eventdate = eventdate.to_pydatetime()
        if eventdate is None:
            continue
        written += gdb.upsert_fact(
            session, gmdl.GameEvent,
            {'gameid': game.gameid, 'eventdate': eventdate},
            event_fields(row))
    if not gdb.safe_commit(session, 'Steam games write'):
        return 0
    logging.info('Games DB: %s game_event row(s) written.', written)
    return written
