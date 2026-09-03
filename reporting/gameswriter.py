"""Normalizes the Steam wide pull into the games schema.

``steapi.get_data`` builds one wide DataFrame per run (ownership
sample, wishlists, player counts, achievements, review summaries, app
details, keyed by appid). This module upserts the ``game`` dimension by
``steam_appid`` and one ``game_event`` observation per appid per run.
Fail-soft by design: any games-DB problem logs and returns 0 so the
raw-CSV pull output is never endangered.
"""
import hashlib
import math
import logging
import reporting.gamesdb as gdb
import reporting.gamesmodels as gmdl

EVENT_MEASURES = (
    'player_count', 'owners_in_sample', 'wishlists_in_sample',
    'avg_achievement_pct', 'review_score', 'total_positive',
    'total_negative', 'total_reviews')
ASSET_KINDS = ('header', 'screenshots', 'movies', 'description')
ASSET_SAMPLE_CHARS = 160


def games_db_available(config='steamdbconfig.json'):
    """True when the games DB is configured (local file or SSM)."""
    return gdb.load_db_config(config) is not None


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


def price_snapshot_fields(value):
    """appdetails ``price_overview`` dict -> ``price_snapshot``
    columns, or None when the block is absent or priceless (free
    titles carry no price_overview; their sale cycle is not a
    thing)."""
    if not isinstance(value, dict):
        return None
    initial, final = value.get('initial'), value.get('final')
    if initial is None and final is None:
        return None
    return {
        'currency': value.get('currency'),
        'base_price': None if initial is None else initial / 100,
        'final_price': None if final is None else final / 100,
        'discount_pct': value.get('discount_percent') or 0,
    }


def _asset_url(value):
    """A storefront asset URL with its ``?t=`` cache-buster stripped —
    Steam re-stamps the query string without changing the asset, so
    hashing the full URL would report a change every re-stamp."""
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip().split('?', 1)[0]


def _asset_urls(items, key):
    """Clean URLs under ``key`` across a list of appdetails dicts."""
    if not isinstance(items, list):
        return []
    urls = (_asset_url(item.get(key)) for item in items
            if isinstance(item, dict))
    return [url for url in urls if url]


def _movie_labels(items):
    """``id:name`` per appdetails movie — the stable identity of a
    trailer (its webm/mp4 URLs are cache-busted like the images)."""
    if not isinstance(items, list):
        return []
    return [f"{item.get('id')}:{item.get('name') or ''}"
            for item in items
            if isinstance(item, dict) and item.get('id') is not None]


def _asset_row(items, sample):
    """One ``store_asset`` measure dict over the normalised items."""
    digest = hashlib.sha1('\n'.join(items).encode('utf-8')).hexdigest()
    return {'digest': digest, 'item_count': len(items),
            'sample': sample[:ASSET_SAMPLE_CHARS]}


def store_asset_fields(data):
    """appdetails payload -> ``[(asset_kind, store_asset columns)]``
    for each storefront asset the payload carries, in
    :data:`ASSET_KINDS` order. Kinds the payload lacks are skipped
    rather than hashed empty, so an absent trailer set is "not
    observed", never "changed to nothing"."""
    if not isinstance(data, dict):
        return []
    out = []
    header = _asset_url(data.get('header_image'))
    if header:
        out.append(('header', _asset_row([header], header)))
    shots = _asset_urls(data.get('screenshots'), 'path_full')
    if shots:
        out.append(('screenshots', _asset_row(shots, shots[0])))
    movies = _movie_labels(data.get('movies'))
    if movies:
        first = (data['movies'][0].get('name') or movies[0])
        out.append(('movies', _asset_row(movies, str(first))))
    description = ' '.join(str(data.get('short_description') or '')
                           .split())
    if description:
        out.append(('description',
                    _asset_row([description], description)))
    return out


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
