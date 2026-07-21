"""Session access + natural-key upserts for the ``games`` schema.

``GamesDB`` targets ``steamdbconfig.json`` — the dedicated steam/games
instance, kept in a separate config from ``dbconfig.json`` (the
reporting DB) so games writes and alembic can never land there — and
adds an ORM session over :mod:`reporting.gamesmodels`.
:func:`load_db_config` resolves the config once per process: a local
config file wins (dev boxes), else the SSM parameter
``/processor/config/steamdbconfig.json`` (prod — configure the secret
once, every deploy picks it up), else None and callers skip quietly.
Writers upsert the ``game`` dimension by an identity column and facts
by their natural key, so every ingest is idempotent — re-running a
day/month replaces that slice instead of duplicating it.
"""
import os
import json
import logging
import datetime as dt
import urllib.parse
import sqlalchemy as sqa
from sqlalchemy.orm import sessionmaker
import reporting.utils as utl
import reporting.export as exp
import reporting.gamesmodels as gmdl

GAME_IDENTITY_COLS = ('registry_slug', 'steam_appid')
# SecureString parameters named after the config file they replace.
SSM_PREFIX = '/processor/config/'
_ssm_cache = {}


def _ssm_config(name):
    """Config dict from the ``SSM_PREFIX + name`` parameter, or None
    (no boto3 creds / parameter absent). Cached per process either way
    — a box without AWS access pays the lookup once, not per run."""
    if name in _ssm_cache:
        return _ssm_cache[name]
    value = None
    try:
        import boto3
        response = boto3.client('ssm').get_parameter(
            Name=SSM_PREFIX + name, WithDecryption=True)
        value = json.loads(response['Parameter']['Value'])
        logging.info('Loaded %s from SSM.', SSM_PREFIX + name)
    except Exception as e:
        logging.info('SSM config %s%s unavailable: %s',
                     SSM_PREFIX, name, e)
    _ssm_cache[name] = value
    return value


def load_db_config(config='steamdbconfig.json', paths=None):
    """The steam/games DB config dict — the first config file found
    (dev), else the SSM parameter (prod), else None. ``paths``
    overrides the file search (the lqapp bridge passes its own)."""
    if paths is None:
        paths = [os.path.join(utl.config_path, config)]
    for path in paths:
        if os.path.isfile(path):
            with open(path, 'r') as f:
                return json.load(f)
    return _ssm_config(config)


class GamesDB(exp.DB):
    def __init__(self, config='steamdbconfig.json'):
        super().__init__(None)
        self.session_maker = None
        cfg = load_db_config(config) if isinstance(config, str) else config
        if cfg:
            self.input_config_dict(cfg)

    def input_config_dict(self, cfg):
        """exp.DB fields + conn string from a config dict (file or SSM
        — exp.DB's own loader is file-only and leaves USER/PASS
        unquoted)."""
        self.config = cfg
        self.user, self.pw = cfg['USER'], cfg['PASS']
        self.host, self.port = cfg['HOST'], cfg['PORT']
        self.db = cfg['DATABASE']
        self.conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(
            urllib.parse.quote_plus(str(self.user)),
            urllib.parse.quote_plus(str(self.pw)),
            self.host, self.port, self.db)

    def get_session(self):
        if self.engine is None:
            self.engine = sqa.create_engine(
                self.conn_string, connect_args={'sslmode': 'prefer'})
        if self.session_maker is None:
            self.session_maker = sessionmaker(bind=self.engine)
        return self.session_maker()


def find_game(session, registry_slug=None, steam_appid=None,
              opencritic_id=None, igdb_id=None, canonical_name=None):
    """The ``game`` row matching the strongest provided identity, or
    None. Identity precedence mirrors trust: registry slug (curated) >
    steam appid > opencritic id > igdb id. ``canonical_name`` is a last
    resort — an exact lowercased name match — used to knit sources
    together before they share an identity column."""
    filters = (('registry_slug', registry_slug),
               ('steam_appid', steam_appid),
               ('opencritic_id', opencritic_id),
               ('igdb_id', igdb_id))
    for col, val in filters:
        if val in (None, ''):
            continue
        game = session.query(gmdl.Game).filter(
            getattr(gmdl.Game, col) == val).first()
        if game is not None:
            return game
    if canonical_name:
        return session.query(gmdl.Game).filter(
            sqa.func.lower(gmdl.Game.canonical_name) ==
            canonical_name.strip().lower()).first()
    return None


def identity_conflict(game, fields):
    """True when ``game`` already carries a *different* value for an
    identity column about to be written — a name collision (remake,
    reboot, re-release), not the same game."""
    for col in GAME_IDENTITY_COLS + ('opencritic_id', 'igdb_id'):
        val = fields.get(col)
        if val in (None, ''):
            continue
        existing = getattr(game, col)
        if existing not in (None, '') and existing != val:
            return True
    return False


def upsert_game(session, canonical_name, match_name=False, **fields):
    """Insert or update one ``game`` dim row.

    Matches on the identity columns present in ``fields``. With
    ``match_name=True``, an exact lowercased ``canonical_name`` match
    is accepted as a last resort so sources without a shared identity
    column (Steam appid vs registry slug) still land on one row — but
    only when the matched row doesn't already carry a *different*
    value for an identity column being written (that's a name
    collision, so a new row is inserted instead). ``canonical_name``
    and identity columns are last-writer-wins; other fields only fill
    NULLs. Touches ``first_seen_at``/``updated_at`` (naive UTC).
    Returns the persistent row (flushed, so ``gameid`` is set)."""
    game = find_game(
        session, registry_slug=fields.get('registry_slug'),
        steam_appid=fields.get('steam_appid'),
        opencritic_id=fields.get('opencritic_id'),
        igdb_id=fields.get('igdb_id'))
    if game is None and match_name and canonical_name:
        game = find_game(session, canonical_name=canonical_name)
        if game is not None and identity_conflict(game, fields):
            game = None
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    if game is None:
        game = gmdl.Game(canonical_name=canonical_name, first_seen_at=now)
        session.add(game)
    if canonical_name:
        game.canonical_name = canonical_name
    for col, val in fields.items():
        if val in (None, ''):
            continue
        if getattr(game, col) in (None, '') or col in GAME_IDENTITY_COLS:
            setattr(game, col, val)
    game.updated_at = now
    session.flush()
    return game


def upsert_fact(session, model, keys, fields):
    """Insert or update one fact row matched by its natural key.

    ``keys`` are the natural-key column/value pairs, ``fields`` the
    measures. Returns 1 on insert, 0 on update (for written counts)."""
    row = session.query(model).filter_by(**keys).first()
    inserted = 0
    if row is None:
        row = model(**keys)
        session.add(row)
        inserted = 1
    for col, val in fields.items():
        setattr(row, col, val)
    return inserted


def safe_commit(session, label):
    """Commit; on failure roll back + log and return False. Games-DB
    writes are fail-soft everywhere — the Notes ingests stay the
    operational source and must never break on a games-DB error."""
    try:
        session.commit()
        return True
    except Exception as e:
        session.rollback()
        logging.warning('%s: games DB commit failed: %s', label, e)
        return False
    finally:
        session.close()
