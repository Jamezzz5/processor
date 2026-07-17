"""Session access + natural-key upserts for the ``games`` schema.

``GamesDB`` reuses :class:`reporting.export.DB`'s config plumbing but
reads ``steamdbconfig.json`` — the dedicated steam/games instance, kept
in a separate config file from ``dbconfig.json`` (the reporting DB) so
games writes and alembic can never land there — and adds an ORM
session over :mod:`reporting.gamesmodels`. Writers upsert the
``game`` dimension by an identity column and facts by their natural key,
so every ingest is idempotent — re-running a day/month replaces that
slice instead of duplicating it.
"""
import logging
import datetime as dt
import urllib.parse
import sqlalchemy as sqa
from sqlalchemy.orm import sessionmaker
import reporting.export as exp
import reporting.gamesmodels as gmdl

GAME_IDENTITY_COLS = ('registry_slug', 'steam_appid')


class GamesDB(exp.DB):
    def __init__(self, config='steamdbconfig.json'):
        super().__init__(config)
        if self.config:
            # exp.DB builds the URL unquoted; rebuild so special chars
            # in USER/PASS survive.
            self.conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(
                urllib.parse.quote_plus(str(self.user)),
                urllib.parse.quote_plus(str(self.pw)),
                self.host, self.port, self.db)
        self.session_maker = None

    def get_session(self):
        if self.engine is None:
            self.engine = sqa.create_engine(
                self.conn_string, connect_args={'sslmode': 'prefer'})
        if self.session_maker is None:
            self.session_maker = sessionmaker(bind=self.engine)
        return self.session_maker()


def find_game(session, registry_slug=None, steam_appid=None,
              opencritic_id=None, canonical_name=None):
    """The ``game`` row matching the strongest provided identity, or
    None. Identity precedence mirrors trust: registry slug (curated) >
    steam appid > opencritic id. ``canonical_name`` is a last resort —
    an exact lowercased name match — used to knit sources together
    before they share an identity column."""
    filters = (('registry_slug', registry_slug),
               ('steam_appid', steam_appid),
               ('opencritic_id', opencritic_id))
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
    for col in GAME_IDENTITY_COLS + ('opencritic_id',):
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
        opencritic_id=fields.get('opencritic_id'))
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
