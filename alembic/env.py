"""Alembic env for the games schema — targets only
``reporting.gamesmodels`` metadata; creds come from
``config/steamdbconfig.json`` or its SSM parameter (the dedicated
steam/games DB — never ``dbconfig.json``, so alembic can't touch the
reporting DB)."""
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

config = context.config
fileConfig(config.config_file_name)

sys.path.insert(0, os.getcwd())
from reporting.gamesmodels import Base  # noqa: E402
from reporting.gamesdb import GamesDB, load_db_config  # noqa: E402
target_metadata = Base.metadata

VERSION_TABLE_SCHEMA = 'games'


def set_url_from_dbconfig():
    """Override the placeholder sqlalchemy.url from the steam/games DB
    config (local file first, then SSM — same resolution the writers
    use)."""
    cfg = load_db_config()
    if cfg is None:
        raise SystemExit(
            'config/steamdbconfig.json not found and the SSM parameter '
            '/processor/config/steamdbconfig.json is unavailable - '
            'alembic only runs against the steam/games DB and refuses '
            'to fall back to dbconfig.json (the reporting DB).')
    url = GamesDB(cfg).conn_string
    config.set_main_option('sqlalchemy.url', url.replace('%', '%%'))


def include_object(obj, name, type_, reflected, compare_to):
    """Keep autogenerate blind to every other schema (lqadb, lqas)."""
    if type_ == 'table':
        return obj.schema == VERSION_TABLE_SCHEMA
    return True


def run_migrations_offline():
    set_url_from_dbconfig()
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url, target_metadata=target_metadata, literal_binds=True,
        version_table_schema=VERSION_TABLE_SCHEMA,
        include_schemas=True, include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    set_url_from_dbconfig()
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        # The version table lives in the games schema, so the schema
        # must exist before alembic's first bookkeeping write.
        connection.exec_driver_sql(
            'CREATE SCHEMA IF NOT EXISTS ' + VERSION_TABLE_SCHEMA)
        connection.commit()
        context.configure(
            connection=connection, target_metadata=target_metadata,
            version_table_schema=VERSION_TABLE_SCHEMA,
            include_schemas=True, include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
