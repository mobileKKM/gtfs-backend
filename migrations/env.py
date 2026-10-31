"""Alembic environment. Migrations are raw SQL; SQLAlchemy is only used as Alembic's driver."""

from alembic import context
from sqlalchemy import create_engine, pool

from gtfs_backend.settings import get_settings

config = context.config


def _sqlalchemy_url() -> str:
    url = config.get_main_option("sqlalchemy.url") or get_settings().database_url
    # psycopg v3 driver for SQLAlchemy
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url.removeprefix("postgresql://")
    return url


def run_migrations_offline() -> None:
    context.configure(url=_sqlalchemy_url(), literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_sqlalchemy_url(), poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
