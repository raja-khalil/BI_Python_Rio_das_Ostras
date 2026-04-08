"""Conexao e sessao com PostgreSQL via SQLAlchemy."""

from __future__ import annotations

from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import get_settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Retorna engine SQLAlchemy singleton."""
    settings = get_settings()
    return create_engine(
        settings.sqlalchemy_database_url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def get_session() -> Session:
    """Cria sessao SQLAlchemy para operacoes transacionais."""
    session_factory = sessionmaker(bind=get_engine(), autoflush=False, autocommit=False)
    return session_factory()
