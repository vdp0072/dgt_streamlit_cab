import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_database_url():
    return os.getenv("DATABASE_URL")


def get_engine(**kwargs):
    """Return SQLAlchemy engine. If DATABASE_URL is not set, fall back to sqlite for local testing."""
    db_url = get_database_url()
    if db_url:
        # try to create an engine and verify connectivity; if that fails, fall back to sqlite
        try:
            engine = create_engine(db_url, future=True, **kwargs)
            # attempt a lightweight connect to verify reachability
            with engine.connect() as conn:
                pass
            return engine
        except Exception:
            # fall back to sqlite to allow local development even when remote DB is unreachable
            fallback = create_engine("sqlite:///data.db", future=True, **kwargs)
            return fallback
    # fallback to local sqlite
    return create_engine("sqlite:///data.db", future=True, **kwargs)


def get_dialect_name(engine):
    return engine.dialect.name
