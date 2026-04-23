from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import settings

engine = create_engine(
    settings.DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
    echo=False,
)


# ── SQLite concurrency config ─────────────────────────────────────────
# Without WAL mode, SQLite serializes readers and writers via a single
# file lock. As soon as an enrichment job (running in a worker thread
# via asyncio.to_thread — see PR #14) starts a write transaction, any
# web request reading the DB blocks up to `timeout=30s` before failing
# with `OperationalError: database is locked` — which is exactly what
# we saw on /api/suggestions after the PR #14 + PR #15 deploy.
#
# WAL mode:
#   - Readers never block writers
#   - Writers never block readers
#   - Still single-writer (same as rollback-journal mode)
#
# `journal_mode=WAL` is persisted in the DB file header, so it sticks
# after the first connection; re-running it is a cheap no-op. The other
# PRAGMAs are per-connection and need to be re-applied on every connect.
if settings.DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")   # WAL-safe, much faster than FULL
        cursor.execute("PRAGMA busy_timeout=30000")   # 30s, matches connect_args timeout
        cursor.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
