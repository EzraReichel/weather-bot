#!/usr/bin/env python3
"""
One-shot migration: copy paper trades out of the trades table into a
dedicated paper_trades archive table, then remove them from trades.

Run with the EXTERNAL Render Postgres URL:
    DATABASE_URL="postgres://..." python scripts/migrate_paper_trades.py
"""
import os
import sys
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("Set DATABASE_URL to the external Render Postgres URL before running.")

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM trades WHERE is_paper = true")).scalar()
    print(f"Found {count} paper trades in trades table.")
    if count == 0:
        print("Nothing to migrate.")
        sys.exit(0)

    conn.execute(text("CREATE TABLE IF NOT EXISTS paper_trades AS SELECT * FROM trades WHERE false"))
    conn.commit()

    inserted = conn.execute(text("INSERT INTO paper_trades SELECT * FROM trades WHERE is_paper = true")).rowcount
    conn.commit()
    print(f"Copied {inserted} rows into paper_trades.")

    archive_count = conn.execute(text("SELECT COUNT(*) FROM paper_trades")).scalar()
    if archive_count != count:
        sys.exit(f"Count mismatch ({archive_count} vs {count}) — aborting delete, check manually.")

    deleted = conn.execute(text("DELETE FROM trades WHERE is_paper = true")).rowcount
    conn.commit()
    print(f"Removed {deleted} rows from trades. trades table is now live-only.")

    print("Done. paper_trades archive has", archive_count, "rows.")
