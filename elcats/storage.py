"""Storage helpers for persisting scraped data."""

from __future__ import annotations

import logging
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional

LOGGER = logging.getLogger(__name__)


def _uuid(value: str) -> str:
    """Return a deterministic UUID5 for the provided value."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"elcats::{value}"))


class Storage:
    """SQLite-backed storage for the elcats catalogue."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._lock = threading.RLock()
        self._init_schema()
        self._brand_ids: set[str] = set()
        self._vehicle_ids: set[str] = set()
        self._category_ids: set[str] = set()
        self._part_ids: set[str] = set()
        self._attribute_keys: set[tuple[str, str, str, str]] = set()
        self._link_keys: set[tuple[str, str]] = set()

    def close(self) -> None:
        LOGGER.debug("Closing storage")
        with self._lock:
            self.conn.commit()
            self.conn.close()

    def _init_schema(self) -> None:
        LOGGER.debug("Initializing database schema")
        with self._lock:
            self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS brands (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS vehicles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                brand_id TEXT NOT NULL REFERENCES brands(id)
            );

            CREATE TABLE IF NOT EXISTS party_categories (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT,
                parent_category_id TEXT REFERENCES party_categories(id)
            );

            CREATE TABLE IF NOT EXISTS parts (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT,
                category_id TEXT REFERENCES party_categories(id)
            );

            CREATE TABLE IF NOT EXISTS parts_to_vehicles (
                id TEXT PRIMARY KEY,
                part_id TEXT NOT NULL REFERENCES parts(id),
                vehicle_id TEXT NOT NULL REFERENCES vehicles(id),
                UNIQUE(part_id, vehicle_id)
            );

            CREATE TABLE IF NOT EXISTS attributes (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                value TEXT,
                vehicle_id TEXT REFERENCES vehicles(id),
                part_id TEXT REFERENCES parts(id),
                UNIQUE(name, value, vehicle_id, part_id)
            );

            CREATE TABLE IF NOT EXISTS scrape_progress (
                id TEXT PRIMARY KEY,
                brand_slug TEXT NOT NULL,
                model_slug TEXT NOT NULL,
                modification_code TEXT NOT NULL,
                status INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE UNIQUE INDEX IF NOT EXISTS idx_scrape_progress_keys
                ON scrape_progress(brand_slug, model_slug, modification_code);
            """
        )
            self.conn.commit()

    @contextmanager
    def transaction(self) -> Iterable[None]:
        with self._lock:
            try:
                yield
            except Exception:
                self.conn.rollback()
                raise
            else:
                self.conn.commit()

    # ------------------------------------------------------------------
    # Brands
    # ------------------------------------------------------------------

    def add_brand(self, slug: str, name: str) -> str:
        slug = slug.lower()
        brand_id = _uuid(f"brand:{slug}")
        with self._lock:
            if brand_id in self._brand_ids:
                return brand_id
            LOGGER.debug("Adding brand %s (%s)", name, slug)
            self.conn.execute(
                "INSERT OR IGNORE INTO brands (id, name) VALUES (?, ?)",
                (brand_id, name.strip()),
            )
            self._brand_ids.add(brand_id)
        return brand_id

    # ------------------------------------------------------------------
    # Vehicles
    # ------------------------------------------------------------------

    def add_vehicle(self, model_id: str, name: str, brand_id: str) -> str:
        vehicle_id = str(uuid.UUID(model_id)) if model_id else _uuid(f"vehicle:{name}")
        with self._lock:
            if vehicle_id in self._vehicle_ids:
                return vehicle_id
            LOGGER.debug("Adding vehicle %s (%s)", name, vehicle_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO vehicles (id, name, brand_id) VALUES (?, ?, ?)",
                (vehicle_id, name.strip(), brand_id),
            )
            self._vehicle_ids.add(vehicle_id)
        return vehicle_id

    def add_vehicle_attribute(self, vehicle_id: str, name: str, value: Optional[str]) -> None:
        if value is None:
            return
        value = value.strip()
        if not value:
            return
        attr_id = _uuid(f"vehicle:{vehicle_id}:{name}:{value}")
        key = (name, value, vehicle_id, "")
        with self._lock:
            if key in self._attribute_keys:
                return
            LOGGER.debug("Adding vehicle attribute %s=%s for %s", name, value, vehicle_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO attributes (id, name, value, vehicle_id, part_id)"
                " VALUES (?, ?, ?, ?, NULL)",
                (attr_id, name, value, vehicle_id),
            )
            self._attribute_keys.add(key)

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------

    def add_category(
        self,
        model_id: str,
        key: str,
        name: str,
        code: Optional[str] = None,
        parent_id: Optional[str] = None,
    ) -> str:
        category_id = _uuid(f"category:{model_id}:{key}")
        with self._lock:
            if category_id in self._category_ids:
                return category_id
            LOGGER.debug(
                "Adding category %s (model=%s, key=%s) -> %s",
                name,
                model_id,
                key,
                category_id,
            )
            self.conn.execute(
                "INSERT OR IGNORE INTO party_categories (id, name, code, parent_category_id)"
                " VALUES (?, ?, ?, ?)",
                (category_id, name.strip(), code, parent_id),
            )
            self._category_ids.add(category_id)
        return category_id

    # ------------------------------------------------------------------
    # Parts
    # ------------------------------------------------------------------

    def add_part(
        self,
        model_id: str,
        sub_id: str,
        part_key: str,
        name: str,
        code: Optional[str],
        category_id: str,
    ) -> str:
        part_id = _uuid(f"part:{model_id}:{sub_id}:{part_key}")
        with self._lock:
            if part_id in self._part_ids:
                return part_id
            LOGGER.debug("Adding part %s (%s) in %s", name, part_key, category_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO parts (id, name, code, category_id) VALUES (?, ?, ?, ?)",
                (part_id, name.strip(), code.strip() if code else None, category_id),
            )
            self._part_ids.add(part_id)
        return part_id

    def add_part_attribute(self, part_id: str, name: str, value: Optional[str]) -> None:
        if value is None:
            return
        value = value.strip()
        if not value:
            return
        key = (name, value, "", part_id)
        with self._lock:
            if key in self._attribute_keys:
                return
            attr_id = _uuid(f"part:{part_id}:{name}:{value}")
            LOGGER.debug("Adding part attribute %s=%s for %s", name, value, part_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO attributes (id, name, value, vehicle_id, part_id)"
                " VALUES (?, ?, ?, NULL, ?)",
                (attr_id, name, value, part_id),
            )
            self._attribute_keys.add(key)

    # ------------------------------------------------------------------
    # Part to vehicle links
    # ------------------------------------------------------------------

    def link_part_vehicle(self, part_id: str, vehicle_id: str) -> None:
        key = (part_id, vehicle_id)
        with self._lock:
            if key in self._link_keys:
                return
            link_id = _uuid(f"link:{part_id}:{vehicle_id}")
            LOGGER.debug("Linking part %s to vehicle %s", part_id, vehicle_id)
            self.conn.execute(
                "INSERT OR IGNORE INTO parts_to_vehicles (id, part_id, vehicle_id)"
                " VALUES (?, ?, ?)",
                (link_id, part_id, vehicle_id),
            )
            self._link_keys.add(key)

    def commit(self) -> None:
        LOGGER.debug("Committing database changes")
        with self._lock:
            self.conn.commit()

    # ------------------------------------------------------------------
    # Progress tracking
    # ------------------------------------------------------------------

    def _progress_id(self, brand_slug: str, model_slug: str, modification_code: str) -> str:
        key = f"progress:{brand_slug}:{model_slug}:{modification_code}"
        return _uuid(key)

    def should_process_modification(
        self, brand_slug: str, model_slug: str, modification_code: str
    ) -> bool:
        progress_id = self._progress_id(brand_slug, model_slug, modification_code)
        with self._lock:
            row = self.conn.execute(
                "SELECT status FROM scrape_progress WHERE id = ?",
                (progress_id,),
            ).fetchone()
            if row and int(row[0]):
                return False
            if not row:
                LOGGER.debug(
                    "Registering new progress entry for %s/%s/%s",
                    brand_slug,
                    model_slug,
                    modification_code,
                )
                self.conn.execute(
                    "INSERT INTO scrape_progress (id, brand_slug, model_slug, modification_code, status)"
                    " VALUES (?, ?, ?, ?, 0)",
                    (
                        progress_id,
                        brand_slug,
                        model_slug,
                        modification_code,
                    ),
                )
            else:
                LOGGER.debug(
                    "Resuming progress for %s/%s/%s", brand_slug, model_slug, modification_code
                )
                self.conn.execute(
                    "UPDATE scrape_progress SET status = 0, updated_at = CURRENT_TIMESTAMP"
                    " WHERE id = ?",
                    (progress_id,),
                )
            self.conn.commit()
        return True

    def mark_modification_completed(
        self, brand_slug: str, model_slug: str, modification_code: str
    ) -> None:
        progress_id = self._progress_id(brand_slug, model_slug, modification_code)
        with self._lock:
            LOGGER.debug(
                "Marking progress complete for %s/%s/%s",
                brand_slug,
                model_slug,
                modification_code,
            )
            self.conn.execute(
                "UPDATE scrape_progress SET status = 1, updated_at = CURRENT_TIMESTAMP"
                " WHERE id = ?",
                (progress_id,),
            )
            self.conn.commit()
