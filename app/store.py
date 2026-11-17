"""Storage engine for the build artifact cache.

Design notes
------------
* Single-process, single SQLite connection guarded by one threading lock.
  Every public method is fully serialized, so acquire / renew / reclaim
  interleavings cannot race inside the process. SQLite is the source of
  truth for crash recovery.
* **Content-addressed physical storage (deduplication).** Verified bytes
  live in a separate ``blobs`` table / blob store, keyed by SHA-256 with a
  deterministic path (``blobs/<aa>/<sha256>``). Artifact rows only hold a
  reference (``blob_sha256``) plus their own, independent generation. Two
  build numbers -- or two re-upload generations -- with identical bytes
  share one physical file; ``blobs.ref_count`` tracks how many artifact
  rows reference it. Artifact identity (number / generation / tokens /
  LRU ordering) stays fully independent per product.
* A blob can only be shared when the file already on disk is itself
  verified: an existing file is read back and hashed before reuse, and a
  missing or corrupt copy is rewritten from the freshly verified upload.
  A damaged on-disk file therefore never serves as a sharing source.
* Every upload gets a fresh *generation* on the artifact row. Tokens pin
  ``(number, generation)`` so a token issued for an older upload can never
  read the re-uploaded content, and a pending reclaim of generation N can
  never affect generation N+1.
* Reclaim is two-phase: a conditional ``UPDATE ... SET state='reclaiming'``
  on the artifact (which atomically re-checks "no active tokens" and blocks
  any new token issuance), then the artifact row is deleted. The underlying
  blob file is removed **only when that was the last reference**; deleting
  one unoccupied shared reference leaves the other artifact fully
  downloadable. If the last blob file cannot be deleted, the blob row
  stays ``reclaiming`` and the sweeper / next reclaim / startup recovery
  retries it, so ``freed_bytes`` counts only space that was really
  released.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time
import uuid

from .config import Config

SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
    sha256    TEXT PRIMARY KEY,
    size      INTEGER NOT NULL,
    ref_count INTEGER NOT NULL,
    state     TEXT NOT NULL DEFAULT 'ready'
              CHECK (state IN ('ready', 'reclaiming'))
);

CREATE TABLE IF NOT EXISTS artifacts (
    number       TEXT PRIMARY KEY,
    generation   INTEGER NOT NULL,
    blob_sha256  TEXT NOT NULL,
    size         INTEGER NOT NULL,
    state        TEXT NOT NULL DEFAULT 'ready'
                 CHECK (state IN ('ready', 'reclaiming')),
    created_at   REAL NOT NULL,
    last_used_at REAL NOT NULL,
    FOREIGN KEY (blob_sha256) REFERENCES blobs(sha256)
);

CREATE TABLE IF NOT EXISTS tokens (
    token      TEXT PRIMARY KEY,
    number     TEXT NOT NULL,
    generation INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    released   INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tokens_active
    ON tokens(number, generation, released, expires_at);
CREATE INDEX IF NOT EXISTS idx_artifacts_blob
    ON artifacts(blob_sha256);
"""

# SQL fragment: an active (unreleased, unexpired) token pinning this exact
# artifact generation exists. Takes one parameter: the current timestamp.
_ACTIVE_TOKEN = """
    EXISTS(SELECT 1 FROM tokens t
           WHERE t.number = artifacts.number
             AND t.generation = artifacts.generation
             AND t.released = 0
             AND t.expires_at > ?)
"""


class StoreError(Exception):
    """Domain error carrying an HTTP status code and a client-safe detail."""

    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class _Txn:
    """Explicit BEGIN IMMEDIATE transaction on the autocommit connection."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.conn.execute("ROLLBACK" if exc_type else "COMMIT")
        return False


class Store:
    def __init__(self, config: Config):
        self.config = config
        self.storage_dir = os.path.abspath(config.storage_dir)
        self.blobs_dir = os.path.join(self.storage_dir, "blobs")
        os.makedirs(self.blobs_dir, exist_ok=True)
        if config.db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(config.db_path))
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(config.db_path, check_same_thread=False)
        self.conn.isolation_level = None  # autocommit; explicit txns via _Txn
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._migrate_legacy_layout()
        self.conn.executescript(SCHEMA)
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ util

    def _txn(self) -> _Txn:
        return _Txn(self.conn)

    def _blob_rel(self, sha256: str) -> str:
        return f"blobs/{sha256[:2]}/{sha256}"

    def _blob_path(self, sha256: str) -> str:
        return os.path.join(self.storage_dir, self._blob_rel(sha256))

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    @staticmethod
    def _now() -> float:
        return time.time()

    def _atomic_write(self, dest: str, data: bytes) -> None:
        """Write ``data`` to ``dest`` atomically (tmp file + fsync + rename)."""
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        tmp = f"{dest}.tmp-{uuid.uuid4().hex}"
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, dest)
        try:  # best-effort directory fsync for rename durability
            dfd = os.open(os.path.dirname(dest), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass

    def _migrate_legacy_layout(self) -> None:
        """Migrate a pre-dedup database (per-upload files, no blobs table).

        Legacy rows referenced unique files under ``<number>/...``. Each
        distinct digest becomes one blob row with a deterministic blob path;
        the legacy file is moved into the blob store when present. Missing or
        unreadable legacy files still get a blob row (the artifact is simply
        not downloadable until re-uploaded, as before).
        """
        tables = {
            r["name"]
            for r in self.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "blobs" in tables or "artifacts" not in tables:
            return
        cols = {
            r["name"]
            for r in self.conn.execute("PRAGMA table_info(artifacts)")
        }
        if "file_path" not in cols or "blob_sha256" in cols:
            return  # unknown / already-migrated shape; leave to SCHEMA DDL
        legacy = self.conn.execute(
            "SELECT number, generation, sha256, size, file_path, state, "
            "created_at, last_used_at FROM artifacts"
        ).fetchall()
        with self._txn() as db:
            db.execute(
                """
                CREATE TABLE blobs (
                    sha256    TEXT PRIMARY KEY,
                    size      INTEGER NOT NULL,
                    ref_count INTEGER NOT NULL,
                    state     TEXT NOT NULL DEFAULT 'ready'
                              CHECK (state IN ('ready', 'reclaiming'))
                )
                """
            )
            db.execute(
                """
                CREATE TABLE artifacts_new (
                    number       TEXT PRIMARY KEY,
                    generation   INTEGER NOT NULL,
                    blob_sha256  TEXT NOT NULL,
                    size         INTEGER NOT NULL,
                    state        TEXT NOT NULL DEFAULT 'ready'
                                 CHECK (state IN ('ready', 'reclaiming')),
                    created_at   REAL NOT NULL,
                    last_used_at REAL NOT NULL
                )
                """
            )
            blobs: dict[str, int] = {}
            for row in legacy:
                digest = row["sha256"]
                rel = self._blob_rel(digest)
                dest = os.path.join(self.storage_dir, rel)
                old_abs = os.path.join(self.storage_dir, row["file_path"])
                if not os.path.exists(dest) and os.path.isfile(old_abs):
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    try:
                        os.replace(old_abs, dest)
                    except OSError:
                        pass  # row stays non-downloadable; re-upload repairs it
                blobs[digest] = blobs.get(digest, 0) + 1
                db.execute(
                    "INSERT INTO artifacts_new(number, generation, blob_sha256, "
                    "size, state, created_at, last_used_at) VALUES (?,?,?,?,?,?,?)",
                    (
                        row["number"],
                        row["generation"],
                        digest,
                        row["size"],
                        row["state"],
                        row["created_at"],
                        row["last_used_at"],
                    ),
                )
            for digest, count in blobs.items():
                size = next(
                    r["size"] for r in legacy if r["sha256"] == digest
                )
                db.execute(
                    "INSERT INTO blobs(sha256, size, ref_count, state) "
                    "VALUES (?,?,?,'ready')",
                    (digest, size, count),
                )
            db.execute("DROP TABLE artifacts")
            db.execute("ALTER TABLE artifacts_new RENAME TO artifacts")
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_tokens_active "
                "ON tokens(number, generation, released, expires_at)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_artifacts_blob "
                "ON artifacts(blob_sha256)"
            )

    # -------------------------------------------------------------- artifacts

    def upload(self, number: str, data: bytes, sha256: str) -> dict:
        """Store a new generation of ``number`` and return its metadata.

        Identical, verified content is deduplicated into the shared blob
        store. The blob file is made durable (or verified to already contain
        the right bytes) before any row is committed, so a crash leaves at
        worst an orphan file (swept later), never a record pointing at
        missing content created by *this* upload.
        """
        now = self._now()
        size = len(data)
        with self.lock:
            with self._txn() as db:
                row = db.execute(
                    "SELECT generation, blob_sha256 FROM artifacts WHERE number=?",
                    (number,),
                ).fetchone()
                generation = (row["generation"] + 1) if row else 1
                old_blob = row["blob_sha256"] if row else None

                blob = db.execute(
                    "SELECT sha256, ref_count, state FROM blobs WHERE sha256=?",
                    (sha256,),
                ).fetchone()
                if blob is None:
                    # Brand-new content: durable physical file first.
                    self._atomic_write(self._blob_path(sha256), data)
                    db.execute(
                        "INSERT INTO blobs(sha256, size, ref_count, state) "
                        "VALUES (?,?, 1, 'ready')",
                        (sha256, size),
                    )
                elif old_blob == sha256:
                    # Same artifact, identical content, new generation: the
                    # single existing reference simply moves with the row, so
                    # ref_count must not change. Still verify / repair the
                    # physical file before trusting it.
                    self._ensure_blob_file_locked(sha256, data)
                    db.execute(
                        "UPDATE blobs SET state='ready' "
                        "WHERE sha256=? AND ref_count > 0",
                        (sha256,),
                    )
                else:
                    # Reuse only a verified on-disk copy; a missing or
                    # corrupt existing file is repaired from this upload.
                    self._ensure_blob_file_locked(sha256, data)
                    if blob["ref_count"] == 0:
                        # A stale zero-ref row left behind by a failed
                        # deletion: replace it instead of reviving the
                        # reclaiming state / inflating the ref count.
                        db.execute(
                            "DELETE FROM blobs WHERE sha256=? AND ref_count=0",
                            (sha256,),
                        )
                        db.execute(
                            "INSERT INTO blobs(sha256, size, ref_count, state) "
                            "VALUES (?,?, 1, 'ready')",
                            (sha256, size),
                        )
                    else:
                        db.execute(
                            "UPDATE blobs SET ref_count = ref_count + 1, "
                            "state='ready' WHERE sha256=?",
                            (sha256,),
                        )

                db.execute(
                    """
                    INSERT INTO artifacts(number, generation, blob_sha256, size,
                                          state, created_at, last_used_at)
                    VALUES (?,?,?,?, 'ready', ?, ?)
                    ON CONFLICT(number) DO UPDATE SET
                        generation   = excluded.generation,
                        blob_sha256  = excluded.blob_sha256,
                        size         = excluded.size,
                        state        = 'ready',
                        last_used_at = excluded.last_used_at
                    """,
                    (number, generation, sha256, size, now, now),
                )
                # NB: tokens pinned to older generations are left in place;
                # they can never match the new generation (download -> 409,
                # renew -> 409) and are cleaned up once they expire.
                drop_blob = None
                if old_blob is not None and old_blob != sha256:
                    cur = db.execute(
                        "UPDATE blobs SET ref_count = ref_count - 1 "
                        "WHERE sha256=? AND ref_count > 0",
                        (old_blob,),
                    )
                    if cur.rowcount:
                        ref = db.execute(
                            "SELECT ref_count, state FROM blobs WHERE sha256=?",
                            (old_blob,),
                        ).fetchone()
                        if ref is not None and ref["ref_count"] == 0:
                            drop_blob = old_blob

            # Outside the transaction: release the now-unreferenced old blob.
            # Any failure is recoverable: the sweeper handles zero-ref blobs.
            if drop_blob is not None:
                self._release_unreferenced_blob(drop_blob)

            return self._artifact_out(number, now)

    def _blob_file_intact(self, sha256: str, expected_size: int) -> bool:
        """True only when the blob file exists, has the recorded length, and
        re-hashes to its content key. Used to avoid ever advertising a
        missing or damaged file as downloadable."""
        path = self._blob_path(sha256)
        try:
            if os.path.getsize(path) != expected_size:
                return False
            with open(path, "rb") as fh:
                return hashlib.sha256(fh.read()).hexdigest() == sha256
        except OSError:
            return False

    def _ensure_blob_file_locked(self, sha256: str, data: bytes) -> None:
        """Make sure the on-disk blob for ``sha256`` equals verified ``data``.

        A present, byte-identical file is reused (the sharing fast path). A
        missing or corrupted file is rewritten atomically from the freshly
        verified upload body, so a damaged existing file can never act as a
        sharing source.
        """
        path = self._blob_path(sha256)
        if os.path.isfile(path):
            try:
                with open(path, "rb") as fh:
                    if hashlib.sha256(fh.read()).hexdigest() == sha256:
                        return  # verified copy already on disk: share it
            except OSError:
                pass
        self._atomic_write(path, data)

    def get_artifact(self, number: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if row is None:
                raise StoreError(404, f"artifact {number!r} not found")
            return self._artifact_out(number, self._now())

    def list_artifacts(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT number FROM artifacts ORDER BY number"
            ).fetchall()
            now = self._now()
            return [self._artifact_out(r["number"], now) for r in rows]

    def _artifact_out(self, number: str, now: float) -> dict:
        row = self.conn.execute(
            "SELECT * FROM artifacts WHERE number=?", (number,)
        ).fetchone()
        if row is None:
            raise StoreError(404, f"artifact {number!r} not found")
        active = self.conn.execute(
            "SELECT COUNT(*) AS c FROM tokens "
            "WHERE number=? AND generation=? AND released=0 AND expires_at>?",
            (number, row["generation"], now),
        ).fetchone()["c"]
        blob = self.conn.execute(
            "SELECT ref_count, state, size FROM blobs WHERE sha256=?",
            (row["blob_sha256"],),
        ).fetchone()
        ref_count = blob["ref_count"] if blob is not None else 0
        # Never claim downloadable unless the record is ready, its blob is
        # ready, and the underlying file is present with the expected length.
        # Full byte-level SHA-256 verification happens on every download.
        downloadable = (
            row["state"] == "ready"
            and blob is not None
            and blob["state"] == "ready"
            and self._blob_file_intact(row["blob_sha256"], blob["size"])
        )
        return {
            "number": row["number"],
            "generation": row["generation"],
            "sha256": row["blob_sha256"],
            "size": row["size"],
            "state": row["state"],
            "created_at": row["created_at"],
            "last_used_at": row["last_used_at"],
            "active_tokens": active,
            "shared": ref_count > 1,
            "references": ref_count,
            "downloadable": downloadable,
        }

    # ----------------------------------------------------------------- tokens

    def acquire(self, number: str) -> dict:
        now = self._now()
        with self.lock, self._txn() as db:
            row = db.execute(
                "SELECT generation, state FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if row is None:
                raise StoreError(404, f"artifact {number!r} not found")
            if row["state"] != "ready":
                raise StoreError(
                    409,
                    f"artifact {number!r} is pending reclamation; "
                    "no new tokens are issued",
                )
            token = uuid.uuid4().hex
            expires_at = now + self.config.token_ttl
            db.execute(
                "INSERT INTO tokens(token, number, generation, expires_at, "
                "released, created_at) VALUES (?,?,?,?,0,?)",
                (token, number, row["generation"], expires_at, now),
            )
            db.execute(
                "UPDATE artifacts SET last_used_at=? WHERE number=?", (now, number)
            )
        return {
            "token": token,
            "number": number,
            "generation": row["generation"],
            "expires_at": expires_at,
            "expires_in": self.config.token_ttl,
        }

    def _load_valid_token(self, token: str, number: str | None) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM tokens WHERE token=?", (token,)
        ).fetchone()
        if row is None or (number is not None and row["number"] != number):
            raise StoreError(403, "invalid token")
        if row["released"]:
            raise StoreError(403, "token has been released")
        if row["expires_at"] <= self._now():
            raise StoreError(403, "token has expired")
        return row

    def download(self, number: str, token: str) -> tuple[bytes, dict]:
        with self.lock:
            tok = self._load_valid_token(token, number)
            art = self.conn.execute(
                "SELECT * FROM artifacts WHERE number=?", (number,)
            ).fetchone()
            if art is None:
                raise StoreError(404, f"artifact {number!r} not found")
            if art["generation"] != tok["generation"]:
                raise StoreError(
                    409,
                    "token was issued for generation "
                    f"{tok['generation']}, but the artifact has been "
                    f"re-uploaded as generation {art['generation']}",
                )
            blob = self.conn.execute(
                "SELECT state FROM blobs WHERE sha256=?",
                (art["blob_sha256"],),
            ).fetchone()
            if blob is None or blob["state"] != "ready":
                raise StoreError(
                    409,
                    "artifact content is not available "
                    "(inconsistent state); refusing to serve",
                )
            path = self._blob_path(art["blob_sha256"])
            try:
                with open(path, "rb") as fh:
                    data = fh.read()
            except FileNotFoundError:
                raise StoreError(
                    409,
                    "artifact record exists but the file is missing on disk "
                    "(inconsistent state); refusing to serve",
                )
            except OSError as exc:
                raise StoreError(409, f"artifact file unreadable: {exc}")
            digest = hashlib.sha256(data).hexdigest()
            if digest != art["blob_sha256"]:
                raise StoreError(
                    409,
                    "artifact file failed integrity check "
                    "(record/file mismatch); refusing to serve",
                )
            now = self._now()
            with self._txn() as db:
                db.execute(
                    "UPDATE artifacts SET last_used_at=? WHERE number=?",
                    (now, number),
                )
            meta = {
                "number": number,
                "generation": art["generation"],
                "sha256": art["blob_sha256"],
                "size": art["size"],
            }
            return data, meta

    def renew(self, token: str) -> dict:
        now = self._now()
        with self.lock, self._txn() as db:
            row = db.execute(
                "SELECT * FROM tokens WHERE token=?", (token,)
            ).fetchone()
            if row is None:
                raise StoreError(404, "unknown token")
            if row["released"]:
                raise StoreError(409, "token has been released")
            if row["expires_at"] <= now:
                raise StoreError(409, "token has expired and cannot be renewed")
            art = db.execute(
                "SELECT generation, state FROM artifacts WHERE number=?",
                (row["number"],),
            ).fetchone()
            if (
                art is None
                or art["generation"] != row["generation"]
                or art["state"] != "ready"
            ):
                raise StoreError(
                    409,
                    "artifact is no longer available for renewal "
                    "(reclaimed, being reclaimed, or re-uploaded)",
                )
            expires_at = now + self.config.token_ttl
            db.execute(
                "UPDATE tokens SET expires_at=? WHERE token=?", (expires_at, token)
            )
        return {
            "token": token,
            "number": row["number"],
            "generation": row["generation"],
            "expires_at": expires_at,
            "expires_in": self.config.token_ttl,
        }

    def release(self, token: str) -> dict:
        with self.lock, self._txn() as db:
            cur = db.execute(
                "UPDATE tokens SET released=1 WHERE token=?", (token,)
            )
            if cur.rowcount == 0:
                raise StoreError(404, "unknown token")
        return {"token": token, "released": True}

    def get_token(self, token: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM tokens WHERE token=?", (token,)
            ).fetchone()
            if row is None:
                raise StoreError(404, "unknown token")
            now = self._now()
            return {
                "token": row["token"],
                "number": row["number"],
                "generation": row["generation"],
                "expires_at": row["expires_at"],
                "released": bool(row["released"]),
                "active": not row["released"] and row["expires_at"] > now,
            }

    # ---------------------------------------------------------------- reclaim

    def reclaim(self, target_bytes: int) -> dict:
        """Free at least ``target_bytes``, LRU-first, never touching occupied
        or already-selected artifacts. Returns what was actually freed.

        Removing one reference of a *shared* blob deletes no file and frees
        no bytes; the blob file is deleted (and its size counted in
        ``freed_bytes``) only when the last unoccupied reference goes away.
        """
        if target_bytes < 0:
            raise StoreError(422, "target_bytes must be >= 0")
        now = self._now()
        freed = 0
        reclaimed: list[dict] = []
        errors: list[dict] = []
        with self.lock:
            # First finish any deletions interrupted earlier (crash or I/O
            # failure); bytes freed now count toward this call's target.
            f, rec, err = self._reclaim_pending_locked()
            freed += f
            reclaimed += rec
            errors += err

            while freed < target_bytes:
                rows = self.conn.execute(
                    f"""
                    SELECT a.number, a.generation, a.size, a.blob_sha256,
                           b.ref_count
                    FROM artifacts a
                    JOIN blobs b ON b.sha256 = a.blob_sha256
                    WHERE a.state='ready'
                      AND NOT {_ACTIVE_TOKEN.replace('artifacts.', 'a.')}
                    ORDER BY a.last_used_at ASC, a.number ASC
                    """,
                    (now,),
                ).fetchall()
                if not rows:
                    break
                progress = False
                for row in rows:
                    if freed >= target_bytes:
                        break
                    status, value = self._mark_and_delete_locked(row, now)
                    if status == "freed":
                        freed += value
                        reclaimed.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "size": row["size"],
                                "file_deleted": True,
                            }
                        )
                        progress = True
                    elif status in ("removed_empty", "shared_ref"):
                        reclaimed.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "size": row["size"],
                                "file_deleted": False,
                            }
                        )
                        progress = True
                    elif status == "failed":
                        errors.append(
                            {
                                "number": row["number"],
                                "generation": row["generation"],
                                "error": value,
                            }
                        )
                    # "skipped" -> lost the conditional update; not counted
                    # as progress so the loop can re-evaluate.
                if not progress:
                    break
        return {
            "target_bytes": target_bytes,
            "freed_bytes": freed,
            "reclaimed": reclaimed,
            "errors": errors,
        }

    def _reclaim_pending_locked(self) -> tuple[int, list[dict], list[dict]]:
        """Retry artifact rows / blobs already marked 'reclaiming'."""
        freed = 0
        reclaimed: list[dict] = []
        errors: list[dict] = []

        # 1) Artifact rows marked reclaiming (crash between the artifact and
        #    blob phases). Re-using the normal delete path keeps one code path.
        rows = self.conn.execute(
            "SELECT number, generation, size, blob_sha256 FROM artifacts "
            "WHERE state='reclaiming'"
        ).fetchall()
        for row in rows:
            blob = self.conn.execute(
                "SELECT ref_count FROM blobs WHERE sha256=?",
                (row["blob_sha256"],),
            ).fetchone()
            ref_count = blob["ref_count"] if blob is not None else 1
            view = _RowView(
                number=row["number"],
                generation=row["generation"],
                size=row["size"],
                blob_sha256=row["blob_sha256"],
                ref_count=ref_count,
            )
            status, value = self._delete_marked_artifact_locked(view)
            if status == "freed":
                freed += value
                reclaimed.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "size": row["size"],
                        "file_deleted": True,
                    }
                )
            elif status in ("removed_empty", "shared_ref"):
                reclaimed.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "size": row["size"],
                        "file_deleted": False,
                    }
                )
            elif status == "failed":
                errors.append(
                    {
                        "number": row["number"],
                        "generation": row["generation"],
                        "error": value,
                    }
                )

        # 2) Zero-ref blobs marked reclaiming (file deletion failed last time).
        for brow in self.conn.execute(
            "SELECT sha256, size FROM blobs "
            "WHERE state='reclaiming' AND ref_count=0"
        ).fetchall():
            status, value = self._delete_blob_file_locked(
                brow["sha256"], brow["size"]
            )
            if status == "freed":
                freed += value
            elif status == "failed":
                errors.append(
                    {"number": brow["sha256"], "generation": None, "error": value}
                )
        return freed, reclaimed, errors

    def _mark_and_delete_locked(self, row, now: float):
        """Atomically select an artifact for reclaim, then delete it.

        The conditional UPDATE re-checks occupancy and readiness, so an
        artifact that gained a token since the candidate query is skipped.
        Once marked 'reclaiming', no new tokens can be issued for it.
        """
        cur = self.conn.execute(
            f"""
            UPDATE artifacts SET state='reclaiming'
            WHERE number=? AND generation=? AND state='ready'
              AND NOT {_ACTIVE_TOKEN}
            """,
            (row["number"], row["generation"], now),
        )
        if cur.rowcount == 0:
            return "skipped", 0
        return self._delete_marked_artifact_locked(row)

    def _delete_marked_artifact_locked(self, row):
        """Delete a 'reclaiming' artifact row, then drop its blob if last ref.

        The artifact row delete is conditional on (number, generation,
        reclaiming): if the artifact was re-uploaded meanwhile, the new row
        is left untouched and the blob ref-count is not decremented.

        The blob file is removed only when this was the last reference.
        Shared content keeps one physical file, so the other artifact stays
        downloadable. A failed blob unlink keeps the blob in 'reclaiming'
        for the sweeper / next reclaim / startup recovery, and no bytes are
        counted until the file is really gone.
        """
        with self._txn() as db:
            cur = db.execute(
                "DELETE FROM artifacts WHERE number=? AND generation=? "
                "AND state='reclaiming'",
                (row["number"], row["generation"]),
            )
            if cur.rowcount == 0:
                return "skipped", 0
            db.execute(
                "DELETE FROM tokens WHERE number=? AND generation=?",
                (row["number"], row["generation"]),
            )
            db.execute(
                "UPDATE blobs SET ref_count = ref_count - 1 "
                "WHERE sha256=? AND ref_count > 0",
                (row["blob_sha256"],),
            )
            blob = db.execute(
                "SELECT ref_count, state FROM blobs WHERE sha256=?",
                (row["blob_sha256"],),
            ).fetchone()
            last_ref = blob is not None and blob["ref_count"] == 0
            if last_ref:
                # Reserve the blob for deletion. Driven purely by the
                # ref-count (a zero-ref blob may still be 'ready' if it only
                # became unreferenced in this very call). A re-upload of the
                # same content flips it back to 'ready' and raises the
                # ref-count above zero, which makes this a no-op.
                db.execute(
                    "UPDATE blobs SET state='reclaiming' "
                    "WHERE sha256=? AND ref_count=0",
                    (row["blob_sha256"],),
                )
        if not last_ref:
            return "shared_ref", 0
        status, value = self._delete_blob_file_locked(
            row["blob_sha256"], row["size"]
        )
        if status == "removed_empty":
            return "removed_empty", 0
        return status, value

    def _delete_blob_file_locked(self, sha256: str, size: int):
        """Unlink a zero-ref blob and remove its row.

        Returns ``('freed', size)`` when bytes were really released,
        ``('removed_empty', 0)`` when the file was already gone, or
        ``('failed', message)`` when the unlink errored -- in which case the
        blob row stays 'reclaiming' for a later retry. A blob that regained a
        reference (re-upload) is never touched: returns 'skipped'.
        """
        row = self.conn.execute(
            "SELECT ref_count, state FROM blobs WHERE sha256=?", (sha256,)
        ).fetchone()
        if row is None:
            return "skipped", 0
        if row["ref_count"] != 0:
            # Re-uploaded while a deletion was pending: restore usability and
            # keep the shared file.
            with self._txn() as db:
                db.execute(
                    "UPDATE blobs SET state='ready' "
                    "WHERE sha256=? AND ref_count > 0",
                    (sha256,),
                )
            return "skipped", 0
        path = self._blob_path(sha256)
        try:
            os.remove(path)
            deleted = True
        except FileNotFoundError:
            deleted = False  # already gone; still drop the stale row
        except OSError as exc:
            return "failed", f"{type(exc).__name__}: {exc}"
        with self._txn() as db:
            db.execute(
                "DELETE FROM blobs WHERE sha256=? AND ref_count=0",
                (sha256,),
            )
        # Prune the now-empty shard directory (best effort).
        try:
            os.rmdir(os.path.dirname(path))
        except OSError:
            pass
        return ("freed", size) if deleted else ("removed_empty", 0)

    def _release_unreferenced_blob(self, sha256: str) -> None:
        """Best-effort cleanup of a blob that lost its last reference during
        an upload (the replaced generation pointed at different content)."""
        row = self.conn.execute(
            "SELECT size, state, ref_count FROM blobs WHERE sha256=?",
            (sha256,),
        ).fetchone()
        if row is None or row["ref_count"] != 0:
            return
        if row["state"] != "reclaiming":
            with self._txn() as db:
                db.execute(
                    "UPDATE blobs SET state='reclaiming' "
                    "WHERE sha256=? AND ref_count=0",
                    (sha256,),
                )
        status, _value = self._delete_blob_file_locked(sha256, row["size"])
        if status == "failed":
            # Leave the row for the sweeper; nothing referenced it, so no
            # downloadable record can ever point at it.
            return

    # ------------------------------------------------------------------ sweep

    def sweep(self) -> dict:
        """Recovery pass: finish interrupted reclaims, drop dead tokens,
        remove unreferenced blobs and orphan files. Runs at startup,
        periodically, and on demand."""
        now = self._now()
        with self.lock:
            freed, reclaimed, errors = self._reclaim_pending_locked()

            # Zero-ref 'ready' blobs (e.g. an upload-time cleanup that failed,
            # or a crash after the refcount hit zero): release them now.
            for row in self.conn.execute(
                "SELECT sha256, size FROM blobs WHERE ref_count=0"
            ).fetchall():
                with self._txn() as db:
                    db.execute(
                        "UPDATE blobs SET state='reclaiming' "
                        "WHERE sha256=? AND ref_count=0",
                        (row["sha256"],),
                    )
                status, value = self._delete_blob_file_locked(
                    row["sha256"], row["size"]
                )
                if status == "freed":
                    freed += value
                elif status == "failed":
                    errors.append(
                        {"number": row["sha256"], "generation": None, "error": value}
                    )

            with self._txn() as db:
                db.execute(
                    "DELETE FROM tokens WHERE released=1 OR expires_at<?", (now,)
                )

            known = {self._blob_rel(r["sha256"])
                     for r in self.conn.execute("SELECT sha256 FROM blobs")}
            orphans_removed: list[str] = []
            for root, _dirs, files in os.walk(self.storage_dir):
                for name in files:
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, self.storage_dir)
                    if rel in known:
                        continue
                    try:
                        st = os.stat(full)
                    except OSError:
                        continue
                    # Grace window: a just-written file may belong to an
                    # in-flight upload whose row is not committed yet.
                    if now - st.st_mtime < self.config.orphan_grace:
                        continue
                    try:
                        os.remove(full)
                        orphans_removed.append(rel)
                    except OSError:
                        pass
            # Prune empty directories bottom-up.
            for root, _dirs, _files in os.walk(self.storage_dir, topdown=False):
                if root == self.storage_dir:
                    continue
                try:
                    os.rmdir(root)
                except OSError:
                    pass
            return {
                "freed_bytes": freed,
                "pending_reclaimed": reclaimed,
                "orphans_removed": orphans_removed,
                "errors": errors,
            }

    # ------------------------------------------------------------------ stats

    def stats(self) -> dict:
        now = self._now()
        with self.lock:
            art = self.conn.execute(
                "SELECT COUNT(*) AS c, COALESCE(SUM(size),0) AS b, "
                "COALESCE(SUM(state='reclaiming'),0) AS p FROM artifacts"
            ).fetchone()
            blob = self.conn.execute(
                "SELECT COUNT(*) AS c, COALESCE(SUM(size),0) AS b, "
                "COALESCE(SUM(state='reclaiming'),0) AS p, "
                "COALESCE(SUM(CASE WHEN ref_count > 1 THEN 1 ELSE 0 END),0) AS s "
                "FROM blobs"
            ).fetchone()
            tok = self.conn.execute(
                "SELECT COUNT(*) AS c FROM tokens "
                "WHERE released=0 AND expires_at>?",
                (now,),
            ).fetchone()
            return {
                "artifacts": art["c"],
                "bytes": art["b"],
                "pending_reclaim": art["p"],
                "active_tokens": tok["c"],
                "blobs": blob["c"],
                "physical_bytes": blob["b"],
                "shared_blobs": blob["s"],
                "pending_blob_reclaim": blob["p"],
            }


class _RowView:
    """Minimal mapping view so pending-retry rows look like sqlite Rows."""

    __slots__ = ("number", "generation", "size", "blob_sha256", "ref_count")

    def __init__(self, number, generation, size, blob_sha256, ref_count):
        self.number = number
        self.generation = generation
        self.size = size
        self.blob_sha256 = blob_sha256
        self.ref_count = ref_count

    def __getitem__(self, key):
        return getattr(self, key)
