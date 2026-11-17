"""Tests for the content-addressed, deduplicating blob store.

Covers:
* identical uploads (different numbers / generations) occupy one physical
  file while keeping independent artifact records, generations and leases;
* reclaim of one shared reference never breaks the other, and
  ``freed_bytes`` only counts bytes really released on disk;
* a corrupt on-disk blob cannot serve as a sharing source and is repaired;
* crash / failed-deletion recovery never leaves a downloadable record
  pointing at deleted content and never deletes still-referenced content;
* migration of the legacy per-upload file layout.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
import time

import httpx
import uvicorn
from fastapi.testclient import TestClient

from app.config import Config
from app.main import create_app


def make_config(tmp_path, **overrides) -> Config:
    kw = dict(
        db_path=str(tmp_path / "cache.db"),
        storage_dir=str(tmp_path / "storage"),
        token_ttl=300.0,
        orphan_grace=0.0,
        sweep_interval=0.0,
    )
    kw.update(overrides)
    return Config(**kw)


def make_client(tmp_path, **overrides) -> TestClient:
    return TestClient(create_app(make_config(tmp_path, **overrides)))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def put(client: TestClient, number: str, data: bytes):
    return client.put(f"/artifacts/{number}", content=data, params={"sha256": sha(data)})


def acquire(client: TestClient, number: str) -> str:
    r = client.post(f"/artifacts/{number}/tokens")
    assert r.status_code == 201, r.text
    return r.json()["token"]


def download(client: TestClient, number: str, token: str):
    return client.get(f"/artifacts/{number}/content", params={"token": token})


def blob_file(store, digest: str) -> str:
    return store._blob_path(digest)


def count_blob_files(storage_dir: str) -> int:
    return sum(len(files) for _, _, files in os.walk(os.path.join(storage_dir, "blobs")))


# ------------------------------------------------------------- dedup basics


def test_identical_content_stored_once_across_numbers(tmp_path):
    with make_client(tmp_path) as c:
        data = b"shared payload " * 100
        put(c, "build-1", data)
        put(c, "build-2", data)
        store = c.app.state.store

        assert count_blob_files(store.storage_dir) == 1
        path = blob_file(store, sha(data))
        assert os.path.isfile(path)
        assert os.path.getsize(path) == len(data)

        with store.lock:
            row = store.conn.execute(
                "SELECT ref_count, size, state FROM blobs WHERE sha256=?",
                (sha(data),),
            ).fetchone()
        assert row["ref_count"] == 2 and row["state"] == "ready"

        for number in ("build-1", "build-2"):
            meta = c.get(f"/artifacts/{number}").json()
            assert meta["sha256"] == sha(data)
            assert meta["shared"] is True and meta["references"] == 2
            assert meta["downloadable"] is True
            assert download(c, number, acquire(c, number)).content == data

        stats = c.get("/stats").json()
        assert stats["artifacts"] == 2
        assert stats["bytes"] == 2 * len(data)           # logical accounting
        assert stats["physical_bytes"] == len(data)      # one copy on disk
        assert stats["blobs"] == 1 and stats["shared_blobs"] == 1


def test_different_content_gets_different_blobs(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"x" * 100)
        put(c, "b", b"y" * 100)
        store = c.app.state.store
        assert count_blob_files(store.storage_dir) == 2
        assert c.get("/stats").json()["physical_bytes"] == 200


def test_reupload_same_content_generation_independent_but_shared(tmp_path):
    with make_client(tmp_path) as c:
        data = b"gen-data"
        put(c, "a", data)
        old_token = acquire(c, "a")
        r = put(c, "a", data)  # same bytes, new generation
        assert r.status_code == 200 and r.json()["generation"] == 2

        store = c.app.state.store
        assert count_blob_files(store.storage_dir) == 1  # still one copy
        with store.lock:
            assert store.conn.execute(
                "SELECT ref_count FROM blobs WHERE sha256=?", (sha(data),)
            ).fetchone()["ref_count"] == 1

        # Old-generation lease must not read the new generation.
        assert download(c, "a", old_token).status_code == 409
        # New generation serves the shared bytes.
        assert download(c, "a", acquire(c, "a")).content == data
        meta = c.get("/artifacts/a").json()
        assert meta["generation"] == 2 and meta["shared"] is False


def test_reupload_different_content_releases_old_blob(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"version-1")
        put(c, "a", b"version-2-longer")
        store = c.app.state.store
        assert count_blob_files(store.storage_dir) == 1
        assert not os.path.exists(blob_file(store, sha(b"version-1")))
        assert os.path.exists(blob_file(store, sha(b"version-2-longer")))
        with store.lock:
            assert store.conn.execute("SELECT COUNT(*) AS c FROM blobs").fetchone()["c"] == 1
        assert download(c, "a", acquire(c, "a")).content == b"version-2-longer"


# -------------------------------------------------- shared reclaim semantics


def test_reclaim_one_shared_ref_keeps_other_downloadable_and_frees_nothing(tmp_path):
    with make_client(tmp_path) as c:
        data = b"shared " * 50
        put(c, "a", data)
        time.sleep(0.02)
        put(c, "b", data)  # b is newer; a is LRU

        # A target satisfied conceptually by one record still cannot count
        # shared bytes: dropping a's reference frees nothing, so b (the last
        # reference) is also reclaimed and only then are bytes released.
        r = c.post("/reclaim", json={"target_bytes": len(data)})
        body = r.json()
        assert body["freed_bytes"] == len(data)
        assert body["reclaimed"][0] == {
            "number": "a",
            "generation": 1,
            "size": len(data),
            "file_deleted": False,
        }
        assert body["reclaimed"][1]["file_deleted"] is True

        # Now the key guarantee with a leased third reference: deleting one
        # unoccupied shared reference must leave the occupied one intact and
        # downloadable, and report zero freed bytes.
        put(c, "a", data)
        time.sleep(0.02)
        put(c, "b", data)
        time.sleep(0.02)
        put(c, "keeper", data)
        keeper_token = acquire(c, "keeper")  # occupancy protects the blob
        r = c.post("/reclaim", json={"target_bytes": len(data)})
        body = r.json()
        assert body["freed_bytes"] == 0  # a, b removed but keeper holds the blob
        assert {item["number"] for item in body["reclaimed"]} == {"a", "b"}
        assert all(not item["file_deleted"] for item in body["reclaimed"])
        assert c.get("/artifacts/a").status_code == 404
        assert c.get("/artifacts/b").status_code == 404
        meta = c.get("/artifacts/keeper").json()
        assert meta["downloadable"] is True and meta["shared"] is False
        assert download(c, "keeper", keeper_token).content == data
        store = c.app.state.store
        assert count_blob_files(store.storage_dir) == 1
        with store.lock:
            assert store.conn.execute(
                "SELECT ref_count FROM blobs WHERE sha256=?", (sha(data),)
            ).fetchone()["ref_count"] == 1

        # Once the lease is released, the last reference reclaims real bytes.
        c.post(f"/tokens/{keeper_token}/release")
        r = c.post("/reclaim", json={"target_bytes": len(data)})
        assert r.json()["freed_bytes"] == len(data)
        assert c.get("/artifacts/keeper").status_code == 404
        assert count_blob_files(store.storage_dir) == 0


def test_reclaim_last_reference_frees_real_bytes(tmp_path):
    with make_client(tmp_path) as c:
        data = b"lonely " * 30
        put(c, "a", data)
        r = c.post("/reclaim", json={"target_bytes": len(data)})
        assert r.json()["freed_bytes"] == len(data)
        assert c.get("/artifacts/a").status_code == 404
        store = c.app.state.store
        assert count_blob_files(store.storage_dir) == 0
        with store.lock:
            assert store.conn.execute("SELECT COUNT(*) AS c FROM blobs").fetchone()["c"] == 0


def test_reclaim_shared_blob_with_occupied_reference(tmp_path):
    """LRU picks the unoccupied shared reference; the occupied one survives
    and the physical file is never deleted while any reference exists."""
    with make_client(tmp_path) as c:
        data = b"shared-occupied " * 20
        put(c, "a", data)
        time.sleep(0.02)
        put(c, "b", data)
        token_b = acquire(c, "b")

        r = c.post("/reclaim", json={"target_bytes": 10_000})
        assert r.json()["freed_bytes"] == 0
        assert c.get("/artifacts/a").status_code == 404
        assert c.get("/artifacts/b").status_code == 200
        assert download(c, "b", token_b).status_code == 200

        c.post(f"/tokens/{token_b}/release")
        r = c.post("/reclaim", json={"target_bytes": 10_000})
        assert r.json()["freed_bytes"] == len(data)
        assert c.get("/artifacts/b").status_code == 404


def test_honest_freed_bytes_when_space_short(tmp_path):
    with make_client(tmp_path) as c:
        put(c, "a", b"a" * 10)
        put(c, "b", b"b" * 20)
        r = c.post("/reclaim", json={"target_bytes": 10_000})
        assert r.json()["freed_bytes"] == 30  # everything available, honestly
        r = c.post("/reclaim", json={"target_bytes": 10_000})
        assert r.json()["freed_bytes"] == 0   # nothing left: no inflated number


# ------------------------------------------------------------- corruption


def test_corrupt_existing_file_is_not_used_as_sharing_source(tmp_path):
    with make_client(tmp_path) as c:
        data = b"important content" * 10
        put(c, "a", data)
        store = c.app.state.store
        path = blob_file(store, sha(data))
        with open(path, "r+b") as fh:
            fh.seek(0)
            fh.write(b"X")  # corrupt the only on-disk copy

        # Metadata cannot claim a healthy file; download refuses to serve it.
        assert c.get("/artifacts/a").json()["downloadable"] is False
        bad = download(c, "a", acquire(c, "a"))
        assert bad.status_code == 409

        # Re-uploading the same (verified) content repairs the damaged file;
        # the corrupt bytes never served as a sharing source.
        put(c, "b", data)
        assert count_blob_files(store.storage_dir) == 1
        with open(path, "rb") as fh:
            assert hashlib.sha256(fh.read()).hexdigest() == sha(data)
        assert download(c, "b", acquire(c, "b")).content == data
        assert download(c, "a", acquire(c, "a")).content == data


def test_missing_blob_file_repaired_by_reupload(tmp_path):
    with make_client(tmp_path) as c:
        data = b"repair-me"
        put(c, "a", data)
        store = c.app.state.store
        os.remove(blob_file(store, sha(data)))
        assert c.get("/artifacts/a").json()["downloadable"] is False

        put(c, "b", data)  # blob row exists but file missing -> rewrite
        assert os.path.isfile(blob_file(store, sha(data)))
        assert download(c, "a", acquire(c, "a")).content == data
        assert download(c, "b", acquire(c, "b")).content == data


# ---------------------------------------------------- failure / restart etc.


def test_failed_blob_deletion_retried_without_dangling_records(tmp_path, monkeypatch):
    with make_client(tmp_path) as c:
        data = b"a" * 10
        put(c, "a", data)
        store = c.app.state.store
        digest = sha(data)

        real_remove = os.remove

        def failing_remove(path, *a, **kw):
            if str(path) == store._blob_path(digest):
                raise OSError("simulated unlink failure")
            return real_remove(path, *a, **kw)

        monkeypatch.setattr("app.store.os.remove", failing_remove)
        r = c.post("/reclaim", json={"target_bytes": 10})
        assert r.json()["freed_bytes"] == 0
        assert len(r.json()["errors"]) == 1
        monkeypatch.undo()

        # No downloadable record points at the stuck content...
        assert c.get("/artifacts/a").status_code == 404
        # ...and the zero-ref blob row is retained for a later retry.
        with store.lock:
            row = store.conn.execute(
                "SELECT state, ref_count FROM blobs WHERE sha256=?", (digest,)
            ).fetchone()
        assert row["state"] == "reclaiming" and row["ref_count"] == 0

        # A re-upload of the same content resurrects a usable, ready blob.
        put(c, "a", data)
        token = acquire(c, "a")
        assert download(c, "a", token).content == data

        # The lease protects the revived artifact until released.
        assert c.post("/reclaim", json={"target_bytes": 10}).json()["freed_bytes"] == 0
        c.post(f"/tokens/{token}/release")

        # Reclaim after the obstacle clears the fresh zero-ref blob.
        c.post("/reclaim", json={"target_bytes": 10})
        with store.lock:
            assert store.conn.execute(
                "SELECT COUNT(*) AS c FROM blobs WHERE sha256=?", (digest,)
            ).fetchone()["c"] == 0


def test_restart_completes_shared_blob_reclaim(tmp_path):
    cfg = make_config(tmp_path)
    with TestClient(create_app(cfg)) as c:
        shared = b"shared bytes"
        unique = b"only on b"
        put(c, "a", shared)
        put(c, "b", shared)
        # Remove a (one shared ref, frees nothing) then point b elsewhere:
        # the shared blob is now unreferenced and physically released...
        c.post("/reclaim", json={"target_bytes": len(shared)})
        put(c, "b", unique)
        store = c.app.state.store
        # ...simulate a crash after a zero-ref blob was marked reclaiming
        # but before its file could be unlinked.
        with store.lock, store._txn() as db:
            db.execute(
                "INSERT INTO blobs(sha256, size, ref_count, state) "
                "VALUES (?,?,0,'reclaiming')",
                (sha(shared), len(shared)),
            )
        os.makedirs(os.path.dirname(store._blob_path(sha(shared))), exist_ok=True)
        with open(store._blob_path(sha(shared)), "wb") as fh:
            fh.write(shared)
        assert os.path.exists(blob_file(store, sha(shared)))

    with TestClient(create_app(cfg)) as c2:  # startup sweep runs
        store2 = c2.app.state.store
        assert not os.path.exists(blob_file(store2, sha(shared)))
        assert os.path.exists(blob_file(store2, sha(unique)))
        # Nothing downloadable ever pointed at the deleted blob.
        assert c2.get("/artifacts/a").status_code == 404
        assert download(c2, "b", acquire(c2, "b")).content == unique


def test_orphan_blob_files_swept_with_grace(tmp_path):
    with make_client(tmp_path, orphan_grace=0.2) as c:
        put(c, "a", b"data")
        store = c.app.state.store
        orphan = os.path.join(store.blobs_dir, "ff", "f" * 64)
        os.makedirs(os.path.dirname(orphan), exist_ok=True)
        with open(orphan, "wb") as fh:
            fh.write(b"orphan")

        store.sweep()  # too recent -> kept
        assert os.path.exists(orphan)
        old = time.time() - 10
        os.utime(orphan, (old, old))
        store.sweep()
        assert not os.path.exists(orphan)
        assert download(c, "a", acquire(c, "a")).status_code == 200


def test_legacy_layout_migration(tmp_path):
    """A pre-dedup database (per-upload files) migrates to the blob store."""
    cfg = make_config(tmp_path)
    os.makedirs(cfg.storage_dir, exist_ok=True)
    legacy_path = os.path.join(cfg.storage_dir, "a", "00000001-deadbeef.bin")
    os.makedirs(os.path.dirname(legacy_path), exist_ok=True)
    data = b"legacy artifact bytes"
    with open(legacy_path, "wb") as fh:
        fh.write(data)
    now = time.time()
    conn = sqlite3.connect(cfg.db_path)
    conn.executescript(
        """
        CREATE TABLE artifacts (
            number TEXT PRIMARY KEY, generation INTEGER NOT NULL,
            sha256 TEXT NOT NULL, size INTEGER NOT NULL, file_path TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'ready',
            created_at REAL NOT NULL, last_used_at REAL NOT NULL
        );
        CREATE TABLE tokens (
            token TEXT PRIMARY KEY, number TEXT NOT NULL, generation INTEGER NOT NULL,
            expires_at REAL NOT NULL, released INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO artifacts VALUES (?,?,?,?,?, 'ready', ?, ?)",
        ("a", 1, sha(data), len(data), "a/00000001-deadbeef.bin", now, now),
    )
    conn.commit()
    conn.close()

    with TestClient(create_app(cfg)) as c:
        meta = c.get("/artifacts/a").json()
        assert meta["sha256"] == sha(data) and meta["downloadable"] is True
        assert download(c, "a", acquire(c, "a")).content == data
        store = c.app.state.store
        assert os.path.isfile(store._blob_path(sha(data)))
        assert not os.path.exists(legacy_path)
        with store.lock:
            assert store.conn.execute(
                "SELECT ref_count FROM blobs WHERE sha256=?", (sha(data),)
            ).fetchone()["ref_count"] == 1


# --------------------------------------------------------- stress: uvicorn


def test_concurrent_shared_content_stress(tmp_path):
    """Mixed upload/download/reclaim traffic over a small content alphabet:
    heavy dedup, no 5xx, no dangling downloads, referenced blobs survive."""
    cfg = make_config(tmp_path, token_ttl=2.0)
    port = 8124
    base = f"http://127.0.0.1:{port}"
    server = uvicorn.Server(
        uvicorn.Config(create_app(cfg), host="127.0.0.1", port=port, log_level="error")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    payloads = [os.urandom(512) for _ in range(4)]
    errors: list[str] = []
    stop = threading.Event()
    try:
        with httpx.Client(base_url=base, timeout=15) as client:
            deadline = time.time() + 15
            while time.time() < deadline:
                try:
                    if client.get("/health").status_code == 200:
                        break
                except httpx.TransportError:
                    time.sleep(0.05)
            r = client.put(
                "/artifacts/pinned", content=payloads[0],
                params={"sha256": sha(payloads[0])},
            )
            assert r.status_code == 200
            pinned_token = client.post("/artifacts/pinned/tokens").json()["token"]

        def keepalive():
            with httpx.Client(base_url=base, timeout=15) as client:
                while not stop.is_set():
                    if client.post(f"/tokens/{pinned_token}/renew").status_code != 200:
                        errors.append("pinned renew failed")
                        return
                    stop.wait(0.4)

        keeper = threading.Thread(target=keepalive, daemon=True)
        keeper.start()

        import random

        def worker(wid):
            rng = random.Random(wid)
            with httpx.Client(base_url=base, timeout=15) as client:
                for _ in range(50):
                    number = f"n{rng.randint(1, 6)}"
                    op = rng.choice(["upload", "download", "lease", "reclaim"])
                    try:
                        if op == "upload":
                            data = rng.choice(payloads)  # repeated content
                            r = client.put(
                                f"/artifacts/{number}", content=data,
                                params={"sha256": sha(data)},
                            )
                        elif op == "download":
                            r = client.post(f"/artifacts/{number}/tokens")
                            if r.status_code == 201:
                                tok = r.json()["token"]
                                r = client.get(
                                    f"/artifacts/{number}/content",
                                    params={"token": tok},
                                )
                                if r.status_code == 200:
                                    assert sha(r.content) == r.headers["x-sha256"]
                        elif op == "lease":
                            r = client.post(f"/artifacts/{number}/tokens")
                            if r.status_code == 201:
                                tok = r.json()["token"]
                                client.post(f"/tokens/{tok}/renew")
                                r = client.post(f"/tokens/{tok}/release")
                        else:
                            r = client.post(
                                "/reclaim", json={"target_bytes": rng.randint(0, 2048)}
                            )
                        if r.status_code >= 500:
                            errors.append(f"w{wid} {op}: {r.status_code} {r.text[:150]}")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"w{wid} {op}: {exc!r}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        stop.set()
        keeper.join()
        assert not errors, "\n".join(errors[:20])

        with httpx.Client(base_url=base, timeout=15) as client:
            # Pinned, continuously-occupied content survived.
            r = client.get("/artifacts/pinned/content", params={"token": pinned_token})
            assert r.status_code == 200 and r.content == payloads[0]

            # Every downloadable record really serves the right bytes, and
            # every referenced blob file exists exactly once on disk.
            listing = client.get("/artifacts").json()
            for meta in listing:
                if meta["downloadable"]:
                    tok = client.post(
                        f"/artifacts/{meta['number']}/tokens"
                    ).json()["token"]
                    r = client.get(
                        f"/artifacts/{meta['number']}/content", params={"token": tok}
                    )
                    assert r.status_code == 200
                    assert sha(r.content) == meta["sha256"]
            stats = client.get("/stats").json()
            assert stats["physical_bytes"] <= stats["bytes"]
            seen: set[str] = set()
            for root, _, files in os.walk(os.path.join(cfg.storage_dir, "blobs")):
                for name in files:
                    assert name not in seen
                    seen.add(name)
            assert len(seen) == stats["blobs"]
    finally:
        stop.set()
        server.should_exit = True
        thread.join(timeout=15)
