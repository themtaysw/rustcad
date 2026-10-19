import pytest

from receiver.app import create_app


def record(seq, signal="engine_speed_rpm", value=1500.0, ts="2026-10-18T10:00:00Z"):
    return {"seq": seq, "ts": ts, "signal": signal, "value": value}


def batch(*records, batch_id="b1", device_id="tractor-1", outbox_id="outbox-a", **extra):
    return {
        "device_id": device_id,
        "outbox_id": outbox_id,
        "batch_id": batch_id,
        "records": list(records),
        **extra,
    }


def count(db, device_id="tractor-1"):
    return db.execute(
        "SELECT count(*) FROM telemetry WHERE device_id = %s", (device_id,)
    ).fetchone()[0]


def test_health_needs_no_token(client):
    del client.headers["Authorization"]
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("header", [None, "Bearer wrong", "Basic dGVzdA=="])
def test_ingest_rejects_missing_or_wrong_token(client, db, header):
    if header is None:
        del client.headers["Authorization"]
    else:
        client.headers["Authorization"] = header
    response = client.post("/ingest", json=batch(record(1)))
    assert response.status_code == 401
    assert count(db) == 0


def test_ingest_and_dedupe(client, db):
    first = batch(record(1), record(2), record(3))
    response = client.post("/ingest", json=first)
    assert response.status_code == 200
    assert response.json() == {"accepted": 3, "inserted": 3, "duplicates": 0}

    # Same batch again, as after a lost response: nothing new is stored.
    response = client.post("/ingest", json=first)
    assert response.json() == {"accepted": 3, "inserted": 0, "duplicates": 3}

    overlapping = batch(record(3), record(4), batch_id="b2")
    response = client.post("/ingest", json=overlapping)
    assert response.json() == {"accepted": 2, "inserted": 1, "duplicates": 1}

    assert count(db) == 4


def test_seq_is_per_device(client, db):
    client.post("/ingest", json=batch(record(1), device_id="tractor-1"))
    client.post("/ingest", json=batch(record(1), device_id="tractor-2"))
    assert count(db, "tractor-1") == 1
    assert count(db, "tractor-2") == 1


def test_seq_is_per_outbox(client, db):
    # A device that got a fresh outbox starts at seq 1 again; those records are new.
    old = client.post("/ingest", json=batch(record(1), record(2), outbox_id="outbox-a"))
    new = client.post("/ingest", json=batch(record(1), record(2), outbox_id="outbox-b"))
    assert old.json() == {"accepted": 2, "inserted": 2, "duplicates": 0}
    assert new.json() == {"accepted": 2, "inserted": 2, "duplicates": 0}
    assert count(db) == 4

    rows = db.execute("SELECT outbox_id, batch_id FROM batches ORDER BY outbox_id").fetchall()
    assert rows == [("outbox-a", "b1"), ("outbox-b", "b1")]


def test_latest_spans_outboxes(client):
    client.post(
        "/ingest",
        json=batch(record(5, value=800.0, ts="2026-10-18T10:00:00Z"), outbox_id="outbox-a"),
    )
    client.post(
        "/ingest",
        json=batch(record(1, value=1900.0, ts="2026-10-18T11:00:00Z"), outbox_id="outbox-b"),
    )
    body = client.get("/devices/tractor-1/latest").json()
    assert body["signals"]["engine_speed_rpm"] == {
        "value": 1900.0,
        "ts": "2026-10-18T11:00:00Z",
        "seq": 1,
    }


def test_duplicate_seq_inside_one_batch(client, db):
    response = client.post("/ingest", json=batch(record(7), record(7)))
    assert response.json() == {"accepted": 2, "inserted": 1, "duplicates": 1}
    assert count(db) == 1


def test_dropped_records_are_stored_once_per_batch(client, db):
    payload = batch(record(1), dropped_records=42)
    client.post("/ingest", json=payload)
    client.post("/ingest", json=payload)
    rows = db.execute("SELECT batch_id, dropped_records FROM batches").fetchall()
    assert rows == [("b1", 42)]


def test_empty_batch_is_accepted(client):
    response = client.post("/ingest", json=batch(dropped_records=5))
    assert response.json() == {"accepted": 0, "inserted": 0, "duplicates": 0}


@pytest.mark.parametrize(
    "bad",
    [
        batch(record(-1)),
        batch(record(1, ts="2026-10-18T10:00:00")),
        batch(record(1, signal="Engine Speed")),
        batch(record(1), device_id=""),
        batch(record(1), device_id="a/b"),
        batch(record(1), batch_id=""),
        batch(record(1), outbox_id=""),
        batch(record(1), outbox_id="../x"),
        batch(record(1), outbox_id=""),
        batch(record(1), outbox_id="x" * 65),
        {"device_id": "tractor-1", "outbox_id": "o", "records": [record(1)]},
        {"device_id": "tractor-1", "batch_id": "b", "records": [record(1)]},
    ],
)
def test_rejects_invalid_batches(client, db, bad):
    assert client.post("/ingest", json=bad).status_code == 422
    assert count(db) == 0


def test_rejects_non_finite_values(client, db):
    # json= refuses to encode NaN, so the body is written by hand. Python's json accepts it.
    body = (
        '{"device_id": "t", "outbox_id": "o", "batch_id": "b", "records": '
        '[{"seq": 1, "ts": "2026-10-18T10:00:00Z", "signal": "x", "value": NaN}]}'
    )
    response = client.post("/ingest", content=body, headers={"Content-Type": "application/json"})
    assert response.status_code == 422
    assert count(db, "t") == 0


def test_latest_returns_newest_value_per_signal(client):
    client.post(
        "/ingest",
        json=batch(
            record(1, "engine_speed_rpm", 800.0, "2026-10-18T10:00:00Z"),
            record(2, "engine_speed_rpm", 2000.0, "2026-10-18T10:00:05Z"),
            record(3, "coolant_temp_c", 85.0, "2026-10-18T10:00:01Z"),
            # Older timestamp with a higher seq must not win.
            record(4, "engine_speed_rpm", 1200.0, "2026-10-18T10:00:02Z"),
        ),
    )
    response = client.get("/devices/tractor-1/latest")
    assert response.status_code == 200
    body = response.json()
    assert body["device_id"] == "tractor-1"
    assert body["signals"]["engine_speed_rpm"]["value"] == 2000.0
    assert body["signals"]["engine_speed_rpm"]["seq"] == 2
    assert body["signals"]["coolant_temp_c"]["value"] == 85.0


def test_latest_unknown_device_is_404(client):
    assert client.get("/devices/nobody/latest").status_code == 404


def test_latest_needs_token(client):
    del client.headers["Authorization"]
    assert client.get("/devices/tractor-1/latest").status_code == 401


def test_empty_token_is_refused_at_startup(database_url):
    with pytest.raises(RuntimeError, match="RECEIVER_TOKEN"):
        create_app(database_url=database_url, token="  ")
