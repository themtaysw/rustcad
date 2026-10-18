CREATE TABLE IF NOT EXISTS telemetry (
    device_id   text             NOT NULL,
    seq         bigint           NOT NULL,
    ts          timestamptz      NOT NULL,
    signal      text             NOT NULL,
    value       double precision NOT NULL,
    received_at timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, seq)
);

CREATE INDEX IF NOT EXISTS telemetry_latest_idx ON telemetry (device_id, signal, ts DESC);

CREATE TABLE IF NOT EXISTS batches (
    device_id       text        NOT NULL,
    batch_id        text        NOT NULL,
    record_count    integer     NOT NULL,
    inserted_count  integer     NOT NULL,
    dropped_records bigint      NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, batch_id)
);
