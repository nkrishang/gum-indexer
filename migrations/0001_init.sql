-- gum-indexer schema. Amounts are uint256 stored as NUMERIC(78,0); addresses and hashes are raw bytes.

-- One row per chain. `confirmed_block` is the durable cursor: every block <= it has been swept authoritatively.
-- The row doubles as the lock that orders watch registration against sweeps (FOR SHARE vs FOR UPDATE).
CREATE TABLE chain_cursors (
    chain_id        BIGINT PRIMARY KEY,
    confirmed_block BIGINT      NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE watches (
    id               UUID PRIMARY KEY,
    -- Monotonic load order. Assigned while holding the chain cursor row FOR SHARE, so a sweep holding it
    -- FOR UPDATE observes a gap-free prefix when it delta-loads `seq > last_seen`.
    seq              BIGSERIAL      NOT NULL UNIQUE,
    chain_id         BIGINT         NOT NULL,
    token_address    BYTEA          NOT NULL CHECK (octet_length(token_address) = 20),
    payment_address  BYTEA          NOT NULL CHECK (octet_length(payment_address) = 20),
    threshold        NUMERIC(78, 0) NOT NULL CHECK (threshold > 0),
    confirmed_amount NUMERIC(78, 0) NOT NULL DEFAULT 0,
    webhook_url      TEXT           NOT NULL,
    status           TEXT           NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'completed', 'cancelled', 'expired')),
    -- Transfers in blocks < start_block never count toward this watch.
    start_block      BIGINT         NOT NULL,
    -- Per-watch webhook event sequence; gives consumers an ordering and the dispatcher a delivery order.
    next_event_seq   BIGINT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ    NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    expires_at       TIMESTAMPTZ
);

-- At most one active watch per (chain, token, address): makes registration naturally idempotent and
-- guarantees a Transfer log maps to at most one watch.
CREATE UNIQUE INDEX watches_active_target ON watches (chain_id, token_address, payment_address)
    WHERE status = 'active';
-- Boot hydration and in-sweep delta loads.
CREATE INDEX watches_active_seq ON watches (chain_id, seq) WHERE status = 'active';
-- Sweep lower bound: min(start_block) over active watches.
CREATE INDEX watches_active_start ON watches (chain_id, start_block) WHERE status = 'active';
CREATE INDEX watches_expiry ON watches (expires_at) WHERE status = 'active' AND expires_at IS NOT NULL;

CREATE TABLE transfers (
    id           BIGSERIAL PRIMARY KEY,
    chain_id     BIGINT         NOT NULL,
    block_hash   BYTEA          NOT NULL CHECK (octet_length(block_hash) = 32),
    log_index    BIGINT         NOT NULL,
    block_number BIGINT         NOT NULL,
    tx_hash      BYTEA          NOT NULL CHECK (octet_length(tx_hash) = 32),
    watch_id     UUID           NOT NULL REFERENCES watches (id),
    from_address BYTEA          NOT NULL,
    amount       NUMERIC(78, 0) NOT NULL,
    -- pending: seen at head. confirmed: canonical at confirmation depth, counted.
    -- orphaned: was pending, not canonical at depth. ignored: canonical, but the watch was no longer active.
    status       TEXT           NOT NULL CHECK (status IN ('pending', 'confirmed', 'orphaned', 'ignored')),
    seen_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    confirmed_at TIMESTAMPTZ,
    -- Idempotency key. block_hash (not tx_hash) so a transaction re-included by a reorg is a distinct row.
    UNIQUE (chain_id, block_hash, log_index)
);

CREATE INDEX transfers_pending ON transfers (chain_id, block_number) WHERE status = 'pending';
CREATE INDEX transfers_watch ON transfers (watch_id, block_number, log_index);

-- Transactional outbox: rows are written in the same transaction as the state change they describe.
CREATE TABLE webhook_outbox (
    id              UUID PRIMARY KEY,           -- event id; consumers dedupe on it
    watch_id        UUID        NOT NULL REFERENCES watches (id),
    seq             BIGINT      NOT NULL,       -- per-watch order
    event_type      TEXT        NOT NULL,
    url             TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'dead')),
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status     INT,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ,
    UNIQUE (watch_id, seq)
);

CREATE INDEX outbox_due ON webhook_outbox (next_attempt_at) WHERE status = 'pending';
CREATE INDEX outbox_watch_pending ON webhook_outbox (watch_id, seq) WHERE status = 'pending';

-- Durable analytics (Prometheus counters reset on restart; these do not). Written only by sweeps, once per
-- chunk: the head path never touches these shared rows, so it cannot queue behind or deadlock with a sweep.
CREATE TABLE stats_rollup (
    chain_id         BIGINT         NOT NULL,
    token_address    BYTEA          NOT NULL,
    confirmed_count  BIGINT         NOT NULL DEFAULT 0,
    orphaned_count   BIGINT         NOT NULL DEFAULT 0,
    confirmed_volume NUMERIC(78, 0) NOT NULL DEFAULT 0,
    thresholds_reached BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, token_address)
);
