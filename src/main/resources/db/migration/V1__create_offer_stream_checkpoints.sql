CREATE TABLE offer_market_state (
    id           TEXT PRIMARY KEY,
    payload      JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_offer_market_payload
        CHECK (
            jsonb_typeof(payload) = 'object'
            AND payload ? 'id'
            AND jsonb_typeof(payload -> 'id') = 'string'
            AND payload ->> 'id' = id
        )
);

-- Root Event aggregates, including EventMarket and EventMarketOutcome nested data.
CREATE TABLE offer_event_state (
    id           TEXT PRIMARY KEY,
    payload      JSONB NOT NULL,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_offer_event_payload
        CHECK (
            jsonb_typeof(payload) = 'object'
            AND payload ? 'id'
            AND jsonb_typeof(payload -> 'id') = 'string'
            AND payload ->> 'id' = id
        )
);

-- A completed checkpoint exists independently for each logical Kafka stream.
-- A missing row means the stream has NEVER been checkpointed, not an empty stream.
CREATE TABLE offer_checkpoint (
    topic             TEXT PRIMARY KEY,
    generation        BIGINT NOT NULL,
    completed_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_offer_checkpoint_topic CHECK (topic IN ('market', 'event')),
    CONSTRAINT chk_offer_checkpoint_generation CHECK (generation >= 1)
);

-- The next record to replay (NOT the last applied record).
-- One row for market partition 0, and one per event partition (0, 1, ...).
-- Rows for one topic are committed atomically WITH that topic's state changes
-- and offer_checkpoint row in one PostgreSQL transaction.
CREATE TABLE offer_checkpoint_offset (
    topic           TEXT NOT NULL,
    partition_id    INTEGER NOT NULL,
    next_offset     BIGINT NOT NULL,

    CONSTRAINT pk_offer_checkpoint_offset
        PRIMARY KEY (topic, partition_id),

    CONSTRAINT fk_offer_checkpoint_offset_checkpoint
        FOREIGN KEY (topic)
        REFERENCES offer_checkpoint(topic)
        ON DELETE CASCADE,

    CONSTRAINT chk_offer_checkpoint_partition
        CHECK (partition_id >= 0),

    CONSTRAINT chk_offer_checkpoint_next_offset
        CHECK (next_offset >= 0)
);