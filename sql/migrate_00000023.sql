CREATE TABLE blocks (
    blocknumber BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    hash VARCHAR NOT NULL,
    parent_hash VARCHAR NOT NULL,
    stale BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS collectible_transfer_logs (
    blocknumber BIGINT NOT NULL,
    log_index INT NOT NULL,
    transaction_hash VARCHAR NOT NULL,
    transaction_log_index INTEGER NOT NULL,

    contract_address VARCHAR NOT NULL,
    token_id VARCHAR NOT NULL,
    from_address VARCHAR NOT NULL,
    to_address VARCHAR NOT NULL,

    PRIMARY KEY (blocknumber, log_index)
);

CREATE INDEX IF NOT EXISTS idx_collectible_transfer_logs_transaction_hash ON collectible_transfer_logs (transaction_hash);

CREATE INDEX IF NOT EXISTS idx_block_blocknumber_desc ON blocks (blocknumber DESC);
CREATE INDEX IF NOT EXISTS idx_block_hash ON blocks (hash);
CREATE INDEX IF NOT EXISTS idx_block_parent_hash ON blocks (parent_hash);
