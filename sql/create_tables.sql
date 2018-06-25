CREATE TYPE transaction_status AS ENUM ('new', 'queued', 'unconfirmed', 'confirmed', 'error');

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL PRIMARY KEY,

    hash VARCHAR NOT NULL,

    from_address VARCHAR NOT NULL,
    to_address VARCHAR NOT NULL,

    nonce BIGINT NOT NULL,

    value VARCHAR NOT NULL,
    gas VARCHAR,
    gas_price VARCHAR,

    data VARCHAR,
    v VARCHAR,
    r VARCHAR,
    s VARCHAR,

    created TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc'),

    -- the last seen status, used to know if PNs should be
    -- sent or not
    status transaction_status DEFAULT 'new',
    -- if confirmed, the block number that this tx is part of
    blocknumber BIGINT,
    error INTEGER,

    -- optional token identifier for the sender
    sender_toshi_id VARCHAR
);

CREATE TABLE blocks (
    blocknumber BIGINT PRIMARY KEY,
    timestamp BIGINT NOT NULL,
    hash VARCHAR NOT NULL,
    parent_hash VARCHAR NOT NULL,
    stale BOOLEAN NOT NULL DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS notification_registrations (
    toshi_id VARCHAR,
    service VARCHAR,
    registration_id VARCHAR,
    eth_address VARCHAR,

    PRIMARY KEY(toshi_id, service, registration_id, eth_address)
);

CREATE TABLE IF NOT EXISTS filter_registrations (
    filter_id VARCHAR,
    registration_id VARCHAR,
    contract_address VARCHAR,
    topic_id VARCHAR,
    topic VARCHAR,

    PRIMARY KEY (filter_id),
    UNIQUE (registration_id, contract_address, topic_id)
);

CREATE TABLE IF NOT EXISTS last_blocknumber (
    blocknumber INTEGER
);

CREATE TABLE IF NOT EXISTS tokens (
    contract_address VARCHAR PRIMARY KEY, -- contract address
    symbol VARCHAR, -- currency symbol
    name VARCHAR, -- verbose name
    decimals INTEGER, -- currency decimal points
    icon BYTEA, -- png data
    hash VARCHAR,
    format VARCHAR,
    ready BOOLEAN DEFAULT FALSE,
    custom BOOLEAN DEFAULT FALSE,
    last_modified TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS token_balances (
    contract_address VARCHAR,
    eth_address VARCHAR,
    balance VARCHAR,

    -- used by custom tokens
    name VARCHAR,
    symbol VARCHAR,
    decimals INTEGER,

    -- blocknumber in which this was last updated
    blocknumber INTEGER DEFAULT 0 NOT NULL,

    visibility INTEGER DEFAULT 1, -- whether to show or hide the token from the balance list
                                  -- 0: never show
                                  -- 1: show if balance is > 0
                                  -- 2: show always

    PRIMARY KEY (contract_address, eth_address)
);

CREATE TABLE IF NOT EXISTS token_transactions (
    transaction_id BIGSERIAL,
    transaction_log_index INTEGER NOT NULL,
    contract_address VARCHAR NOT NULL,
    from_address VARCHAR NOT NULL,
    to_address VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    status VARCHAR,

    PRIMARY KEY (transaction_id, transaction_log_index)
);

CREATE TABLE IF NOT EXISTS token_registrations (
    eth_address VARCHAR PRIMARY KEY,
    last_queried TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE IF NOT EXISTS collectibles (
    contract_address VARCHAR PRIMARY KEY,
    name VARCHAR,
    icon VARCHAR,
    url VARCHAR,
    image_url_format_string VARCHAR,
    type INTEGER DEFAULT 721,  -- valid types:
                               -- 721: follows erc721 spec perfectly
                               -- 1: erc721 but requiring some special treatment
                               -- 2: fungible collectible
                               -- 0: very special collectible
    last_block INTEGER DEFAULT 0,
    -- whether or not the collectible has been initialised and is ready to be shown to users
    ready BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS collectible_transfer_events (
    collectible_transfer_event_id SERIAL PRIMARY KEY,
    collectible_address VARCHAR,
    contract_address VARCHAR,
    name VARCHAR DEFAULT 'Transfer',
    topic_hash VARCHAR DEFAULT '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
    arguments VARCHAR[] DEFAULT ARRAY['address','address','uint256'],
    indexed_arguments BOOLEAN[] DEFAULT ARRAY[TRUE, TRUE, FALSE],
    to_address_offset INTEGER DEFAULT 1,
    token_id_offset INTEGER DEFAULT 2
);

CREATE TABLE IF NOT EXISTS collectible_tokens (
    contract_address VARCHAR,
    token_id VARCHAR,
    owner_address VARCHAR,
    token_uri VARCHAR,
    name VARCHAR,
    image VARCHAR,
    description VARCHAR,

    PRIMARY KEY(contract_address, token_id)
);

CREATE TABLE IF NOT EXISTS fungible_collectibles (
    contract_address VARCHAR PRIMARY KEY,
    collectible_address VARCHAR,
    name VARCHAR,
    token_uri VARCHAR,
    creator_address VARCHAR,
    image VARCHAR,
    description VARCHAR,
    last_block INTEGER DEFAULT 0,
    ready BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS fungible_collectible_balances (
    contract_address VARCHAR,
    owner_address VARCHAR,
    balance VARCHAR,

    PRIMARY KEY (contract_address, owner_address)
);

CREATE TABLE IF NOT EXISTS from_address_gas_price_whitelist (
    address VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS to_address_gas_price_whitelist (
    address VARCHAR PRIMARY KEY
);

CREATE INDEX IF NOT EXISTS idx_transactions_hash ON transactions (hash);
CREATE INDEX IF NOT EXISTS idx_transactions_hash_by_id_sorted ON transactions (hash, transaction_id DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_from_address_updated ON transactions (from_address, updated NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address_updated ON transactions (to_address, updated NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_transactions_from_address_nonce ON transactions (from_address, nonce DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_from_address_status_blocknumber_desc ON transactions (from_address, status, blocknumber DESC NULLS FIRST);
CREATE INDEX IF NOT EXISTS idx_transactions_to_address_status_blocknumber_desc ON transactions (to_address, status, blocknumber DESC NULLS FIRST);

CREATE INDEX IF NOT EXISTS idx_notification_registrations_eth_address ON notification_registrations (eth_address);

CREATE INDEX IF NOT EXISTS idx_transactions_status_v_created ON transactions (status NULLS FIRST, v NULLS LAST, created DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions (created);

CREATE INDEX IF NOT EXISTS idx_filter_registrations_contract_address_topic ON filter_registrations (contract_address, topic_id);
CREATE INDEX IF NOT EXISTS idx_filter_registrations_filter_id_registration_id ON filter_registrations (filter_id, registration_id);

CREATE INDEX IF NOT EXISTS idx_tokens_contract_address ON tokens (contract_address);
CREATE INDEX IF NOT EXISTS idx_token_registrations_last_queried ON token_registrations (last_queried ASC);

CREATE INDEX IF NOT EXISTS idx_token_balance_eth_address ON token_balances (eth_address);
CREATE INDEX IF NOT EXISTS idx_token_balance_eth_address_contract_address ON token_balances (eth_address, contract_address);
CREATE INDEX IF NOT EXISTS idx_token_balance_eth_address_visibility_balance ON token_balances (eth_address, visibility, balance);

CREATE INDEX IF NOT EXISTS idx_collectible_transfer_events_collectible_address ON collectible_transfer_events (collectible_address);

CREATE INDEX IF NOT EXISTS idx_block_blocknumber_desc ON blocks (blocknumber DESC);
CREATE INDEX IF NOT EXISTS idx_block_hash ON blocks (hash);
CREATE INDEX IF NOT EXISTS idx_block_parent_hash ON blocks (parent_hash);

UPDATE database_version SET version_number = 23;
