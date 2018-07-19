CREATE TABLE gas_price_history (
    timestamp BIGINT PRIMARY KEY,
    blocknumber BIGINT NOT NULL,
    gas_station_fast VARCHAR,
    gas_station_standard VARCHAR,
    gas_station_safelow VARCHAR,
    eth_gasprice VARCHAR
);
