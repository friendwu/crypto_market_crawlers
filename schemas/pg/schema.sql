CREATE TABLE IF NOT EXISTS blockchain_overview.gecko_markets (
    id BIGSERIAL NOT NULL,
    gecko_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,

    price_usd NUMERIC,
    price_btc NUMERIC,
    price_eth NUMERIC,
    price_bnb NUMERIC,

    market_cap_usd NUMERIC,
    market_cap_btc NUMERIC,
    market_cap_eth NUMERIC,
    market_cap_bnb NUMERIC,

    volume_24h_usd NUMERIC,
    volume_24h_btc NUMERIC,
    volume_24h_eth NUMERIC,

    twitter_followers BIGINT,
    reddit_average_posts_48h NUMERIC,
    reddit_subscribers BIGINT,
    reddit_accounts_active_48h NUMERIC,

    forks BIGINT,
    stars BIGINT,
    subscribers BIGINT,
    total_issues BIGINT,
    closed_issues BIGINT,
    pull_requests_merged BIGINT,
    pull_request_contributors BIGINT,
    commit_count_4_weeks BIGINT,
    PRIMARY KEY (gecko_id, timestamp)
);

CREATE TABLE IF NOT EXISTS blockchain_overview.binance_um_funding_rates (
    id BIGSERIAL NOT NULL,
    symbol TEXT NOT NULL,
    calc_timestamp BIGINT NOT NULL,
    calc_datetime TIMESTAMPTZ NOT NULL,
    funding_interval_hours INT NOT NULL,
    last_funding_rate NUMERIC NOT NULL,

    PRIMARY KEY (symbol, calc_datetime)
);


CREATE TABLE IF NOT EXISTS blockchain_overview.binance_um_metrics (
    create_time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    sum_open_interest NUMERIC,
    sum_open_interest_value NUMERIC,
    count_toptrader_long_short_ratio NUMERIC,
    sum_toptrader_long_short_ratio NUMERIC,
    count_long_short_ratio NUMERIC,
    sum_taker_long_short_vol_ratio NUMERIC,

    PRIMARY KEY (symbol, create_time)
);

CREATE TABLE IF NOT EXISTS blockchain_overview.gateio_um_funding_applies (
    id BIGSERIAL NOT NULL,
    symbol TEXT NOT NULL,
    timestamp BIGINT NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    funding_rate NUMERIC,

    PRIMARY KEY (symbol, datetime)
);
