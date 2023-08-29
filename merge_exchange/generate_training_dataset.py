# feature set: price(daily), volume_24h, marketcap, open_time
# token_symbol
# token_name
# price_usd
# price_btc
# price_eth
# volume_all_24h
# volume_24h
# exchange
# ema / sma
# price_usd_pct_1d / 3d / 7d / 30d / 90d / 1y
# price_btc_pct_1d / 3d / 7d / 30d / 90d / 1y
# price_eth_pct_1d / 3d / 7d / 30d / 90d / 1y

# get raw data from gateio, gecko.
# symbol, name, price_usd, volume_24h, exchange(binance/gateio), datetime, marketcap_usd, marketcap_btc.
# given a symbol list that marketcap reached 100million.
# generate volatility, momentum,

from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset", echo=True
)

# get all gateio coin prices/volume_24h.
# generate pct_change_1d, 3d, 7d, 30d, 90d, 1y.
# generate volatility 30d, 90d
# generate momentum following greedy fear.
sql = """
WITH all_gateio_coins AS (
    SELECT * 
    FROM blockchain_overview.exchange_coins 
    WHERE exists_in_gateio = true
), 
gateio_coins AS (
    SELECT distinct gecko_id, UPPER(symbol) AS symbol -- , market_cap_usd 
    FROM blockchain_overview.gecko_markets 
    WHERE market_cap_usd >= 1e7 
    AND gecko_id IN (
        SELECT gecko_id 
        FROM all_gateio_coins
    ) 
    AND date_trunc('day', datetime) = '2023-06-01'
    AND symbol not like '%%usd%%'
    and symbol not in ('dai')
), 

raw_gateio_data AS (
    SELECT DATE(datetime) AS date, REPLACE(symbol, '_USDT', '') AS symbol, MIN(DATE(datetime)) OVER(PARTITION BY symbol) AS first_appear_date, SUM(volume) AS volume_24h, AVG(open) AS price 
    FROM blockchain_overview.gateio_spot_candlesticks_5m 
    WHERE symbol IN (
        SELECT concat(symbol, '_USDT') AS symbol 
        FROM gateio_coins
    ) 
    GROUP BY DATE(datetime), symbol
),

log_returns AS ( 
    SELECT  
        DATE(datetime) AS date, 
        REPLACE(symbol, '_USDT', '') AS symbol, 
        log(open / LAG(open) OVER (PARTITION BY symbol ORDER BY datetime)) as log_return
    FROM 
        blockchain_overview.gateio_spot_candlesticks_5m 
    WHERE 
        symbol IN (
            SELECT 
                CONCAT(symbol, '_USDT') AS symbol 
            FROM 
                gateio_coins
        ) 
),
volatility AS (
    SELECT  
        date,
        symbol, 
        stddev(log_return) AS daily_volatility
    FROM 
        log_returns
    GROUP BY 
        date, 
        symbol
),

gateio_data AS ( 
    SELECT A.*, B.gecko_id 
    FROM raw_gateio_data A 
    LEFT JOIN gateio_coins B ON A.symbol = B.symbol 
), 

gecko_data AS (
    SELECT DATE(datetime) AS date, gecko_id, UPPER(symbol) as symbol, AVG(price_usd) AS price_usd, AVG(market_cap_usd) AS market_cap_usd 
    FROM blockchain_overview.gecko_markets 
    WHERE gecko_id IN (
        SELECT gecko_id 
        FROM gateio_coins
    ) 
    GROUP BY DATE(datetime), gecko_id, symbol
),

final AS (
    SELECT  
        A.date, 
        A.first_appear_date, 
        A.symbol, 
        A.gecko_id, 
        A.price AS price_usd, 
        A.volume_24h AS volume_usd_24h, 
        B.market_cap_usd,
        C.daily_volatility
    FROM 
        gateio_data A 
        LEFT JOIN gecko_data B ON A.gecko_id = B.gecko_id AND A.date = B.date
        LEFT JOIN volatility C ON A.symbol = C.symbol AND A.date = C.date
    WHERE 
        A.date >= '2018-01-01' 
        AND A.first_appear_date <= '2020-01-01'  
)

SELECT *
FROM final
ORDER BY gecko_id, date DESC
"""
