from sqlalchemy import create_engine
import json


def get_gecko_coins(engine):
    coins = engine.execute(
        """SELECT gecko_id, symbol, datetime, price_usd, marketcap_usd FROM ( 
            SELECT 
                gecko_id, UPPER(symbol) AS symbol, datetime, price_usd, market_cap_usd as marketcap_usd, 
                ROW_NUMBER() OVER (PARTITION BY gecko_id ORDER BY datetime DESC) as rn 
            FROM blockchain_overview.gecko_markets where DATE_TRUNC('hour', datetime) = '2023-06-01' and market_cap_usd >= 100000
            ) AS t WHERE rn = 1"""
    ).fetchall()

    gecko_coins = {}
    for coin in coins:
        if gecko_coins.get(coin["symbol"]):
            print(
                "duplicate symbol: ",
                coin["symbol"],
                coin["gecko_id"],
                coin["marketcap_usd"] / 10000,
                "old: ",
                gecko_coins[coin["symbol"]]["gecko_id"],
                gecko_coins[coin["symbol"]]["marketcap_usd"] / 10000,
            )

        if (
            not gecko_coins.get(coin["symbol"])
            or coin["marketcap_usd"] > gecko_coins[coin["symbol"]]["marketcap_usd"]
        ):
            gecko_coins[coin["symbol"]] = {
                "price_usd": float(coin["price_usd"]),
                "datetime": coin["datetime"],
                "symbol": coin["symbol"],
                "gecko_id": coin["gecko_id"],
                "marketcap_usd": coin["marketcap_usd"],
            }
        # gecko_coins[coin["symbol"]].append(
        #     {
        #         "price_usd": coin["price_usd"],
        #         "datetime": coin["datetime"],
        #         "symbol": coin["symbol"],
        #         "gecko_id": coin["gecko_id"],
        #         "marketcap_usd": coin["marketcap_usd"],
        #     }
        # )

    return gecko_coins


def get_binance_coins(engine):
    coins = engine.execute(
        """SELECT symbol, datetime, price_usd FROM (
                SELECT 
                    REPLACE(pair, 'USDT', '') AS symbol, open_datetime as datetime, 
                    open as price_usd, ROW_NUMBER() OVER (PARTITION BY pair ORDER BY open_datetime DESC) as rn 
                FROM 
                    blockchain_overview.binance_spot_klines_5m 
                WHERE 
                    DATE_TRUNC('hour', open_datetime) = '2023-06-01' and pair like '%%USDT') AS t WHERE rn = 1"""
    ).fetchall()

    binance_coins = {}
    for coin in coins:
        binance_coins[coin[0]] = {
            "price_usd": float(coin["price_usd"]),
            "datetime": coin["datetime"],
            "symbol": coin["symbol"],
        }

    return binance_coins


def get_gateio_coins(engine):
    coins = engine.execute(
        """
            SELECT symbol, datetime, price_usd FROM (
                SELECT 
                    REPLACE(symbol, '_USDT', '') AS symbol, datetime, 
                    open as price_usd, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY datetime DESC) as rn 
                FROM 
                    blockchain_overview.gateio_spot_candlesticks_5m 
                WHERE 
                    DATE_TRUNC('hour', datetime) = '2023-06-01' and symbol like '%%_USDT') AS t WHERE rn = 1
        """
    ).fetchall()

    gateio_coins = {}
    for coin in coins:
        gateio_coins[coin[0]] = {
            "price_usd": float(coin["price_usd"]),
            "datetime": coin["datetime"],
            "symbol": coin["symbol"],
        }

    return gateio_coins


engine = create_engine(
    "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset", echo=True
)
# gecko_coins = get_gecko_coins(engine)
# # print(gecko_coins)

# binance_coins = get_binance_coins(engine)
# # print("binance ", binance_coins)

# gateio_coins = get_gateio_coins(engine)
# # print("gateio ", binance_coins)

# merged_coins = {}
# for symbol in gecko_coins:
#     merged_coins[symbol] = {
#         "gecko_id": gecko_coins[symbol]["gecko_id"],
#         "symbol": gecko_coins[symbol]["symbol"],
#         "gecko_price_usd": float(gecko_coins[symbol]["price_usd"]),
#         "gecko_marketcap_usd": float(gecko_coins[symbol]["marketcap_usd"]),
#         "binance_price_usd": None,
#         "gateio_price_usd": None,
#     }

#     if binance_coins.get(symbol):
#         merged_coins[symbol]["binance_price_usd"] = float(
#             binance_coins[symbol]["price_usd"]
#         )

#     if gateio_coins.get(symbol):
#         merged_coins[symbol]["gateio_price_usd"] = float(
#             gateio_coins[symbol]["price_usd"]
#         )

# print(merged_coins)
# with open("merge_coins.json", "w") as f:
#     json.dump(merged_coins, f)

with open("merge_coins.json", "r") as f:
    merged_coins = json.load(f)
    for symbol in merged_coins:
        engine.execute(
            f"""INSERT INTO blockchain_overview.exchange_coins (symbol, gecko_id, exists_in_binance, exists_in_gateio) """
            f"""VALUES ('{symbol}', '{merged_coins[symbol]['gecko_id']}', {merged_coins[symbol]['binance_price_usd'] is not None}, {merged_coins[symbol]['gateio_price_usd'] is not None})"""
        )
