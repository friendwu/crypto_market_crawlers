import pandas as pd
from sqlalchemy import create_engine
import os

# 数据库连接配置
engine = create_engine(
    "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset",
    echo=False,
)


# 创建输出目录
output_dir = 'output_data'
os.makedirs(output_dir, exist_ok=True)

# 查询所有可用的 symbols
symbols_query = """
SELECT DISTINCT symbol 
FROM blockchain_overview.binance_spot_klines_v2_1h 
-- WHERE timestamp >= '2021/01/01 01:00:00' AND timestamp <= '2021/01/05 01:00:00'
"""

symbols_df = pd.read_sql(symbols_query, engine)
symbols = symbols_df['symbol'].tolist()

# 处理每个 symbol
for i, symbol in enumerate(symbols):
    print(f"Processing symbol: {symbol}")
    
    # 为每个 symbol 构建查询
    sql_query = f"""
    WITH gecko_tickers AS (
        SELECT symbol, gecko_id
        FROM blockchain_overview.gecko_tickers
        WHERE exchange = 'binance'
    ),
    marketcap AS (
        SELECT
            timestamp,
            gecko_id,
            concat(UPPER(symbol), 'USDT') AS symbol,
            price,
            marketcap,
            marketcap / NULLIF(price, 0) AS total_supply
        FROM blockchain_overview.gecko_markets_usd
        WHERE gecko_id IN (SELECT DISTINCT gecko_id FROM gecko_tickers)
    ),
    final AS (
        SELECT
            B.gecko_id,
            B.total_supply,
            A.*
            -- A.symbol,
            -- A.timestamp,
            -- A.open,
            -- A.high,
            -- A.close,
            -- A.low,
            -- B.marketcap AS marketcap_usd
        FROM blockchain_overview.binance_spot_klines_v2_1h A
        LEFT JOIN marketcap B ON A.timestamp = B.timestamp AND A.symbol = B.symbol
        WHERE A.symbol = '{symbol}'
          AND B.marketcap IS NOT NULL
    )
    SELECT * FROM final
    """
    
    # 读取数据
    df = pd.read_sql(sql_query, engine)
    
    # 如果查询结果不为空，则保存文件
    if not df.empty:
        del df["ignore"]
        df = df.ffill()
        filename = f"{output_dir}/{symbol}.csv"
        df.to_csv(filename, index=False)
        # df.to_feather(filename, index=False)
        print(f"Saved data for {symbol} to {filename}")
        if i == 10:
            break
    else:
        print(f"No data found for {symbol}")

print("Data processing complete.")
