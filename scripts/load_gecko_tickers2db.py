import os
from sqlalchemy import (
    create_engine,
    text,
    Table,
    Column,
    TIMESTAMP,
    Boolean,
    Float,
    Integer,
    MetaData,
    String,
    and_,
)
import numpy as np
import logging
import pandas as pd
import requests

engine = create_engine(
    "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset",
    echo=False,
)


def _load_tickers_df():

    proxies = {
        "http": "http://127.0.0.1:8443",
        "https": "http://127.0.0.1:8443",
    }
    tickers = []
    page = 1
    d = set()

    while True:
        url = f"https://api.coingecko.com/api/v3/coins/tether/tickers?exchange_ids=binance&include_exchange_logo=false&page={page}&order=volume_desc&depth=false"

        headers = {
            "accept": "application/json",
        }

        response = requests.get(url, headers=headers, proxies=proxies).json()

        if len(response["tickers"]) == 0:
            break

        for ticker in response["tickers"]:
            if ticker["coin_id"] in d:
                print(f"Duplicate: {ticker['coin_id']} / {ticker}")
                continue

            # tickers[ticker["coin_id"]] = ticker["base"]
            tickers.append(
                {
                    "gecko_id": ticker["coin_id"],
                    "symbol": ticker["base"],
                    "exchange": ticker["market"]["identifier"],
                    "is_stale": ticker["is_stale"],
                    "is_anomaly": ticker["is_anomaly"],
                    "trust_score": ticker["trust_score"],
                }
            )

            d.add(ticker["coin_id"])

        page += 1

    return pd.DataFrame(tickers)


df = _load_tickers_df()
df.to_csv("tmp.csv", index=False)

_schema = "blockchain_overview"

table_name = "gecko_tickers"
full_table_name = f"{_schema}.{table_name}"

# 创建表格元数据
metadata = MetaData()
table = Table(
    table_name,
    metadata,
    Column("gecko_id", String(50), primary_key=True),
    Column("exchange", String(50), primary_key=True),
    Column("symbol", String(20)),
    Column("is_stale", Boolean),
    Column("is_anomaly", Boolean),
    Column("trust_score", String(20)),
    schema=_schema,
)

# 创建表格(如果不存在)
metadata.create_all(engine)


with engine.connect() as con:
    df.to_sql(
        table_name,
        con=con,
        schema=_schema,
        if_exists="append",
        index=False,
    )
