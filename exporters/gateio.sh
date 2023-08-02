#!/bin/bash 
python exporter.py --pipeline gateio_um_funding_applies --data-path "/root/data/gateio/futures_usdt_funding_applies" \
 --dup-path "/root/data/gateio/futures_usdt_funding_applies/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"

python exporter.py --pipeline gateio_um_funding_updates --data-path "/root/data/gateio/futures_usdt_funding_updates"  \
    --dup-path "/root/data/gateio/futures_usdt_funding_updates/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"


python exporter.py --pipeline gateio_spot_candlesticks_5m --data-path "/root/data/gateio/spot_candlesticks_5m" \
 --dup-path "/root/data/gateio/spot_candlesticks_5m/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"
