#!/bin/bash

python exporter.py --pipeline binance_future_um_funding_rates --data-path "/root/data/binance/futures_um_fundingRate/monthly"     --dup-path "/root/data/binance/futures_um_fundingRate/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"
python exporter.py --pipeline binance_future_um_metrics --data-path "/root/data/binance/futures_um_metrics/daily"     --dup-path "/root/data/binance/futures_um_metrics/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset" 