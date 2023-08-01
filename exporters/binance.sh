#!/bin/bash

python exporter.py --pipeline binance_future_um_funding_rates --data-path "/root/data/binance/futures_um_fundingRate/monthly"     --dup-path "/root/data/binance/futures_um_fundingRate/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"
