#!/bin/bash 
python exporter.py --pipeline gateio_um_funding_applies --data-path "/root/data/gateio/futures_usdt_funding_applies" \
 --dup-path "/root/data/gateio/futures_usdt_funding_applies/dup_file" --pg-url "postgresql://postgres:svEwrp/ofET-LNqv@127.0.0.1:5432/superset"
