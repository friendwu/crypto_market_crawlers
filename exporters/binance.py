from dataclasses import dataclass
from overviewetl.domain.domain import Domain
from item_pipeline import ItemPipeline
from blockchainetl.jobs.exporters.gen_multi_item_exporter import gen_multi_item_exporter
import re
import zipfile
from datetime import datetime
import pandas as pd
import os

BINANCE_UM_FUNDING_RATES = "binance_um_funding_rates"
BINANCE_UM_METRICS = "binance_um_metrics"


@dataclass
class BinanceUmFundingRate(Domain):
    symbol: str
    calc_timestamp: int
    calc_datetime: str
    funding_interval_hours: int
    last_funding_rate: float


@dataclass
class BinanceUmMetrics(Domain):
    create_time: str
    symbol: str
    sum_open_interest: float
    sum_open_interest_value: float
    count_toptrader_long_short_ratio: float
    sum_toptrader_long_short_ratio: float
    count_long_short_ratio: float
    sum_taker_long_short_vol_ratio: float


class BinanceUmFundingRateItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            BINANCE_UM_FUNDING_RATES,
            BINANCE_UM_FUNDING_RATES,
            BinanceUmFundingRate.fields,
        )

        self.item_exporter.open()
        self.file_pattern = r"^(\w+)-fundingRate-\d{4}-\d{2}\.zip$"

        pass

    def is_valid_file(self, file_path):
        if re.match(self.file_pattern, os.path.basename(file_path)):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        items = []
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            symbol = re.match(self.file_pattern, os.path.basename(file_path)).group(1)

            csv_file = zip_ref.open(zip_ref.namelist()[0])
            df = pd.read_csv(csv_file)

            # convert df items into BinanceUmFundingRate
            for index, row in df.iterrows():
                calc_timestamp = int(row["calc_time"] / 1000)
                item = BinanceUmFundingRate(
                    symbol=symbol,
                    calc_timestamp=calc_timestamp,
                    calc_datetime=datetime.fromtimestamp(calc_timestamp).strftime(
                        "%Y-%m-%d %H:%M:%S %Z%z"
                    ),
                    funding_interval_hours=row["funding_interval_hours"],
                    last_funding_rate=row["last_funding_rate"],
                )

                item = item.asdict()
                item["type"] = BINANCE_UM_FUNDING_RATES
                items.append(item)

        self.item_exporter.export_items(items)


class BinanceUmMetricsItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            BINANCE_UM_METRICS,
            BINANCE_UM_METRICS,
            BinanceUmMetrics.fields,
        )

        self.item_exporter.open()
        self.file_pattern = r"^(\w+)-metrics-\d{4}-\d{2}-\d{2}\.zip$"

    def is_valid_file(self, file_path):
        if re.match(self.file_pattern, os.path.basename(file_path)):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        items = []
        with zipfile.ZipFile(file_path, "r") as zip_ref:
            csv_file = zip_ref.open(zip_ref.namelist()[0])
            df = pd.read_csv(csv_file)

            for index, row in df.iterrows():
                item = BinanceUmMetrics(
                    create_time=row["create_time"],
                    symbol=row["symbol"],
                    sum_open_interest=row["sum_open_interest"],
                    sum_open_interest_value=row["sum_open_interest_value"],
                    count_toptrader_long_short_ratio=row[
                        "count_toptrader_long_short_ratio"
                    ],
                    sum_toptrader_long_short_ratio=row[
                        "sum_toptrader_long_short_ratio"
                    ],
                    count_long_short_ratio=row["count_long_short_ratio"],
                    sum_taker_long_short_vol_ratio=row[
                        "sum_taker_long_short_vol_ratio"
                    ],
                )

                item = item.asdict()
                item["type"] = BINANCE_UM_METRICS
                items.append(item)

        self.item_exporter.export_items(items)
