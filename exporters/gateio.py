from dataclasses import dataclass
from overviewetl.domain.domain import Domain
from item_pipeline import ItemPipeline
import re
import os
import pandas as pd
from blockchainetl.jobs.exporters.gen_multi_item_exporter import gen_multi_item_exporter

GATEIO_UM_FUNDING_APPLIES = "gateio_um_funding_applies"
GATEIO_UM_FUNDING_UPDATES = "gateio_um_funding_updates"
GATEIO_SPOT_CANDLESTICKS_5M = "gateio_spot_candlesticks_5m"


@dataclass
class GateioUmFundingApplies(Domain):
    timestamp: int
    symbol: str
    datetime: str
    funding_rate: float


@dataclass
class GateioUmFundingUpdates(Domain):
    timestamp: int
    symbol: str
    datetime: str
    funding_rate: float
    interest_rate: float
    bid_diff: float
    ask_diff: float
    mark_price: float
    index_price: float
    update_count: int


@dataclass
class GateioSpotCandlesticks5M(Domain):
    timestamp: int
    symbol: str
    datetime: str 
    open: float
    high: float
    low: float
    close: float
    volume: float


class GateioUmFundingAppliesItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            GATEIO_UM_FUNDING_APPLIES,
            GATEIO_UM_FUNDING_APPLIES,
            GateioUmFundingApplies.fields,
        )

        self.item_exporter.open()
        self.file_pattern = r"(\w+)_(\w+)\w+-\d{6}.csv.gz"

    def is_valid_file(self, file_path):
        if re.match(self.file_pattern, os.path.basename(file_path)):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        column_names = ["timestamp", "funding_rate"]
        df = pd.read_csv(file_path, compression="gzip", header=None, names=column_names)
        df["symbol"] = os.path.basename(file_path).split("-")[0]

        items = []
        for index, row in df.iterrows():
            item = GateioUmFundingApplies(
                timestamp=row["timestamp"],
                datetime=pd.to_datetime(row["timestamp"], unit="s").strftime(
                    "%Y-%m-%d %H:%M:%S %Z%z"
                ),
                symbol=row["symbol"],
                funding_rate=row["funding_rate"],
            )
            item = item.asdict()
            item["type"] = GATEIO_UM_FUNDING_APPLIES

            items.append(item)

        self.item_exporter.export_items(items)



class GateioUmFundingUpdatesItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            GATEIO_UM_FUNDING_UPDATES,
            GATEIO_UM_FUNDING_UPDATES,
            GateioUmFundingUpdates.fields,
        )

        self.item_exporter.open()
        self.file_pattern = r"(\w+)_(\w+)\w+-\d{6}.csv.gz"

    def is_valid_file(self, file_path):
        if re.match(self.file_pattern, os.path.basename(file_path)):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        column_names = ['timestamp', 'funding_rate', 'interest_rate', 'bid_diff', 'ask_diff', 'mark_price', 'index_price', 'update_count']
        df = pd.read_csv(file_path, compression="gzip", header=None, names=column_names)
        df["symbol"] = os.path.basename(file_path).split("-")[0]

        items = []
        for index, row in df.iterrows():
            item = GateioUmFundingUpdates(
                timestamp=row["timestamp"],
                datetime=pd.to_datetime(row["timestamp"], unit="s").strftime(
                    "%Y-%m-%d %H:%M:%S %Z%z"
                ),
                symbol=row["symbol"],
                funding_rate=row["funding_rate"],
                interest_rate=row["interest_rate"],
                bid_diff=row["bid_diff"],
                ask_diff=row["ask_diff"],
                mark_price=row["mark_price"],
                index_price=row["index_price"],
                update_count=row["update_count"],

            )
            item = item.asdict()
            item["type"] = GATEIO_UM_FUNDING_UPDATES

            items.append(item)

        self.item_exporter.export_items(items)



class GateioSpotCandlesticks5MItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            GATEIO_SPOT_CANDLESTICKS_5M,
            GATEIO_SPOT_CANDLESTICKS_5M,
            GateioSpotCandlesticks5M.fields,
        )

        self.item_exporter.open()
        self.file_pattern = r"(\w+)_(\w+)\w+-\d{6}.csv.gz"

    def is_valid_file(self, file_path):
        if re.match(self.file_pattern, os.path.basename(file_path)):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        column_names = ['timestamp', 'volume', 'close', 'high', 'low', 'open']
        df = pd.read_csv(file_path, compression="gzip", header=None, names=column_names)
        df["symbol"] = os.path.basename(file_path).split("-")[0]

        items = []
        for index, row in df.iterrows():
            item = GateioSpotCandlesticks5M(
                timestamp=row["timestamp"],
                datetime=pd.to_datetime(row["timestamp"], unit="s").strftime(
                    "%Y-%m-%d %H:%M:%S %Z%z"
                ),
                symbol=row["symbol"],
                volume=row["volume"],
                close=row["close"],
                high=row["high"],
                low=row["low"],
                open=row["open"],
            )
            item = item.asdict()
            item["type"] = GATEIO_SPOT_CANDLESTICKS_5M

            items.append(item)

        self.item_exporter.export_items(items)
