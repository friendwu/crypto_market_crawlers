from overviewetl.domain.domain import Domain
from dataclasses import dataclass
from item_pipeline import ItemPipeline
from blockchainetl.jobs.exporters.gen_multi_item_exporter import gen_multi_item_exporter
import time
import json
from datetime import datetime
import re


@dataclass
class GeckoMarketItem(Domain):
    gecko_id: str
    symbol: str
    name: str
    timestamp: int
    datetime: str

    price_usd: float
    price_btc: float
    price_eth: float
    price_bnb: float

    market_cap_usd: float
    market_cap_btc: float
    market_cap_eth: float
    market_cap_bnb: float

    volume_24h_usd: float
    volume_24h_btc: float
    volume_24h_eth: float

    twitter_followers: int
    reddit_average_posts_48h: float
    reddit_subscribers: int
    reddit_accounts_active_48h: float

    forks: int
    stars: int
    subscribers: int
    total_issues: int
    closed_issues: int
    pull_requests_merged: int
    pull_request_contributors: int
    commit_count_4_weeks: int


class GeckoMarketItemPipeline(ItemPipeline):
    def __init__(self, pg_url):
        # TODO: maybe no need for multiple item exporter.
        self.item_exporter = gen_multi_item_exporter(
            None,
            pg_url,
            "gecko_market_item",
            "gecko_markets",
            GeckoMarketItem.fields,
        )

        self.item_exporter.open()

        pass

    def is_valid_file(self, file_path):
        pattern = r"^\d+-\d+-\d+\.json$"
        if re.match(pattern, file_path):
            return True
        else:
            return False

    def extract_and_export_items(self, file_path):
        with open(file_path, "r") as f:
            try:
                data = json.load(f)
            except Exception as e:
                # log.error("failed to load %s: %s", file_path, e)
                return None

        if data["market_data"] is None:
            # self.dup_filter.update_dupe_url(file_path)
            return None

        date = file_path.split("/")[-1].split(".")[0]
        timestamp = int(time.mktime(time.strptime(date, "%d-%m-%Y")))
        dt = datetime.fromtimestamp(timestamp)

        item = GeckoMarketItem(
            gecko_id=data["id"],
            symbol=data["symbol"],
            name=data["name"],
            timestamp=timestamp,
            datetime=dt.strftime("%Y-%m-%d %H:%M:%S %Z%z"),
            price_usd=data["market_data"]["current_price"].get("usd"),
            price_btc=data["market_data"]["current_price"].get("btc"),
            price_eth=data["market_data"]["current_price"].get("eth"),
            price_bnb=data["market_data"]["current_price"].get("bnb"),
            market_cap_usd=data["market_data"]["market_cap"].get("usd"),
            market_cap_btc=data["market_data"]["market_cap"].get("btc"),
            market_cap_eth=data["market_data"]["market_cap"].get("eth"),
            market_cap_bnb=data["market_data"]["market_cap"].get("bnb"),
            volume_24h_usd=data["market_data"]["total_volume"].get("usd"),
            volume_24h_btc=data["market_data"]["total_volume"].get("btc"),
            volume_24h_eth=data["market_data"]["total_volume"].get("eth"),
            twitter_followers=data["community_data"]["twitter_followers"],
            reddit_average_posts_48h=data["community_data"]["reddit_average_posts_48h"],
            reddit_subscribers=data["community_data"]["reddit_subscribers"],
            reddit_accounts_active_48h=data["community_data"][
                "reddit_accounts_active_48h"
            ],
            forks=data["developer_data"]["forks"],
            stars=data["developer_data"]["stars"],
            subscribers=data["developer_data"]["subscribers"],
            total_issues=data["developer_data"]["total_issues"],
            closed_issues=data["developer_data"]["closed_issues"],
            pull_requests_merged=data["developer_data"]["pull_requests_merged"],
            pull_request_contributors=data["developer_data"][
                "pull_request_contributors"
            ],
            commit_count_4_weeks=data["developer_data"]["commit_count_4_weeks"],
        )

        # FIXME: should remove this logic.
        item = item.asdict()
        item["type"] = "gecko_market_item"

        self.item_exporter.export_items([item])
