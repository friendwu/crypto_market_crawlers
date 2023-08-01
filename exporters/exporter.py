import time
from watchdog.observers import Observer
from file_handler import FileHandler
import logging
import click
from gecko import *
from binance import *


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)

log = logging.getLogger(__name__)


def new_item_pipeline(name, pg_url):
    if name == "gecko_market":
        return GeckoMarketItemPipeline(pg_url)
    elif name == "binance_future_um_funding_rates":
        return BinanceUmFundingRateItemPipeline(pg_url)
    elif name == "binance_future_um_metrics":
        return BinanceUmMetricsItemPipeline(pg_url)
    else:
        return None


@click.command()
@click.option(
    "--pipeline",
    type=click.Choice(
        ["gecko_market", "binance_future_um_funding_rates", "binance_future_um_metrics"]
    ),
    prompt="pipeline",
    help="Enter the pipeline name",
)
@click.option("--data-path", prompt="data path", help="Enter the data path")
@click.option(
    "--dup-path",
    prompt="duplicate path",
    help="Enter the path to store duplicate files",
)
@click.option("--pg-url", prompt="postgres url", help="Enter the postgres url")
def watch(pipeline, data_path, dup_path, pg_url):
    observer = Observer()
    event_handler = FileHandler(
        dup_path=dup_path, item_pipeline=new_item_pipeline(pipeline, pg_url)
    )
    observer.schedule(event_handler, data_path, recursive=True)
    event_handler.process_existing_files(data_path)

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == "__main__":
    watch()
