from .scraper_worker import ScraperWorker
from .data_worker import DataWorker
from .url_worker import UrlWorker
from .url_feeder import UrlFeeder

# Shared worker instances
data_worker = DataWorker()
url_worker = UrlWorker()
url_feeder = UrlFeeder()

__all__ = [
    "ScraperWorker",
    "DataWorker",
    "UrlWorker",
    "UrlFeeder",
    "data_worker",
    "url_worker",
    "url_feeder",
]


