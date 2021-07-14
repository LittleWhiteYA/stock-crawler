import requests
import json
import pytz
from requests.adapters import HTTPAdapter
from datetime import datetime

TPE_TIMEZONE = pytz.timezone("Asia/Taipei")

session = requests.Session()
adapter = HTTPAdapter(max_retries=5)
session.mount("http://", adapter)
session.mount("https://", adapter)


def crawl_stock_daily_prices(stock_id, since_date):
    since_date_secs = int(since_date.timestamp() * 1000) + 1000

    url = (
        f"https://www.wantgoo.com/investrue/{stock_id}/daily-candlesticks?"
        f"after={since_date_secs}"
    )

    res = session.get(
        url,
        headers={
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            "(KHTML, likeGecko) Chrome/90.0.1234.56 Safari/537.36",
        },
    )

    daily_prices = json.loads(res.text)

    return daily_prices


def get_stock_prices(stock_id, since_date):
    daily_prices = crawl_stock_daily_prices(stock_id, since_date)

    if daily_prices:
        format_daily_prices = map(
            lambda daily_price: {
                "stockId": stock_id,
                "date": datetime.fromtimestamp(
                    daily_price["tradeDate"] / 1000, tz=TPE_TIMEZONE
                ),
                **daily_price,
            },
            daily_prices,
        )

        return format_daily_prices
