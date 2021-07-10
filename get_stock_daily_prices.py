import os
import requests
import json
import time
import pytz
from requests.adapters import HTTPAdapter
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

TPE_TIMEZONE = pytz.timezone("Asia/Taipei")

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
STOCK_IDS = os.environ.get("STOCK_IDS")
UPDATE_EXISTED_COMPANY = os.environ.get("UPDATE_EXISTED_COMPANY")

#  if not STOCK_IDS:
#      STOCK_IDS = input("input new stock ids or empty: ")


mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()
prices_col_name = "dailyPrices"

session = requests.Session()
adapter = HTTPAdapter(max_retries=5)
session.mount("http://", adapter)
session.mount("https://", adapter)


def get_since_date():
    SINCE_DATE = os.environ.get("SINCE_DATE")

    if not SINCE_DATE:
        raise ValueError(SINCE_DATE, "SINCE_DATE is missing")

    since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")

    return since_date.astimezone(tz=TPE_TIMEZONE)


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
        db[prices_col_name].insert_many(format_daily_prices)


def main():
    since_date = get_since_date()

    existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])
    existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))

    # crawl new
    if STOCK_IDS:
        stock_ids = STOCK_IDS.split(",")
        for stock_id in stock_ids:
            print(f"since_date: {since_date}")
            print(f"stock_id: {stock_id}")

            if stock_id in existed_stock_ids:
                print(f"stock_id {stock_id} already exists, skip")
                continue

            get_stock_prices(stock_id, since_date)
            db["stocks"].insert_one(
                {
                    "stockId": stock_id,
                    "shouldSkip": False,
                }
            )

    if UPDATE_EXISTED_COMPANY:
        for stock_id in existed_stock_ids:
            latest_data = db[prices_col_name].find_one(
                {"stockId": stock_id}, sort=[("date", -1)]
            )

            last_until_date = (
                latest_data["date"].astimezone(tz=TPE_TIMEZONE)
                if latest_data
                else since_date
            )
            print(f"last until date: {last_until_date}")
            print(f"existed stock id: {stock_id}")

            get_stock_prices(stock_id, last_until_date)

            time.sleep(1)

    mongo_client.close()


if __name__ == "__main__":
    main()
