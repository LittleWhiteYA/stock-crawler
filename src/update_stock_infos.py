import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

from get_stock_daily_prices import get_stock_prices
from get_stock_chips import get_stock_chips, get_access_token

load_dotenv()

SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


def main():

    existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])
    existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")

    else:  # from the first weekday of this week
        today = datetime.now()
        since_date = today + timedelta(days=-today.weekday())

    if UNTIL_DATE:
        until_date = min(datetime.strptime(UNTIL_DATE, "%Y-%m-%d"), datetime.now())
    else:
        until_date = datetime.now()

    print("update stock chips")
    access_token = get_access_token()

    for stock_id in existed_stock_ids:
        get_stock_chips(stock_id, since_date, until_date, access_token)

    print("update stock daily prices")
    prices_collection = "dailyPrices"
    date_column = "日期"

    for stock_id in existed_stock_ids:
        latest_data = db[prices_collection].find_one(
            {"stockId": stock_id}, sort=[(date_column, -1)]
        )

        last_until_date = latest_data[date_column] if latest_data else since_date
        print(f"stock id: {stock_id}")
        print(f"since_date: {last_until_date}")

        format_daily_prices = get_stock_prices(stock_id, last_until_date)

        if format_daily_prices:
            now = datetime.now()

            for price in format_daily_prices:
                db[prices_collection].update_one(
                    {
                        "stockId": price["stockId"],
                        date_column: price[date_column],
                    },
                    {
                        "$setOnInsert": {
                            **price,
                            "createdAt": now,
                        }
                    },
                    upsert=True,
                )

        time.sleep(1)

    mongo_client.close()


if __name__ == "__main__":
    main()
