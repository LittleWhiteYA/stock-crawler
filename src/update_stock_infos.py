import os
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

from get_stock_daily_prices import get_stock_prices
from get_stock_chips import get_stock_chips

load_dotenv()

SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

def get_stock_id():
    stock_id = input("input stock id or 'all': ")

    if not stock_id:
        raise ValueError(stock_id, "stock_id is missing")

    if stock_id == "all":
        return "all"

    return stock_id


def main():
    stock_id = get_stock_id()

    if stock_id == "all":
        existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])
        #  existed_stocks = db["stocks"].find({}, sort=[("stockId", 1)])
        existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))
    else:
        existed_stock_ids = [stock_id]

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")

    if UNTIL_DATE:
        until_date = min(datetime.strptime(UNTIL_DATE, "%Y-%m-%d"), datetime.now())
    else:
        until_date = datetime.now()

    date_column = "日期"

    print("update stock chips")
    chips_collection = "chips"

    for stock_id in existed_stock_ids:
        latest_data = db[chips_collection].find_one(
            {"stockId": stock_id}, sort=[(date_column, -1)]
        )
        since_date = (
            latest_data[date_column] + timedelta(days=1) if latest_data else since_date
        )
        print(f"stock id: {stock_id}")
        print(f"since_date: {since_date}")
        print(f"until_date: {until_date}")

        chips = get_stock_chips(stock_id, since_date, until_date)

        now = datetime.now()

        for chip in chips:
            chip["createdAt"] = now

            db[chips_collection].update_one(
                {
                    "stockId": chip["stockId"],
                    date_column: chip[date_column],
                },
                {
                    "$setOnInsert": chip,
                },
                upsert=True,
            )

    prices_collection = "dailyPrices"
    print(f"update stock daily prices in collection {prices_collection}")

    for stock_id in existed_stock_ids:
        if int(stock_id) <= 1100:
            continue

        latest_data = db[prices_collection].find_one(
            {"stockId": stock_id}, sort=[(date_column, -1)]
        )

        if since_date:
            last_until_date = since_date
        else:
            latest_data[date_column]

        print(f"stock id: {stock_id}")
        print(f"since_date: {last_until_date}")

        format_daily_prices = get_stock_prices(stock_id, last_until_date, until_date)

        now = datetime.now()

        for price in format_daily_prices:
            price["createdAt"] = now

            db[prices_collection].update_one(
                {
                    "stockId": price["stockId"],
                    date_column: price[date_column],
                },
                {
                    "$setOnInsert": price,
                },
                upsert=True,
            )

        #  if format_daily_prices:
        #      db[prices_collection].insert_many(format_daily_prices)

    mongo_client.close()


if __name__ == "__main__":
    main()
