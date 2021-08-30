import os
import time
import pytz
from pymongo import MongoClient
from dotenv import load_dotenv

from get_stock_daily_prices import get_stock_prices
from get_stock_chips import get_date_range, get_stock_chips

load_dotenv()

SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")

MONGO_URL = os.environ.get("MONGO_URL")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")
TPE_TIMEZONE = pytz.timezone("Asia/Taipei")

mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()


def main():

    existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])
    existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))

    since_date, until_date = get_date_range(SINCE_DATE, UNTIL_DATE)

    print('update stock chips')
    chips_col_name = "chips"
    for stock_id in existed_stock_ids:
        latest_data = db[chips_col_name].find_one(
            {"stockId": stock_id}, sort=[("untilDate", -1)]
        )
        last_until_date = (
            latest_data["untilDate"].astimezone(tz=TPE_TIMEZONE)
            if latest_data
            else since_date
        )
        print(f"stock id: {stock_id}")
        print(f"since_date: {last_until_date}")
        print(f"until_date: {until_date}")

        get_stock_chips(stock_id, last_until_date, until_date)

    print('update stock daily prices')
    prices_col_name = "dailyPrices"
    for stock_id in existed_stock_ids:
        latest_data = db[prices_col_name].find_one(
            {"stockId": stock_id}, sort=[("date", -1)]
        )

        last_until_date = (
            latest_data["date"].astimezone(tz=TPE_TIMEZONE)
            if latest_data
            else since_date
        )
        print(f"stock id: {stock_id}")
        print(f"since_date: {last_until_date}")

        format_daily_prices = get_stock_prices(stock_id, last_until_date)

        if format_daily_prices:
            db[prices_col_name].insert_many(format_daily_prices)

        time.sleep(1)

    mongo_client.close()


if __name__ == "__main__":
    main()