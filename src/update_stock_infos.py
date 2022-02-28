import os
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

from stock_crawler import StockCrawler
from stock import Stock

load_dotenv()

SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")

MONGO_URL = os.environ.get("MONGO_URL")


def get_stock_id():
    stock_id = input("input stock id or 'all': ")

    if not stock_id:
        raise ValueError(stock_id, "stock_id is missing")

    if stock_id == "all":
        return "all"

    return stock_id


def main():
    mongo_client = MongoClient(MONGO_URL)
    db = mongo_client.get_default_database()

    stock_id_input = get_stock_id()
    date_column = "日期"

    if stock_id_input == "all":
        existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])
        #  existed_stocks = db["stocks"].find({}, sort=[("stockId", 1)])
        existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))
    else:
        existed_stock_ids = [stock_id_input]

    stock_crawlers = []

    for stock_id in existed_stock_ids:
        stock_crawler = StockCrawler(stock_id, date_column)

        stock_crawlers.append(stock_crawler)

    since_date = None
    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")

    if UNTIL_DATE:
        until_date = min(datetime.strptime(UNTIL_DATE, "%Y-%m-%d"), datetime.now())
    else:
        until_date = datetime.now()

    print(f"env since_date: {since_date}")
    print(f"env until_date: {until_date}")

    now = datetime.now()

    # chips ######################################################
    should_continue = input(f"should update stock '{stock_id_input}' chips (y/n) ?")

    if should_continue == "y":
        print("update stock chips")
        chips_collection = "chips"

        for stock_crawler in stock_crawlers:
            stock_id = stock_crawler.stock_id

            latest_data = db[chips_collection].find_one(
                {"stockId": stock_id}, sort=[(date_column, -1)]
            )

            if since_date:
                tmp_since_date = since_date
            else:
                tmp_since_date = latest_data[date_column] + timedelta(days=1)

            print(f"stock id: {stock_id}")
            print(f"since_date: {tmp_since_date}")
            print(f"until_date: {until_date}")

            chips = stock_crawler.get_chips(tmp_since_date, until_date)

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

    # price ######################################################
    should_continue = input(f"should update stock '{stock_id_input}' prices (y/n) ?")

    if should_continue == "y":
        prices_collection = "dailyPrices"
        print(f"update stock daily prices in collection {prices_collection}")

        for stock_crawler in stock_crawlers:
            stock_id = stock_crawler.stock_id

            if int(stock_id) <= 1100:
                continue

            latest_data = db[prices_collection].find_one(
                {"stockId": stock_id}, sort=[(date_column, -1)]
            )

            if since_date:
                tmp_since_date = since_date
            else:
                tmp_since_date = latest_data[date_column]

            print(f"stock id: {stock_id}")
            print(f"since_date: {tmp_since_date}")
            print(f"until_date: {until_date}")

            format_daily_prices = stock_crawler.get_daily_prices(
                tmp_since_date, until_date
            )

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

    # reports ######################################################
    should_continue = input(
        f"should update stock '{stock_id_input}' year reports (y/n) ?"
    )

    if should_continue == "y":
        if stock_id_input == "all":
            stocks = list(db.stocks.find().sort([("stockId", 1)]))
        else:
            stocks = [{"stockId": stock_id_input}]

        start_year = 2012
        end_year = 2021

        for stock in stocks:
            stock_id = stock["stockId"]

            stock_crawler = StockCrawler(stock_id, date_column)

            print(f"stock id: {stock_id}")
            stock_infos = stock_crawler.get_year_report(start_year, end_year)

            for info in stock_infos:
                stock_id = info["stockId"]
                quarter = info["會計年季度"]

                print(f"stockId: {stock_id}, quarter year: {quarter}")

                info["createdAt"] = now

                #  new_keys = [
                #      "每股淨值",
                #      "單季EPS",
                #      "ROE",
                #      "ROA",
                #      "單季營收季增率",
                #      "毛利率",
                #      "單季毛利季增率",
                #      "營業利益率",
                #      "單季營業利益季增率",
                #      "稅前淨利率",
                #      "稅後淨利率",
                #      "單季稅後淨利季增率",
                #      "單季EPS季增率",
                #  ]

                db.stock_infos_quarter.update_one(
                    {
                        "stockId": stock_id,
                        "會計年季度": quarter,
                    },
                    {
                        "$setOnInsert": info,
                        #  "$set": {new_key: info[new_key] for new_key in new_keys},
                    },
                    upsert=True,
                )

                stock = Stock(db, stock_id)

                stock_price = stock.get_price_from_daily_collection(quarter)

                if stock_price:
                    db.prices_quarter.update_one(
                        {
                            "stockId": stock_id,
                            "會計年季度": quarter,
                        },
                        {
                            "$setOnInsert": {
                                "stockId": stock_id,
                                "會計年季度": quarter,
                                "price": stock_price["收盤價"],
                                "createdAt": now,
                            }
                        },
                        upsert=True,
                    )
                else:
                    print(f"missing price in stockId {stock_id}, quarter {quarter}")

            time.sleep(0.1)

    mongo_client.close()


if __name__ == "__main__":
    main()
