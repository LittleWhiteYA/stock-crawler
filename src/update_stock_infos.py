import os
import time
import traceback
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

from stock_crawler import StockCrawler
from stock import Stock

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

date_column = "日期"
stocks_collection = "stocks"
chips_collection = "chips"
prices_collection = "dailyPrices"


def get_stock_id():
    stock_id = input("input stock id or 'all': ")

    if not stock_id:
        raise ValueError(stock_id, "stock_id is missing")

    if stock_id == "all":
        return "all"

    return stock_id


def crawl_new_stock():
    now = datetime.now()

    new_stock_id = input(f"new stock id: ")
    new_until_date = datetime.now()

    if not new_stock_id:
        raise ValueError(new_stock_id, "stock_id is missing")

    print(f"insert new stock into {stocks_collection} collection")
    db[stocks_collection].update_one(
        {
            "stockId": "1342",
        },
        {
            "stockId": "1342",
            "shouldSkip": False,
        },
        upsert=True,
    )

    new_since_date = input(f"new_since_date for chips (e.g. 2021-01-01): ")
    new_since_date = datetime.strptime(new_since_date, "%Y-%m-%d")

    print(f"new_stock_id: {new_stock_id}")
    print(f"new_since_date of chips: {new_since_date}")
    print(f"new_until_date of chips: {new_until_date}")
    print("get new stock chips")
    stock_crawler = StockCrawler(new_stock_id, date_column)
    new_chips = stock_crawler.get_chips(new_since_date, new_until_date)

    for chip in new_chips:
        chip["createdAt"] = now

    if new_chips:
        db[chips_collection].insert_many(new_chips)

    new_since_date = input(f"new_since_date for prices (e.g. 2012-01-01): ")
    new_since_date = datetime.strptime(new_since_date, "%Y-%m-%d")
    print(f"new_since_date of prices: {new_since_date}")
    print(f"new_until_date of prices: {new_until_date}")
    print("get new stock prices")
    stock_crawler = StockCrawler(new_stock_id, date_column)
    new_format_daily_prices = stock_crawler.get_daily_prices(
        new_since_date, new_until_date
    )

    for price in new_format_daily_prices:
        price["createdAt"] = now

    if new_format_daily_prices:
        db[prices_collection].insert_many(new_format_daily_prices)


def main():

    # new stock ######################################################
    is_new_stock = input(f"should crawl new stock (y/n) ?")

    if is_new_stock == "y":
        crawl_new_stock()

    stock_id_input = get_stock_id()
    now = datetime.now()

    if stock_id_input == "all":
        existed_stocks = db[stocks_collection].find(
            {"shouldSkip": False}, sort=[("stockId", 1)]
        )
        #  existed_stocks = db[stocks_collection].find({}, sort=[("stockId", 1)])
        existed_stock_ids = list(map(lambda stock: stock["stockId"], existed_stocks))
    else:
        existed_stock_ids = [stock_id_input]

    stock_crawlers = []

    for stock_id in existed_stock_ids:
        stock_crawler = StockCrawler(stock_id, date_column)

        stock_crawlers.append(stock_crawler)

    since_date = input(f"since_date (e.g. 2021-01-01): ")
    if since_date:
        since_date = datetime.strptime(since_date, "%Y-%m-%d")

    until_date = input(f"until_date (e.g. 2021-01-01): ")
    if until_date:
        until_date = min(datetime.strptime(until_date, "%Y-%m-%d"), datetime.now())
    else:
        until_date = datetime.now()

    print(
        f"env since_date: {since_date}, if None will get oldest depends on each stock history"
    )
    print(f"env until_date: {until_date}")

    # chips ######################################################
    should_continue = input(f"should update stock '{stock_id_input}' chips (y/n) ?")

    if should_continue == "y":
        print("update stock chips")

        for stock_crawler in stock_crawlers:
            stock_id = stock_crawler.stock_id
            print(f"stock id: {stock_id}")

            try:
                latest_data = db[chips_collection].find_one(
                    {"stockId": stock_id}, sort=[(date_column, -1)]
                )

                if since_date:
                    tmp_since_date = since_date
                else:
                    tmp_since_date = latest_data[date_column] + timedelta(days=1)

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
            except Exception:
                traceback.print_exc()

    # price ######################################################
    should_continue = input(f"should update stock '{stock_id_input}' prices (y/n) ?")

    if should_continue == "y":
        print(f"update stock daily prices in collection {prices_collection}")

        for stock_crawler in stock_crawlers:
            stock_id = stock_crawler.stock_id
            print(f"stock id: {stock_id}")

            if int(stock_id) <= 1100:
                continue

            latest_data = db[prices_collection].find_one(
                {"stockId": stock_id}, sort=[(date_column, -1)]
            )

            if since_date:
                tmp_since_date = since_date
            else:
                tmp_since_date = latest_data[date_column]

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

        # FIXME: should not always fetch start from 2012 and end at 2021
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
