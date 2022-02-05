import os
from pymongo import MongoClient
from dotenv import load_dotenv
import heapq
import pandas as pd

from portfolio import Portfolio

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


class Stock:
    def __init__(self, db, quarter_info):
        self.db = db
        self.quarter_info = quarter_info
        self.stock_id = quarter_info["stockId"]
        self.price = None

    @property
    def id(self):
        return self.stock_id

    @property
    def price_after_quarter_report(self):
        if self.price:
            return self.price

        #  from datetime import datetime, timedelta

        #  year = int(self.quarter_info["會計年季度"][:-1])
        #  quarter = int(self.quarter_info["會計年季度"][-1])

        #  if quarter == 1:
        #      after_date = datetime(year, 5, 15)
        #  elif quarter == 2:
        #      after_date = datetime(year, 8, 14)
        #  elif quarter == 3:
        #      after_date = datetime(year, 11, 14)
        #  elif quarter == 4:
        #      after_date = datetime(year + 1, 3, 31)

        #  price = db.dailyPrices.find_one(
        #      {
        #          "stockId": self.stock_id,
        #          "日期": { "$gt": after_date, "$lt": after_date + timedelta(days=10) },
        #      },
        #      sort=[("日期", 1)]
        #  )

        #  price = price["收盤價"] if price else 0

        #  db.prices_quarter.update_one(
        #      {
        #          "stockId": self.stock_id,
        #          "會計年季度": self.quarter_info["會計年季度"],
        #      },
        #      {
        #          "$setOnInsert": {
        #              "stockId": self.stock_id,
        #              "會計年季度": self.quarter_info["會計年季度"],
        #              "price": price,
        #          }
        #      },
        #      upsert=True
        #  )

        price_quarter = self.db.prices_quarter.find_one(
            {
                "stockId": self.stock_id,
                "會計年季度": self.quarter_info["會計年季度"],
            },
        )

        self.price = price_quarter["price"]

        #  if not price:
        #      raise ValueError(f'missing price in stockId {self.stock_id}, year {year}')

        return self.price

    @property
    def EV(self):
        # EV＝市值＋總負債－總現金
        # https://smart.businessweekly.com.tw/Reading/WebArticle.aspx?id=65053&p=1

        price = self.price_after_quarter_report
        info = self.quarter_info

        market_value = int(price * info["普通股股本"])

        if market_value == 0:
            return 0

        EV = market_value + info["總負債"] - info["現金及約當現金"] - info["短期投資"]

        return EV

    @property
    def EBITDA(self):
        # EBITDA＝營業利益＋折舊＋攤銷
        # https://smart.businessweekly.com.tw/Reading/WebArticle.aspx?id=65053&p=1

        return (
            self.quarter_info["營業利益"]
            + self.quarter_info["折舊"]
            + self.quarter_info["攤銷"]
        )

    @property
    def EBITDA_mod_EV(self):
        if self.EV <= 0:
            return 0

        return round(self.EBITDA * 100 / self.EV, 4)


def main():
    account_quarters = []

    start_year = 2016
    end_year = 2020
    top_num_stocks = 30

    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            account_quarters.append(f"{year}{quarter}")

    stocks_quarter_rank = {}

    #  tmp = None

    for quarter in account_quarters:
        print(f"quarter: {quarter}")
        stock_infos = db.stock_infos_quarter.find({"會計年季度": quarter})

        stocks_EBITDA_mod_EV = {}

        for info in stock_infos:
            stock = Stock(db, info)

            val = stock.EBITDA_mod_EV
            if val > 0:
                stocks_EBITDA_mod_EV[stock.id] = val
                #  stocks_EBITDA_mod_EV[stock.id] = stock

        #  tmp = dict(sorted(stocks_EBITDA_mod_EV.items(), key=lambda x: x[1], reverse=True)[:30])
        #  print(tmp)

        stocks_quarter_rank[quarter] = heapq.nlargest(
            top_num_stocks, stocks_EBITDA_mod_EV, key=stocks_EBITDA_mod_EV.get
        )

    stocks_rank_df = pd.DataFrame(stocks_quarter_rank)

    init_money = 3000
    first_quarter = stocks_rank_df.columns[0]
    first_quarter_stock_ids = stocks_rank_df[first_quarter]

    porfolio = Portfolio(db, top_num_stocks, init_money, first_quarter)

    for stock_id in first_quarter_stock_ids:
        porfolio.buy(stock_id, first_quarter)

    prev_quarter_stock_ids = first_quarter_stock_ids

    for curr_quarter in stocks_rank_df.iloc[:, 1:]:

        curr_stock_ids = stocks_rank_df[curr_quarter]

        sell_stock_ids = set(prev_quarter_stock_ids.values) - set(curr_stock_ids.values)
        buy_stock_ids = set(curr_stock_ids.values) - set(prev_quarter_stock_ids.values)

        for sell_stock_id in sell_stock_ids:
            porfolio.sell(sell_stock_id, curr_quarter)

        for buy_stock_id in buy_stock_ids:
            porfolio.buy(buy_stock_id, curr_quarter)

        prev_quarter_stock_ids = curr_stock_ids


if __name__ == "__main__":
    main()
