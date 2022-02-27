import os
from pymongo import MongoClient
from dotenv import load_dotenv
import heapq
import pandas as pd

from portfolio import Portfolio
from stock import StockQuarter

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


def generate_quarters(start_quarter, end_quarter):
    start_year = int(start_quarter[:-1])
    start_quarter_num = int(start_quarter[-1])

    end_year = int(end_quarter[:-1])
    end_quarter_num = int(end_quarter[-1])

    account_quarters = []

    end_quarter_in_first_year = 5
    if start_year == end_year:
        end_quarter_in_first_year = end_quarter_num

    for quarter in range(start_quarter_num, end_quarter_in_first_year):
        account_quarters.append(f"{start_year}{quarter}")

    for year in range(start_year + 1, end_year):
        for quarter in range(1, 5):
            account_quarters.append(f"{year}{quarter}")

    if end_year > start_year:
        for quarter in range(1, end_quarter_num):
            account_quarters.append(f"{end_year}{quarter}")

    return account_quarters


def EBITDA_strategy(account_quarters, portfolio_count):
    stocks_quarter_rank = {}

    for quarter in account_quarters:
        print(f"quarter: {quarter}")
        stock_infos = db.stock_infos_quarter.find(
            {
                "會計年季度": quarter,
                "普通股股本": {"$gt": 0},
                "毛利": {"$ne": 0},
                "營業利益": {"$ne": 0},
            }
        )

        stocks_EBITDA_mod_EV = {}

        for info in stock_infos:
            stock_quarter = StockQuarter(db, info)

            val = stock_quarter.EBITDA_mod_EV
            if val > 0:
                stocks_EBITDA_mod_EV[stock_quarter.stock.id] = val

        stocks_quarter_rank[quarter] = heapq.nlargest(
            portfolio_count, stocks_EBITDA_mod_EV, key=stocks_EBITDA_mod_EV.get
        )

    return stocks_quarter_rank


def main():
    account_quarters = []

    start_quarter = "20161"
    end_quarter = "20211"
    init_money = 3000
    portfolio_count = 20

    account_quarters = generate_quarters(start_quarter, end_quarter)

    market_portfolio = Portfolio(db, init_money, start_quarter)

    for curr_quarter in account_quarters:
        market_portfolio.sell_all_stocks(curr_quarter)

        market_portfolio.buy_stocks(["0050"], curr_quarter)

    market_portfolio.sell_all_stocks(end_quarter)

    print("=============================")
    print("0050 portfolio")
    print(market_portfolio.assets_history)
    print(market_portfolio.get_history_profit(end_quarter))
    print(f"win rate: {market_portfolio.win_rate * 100} %")
    print("=============================")

    stocks_quarter_rank = EBITDA_strategy(account_quarters, portfolio_count)

    stocks_rank_df = pd.DataFrame(stocks_quarter_rank)

    portfolio = Portfolio(db, init_money, start_quarter)

    for curr_quarter in account_quarters:
        buy_stock_ids = stocks_rank_df[curr_quarter]

        portfolio.sell_all_stocks(curr_quarter)

        portfolio.buy_stocks(buy_stock_ids, curr_quarter)

    portfolio.sell_all_stocks(end_quarter)

    print("=============================")
    print("EBITDA portfolio")
    print(portfolio.assets_history)
    print(portfolio.get_history_profit(end_quarter))
    print(f"win rate: {portfolio.win_rate * 100} %")
    print("=============================")


if __name__ == "__main__":
    main()
