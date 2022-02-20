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


def main():
    account_quarters = []

    start_year = 2016
    end_year = 2020
    init_money = 3000
    portfolio_count = 20
    end_quarter = f"{end_year + 1}1"

    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            account_quarters.append(f"{year}{quarter}")

    stocks_quarter_rank = {}

    market_portfolio = Portfolio(db, init_money, account_quarters[0])

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

    #  tmp = None

    for quarter in account_quarters:
        print(f"quarter: {quarter}")
        stock_infos = db.stock_infos_quarter.find({"會計年季度": quarter})

        stocks_EBITDA_mod_EV = {}

        for info in stock_infos:
            stock_quarter = StockQuarter(db, info)

            val = stock_quarter.EBITDA_mod_EV
            if val > 0:
                stocks_EBITDA_mod_EV[stock_quarter.stock.id] = val
                #  stocks_EBITDA_mod_EV[stock_quarter.stock.id] = stock

        #  tmp = dict(sorted(stocks_EBITDA_mod_EV.items(), key=lambda x: x[1], reverse=True)[:30])
        #  print(tmp)

        stocks_quarter_rank[quarter] = heapq.nlargest(
            portfolio_count, stocks_EBITDA_mod_EV, key=stocks_EBITDA_mod_EV.get
        )

    stocks_rank_df = pd.DataFrame(stocks_quarter_rank)

    first_quarter = stocks_rank_df.columns[0]

    portfolio = Portfolio(db, init_money, first_quarter)

    ### LEGACY ###
    #  first_quarter_stock_ids = stocks_rank_df[first_quarter]
    #  portfolio.buy_stocks(first_quarter_stock_ids, first_quarter)

    #  prev_quarter_stock_ids = first_quarter_stock_ids

    #  for curr_quarter in stocks_rank_df.iloc[:, 1:]:
    #      curr_stock_ids = stocks_rank_df[curr_quarter]

    #      sell_stock_ids = set(prev_quarter_stock_ids.values) - set(curr_stock_ids.values)
    #      buy_stock_ids = set(curr_stock_ids.values) - set(prev_quarter_stock_ids.values)

    #      portfolio.sell_stocks(sell_stock_ids, curr_quarter)

    #      portfolio.buy_stocks(buy_stock_ids, curr_quarter)

    #      prev_quarter_stock_ids = curr_stock_ids

    for curr_quarter in stocks_rank_df:
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
