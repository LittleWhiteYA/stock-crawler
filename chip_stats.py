import os
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")


mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()


def get_stock_id():
    stock_id = input("input stock id or 'all': ")

    if not stock_id:
        raise ValueError(stock_id, "stock_id is missing")

    if stock_id == "all":
        return "all"

    return stock_id


def get_threshold(stock_id):
    stock = db["stocks"].find_one({"stockId": stock_id})

    default_threshold = 100

    big_trader_threshold = (
        stock["threshold"] if "threshold" in stock else default_threshold
    )

    return big_trader_threshold


def get_trade_dataframe(stock_id, big_trader_threshold):
    weekly_trades = list(
        db["chips"].find(
            {"stockId": stock_id, "untilDate": {"$gte": datetime(2021, 1, 1)}}
        )
    )

    if not weekly_trades:
        raise ValueError(weekly_trades, f"stock id {stock_id} may not be crawled yet")

    weekly_trades_stats = []

    for weekly_trade in weekly_trades:
        count_weekly_traders = map(
            lambda trade: {
                **trade,
                "quantitiesCount": trade["buyQuantities"] - trade["sellQuantities"],
            },
            weekly_trade["data"],
        )
        big_traders = list(count_weekly_traders)

        weekly_trades_stats.append(
            {trader["agentName"]: trader["quantitiesCount"] for trader in big_traders}
        )

    weekly_trades_df = pd.DataFrame(
        weekly_trades_stats,
        index=map(lambda trade: trade["untilDate"].date(), weekly_trades),
    )

    # fill NaN to 0 and do cumsum
    cusum_trades_df = weekly_trades_df.fillna(0).cumsum()

    # filter columns according to last row value
    big_trader_filter = abs(cusum_trades_df[-1:].squeeze()) > big_trader_threshold

    big_trades_df = cusum_trades_df.loc[:, big_trader_filter]

    # sort columns according to last row value
    big_trades_df = big_trades_df.sort_values(
        big_trades_df.last_valid_index(), axis="columns", ascending=False
    )

    #  print(big_trades_df.to_markdown())

    return big_trades_df


def get_price_dataframe(stock_id):
    prices = list(
        db.dailyPrices.find({"stockId": stock_id}, {"_id": 0, "close": 1, "date": 1})
    )

    prices_df = pd.DataFrame(
        map(lambda e: {"price": e["close"]}, prices),
        index=map(lambda p: p["date"].date(), prices),
    )

    #  print(prices_df.to_markdown())

    return prices_df


def main(stock_id, big_trader_threshold):

    trade_df = get_trade_dataframe(stock_id, big_trader_threshold)
    prices_df = get_price_dataframe(stock_id)

    # from matplotlib import font_manager
    # font_set = {f.name for f in font_manager.fontManager.ttflist}
    plt.rcParams["font.sans-serif"] = "Noto Sans CJK JP"
    plt.rcParams["figure.dpi"] = 200

    ax = trade_df.plot(
        figsize=(10, 5),
        fontsize=15,
        style="o-",
        grid=True,
    )

    ax.set_title(
        f"{stock_id} (threshold: {big_trader_threshold})",
        fontsize=30,
    )
    ax.set_xlabel("date", fontsize=10)
    ax.set_ylabel("count", fontsize=10)
    ax.legend(loc="upper left", prop={"size": 12}, bbox_to_anchor=(1.05, 1))

    plt.minorticks_on()

    right_ax = ax.twinx()
    right_ax.plot(
        prices_df["price"],
        color="black",
    )
    right_ax.tick_params(axis="y", labelsize=15)
    right_ax.set_ylabel("price", fontsize=10)

    plt.minorticks_on()

    print(f"generate {stock_id} png")
    #  plt.show()
    ax.figure.savefig(f"png/{stock_id}.png", bbox_inches="tight")


if __name__ == "__main__":
    stock_id = get_stock_id()

    if stock_id == "all":
        existed_stocks = db["stocks"].find({"shouldSkip": False}, sort=[("stockId", 1)])

        for stock in existed_stocks:
            stock_id = stock["stockId"]
            big_trader_threshold = get_threshold(stock_id)
            main(stock_id, big_trader_threshold)
    else:
        while True:
            big_trader_threshold = get_threshold(stock_id)
            main(stock_id, big_trader_threshold)

            stock_id = get_stock_id()
