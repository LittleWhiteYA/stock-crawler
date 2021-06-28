import os
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
THRESHOLD = os.environ.get("THRESHOLD")

stock_default_threshold = [
    {
        "stock_id": "1301",
        "default_threshold": 4000,
    },
    {
        "stock_id": "1539",
        "default_threshold": 100,
    },
    {
        "stock_id": "1786",
        "default_threshold": 100,
    },
    {
        "stock_id": "1810",
        "default_threshold": 500,
    },
    {
        "stock_id": "2065",
        "default_threshold": 150,
    },
    {
        "stock_id": "2352",
        "default_threshold": 5000,
    },
    {
        "stock_id": "2451",
        "default_threshold": 1000,
    },
    {
        "stock_id": "4930",
        "default_threshold": 400,
    },
    {
        "stock_id": "4994",
        "default_threshold": 100,
    },
    {
        "stock_id": "5425",
        "default_threshold": 900,
    },
    {
        "stock_id": "5490",
        "default_threshold": 150,
    },
    {
        "stock_id": "6505",
        "default_threshold": 2000,
    },
    {
        "stock_id": "8446",
        "default_threshold": 200,
    },
    {
        "stock_id": "9924",
        "default_threshold": 200,
    },
    {
        "stock_id": "9958",
        "default_threshold": 700,
    },
]


mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


def get_stock_id():
    stock_id = input("input stock id: ")

    if not stock_id:
        raise ValueError(stock_id, "stock_id is missing")

    if stock_id == "all":
        return "all"

    return stock_id


def get_threshold(stock_id):
    if THRESHOLD:
        big_trader_threshold = int(THRESHOLD)
    else:
        threshold = next(
            (
                config["default_threshold"]
                for config in stock_default_threshold
                if config["stock_id"] == stock_id
            ),
            None,
        )

        big_trader_threshold = threshold if threshold else 100

    return big_trader_threshold


def get_trade_dataframe(stock_id, big_trader_threshold):
    col_name = f"company_{stock_id}"
    weekly_trades = list(
        db[col_name].find({"sinceDate": {"$gte": datetime(2021, 1, 1)}})
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
        index=map(lambda trade: trade["sinceDate"].date(), weekly_trades),
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
        legend=2,
    )

    ax.set_title(
        f"{stock_id} (threshold: {big_trader_threshold})",
        fontsize=30,
    )
    ax.set_xlabel("date", fontsize=10)
    ax.set_ylabel("count", fontsize=10)
    ax.legend(loc="upper left", prop={"size": 15})

    plt.minorticks_on()

    right_ax = ax.twinx()
    right_ax.plot(
        prices_df["price"],
        color="black",
    )
    right_ax.tick_params(axis='y', labelsize=15)
    right_ax.set_ylabel("price", fontsize=10)
    #  right_ax.tick_params(axis='y', which='minor', bottom=False)

    plt.minorticks_on()
    plt.xticks(rotation=25)

    print(f"generate {stock_id} png")
    #  plt.show()
    ax.figure.savefig(f"png/{stock_id}.png")


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
