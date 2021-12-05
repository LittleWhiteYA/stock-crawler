import os
from pymongo import MongoClient, ASCENDING
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pytz
from pathlib import Path

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
TPE_TIMEZONE = pytz.timezone("Asia/Taipei")


mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()

SIX_MONTH = 6

months_before = datetime.now() - relativedelta(months=SIX_MONTH)


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
    chips_collection = "chips"
    date = "日期"
    buyNum = "買張"
    sellNum = "賣張"
    agentName = "分點名稱"

    daily_trades = list(
        db[chips_collection].find({"stockId": stock_id, date: {"$gte": months_before}})
    )

    if not daily_trades:
        raise ValueError(daily_trades, f"stock id {stock_id} may not be crawled yet")

    daily_trades_stats = []

    for daily_trade in daily_trades:
        count_daily_traders = map(
            lambda trade: {
                **trade,
                "quantitiesCount": trade[buyNum] - trade[sellNum],
            },
            daily_trade["data"],
        )
        big_traders = list(count_daily_traders)

        daily_trades_stats.append(
            {trader[agentName]: trader["quantitiesCount"] for trader in big_traders}
        )

    daily_trades_df = pd.DataFrame(
        daily_trades_stats,
        index=map(lambda trade: trade[date].date(), daily_trades),
    )

    # fill NaN to 0 and do cumsum
    cusum_trades_df = daily_trades_df.fillna(0).cumsum()

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
        db.dailyPrices.find(
            {"stockId": stock_id, "日期": {"$gte": months_before}},
            {"_id": 0, "收盤價": 1, "日期": 1},
        ).sort([("日期", ASCENDING)])
    )

    prices_df = pd.DataFrame(
        map(lambda e: {"price": e["收盤價"]}, prices),
        index=map(lambda p: p["日期"].astimezone(TPE_TIMEZONE).date(), prices),
    )

    #  print(prices_df.to_markdown())

    return prices_df


def main(stock_id, big_trader_threshold):

    trade_df = get_trade_dataframe(stock_id, big_trader_threshold)
    prices_df = get_price_dataframe(stock_id)
    last_day = prices_df.index[-1]

    # from matplotlib import font_manager
    # font_set = {f.name for f in font_manager.fontManager.ttflist}
    plt.rcParams["font.sans-serif"] = "Noto Sans TC"
    plt.rcParams["figure.dpi"] = 200

    ax = trade_df.plot(
        figsize=(10, 5),
        fontsize=15,
        style="-",
        linewidth=3,
        grid=True,
    )

    ax.set_title(
        f"{stock_id} (TH: {big_trader_threshold}), {last_day}",
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

    ax.xaxis.set_major_formatter(DateFormatter("%m-%d"))

    plt.minorticks_on()

    print(f"generate {stock_id} png")
    #  plt.show()
    ax.figure.savefig(
        f"{Path().parent}/png/{stock_id}_{last_day}.png", bbox_inches="tight"
    )


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
