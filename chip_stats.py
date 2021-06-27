import os
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
THRESHOLD = os.environ.get("THRESHOLD")

company_default_threshold = [
    {
        "company_code": "company_1301",
        "default_threshold": 4000,
    },
    {
        "company_code": "company_1539",
        "default_threshold": 100,
    },
    {
        "company_code": "company_1786",
        "default_threshold": 100,
    },
    {
        "company_code": "company_1810",
        "default_threshold": 500,
    },
    {
        "company_code": "company_2065",
        "default_threshold": 150,
    },
    {
        "company_code": "company_2352",
        "default_threshold": 5000,
    },
    {
        "company_code": "company_2451",
        "default_threshold": 1000,
    },
    {
        "company_code": "company_4930",
        "default_threshold": 400,
    },
    {
        "company_code": "company_4994",
        "default_threshold": 100,
    },
    {
        "company_code": "company_5425",
        "default_threshold": 900,
    },
    {
        "company_code": "company_5490",
        "default_threshold": 150,
    },
    {
        "company_code": "company_6505",
        "default_threshold": 2000,
    },
    {
        "company_code": "company_8446",
        "default_threshold": 200,
    },
    {
        "company_code": "company_9924",
        "default_threshold": 200,
    },
    {
        "company_code": "company_9958",
        "default_threshold": 700,
    },
]


mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


def get_company_code():
    company_code = input("input company code: ")

    if company_code == "all":
        return "all"

    if not company_code:
        raise ValueError(company_code, "company_code is missing")

    company_code = f"company_{company_code}"
    return company_code


def get_threshold(company_code):
    if THRESHOLD:
        big_trader_threshold = int(THRESHOLD)
    else:
        threshold = next(
            (
                config["default_threshold"]
                for config in company_default_threshold
                if config["company_code"] == company_code
            ),
            None,
        )

        big_trader_threshold = threshold if threshold else 100

    return big_trader_threshold


def main(company_code, big_trader_threshold):
    weekly_trades = list(
        db[company_code].find({"sinceDate": {"$gte": datetime(2021, 1, 1)}})
    )

    if not weekly_trades:
        raise ValueError(
            weekly_trades, f"company code {company_code} may not be crawled yet"
        )

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

    # from matplotlib import font_manager
    # font_set = {f.name for f in font_manager.fontManager.ttflist}
    plt.rcParams["font.sans-serif"] = "Noto Sans CJK JP"
    plt.rcParams["figure.dpi"] = 200

    ax = big_trades_df.plot(
        figsize=(10, 5),
        fontsize=15,
        style="o-",
        grid=True,
        legend=2,
    )

    ax.set_title(
        f"{company_code} (threshold: {big_trader_threshold})",
        fontsize=30,
    )
    ax.set_xlabel("date", fontsize=10)
    ax.set_ylabel("count", fontsize=10)
    plt.minorticks_on()
    plt.xticks(rotation=25)

    ax.legend(loc="upper left", prop={"size": 15})

    print(f"generate {company_code} png")
    #  plt.show()
    ax.figure.savefig(f"png/{company_code}.png")


if __name__ == "__main__":
    company_code = get_company_code()

    if company_code == "all":
        for company in company_default_threshold:
            company_code = company["company_code"]
            big_trader_threshold = get_threshold(company_code)
            main(company_code, big_trader_threshold)
    else:
        while True:
            big_trader_threshold = get_threshold(company_code)
            main(company_code, big_trader_threshold)

            company_code = get_company_code()
