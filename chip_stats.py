import os
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
COMPANY_CODE = os.environ.get("COMPANY_CODE")
WEEKLY_THRESHOLD = os.environ.get("WEEKLY_THRESHOLD")
TOTAL_THRESHOLD = os.environ.get("TOTAL_THRESHOLD")

if not COMPANY_CODE:
    COMPANY_CODE = input("input company code: ")

if not COMPANY_CODE:
    raise ValueError(COMPANY_CODE, "COMPANY_CODE is missing")

company_code = f"company_{COMPANY_CODE}"

company_default_threshold = [
    {
        "company_code": "company_1539",
        "default_threshold": 100,
    },
    {
        "company_code": "company_1786",
        "default_threshold": 100,
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
        "company_code": "company_8446",
        "default_threshold": 200,
    },
    {
        "company_code": "company_9924",
        "default_threshold": 200,
    },
]

if WEEKLY_THRESHOLD:
    weekly_big_trader_threshold = int(WEEKLY_THRESHOLD)
else:
    weekly_big_trader_threshold = 0

if TOTAL_THRESHOLD:
    total_big_trader_threshold = int(TOTAL_THRESHOLD)
else:
    threshold = next(
        (
            config["default_threshold"]
            for config in company_default_threshold
            if config["company_code"] == company_code
        ),
        None,
    )

    total_big_trader_threshold = threshold if threshold else 100

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

weekly_trades = list(db[company_code].find({}))

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
    big_traders = list(
        filter(
            lambda trade: abs(trade["quantitiesCount"]) >= weekly_big_trader_threshold,
            count_weekly_traders,
        )
    )

    weekly_trades_stats.append(
        {trader["agentName"]: trader["quantitiesCount"] for trader in big_traders}
    )

weekly_trades_df = pd.DataFrame(
    weekly_trades_stats, index=map(lambda trade: trade["sinceDate"], weekly_trades)
)

# fill NaN to 0 and do cumsum
cusum_trades_df = weekly_trades_df.fillna(0).cumsum()

# filter columns according to last row value
big_trader_filter = abs(cusum_trades_df[-1:].squeeze()) > total_big_trader_threshold

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

ax = big_trades_df.plot(figsize=(20, 10), fontsize=15, style="o-", grid=True, legend=2)
ax.set_title(
    f"{company_code} (threshold: {weekly_big_trader_threshold}, {total_big_trader_threshold})",
    fontsize=30,
)
ax.set_xlabel("date", fontsize=10)
ax.set_ylabel("count", fontsize=10)
ax.legend(loc="upper left", prop={"size": 10})

plt.show()
