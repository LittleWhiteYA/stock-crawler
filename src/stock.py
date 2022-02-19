from pymongo import ASCENDING
import pandas as pd
import pytz

TPE_TIMEZONE = pytz.timezone("Asia/Taipei")


class StockQuarter:
    def __init__(self, db, quarter_info):
        self.db = db
        self.quarter_info = quarter_info
        self.stock = Stock(db, quarter_info["stockId"])

    @property
    def EV(self):
        # EV＝市值＋總負債－總現金
        # https://smart.businessweekly.com.tw/Reading/WebArticle.aspx?id=65053&p=1

        price = self.stock.price_after_quarter_report(self.quarter_info["會計年季度"])
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


class Stock:
    def __init__(self, db, id):
        self.id = id
        self.db = db
        self.quarter_price_map = {}

    def price_after_quarter_report(self, quarter):
        if self.quarter_price_map[quarter]:
            return self.quarter_price_map[quarter]

        price_quarter = self.db.prices_quarter.find_one(
            {
                "stockId": self.stock.id,
                "會計年季度": quarter,
            },
        )

        self.quarter_price_map[quarter] = price_quarter["price"]

        #  if not price:
        #      raise ValueError(f'missing price in stockId {self.stock.id}, year {year}')

        return self.quarter_price_map[quarter]

    def get_custom_threshold(self):
        stock = self.db["stocks"].find_one({"stockId": self.id})

        default_threshold = 100

        big_trader_threshold = (
            stock["threshold"] if "threshold" in stock else default_threshold
        )

        return big_trader_threshold

    def get_trade_df(self, chips_since_date, big_trader_threshold):
        chips_collection = "chips"
        date = "日期"
        buyNum = "買張"
        sellNum = "賣張"
        agentName = "分點名稱"

        daily_trades = list(
            self.db[chips_collection].find({"stockId": self.id, date: {"$gte": chips_since_date}})
        )

        if not daily_trades:
            raise ValueError(daily_trades, f"stock id {self.id} may not be crawled yet")

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

    def get_price_dataframe(self, chips_since_date):
        prices = list(
            self.db.dailyPrices.find(
                {"stockId": self.id, "日期": {"$gte": chips_since_date}},
                {"_id": 0, "收盤價": 1, "日期": 1},
            ).sort([("日期", ASCENDING)])
        )

        prices_df = pd.DataFrame(
            map(lambda e: {"price": e["收盤價"]}, prices),
            index=map(lambda p: p["日期"].astimezone(TPE_TIMEZONE).date(), prices),
        )

        #  print(prices_df.to_markdown())

        return prices_df

