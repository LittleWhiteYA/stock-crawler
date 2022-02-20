import pydash

from stock import Stock

class Portfolio:
    def __init__(self, db, total_count, init_money, first_quarter):
        self.db = db
        self.trade_history = []
        self.current_quarter = first_quarter
        self.current_portfolio = []
        self.total_count = total_count
        self.init_money = init_money

    #  # FIXME: this function should move to class Stock
    #  def get_current_price(self, stock_id, quarter):
    #      price = self.db.prices_quarter.find_one(
    #          {
    #              "stockId": stock_id,
    #              "會計年季度": quarter,
    #          },
    #      )

    #      if not price:
    #          print(f'stock_id: {stock_id}, quarter: {quarter}, can not find price')
    #          return -1

    #      return price["price"]

    #  # FIXME: this function should move to class Stock
    #  def get_end_quarter_price(self, stock_id, quarter):
    #      from datetime import datetime, timedelta

    #      year = int(quarter[:-1])
    #      quarter_num = int(quarter[-1])

    #      if quarter_num == 1:
    #          after_date = datetime(year, 5, 15)
    #      elif quarter_num == 2:
    #          after_date = datetime(year, 8, 14)
    #      elif quarter_num == 3:
    #          after_date = datetime(year, 11, 14)
    #      elif quarter_num == 4:
    #          after_date = datetime(year + 1, 3, 31)

    #      price = self.db.dailyPrices.find_one(
    #          {
    #              "stockId": stock_id,
    #              "日期": { "$gt": after_date, "$lt": after_date + timedelta(days=10) },
    #          },
    #          sort=[("日期", 1)]
    #      )

    #      price = price["收盤價"]

    #      if not price:
    #          raise ValueError(f'missing price in stockId {stock_id}, quarter {quarter_num}')

    #      return price

    def buy(self, stock_id, quarter):
        if int(self.current_quarter) > int(quarter):
            raise ValueError(f'current quarter: {self.current_quarter}, buy quarter: {quarter}')

        stock = Stock(self.db, stock_id)
        price = stock.price_after_quarter_report(quarter, raise_error=True)

        self.current_portfolio.append({
            "stock_id": stock_id,
            "buy_price": price,
            "buy_unit": round(self.init_money / self.total_count / price, 4),
            "buy_quarter": quarter,
        })

    def sell(self, stock_id, quarter):
        if stock_id not in [stock["stock_id"] for stock in self.current_portfolio]:
            raise ValueError(f"stock_id does not exist in porfolio")

        stock = Stock(self.db, stock_id)
        price = stock.price_after_quarter_report(quarter, raise_error=True)

        if price != -1:
            stock = next(stock for stock in self.current_portfolio if stock["stock_id"] == stock_id)

            self.trade_history.append({
                **stock,
                "sell_price": price,
                "sell_quarter": quarter,
                "is_profit": stock["buy_price"] < price,
            })

        self.current_portfolio = pydash.remove(self.current_portfolio, lambda x: x["stock_id"] != stock_id)

    @property
    def history_profit(self):
        profit = 0

        for trade in self.trade_history:
            profit += (trade["sell_price"] - trade["buy_price"]) * trade["buy_unit"]

        for portfolio in self.current_portfolio:
            stock = Stock(self.db, portfolio["stock_id"])
            price = stock.price_after_quarter_report(self.current_quarter, raise_error=True)

            profit += (portfolio["buy_price"] - price) * portfolio["buy_unit"]

        return round(profit, 4)

    @property
    def return_on_investment(self):
        return round(self.history_profit / self.init_money * 100, 4)

    @property
    def win_rate(self):
        win_time = 0

        for trade in self.trade_history:
            if trade["buy_price"] < trade["sell_price"]:
                win_time += 1

        return round(win_time / len(self.trade_history), 4)
