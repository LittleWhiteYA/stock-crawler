#  import pydash
from stock import Stock


class Portfolio:
    def __init__(self, db, init_money):
        self.db = db
        self.trade_history = []
        self.current_portfolio = []
        self.current_cash = init_money
        self.assets_history = []

    def buy_stocks(self, buy_stock_ids, quarter):
        cash_use_in_each_stock = self.current_cash / len(buy_stock_ids)

        for stock_id in buy_stock_ids:
            stock = Stock(self.db, stock_id)
            price = stock.price_after_quarter_report(quarter, raise_error=True)

            self.current_portfolio.append(
                {
                    "stock_id": stock_id,
                    "buy_price": price,
                    "buy_unit": round(cash_use_in_each_stock / price, 4),
                    "buy_quarter": quarter,
                }
            )

        self.current_cash = 0

    def sell_all_stocks(self, quarter, days_delay=None):
        for port in self.current_portfolio:
            stock = Stock(self.db, port["stock_id"])

            if not days_delay:
                price = stock.price_after_quarter_report(quarter)

                stock_unit = port["buy_unit"]

                if price != -1:
                    self.current_cash += stock_unit * price

                    self.trade_history.append(
                        {
                            **port,
                            "sell_price": price,
                            "sell_quarter": quarter,
                            "is_profit": port["buy_price"] < price,
                        }
                    )
                else:
                    # FIXME: should get last price before stock disappeared
                    self.current_cash += stock_unit * port["buy_price"]

                    self.trade_history.append(
                        {
                            **port,
                            "sell_price": -1,
                            "sell_quarter": quarter,
                            "is_profit": None,
                        }
                    )
            else:
                price = stock.get_price_from_daily_collection(quarter, days_delay)

                stock_unit = port["buy_unit"]
                self.current_cash += stock_unit * price

                self.trade_history.append(
                    {
                        **port,
                        "sell_price": price,
                        "sell_quarter": quarter,
                        "is_profit": port["buy_price"] < price,
                    }
                )

        self.current_portfolio = []

        self.assets_history.append(
            {
                "assets": round(self.current_cash, 4),
                "quarter": quarter,
            }
        )

    #  def sell_stocks(self, sell_stock_ids, quarter):
    #      for stock_id in sell_stock_ids:
    #          if stock_id not in [stock["stock_id"] for stock in self.current_portfolio]:
    #              raise ValueError(f"stock_id {stock_id} does not exist in portfolio")

    #          stock = Stock(self.db, stock_id)
    #          price = stock.price_after_quarter_report(quarter, raise_error=True)

    #          if price != -1:
    #              stock = next(stock for stock in self.current_portfolio if stock["stock_id"] == stock_id)

    #              self.trade_history.append({
    #                  **stock,
    #                  "sell_price": price,
    #                  "sell_quarter": quarter,
    #                  "is_profit": stock["buy_price"] < price,
    #              })

    #          self.current_portfolio = pydash.remove(self.current_portfolio, lambda x: x["stock_id"] != stock_id)

    def get_history_profit(self, end_quarter):
        profit = 0

        for trade in self.trade_history:
            profit += (trade["sell_price"] - trade["buy_price"]) * trade["buy_unit"]

        for portfolio in self.current_portfolio:
            stock = Stock(self.db, portfolio["stock_id"])
            price = stock.price_after_quarter_report(end_quarter, raise_error=True)

            profit += (portfolio["buy_price"] - price) * portfolio["buy_unit"]

        return round(profit, 4)

    @property
    def win_rate(self):
        win_time = 0

        for trade in self.trade_history:
            if trade["is_profit"] is True:
                win_time += 1

        return round(win_time / len(self.trade_history), 4)
