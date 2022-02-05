import os
import requests
import json
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

pick_column = [
    # 損益表
    "Revenue",
    "GrossProfit",
    "SellingExpenses",
    "AdministrativeExpenses",
    "ResearchAndDevelopmentExpenses",
    "OperatingExpenses",
    "OperatingIncome",
    "ProfitBeforeTax",
    "NetIncome",
    "NetIncomeAttributableToOwnersOfTheParent",
    # 總資產
    "CashAndCashEquivalents",
    "ShortTermInvestment",
    "AccountsAndNotesReceivable",
    "Inventories",
    "CurrentAssets",
    "LongTermInvestment",
    "FixedAssets",
    "Assets",
    # 負債與股東權益
    "ShortTermBorrowings",
    "ShortTermNotesAndBillsPayable",
    "AccountsAndNotesPayable",
    "AdvanceReceipts",
    "LongTermLiabilitiesCurrentPortion",
    "CurrentLiabilities",
    "LongTermLiabilities",
    "Liabilities",
    "Equity",
    # 負債與股東權益的股東權益
    "CommonStocks",
    "RetainedEarnings",
    # 現金流量表
    "Depreciation",
    "Amortization",
    "OperatingCashFlow",
    "InvestingCashFlow",
    "FinancingCashFlow",
    "CAPEX",
    "FreeCashFlow",
    "NetCashFlow",
]

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()


def get_stock_infos(stock_id, since_year, until_year):
    url = f"https://statementdog.com/api/v2/fundamentals/{stock_id}/{since_year}/{until_year}/cf"

    res = requests.get(url)

    stock_info = json.loads(res.text)

    # 日曆年季度
    calendar_quarter = stock_info["common"]["TimeCalendarQ"]
    # 會計年季度
    fiscal_quarter = stock_info["common"]["TimeFiscalQ"]

    stock_quarter = stock_info["quarterly"]

    infos = []

    for idx in range(len(calendar_quarter["data"])):
        calendar_quarter_date = calendar_quarter["data"][idx][1]
        fiscal_quarter_count = fiscal_quarter["data"][idx][1]

        info = {}

        for column in pick_column:
            try:
                val = stock_quarter[column]["data"][idx][1]
                val = int(val) if val != "無" else 0

                info[stock_quarter[column]["label"]] = val
            except ValueError as e:
                print(
                    f"stock_id: {stock_id}, calendar_quarter: {calendar_quarter_date} raise Error"
                )
                print(f"column: {column}, ", e)

        try:
            val = stock_quarter["DebtRatio"]["data"][idx][1]
            val = float(val) if val != "無" else 0

            info[stock_quarter["DebtRatio"]["label"]] = val
        except ValueError as e:
            print(
                f"stock_id: {stock_id}, calendar_quarter: {calendar_quarter_date} raise Error"
            )
            print(f"column: {column}, ", e)

        infos.append(
            {
                **(
                    {
                        "stockId": stock_id,
                        calendar_quarter["label"]: datetime.strptime(
                            calendar_quarter_date, "%Y-%m-%d"
                        ),
                        fiscal_quarter["label"]: fiscal_quarter_count,
                    }
                ),
                **info,
            }
        )

    return infos


def main():
    stocks = list(db.stocks.find().sort([("stockId", 1)]))

    for stock in stocks:
        stock_id = stock["stockId"]

        print(f"stock id: {stock_id}")
        stock_infos = get_stock_infos(stock_id, 2012, 2021)

        now = datetime.now()
        for info in stock_infos:
            stock_id = info["stockId"]
            quarter = info["會計年季度"]

            print(f"stockId: {stock_id}, quarter year: {quarter}")

            info["createdAt"] = now
            db.stock_infos_quarter.update_one(
                {
                    "stockId": stock_id,
                    "會計年季度": quarter,
                },
                {
                    "$setOnInsert": info,
                },
                upsert=True,
            )

            year = int(info["會計年季度"][:-1])
            quarter_num = int(info["會計年季度"][-1])

            if quarter_num == 1:
                after_date = datetime(year, 5, 15)
            elif quarter_num == 2:
                after_date = datetime(year, 8, 14)
            elif quarter_num == 3:
                after_date = datetime(year, 11, 14)
            elif quarter_num == 4:
                after_date = datetime(year + 1, 3, 31)

            price = db.dailyPrices.find_one(
                {
                    "stockId": stock_id,
                    "日期": { "$gt": after_date, "$lt": after_date + timedelta(days=10) },
                },
                sort=[("日期", 1)]
            )

            db.prices_quarter.update_one(
                {
                    "stockId": stock_id,
                    "會計年季度": quarter,
                },
                {
                    "$setOnInsert": {
                        "stockId": stock_id,
                        "會計年季度": quarter,
                        "price": price,
                        "createdAt": now,
                    }
                },
                upsert=True
            )

        time.sleep(1)


if __name__ == "__main__":
    main()
