import os
import requests
import json
import time
from datetime import timedelta, datetime
from dateutil.rrule import rrule, DAILY
from dotenv import load_dotenv
from urllib.parse import urlencode

load_dotenv()

CMONEY_ACCOUNT = os.environ.get("CMONEY_ACCOUNT")
CMONEY_HASHED_PASSWORD = os.environ.get("CMONEY_HASHED_PASSWORD")


class StockCrawler:
    def __init__(self, stock_id, date_column):
        self.stock_id = stock_id
        self.date_column = date_column
        self.access_token = None

    def get_daily_prices(self, since_date, until_date):
        url = "https://api.cmoney.tw/MobileService/ashx/GetDtnoData.ashx"

        days = (datetime.now() - since_date).days

        api_paramStr = (
            f"AssignID={self.stock_id};DTMode=0;DTRange={days};"
            "DTOrder=1;MajorTable=M002;MTPeriod=0;"
        )

        params = {
            "paramStr": api_paramStr,
            "dtNo": "10519905",
            "action": "GetDtNoData",
            "filterNo": "0",
        }

        time.sleep(0.1)
        res = requests.get(url, params=urlencode(params, safe=";"))

        daily_prices = json.loads(res.text)

        format_daily_prices = []

        for data in daily_prices["Data"]:
            date = datetime.strptime(data[0], "%Y%m%d")
            if date <= since_date:
                continue

            if date > until_date:
                continue

            format_daily_prices.append(
                {
                    "stockId": self.stock_id,
                    self.date_column: date,
                    "開盤價": float(data[1]),
                    "最高價": float(data[2]),
                    "最低價": float(data[3]),
                    "收盤價": float(data[4]),
                    "漲跌": float(data[5]),
                    "漲幅(%)": float(data[6]),
                    "成交量": float(data[7]),
                }
            )

        return format_daily_prices

    def get_chips(self, since_date, until_date):
        dates = [
            dt
            for dt in rrule(
                DAILY,
                dtstart=since_date,
                until=until_date - timedelta(days=1),
                byweekday=[0, 1, 2, 3, 4],
            )
        ]

        daily_chips = []

        for date in dates:
            print(f"date: {date}")

            time.sleep(0.1)
            chips = self.get_date_chips(date)

            daily_chips.append(
                {
                    "stockId": self.stock_id,
                    self.date_column: date,
                    "data": chips,
                }
            )

        return daily_chips

    def get_date_chips(self, date):
        access_token = self.get_cmoney_access_token()

        date_str = date.strftime("%Y%m%d")

        params = f"{self.stock_id}_{date_str}_1_1"
        chips_url = "http://datasv.cmoney.tw:5000/api/chipk"

        res = requests.get(
            chips_url,
            params={
                "appId": "2",
                "needLog": "true",
                "fundId": "2",
                "params": params,
            },
            headers={
                "Authorization": f"Bearer {access_token}",
            },
        )

        daily_chips = json.loads(res.text)

        format_daily_chips = []

        for data in daily_chips["data"]:
            format_daily_chips.append(
                {
                    "分點代號": data[1],
                    "分點名稱": data[2],
                    "買張": int(data[3]),
                    "賣張": int(data[4]),
                    "買金額": int(data[5]),
                    "賣金額": int(data[6]),
                }
            )

        return format_daily_chips

    def get_cmoney_access_token(self):
        if self.access_token:
            return self.access_token

        access_token_url = "https://api.cmoney.tw/identity/token"

        data = {
            "account": CMONEY_ACCOUNT,
            "hashed_password": CMONEY_HASHED_PASSWORD,
            "grant_type": "password",
            "client_id": "cmchipkmobile",
            "login_method": "email",
        }

        res = requests.post(access_token_url, data=data)

        token_info = json.loads(res.text)

        self.access_token = token_info["access_token"]

        return self.access_token

    def get_year_report(self, since_year, until_year):
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

        url = f"https://statementdog.com/api/v2/fundamentals/{self.stock_id}/{since_year}/{until_year}/cf"

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
                        f"stock_id: {self.stock_id}, calendar_quarter: {calendar_quarter_date} raise Error"
                    )
                    print(f"column: {column}, ", e)

            try:
                val = stock_quarter["DebtRatio"]["data"][idx][1]
                val = float(val) if val != "無" else 0

                info[stock_quarter["DebtRatio"]["label"]] = val
            except ValueError as e:
                print(
                    f"stock_id: {self.stock_id}, calendar_quarter: {calendar_quarter_date} raise Error"
                )
                print(f"column: {column}, ", e)

            infos.append(
                {
                    **(
                        {
                            "stockId": self.stock_id,
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
