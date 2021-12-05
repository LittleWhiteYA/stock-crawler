import requests
import json
from datetime import datetime
from urllib.parse import urlencode


url = "https://api.cmoney.tw/MobileService/ashx/GetDtnoData.ashx"


def get_stock_prices(stock_id, since_date):
    days = (datetime.now() - since_date).days

    api_paramStr = (
        f"AssignID={stock_id};DTMode=0;DTRange={days};"
        "DTOrder=1;MajorTable=M002;MTPeriod=0;"
    )

    params = {
        "paramStr": api_paramStr,
        "dtNo": "10519905",
        "action": "GetDtNoData",
        "filterNo": "0",
    }

    res = requests.get(url, params=urlencode(params, safe=";"))

    daily_prices = json.loads(res.text)

    format_daily_prices = []

    for data in daily_prices["Data"]:
        date = datetime.strptime(data[0], "%Y%m%d")
        if date <= since_date:
            continue

        format_daily_prices.append(
            {
                "stockId": stock_id,
                "日期": date,
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
