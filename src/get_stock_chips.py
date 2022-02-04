import os
import requests
import json
import time
from datetime import timedelta
from dateutil.rrule import rrule, DAILY
from pymongo import MongoClient
from dotenv import load_dotenv
from random import randint

load_dotenv()

CMONEY_ACCOUNT = os.environ.get("CMONEY_ACCOUNT")
CMONEY_HASHED_PASSWORD = os.environ.get("CMONEY_HASHED_PASSWORD")

MONGO_URL = os.environ.get("MONGO_URL")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

global_access_token = None


def get_access_token():
    global global_access_token

    if global_access_token:
        return global_access_token

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

    global_access_token = token_info["access_token"]

    return global_access_token


def crawl_stock_date_chips(stock_id, date, access_token):
    date_str = date.strftime("%Y%m%d")

    params = f"{stock_id}_{date_str}_1_1"
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


def get_stock_chips(stock_id, since_date, until_date):
    date_column = "日期"
    access_token = get_access_token()

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
        chips = crawl_stock_date_chips(stock_id, date, access_token)

        daily_chips.append(
            {
                "stockId": stock_id,
                date_column: date,
                "data": chips,
            }
        )

    return daily_chips
