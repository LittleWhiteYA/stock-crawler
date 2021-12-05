import os
import requests
import json
import time
from datetime import timedelta, datetime
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


def get_access_token():
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

    return token_info["access_token"]


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


def get_stock_chips(stock_id, since_date, until_date, access_token):
    date_column = "日期"
    chips_collection = "chips"

    latest_data = db[chips_collection].find_one(
        {"stockId": stock_id}, sort=[(date_column, -1)]
    )
    since_date = (
        latest_data[date_column] + timedelta(days=1) if latest_data else since_date
    )
    print(f"stock id: {stock_id}")
    print(f"since_date: {since_date}")
    print(f"until_date: {until_date}")

    dates = [
        dt
        for dt in rrule(
            DAILY,
            dtstart=since_date,
            until=until_date - timedelta(days=1),
            byweekday=[0, 1, 2, 3, 4],
        )
    ]

    now = datetime.now()

    # TODO: move to update_stock_infos.py
    for date in dates:
        print(f"date: {date}")

        chips = crawl_stock_date_chips(stock_id, date, access_token)

        doc = {
            "stockId": stock_id,
            date_column: date,
        }

        db[chips_collection].update_one(
            doc,
            {
                "$setOnInsert": {
                    **doc,
                    "data": chips,
                    "createdAt": now,
                }
            },
            upsert=True,
        )

        time.sleep(randint(1, 3))
