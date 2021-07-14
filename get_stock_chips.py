import os
import requests
import json
import time
import pytz
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY, WEEKLY, MO
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")
TPE_TIMEZONE = pytz.timezone("Asia/Taipei")

mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()
chips_col_name = "chips"

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, "WANTGOO_MEMBER_TOKEN is missing")
cookie = f"member_token={WANTGOO_MEMBER_TOKEN}"

session = requests.Session()
adapter = HTTPAdapter(max_retries=5)
session.mount("http://", adapter)
session.mount("https://", adapter)


def get_date_range():

    SINCE_DATE = os.environ.get("SINCE_DATE")
    UNTIL_DATE = os.environ.get("UNTIL_DATE")
    DATE_INTERVAL = os.environ.get("DATE_INTERVAL")

    if not DATE_INTERVAL:
        date_interval = 7
    else:
        date_interval = int(DATE_INTERVAL)
        if date_interval not in [1, 7]:
            raise ValueError(date_interval, "Invalid date_interval: 1|7")

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")
    elif date_interval == 7:
        since_date = since_date + timedelta(days=-since_date.weekday(), weeks=1)
    else:  # from the first weekday of this week
        today = datetime.now()
        since_date = today + timedelta(days=-today.weekday())

    if UNTIL_DATE:
        until_date = datetime.strptime(UNTIL_DATE, "%Y-%m-%d")
    else:
        until_date = datetime.now()

    # check invalid
    if until_date > datetime.now():
        until_date = datetime.now()

    return (
        since_date.astimezone(tz=TPE_TIMEZONE),
        until_date.astimezone(tz=TPE_TIMEZONE),
        date_interval,
    )


def crawl_stock_date_chips(stock_id, since_date, until_date):
    since = since_date.strftime("%Y/%m/%d")
    until = until_date.strftime("%Y/%m/%d")

    url = (
        f"https://www.wantgoo.com/stock/{stock_id}/major-investors/branch-buysell-data?"
        "isOverBuy=true&"
        f"endDate={until}&"
        f"beginDate={since}"
    )

    res = session.get(
        url,
        headers={
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
            "(KHTML, likeGecko) Chrome/90.0.1234.56 Safari/537.36",
            "cookie": cookie,
        },
    )

    res = json.loads(res.text)

    return res["data"]


def get_stock_chips(stock_id, since_date, until_date, date_interval):
    if date_interval == 7:
        dates = [
            dt
            for dt in rrule(
                WEEKLY,
                dtstart=since_date,
                until=until_date,
                byweekday=[MO],
            )
        ]
    else:
        dates = [dt for dt in rrule(DAILY, dtstart=since_date, until=until_date)]

    for date in dates:
        since = date
        until = date
        if date_interval == 7:
            until += timedelta(days=4)

        print(f"date range: {since} ~ {until}")

        if until.date() >= datetime.now().date():
            print(f"until date {until} is gte today, skip")
            break

        chips = crawl_stock_date_chips(stock_id, since, until)

        doc = {
            "stockId": stock_id,
            "sinceDate": since,
            "untilDate": until,
        }

        db[chips_col_name].update_one(
            doc,
            {
                "$setOnInsert": {
                    **doc,
                    "data": chips,
                }
            },
            upsert=True,
        )

        time.sleep(1)
