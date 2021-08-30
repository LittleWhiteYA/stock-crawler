import os
import requests
import json
import time
import pytz
from datetime import datetime, timedelta
from dateutil.rrule import rrule, WEEKLY, MO
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")
WANTGOO_BID = os.environ.get("WANTGOO_BID")
WANTGOO_CLIENT_FINGERPRINT = os.environ.get("WANTGOO_CLIENT_FINGERPRINT")
WANTGOO_CLIENT_SIGNATURE = os.environ.get("WANTGOO_CLIENT_SIGNATURE")
TPE_TIMEZONE = pytz.timezone("Asia/Taipei")

mongo_client = MongoClient(MONGO_URL, tz_aware=True)
db = mongo_client.get_default_database()
chips_col_name = "chips"

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, "WANTGOO_MEMBER_TOKEN is missing")


def get_date_range(SINCE_DATE, UNTIL_DATE):
    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")
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
    )


def crawl_stock_date_chips(stock_id, since_date, until_date):
    since = since_date.strftime("%Y/%m/%d")
    until = until_date.strftime("%Y/%m/%d")

    url = f"https://www.wantgoo.com/stock/{stock_id}/major-investors/branch-buysell-data"
    referer = f"https://www.wantgoo.com/stock/{stock_id}/major-investors/branch-buysell"

    res = requests.get(
        url,
        params={
            "isOverBuy": "true",
            "beginDate": since,
            "endDate": until,
        },
        headers={
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36",
            "x-client-signature": WANTGOO_CLIENT_SIGNATURE,
            "referer": referer,
        },
        cookies={
            "member_token": WANTGOO_MEMBER_TOKEN,
            "BID": WANTGOO_BID,
            "client_fingerprint": WANTGOO_CLIENT_FINGERPRINT,
        },
    )

    res = json.loads(res.text)

    return res["data"]


def get_stock_chips(stock_id, since_date, until_date):
    dates = [
        dt
        for dt in rrule(
            WEEKLY,
            dtstart=since_date,
            until=until_date,
            byweekday=[MO],
        )
    ]

    for date in dates:
        since = date
        until = date + timedelta(days=4)

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
