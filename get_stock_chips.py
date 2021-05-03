import os
import requests
import json
import time
from requests.adapters import HTTPAdapter
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
WANTGOO_MEMBER_TOKEN = os.environ.get("WANTGOO_MEMBER_TOKEN")
COMPANY_CODES = os.environ.get("COMPANY_CODES")
UPDATE_EXISTED_COMPANY = os.environ.get("UPDATE_EXISTED_COMPANY")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

if not WANTGOO_MEMBER_TOKEN:
    raise ValueError(WANTGOO_MEMBER_TOKEN, "WANTGOO_MEMBER_TOKEN is missing")
cookie = f"member_token={WANTGOO_MEMBER_TOKEN}"

if not COMPANY_CODES and not UPDATE_EXISTED_COMPANY:
    raise ValueError(COMPANY_CODES, "COMPANY_CODES or UPDATE_EXISTED_COMPANY is missing")

session = requests.Session()
adapter = HTTPAdapter(max_retries=5)
session.mount("http://", adapter)
session.mount("https://", adapter)


def get_date_range():

    SINCE_DATE = os.environ.get('SINCE_DATE')
    UNTIL_DATE = os.environ.get('UNTIL_DATE')
    DATE_INTERVAL = os.environ.get('DATE_INTERVAL')

    if not DATE_INTERVAL:
        date_interval = 7
    else:
        date_interval = int(DATE_INTERVAL)
        if date_interval not in [1, 7]:
            raise ValueError(date_interval, 'Invalid date_interval: 1|7')

    if SINCE_DATE:
        since_date = datetime.strptime(SINCE_DATE, '%Y-%m-%d')
    elif date_interval == 7:
        since_date = since_date + timedelta(days=-since_date.weekday(), weeks=1)
    else:  # from the first weekday of this week
        today = datetime.now()
        since_date = today + timedelta(days=-today.weekday())

    if UNTIL_DATE:
        until_date = datetime.strptime(UNTIL_DATE, '%Y-%m-%d')
    else:
        until_date = datetime.now()

    # check invalid
    if until_date > datetime.now():
        until_date = datetime.now()

    return since_date, until_date, date_interval


def crawl_stock_date_chips(company_code, since_date, until_date):
    since = since_date.strftime("%Y/%m/%d")
    until = until_date.strftime("%Y/%m/%d")

    url = (
        f"https://www.wantgoo.com/stock/{company_code}/major-investors/branch-buysell-data?"
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

    return {
        "sinceDate": since_date,
        "untilDate": until_date,
        "data": res["data"],
    }


def get_stock_chips(company_code, since_date, until_date, date_interval):

    dates = [dt for dt in rrule(DAILY, dtstart=since_date, until=until_date)]
    dates = dates[0:: date_interval]

    for date in dates:

        since = date
        until = date + timedelta(days=date_interval - 1)
        print(f"date range: {since} ~ {until}")

        if until.date() >= datetime.now().date():
            print(f"until date {until} is gte today, skip")
            break

        chips = crawl_stock_date_chips(company_code, since, until)

        db[f"company_{company_code}"].update_one(
            {
                "sinceDate": chips["sinceDate"],
                "untilDate": chips["untilDate"],
            },
            {"$setOnInsert": chips},
            upsert=True,
        )

        time.sleep(3)


def main():

    since_date, until_date, date_interval = get_date_range()

    existed_company_codes = db.list_collection_names(
        filter={"name": {"$regex": "^company_\\d\\d\\d\\d$"}}
    )
    existed_company_codes = [item.replace('company_', '') for item in existed_company_codes]

    # crawl new
    if COMPANY_CODES:
        company_codes = COMPANY_CODES.split(",")
        for company_code in company_codes:
            print(f"since_date: {since_date}")
            print(f"until_date: {until_date}")
            print(f"company_code: {company_code}")

            if company_code in existed_company_codes:
                print(f"company_code {company_code} collection already exists, skip")
                continue

            get_stock_chips(company_code, since_date, until_date, date_interval)

    # update origin
    if UPDATE_EXISTED_COMPANY:
        for existed_code in existed_company_codes:
            col_name = f'company_{existed_code}'
            latest_data = db[col_name].find_one({}, sort=[("sinceDate", -1)])
            since_date = latest_data["sinceDate"]
            print(f"since_date: {since_date}")
            print(f"until_date: {until_date}")
            print(f"existed_company_code: {existed_code}")

            get_stock_chips(existed_code, since_date, until_date, date_interval)

    mongo_client.close()


if __name__ == '__main__':
    main()
