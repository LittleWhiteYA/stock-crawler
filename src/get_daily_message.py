import os
import requests
import time
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from urllib.parse import quote
from datetime import datetime
from dateutil.rrule import rrule, DAILY
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.environ.get("MONGO_URL")
SINCE_DATE = os.environ.get("SINCE_DATE")
UNTIL_DATE = os.environ.get("UNTIL_DATE")

mongo_client = MongoClient(MONGO_URL)
db = mongo_client.get_default_database()

if SINCE_DATE:
    since_date = datetime.strptime(SINCE_DATE, "%Y-%m-%d")
else:
    latest_msg = db.company_daily_messages.find_one({}, sort=[("date", -1)])
    if latest_msg:
        since_date = latest_msg["date"]
    else:
        since_date = datetime.now()

if UNTIL_DATE:
    until_date = datetime.strptime(UNTIL_DATE, "%Y-%m-%d")
else:
    until_date = datetime.now()

print(f"since_date: {since_date}")
print(f"until_date: {until_date}")

url = "https://mops.twse.com.tw/mops/web/ajax_t05st02"


def get_response(date):
    year = date.year - 1911
    month = date.month
    day = date.day
    print(f"date: {year}/{month}/{day}")

    data = {
        "encodeURIComponent": 1,
        "step": 1,
        "step00": 0,
        "firstin": 1,
        "off": 1,
        "TYPEK": "all",
        "year": year,
        "month": month,
        "day": day,
    }

    retry_time = 0
    while retry_time < 10:
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=5)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        res = session.post(url, data=data)

        if "overrun" not in res.text.lower():
            return res

        print("response text contains 'overrun', sleep 10 s")
        time.sleep(10)
        retry_time += 1

    raise Exception("failed after crawler retry 10 time")


def get_company_messages(date):
    res = get_response(date)

    soup = BeautifulSoup(res.text, "html.parser")

    company_msg = []
    for tr in soup.find("form", {"action": "/mops/web/ajax_t05st02"}).find_all("tr"):
        tds = tr.find_all("td")
        if tds:
            inputs = tds[5].find_all("input")

            onclick_val = tds[5].find("input", onclick=True)["onclick"]
            if "sii" in onclick_val:
                typek = "sii"
            elif "rotc" in onclick_val:
                typek = "rotc"
            elif "otc" in onclick_val:
                typek = "otc"
            elif "pub" in onclick_val:
                continue
            else:
                print("get strange typek")
                print(onclick_val)
                continue

            row = [td.text.strip().replace("\r\n", "") for td in tds]

            date_str, time, code, name, title, _ = row
            date_str = str(int(date_str[:3]) + 1911) + date_str[3:]
            date = datetime.strptime(date_str, "%Y/%m/%d")

            url = (
                "https://mops.twse.com.tw/mops/web/t05st02?"
                "step=1&off=1&firstin=1&"
                f"TYPEK={typek}&i=1&"
                f"h10={quote(inputs[0]['value'])}&"
                f"h11={quote(inputs[1]['value'])}&"
                f"h12={quote(inputs[2]['value'])}&"
                f"h13={quote(inputs[3]['value'])}&"
                f"h14={quote(inputs[4]['value'])}&"
                f"h15={quote(inputs[5]['value'])}&"
                "pgname=t05st02"
            )

            company_msg.append(
                {
                    "date": date,
                    "time": time,
                    "company_code": code,
                    "company_name": name,
                    "title": title,
                    "typek": typek,
                    "url": url,
                }
            )

    return company_msg


dates = [
    dt
    for dt in rrule(
        DAILY,
        dtstart=since_date,
        until=until_date,
    )
]

for date in dates:
    company_msg = get_company_messages(date)

    for msg in company_msg:
        db.company_daily_messages.update_one(
            {
                "date": msg["date"],
                "time": msg["time"],
                "company_code": msg["company_code"],
            },
            {"$setOnInsert": msg},
            upsert=True,
        )

    time.sleep(3)

mongo_client.close()
