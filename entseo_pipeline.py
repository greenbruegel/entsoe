
import os
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, UTC
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
import logging
import time

# === Setup ===
load_dotenv()
API_KEY = os.getenv("API_KEY")
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "entsoe_db"
COLLECTION_NAME = "entsoe_test"

# === Logging setup ===
os.makedirs("logs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = f"logs/entsoe_log_{timestamp}.log"
logging.basicConfig(
    filename=log_file,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Namespaces ===
NS_GEN = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
NS_PRICE = {'ns': 'urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3'}


class EntsoePipeline:
    def __init__(self, api_key, mongo_uri, db_name, collection_name):
        self.api_key = api_key
        self.client = MongoClient(mongo_uri)
        self.collection = self.client[db_name][collection_name]

    def get_existing_fields_by_timestamp(self, bidding_zone):
        cursor = self.collection.find(
            {"bidding_zone": bidding_zone},
            {"timestamp": 1, "A75_A16_B16": 1, "A44_A01": 1, "A44_A07": 1}
        )
        mapping = {}
        for doc in cursor:
            ts = doc["timestamp"]
            keys = {k for k in ["A75_A16_B16", "A44_A01", "A44_A07"] if k in doc}
            mapping[ts] = keys
        return mapping

    def fetch_generation(self, eic, start, end):
        params = {
            "securityToken": self.api_key,
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": eic,
            "periodStart": start,
            "periodEnd": end,
            "psrType": "B16"
        }
        r = requests.get("https://web-api.tp.entsoe.eu/api", params=params)
        if r.status_code != 200:
            logging.warning(f"[GEN] HTTP {r.status_code}: {r.text[:300]}")
            return []
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            logging.warning(f"[GEN] XML parse error: {e}")
            return []

        results = []
        for ts in root.findall(".//ns:TimeSeries", NS_GEN):
            period = ts.find("ns:Period", NS_GEN)
            if period is None:
                continue
            start_time = datetime.fromisoformat(period.find("ns:timeInterval/ns:start", NS_GEN).text)
            resolution = period.find("ns:resolution", NS_GEN).text
            interval = {"PT15M": 15, "PT60M": 60}.get(resolution, 60)
            for point in period.findall("ns:Point", NS_GEN):
                pos = int(point.find("ns:position", NS_GEN).text)
                qty = float(point.find("ns:quantity", NS_GEN).text)
                ts = (start_time + timedelta(minutes=(pos - 1) * interval)).isoformat() + "Z"
                results.append((ts, qty))
        return results

    def fetch_prices(self, eic, start, end):
        params = {
            "securityToken": self.api_key,
            "documentType": "A44",
            "in_Domain": eic,
            "out_Domain": eic,
            "periodStart": start,
            "periodEnd": end
        }
        r = requests.get("https://web-api.tp.entsoe.eu/api", params=params)
        if r.status_code != 200:
            logging.warning(f"[PRICE] HTTP {r.status_code}: {r.text[:300]}")
            return {"A44_A01": [], "A44_A07": []}
        try:
            root = ET.fromstring(r.content)
        except ET.ParseError as e:
            logging.warning(f"[PRICE] XML parse error: {e}")
            return {"A44_A01": [], "A44_A07": []}

        results = {"A44_A01": [], "A44_A07": []}
        for ts in root.findall(".//ns:TimeSeries", NS_PRICE):
            ctype = ts.find("ns:contract_MarketAgreement.type", NS_PRICE)
            if ctype is None:
                continue
            label = f"A44_{ctype.text}"
            if label not in results:
                results[label] = []

            period = ts.find("ns:Period", NS_PRICE)
            if period is None:
                continue
            start_time = datetime.fromisoformat(
                period.find("ns:timeInterval/ns:start", NS_PRICE).text.replace("Z", "")
            )
            resolution = period.find("ns:resolution", NS_PRICE).text
            interval = {"PT15M": 15, "PT60M": 60}.get(resolution, 60)

            for point in period.findall("ns:Point", NS_PRICE):
                pos = point.find("ns:position", NS_PRICE)
                val = point.find("ns:price.amount", NS_PRICE)
                if pos is None or val is None:
                    continue
                ts_point = (start_time + timedelta(minutes=(int(pos.text) - 1) * interval)).isoformat() + "Z"
                results[label].append((ts_point, float(val.text) / 10))
        return results

    def run(self, bidding_zones):
        now = datetime.now(UTC)
        start = datetime(2025, 4, 1, tzinfo=UTC)
        window_size = timedelta(days=32)

        for zone, eic in bidding_zones.items():
            print(f"\\n🔄 Fetching data for {zone}")
            logging.info(f"Start processing {zone}")
            current = start
            existing = self.get_existing_fields_by_timestamp(zone)

            while current < now:
                period_start = current.strftime("%Y%m%d%H%M")
                next_month = (current + window_size).replace(day=1)
                period_end = min(next_month, now).strftime("%Y%m%d%H%M")

                print(f"🗓️  {zone}: {period_start} → {period_end}")
                logging.info(f"{zone}: {period_start} → {period_end}")

                gen_data = self.fetch_generation(eic, period_start, period_end)
                price_data = self.fetch_prices(eic, period_start, period_end)

                merged = {}
                for ts, val in gen_data:
                    if ts in existing and "A75_A16_B16" in existing[ts]:
                        continue
                    merged.setdefault(ts, {"bidding_zone": zone, "timestamp": ts})
                    merged[ts]["A75_A16_B16"] = val

                for label, series in price_data.items():
                    for ts, val in series:
                        if ts in existing and label in existing[ts]:
                            continue
                        merged.setdefault(ts, {"bidding_zone": zone, "timestamp": ts})
                        merged[ts][label] = val

                records = list(merged.values())
                if records:
                    self.collection.bulk_write([
                        UpdateOne(
                            {"bidding_zone": rec["bidding_zone"], "timestamp": rec["timestamp"]},
                            {"$set": rec},
                            upsert=True
                        ) for rec in records
                    ])
                    print(f"✅ Inserted/Updated {len(records)} records")
                    logging.info(f"{zone}: Inserted {len(records)} records")
                else:
                    print("ℹ️ No new data for this month")
                    logging.info(f"{zone}: No new data")

                current = next_month


if __name__ == "__main__":
    start_time = time.time()

    pipeline = EntsoePipeline(
        api_key=API_KEY,
        mongo_uri=MONGO_URI,
        db_name=DB_NAME,
        collection_name=COLLECTION_NAME
    )

    bidding_zones = {
        "AT": "10YAT-APG------L",
        "BE": "10YBE----------2"
    }

    pipeline.run(bidding_zones)

    duration = time.time() - start_time
    mins, secs = divmod(duration, 60)
    print(f"\\n🏁 Pipeline completed in {int(mins)} min {int(secs)} sec")
    print(f"📄 Log saved to: {log_file}")
