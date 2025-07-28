import json
import os
import requests
from datetime import datetime
from app.config.settings import API_URL, CURSOR_FILE, PAGE_NUMBER, LIMIT, BATCH_SIZE, RAW_DATA_DIR, KAFKA_HOST, KAFKA_TOPIC_NAME
from kafka import KafkaProducer

class SaasIngestor:
    def __init__(self):
        self.api_url = API_URL
        self.cursor_file = CURSOR_FILE
        self.next_page_number = int(PAGE_NUMBER)
        self.limit = int(LIMIT)
        self.batch_size = int(BATCH_SIZE)
        self.last_page_count = 0
        self.raw_data_dir = RAW_DATA_DIR
        self.load_cursor()
        self.kafka_topic_name = KAFKA_TOPIC_NAME
        self.producer = KafkaProducer(bootstrap_servers=KAFKA_HOST)


    def load_cursor(self):
        try:
            with open(self.cursor_file, "r") as f:
                cursor_data = json.load(f)
                self.next_page_number = int(cursor_data.get("next_page_number", 1))
                self.last_page_count = int(cursor_data.get("last_page_count", 0))
        except FileNotFoundError:
            self.next_page_number = 1
            self.last_page_count = 0


    def save_cursor(self):
        with open(self.cursor_file, "w") as f:
            json.dump({"next_page_number": self.next_page_number, "last_page_count": self.last_page_count}, f)
        print(f"[INFO] Cursor updated")

    def fetch_data(self):
        accumulated_data = []
        count = self.last_page_count

        while len(accumulated_data) < self.batch_size:
            try:

                fully_read = self.last_page_count != 0 and self.last_page_count < self.limit

                if fully_read:
                    self.next_page_number -= 1

                response = requests.get(f"{self.api_url}/user?page={self.next_page_number}&limit={self.limit}")
                response.raise_for_status()
                all_log = response.json()
                data = all_log.get("data")

                
                if not data:
                    print("[INFO] No data available from API.")
                    break

                if fully_read:
                    print(f"Skipping {self.last_page_count} already processed records")
                    data = data[self.last_page_count:]

                space_left = self.batch_size - len(accumulated_data)


                if len(data) > space_left:
                    data = data[:space_left]
                    print(f"Truncating to fit batch size: {space_left} items")

                accumulated_data.extend(data)
                count += len(data)
                if count > self.limit:
                    count -= self.limit

                self.next_page_number += 1

                self.last_page_count = count

            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Failed to fetch data: {e}")
                continue

        return accumulated_data 

    def save_data(self, data):
        os.makedirs(self.raw_data_dir, exist_ok=True)
        filename = f"logs_{datetime.now().isoformat().replace(':', '-')}.json"
        filepath = os.path.join(self.raw_data_dir, filename)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"[INFO] Saved {len(data)} logs to {filepath}")
        return filepath
    
    def send_to_kafka_queue(self, filepath):
        self.producer.send(self.kafka_topic_name, filepath.encode('utf-8'))
        self.producer.flush()
    

    def run(self):
        data = self.fetch_data()

        if data:
            filepath = self.save_data(data)
            self.send_to_kafka_queue(filepath)
            self.save_cursor()
        else:
            print("[INFO] No new data found")
