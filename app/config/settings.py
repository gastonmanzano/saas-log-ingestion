import os
from dotenv import load_dotenv

load_dotenv()

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "raw_data")
CURSOR_FILE = os.getenv("CURSOR_FILE", "cursor.json")
API_URL = os.getenv("API_URL", "http://localhost:3000/api")
PAGE_NUMBER = os.getenv("PAGE_NUMBER", "1")
LIMIT = os.getenv("LIMIT", "100")
BATCH_SIZE = os.getenv("BATCH_SIZE", "1000")