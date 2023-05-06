import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

dotenv_path = os.path.join(BASE_DIR, '.env')

if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

DEBUG = os.environ.get('DEBUG')

try:
    ADDED_DELAY = float(os.environ.get('ADDED_DELAY'))
except:
    ADDED_DELAY = 0.00

DATABASE_HOST = os.environ.get('DATABASE_HOST')
DATABASE_PORT = os.environ.get('DATABASE_PORT')
DATABASE_NAME = os.environ.get('DATABASE_NAME')
DATABASE_USER = os.environ.get('DATABASE_USER')
DATABASE_PASSWORD = os.environ.get('DATABASE_PASSWORD')

BYBIT_API_KEY = os.environ.get('BYBIT_API_KEY')
BYBIT_API_SECRET = os.environ.get('BYBIT_API_SECRET')

BASE_SYMBOLS = os.environ.get('BASE_SYMBOLS').split(',')
SYMBOLS = os.environ.get('SYMBOLS').split(',')
for s in BASE_SYMBOLS:
    if s not in SYMBOLS:
        SYMBOLS.append(s)

try:
    PARALLEL_REQUESTS = int(os.environ.get('PARALLEL_REQUESTS'))
except:
    PARALLEL_REQUESTS = 100

try:
    LIMIT_PER_HOST = int(os.environ.get('LIMIT_PER_HOST'))
except:
    LIMIT_PER_HOST = 0

try:
    LIMIT = int(os.environ.get('LIMIT'))
except:
    LIMIT = 0

try:
    TTL_DNS_CACHE = int(os.environ.get('TTL_DNS_CACHE'))
except:
    TTL_DNS_CACHE = 300

