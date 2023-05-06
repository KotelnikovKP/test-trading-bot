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

