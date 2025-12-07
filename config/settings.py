import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# API Keys
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if not GEMINI_API_KEY:
    print(" WARNING: GEMINI_API_KEY is missing! Add it to your .env file.")

# Gemini Model Settings
GEMINI_TEXT_MODEL = "models/gemini-2.5-flash"  
GEMINI_VISION_MODEL = "models/gemini-2.5-pro"  

OCR_LANG = "en"

# Base Directories
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_DIR = os.path.join(DATA_DIR, "input")
PDF_DIR = os.path.join(INPUT_DIR, "pdfs")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

# File Paths
CSV_INPUT_PATH = os.path.join(INPUT_DIR, "providers.csv")

# processed data
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")

# PDF raw extraction files
RAW_PDF_TEXT_PATH = os.path.join(PROCESSED_DIR, "pdf_raw_text.json")
RAW_OCR_OUTPUT_PATH = os.path.join(PROCESSED_DIR, "pdf_ocr_output.json")

# output CSVs
UPDATED_CSV = os.path.join(OUTPUT_DIR, "updated_providers.csv")
REVIEW_QUEUE_CSV = os.path.join(OUTPUT_DIR, "manual_review_queue.csv")

# excel reports
EXCEL_REPORT_PATH = os.path.join(OUTPUT_DIR, "provider_report.xlsx")

# Database Settings
DATABASE_PATH = os.path.join(BASE_DIR, "database", "provider_data.db")
AUDIT_LOG_PATH = os.path.join(BASE_DIR, "database", "audit_logs.db")

# Confidence Scoring
CONFIDENCE_THRESHOLD = 0.80

WEIGHT_PDF = 0.45
WEIGHT_GOOGLE = 0.15
WEIGHT_SIMULATED_NPI = 0.40   # NPI is simulated for India

# Feature Toggles
ENABLE_PDF_EXTRACTION = True
ENABLE_GOOGLE_VALIDATION = True
ENABLE_MAP_VALIDATION = True       # NEW
ENABLE_WEBSCRAPING = True
ENABLE_SIMULATED_NPI_LOOKUP = True

# System Settings
DEBUG = True
MAX_PROVIDERS_PER_BATCH = 200

TEMP_DIR = os.path.join(BASE_DIR, "temp")
LOG_FILE = os.path.join(BASE_DIR, "system.log")

# Ensure required directories exist
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
