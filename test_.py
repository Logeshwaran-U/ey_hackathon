# from google import genai
# from dotenv import load_dotenv
# import os

# load_dotenv()

# API_KEY = os.getenv("GEMINI_API_KEY")
# client = genai.Client(api_key=API_KEY)

# try:
#     response = client.models.generate_content(
#         model="models/gemini-2.5-flash",
#         contents="Hello Gemini! Test connection successful?"
#     )
#     print("✔ GEMINI API WORKING!")
#     print(response.text)

# except Exception as e:
#     print("❌ Error:", e)


# from config import settings

# print("GEMINI_API_KEY:", settings.GEMINI_API_KEY[:6] + "*******")
# print("TEXT MODEL:", settings.GEMINI_TEXT_MODEL)
# print("VISION MODEL:", settings.GEMINI_VISION_MODEL)

# print("CSV PATH:", settings.CSV_INPUT_PATH)
# print("PDF DIR:", settings.PDF_DIR)
# print("DATABASE PATH:", settings.DATABASE_PATH)

# print("All directories exist:")
# print(" - TEMP:", settings.TEMP_DIR)
# print(" - PROCESSED:", settings.PROCESSED_DIR)
# print(" - OUTPUT:", settings.OUTPUT_DIR)


# import requests
# import os
# from dotenv import load_dotenv

# load_dotenv()

# key = os.getenv("GOOGLE_MAPS_API_KEY")

# url = f"https://maps.googleapis.com/maps/api/geocode/json?address=Chennai&key={key}"

# res = requests.get(url).json()

# if "results" in res:
#     print("✔ Google Maps API working!")
#     print(res["results"][0]["formatted_address"])
# else:
#     print("❌ Error:", res)


# from config import settings

# print("\n===== SETTINGS.PY CHECK =====")

# print("GEMINI_API_KEY:", settings.GEMINI_API_KEY[:6] + "*******")
# print("GOOGLE_MAPS_API_KEY:", (settings.GOOGLE_MAPS_API_KEY or "None")[:6] + "*******")

# print("\n--- MODEL CHECKS ---")
# print("TEXT MODEL:", settings.GEMINI_TEXT_MODEL)
# print("VISION MODEL:", settings.GEMINI_VISION_MODEL)

# print("\n--- PATH CHECKS ---")
# print("BASE_DIR:", settings.BASE_DIR)
# print("CSV_INPUT_PATH:", settings.CSV_INPUT_PATH)
# print("PDF_DIR:", settings.PDF_DIR)
# print("VALIDATED_JSON:", settings.VALIDATED_JSON)
# print("ENRICHED_JSON:", settings.ENRICHED_JSON)
# print("RAW_PDF_TEXT_PATH:", settings.RAW_PDF_TEXT_PATH)
# print("RAW_OCR_OUTPUT_PATH:", settings.RAW_OCR_OUTPUT_PATH)

# print("\n--- OUTPUT PATHS ---")
# print("UPDATED_CSV:", settings.UPDATED_CSV)
# print("REVIEW_QUEUE_CSV:", settings.REVIEW_QUEUE_CSV)
# print("EXCEL_REPORT_PATH:", settings.EXCEL_REPORT_PATH)

# print("\n--- DATABASE PATHS ---")
# print("DATABASE_PATH:", settings.DATABASE_PATH)
# print("AUDIT_LOG_PATH:", settings.AUDIT_LOG_PATH)

# print("\n--- SYSTEM ---")
# print("TEMP_DIR:", settings.TEMP_DIR)
# print("LOG_FILE:", settings.LOG_FILE)

# print("\n===== END CHECK =====\n")


# import os
# import glob
# from services.pdf_vlm_extractor import PDFVLMExtractor

# PDF_DIR = "data/input/pdfs"

# def run_batch():
#     extractor = PDFVLMExtractor()
#     pdfs = glob.glob(os.path.join(PDF_DIR, "*.pdf"))
#     for pdf_path in pdfs:
#         provider_id = os.path.splitext(os.path.basename(pdf_path))[0].upper()
#         print(f"\nProcessing: {provider_id}")
#         extractor.run(pdf_path, provider_id)

# if __name__ == "__main__":
#     run_batch()



import os
import json
import glob

from agents.data_validation_agent import DataValidationAgent

# folders
CSV_FILE = "data/input/providers.csv"      # optional
PDF_EXTRACTED_JSON = "data/processed/enriched_data.json"   # output of PDF extractor
VALIDATION_OUTPUT = "data/processed/validated_data.json"

def load_csv_rows():
    """
    Loads providers.csv as a dict keyed by provider_id.
    If CSV is not used in your project, returns empty dict.
    """
    if not os.path.exists(CSV_FILE):
        print("⚠ No providers.csv found → continuing without CSV rows.")
        return {}

    import csv
    rows = {}
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            pid = (r.get("provider_id") or r.get("id") or "").strip()
            if pid:
                rows[pid.upper()] = r
    return rows


def load_pdf_extracted():
    """
    Loads the entire enriched_data.json produced by pdf_vlm_extractor.
    Each key: provider_id → extracted fields.
    """
    if not os.path.exists(PDF_EXTRACTED_JSON):
        print("❌ ERROR: enriched_data.json not found. Run pdf_vlm_extractor first.")
        return {}

    return json.load(open(PDF_EXTRACTED_JSON, "r", encoding="utf-8"))


def run_validation_batch():
    print("\n🚀 Starting Data Validation Batch...\n")

    csv_rows = load_csv_rows()
    pdf_json_all = load_pdf_extracted()

    agent = DataValidationAgent()
    validated_count = 0

    for provider_id, extracted_json in pdf_json_all.items():
        provider_id = provider_id.upper()

        csv_row = csv_rows.get(provider_id)

        print(f"\n🔍 Validating Provider: {provider_id}")
        result = agent.run(provider_id, csv_row=csv_row, extracted_json=extracted_json)

        print("✔ Validation Complete:")
        print(json.dumps(result, indent=2, ensure_ascii=False))

        validated_count += 1

    print("\n==============================================")
    print(f"✅ TOTAL VALIDATED PROVIDERS: {validated_count}")
    print(f"📄 Output file: {VALIDATION_OUTPUT}")
    print("==============================================\n")


if __name__ == "__main__":
    run_validation_batch()
    
