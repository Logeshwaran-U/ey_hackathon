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


from config import settings

print("\n===== SETTINGS.PY CHECK =====")

print("GEMINI_API_KEY:", settings.GEMINI_API_KEY[:6] + "*******")
print("GOOGLE_MAPS_API_KEY:", (settings.GOOGLE_MAPS_API_KEY or "None")[:6] + "*******")

print("\n--- MODEL CHECKS ---")
print("TEXT MODEL:", settings.GEMINI_TEXT_MODEL)
print("VISION MODEL:", settings.GEMINI_VISION_MODEL)

print("\n--- PATH CHECKS ---")
print("BASE_DIR:", settings.BASE_DIR)
print("CSV_INPUT_PATH:", settings.CSV_INPUT_PATH)
print("PDF_DIR:", settings.PDF_DIR)
print("VALIDATED_JSON:", settings.VALIDATED_JSON)
print("ENRICHED_JSON:", settings.ENRICHED_JSON)
print("RAW_PDF_TEXT_PATH:", settings.RAW_PDF_TEXT_PATH)
print("RAW_OCR_OUTPUT_PATH:", settings.RAW_OCR_OUTPUT_PATH)

print("\n--- OUTPUT PATHS ---")
print("UPDATED_CSV:", settings.UPDATED_CSV)
print("REVIEW_QUEUE_CSV:", settings.REVIEW_QUEUE_CSV)
print("EXCEL_REPORT_PATH:", settings.EXCEL_REPORT_PATH)

print("\n--- DATABASE PATHS ---")
print("DATABASE_PATH:", settings.DATABASE_PATH)
print("AUDIT_LOG_PATH:", settings.AUDIT_LOG_PATH)

print("\n--- SYSTEM ---")
print("TEMP_DIR:", settings.TEMP_DIR)
print("LOG_FILE:", settings.LOG_FILE)

print("\n===== END CHECK =====\n")
