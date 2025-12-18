import os
import json
import pdfplumber
import pytesseract
import pypdfium2 as pdfium
from google import genai
from io import BytesIO
from PIL import Image
     
import time
import random

from config import settings

client = genai.Client(api_key=settings.GEMINI_API_KEY)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


class PDFVLMExtractor:

    def extract_text(self, pdf_path: str) -> str:
        text_data = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text_data += (page.extract_text() or "") + "\n"
        except Exception as e:
            print(f"[ERROR] extract_text failed: {e}")

        return text_data.strip()

    def extract_ocr(self, pdf_path: str) -> str:
        try:
            pdf = pdfium.PdfDocument(pdf_path)
            text_output = ""

            for i in range(len(pdf)):
                page = pdf.get_page(i)
                bitmap = page.render(scale=2).to_pil()

                text = pytesseract.image_to_string(bitmap)
                text_output += text + "\n"

            return text_output.strip()

        except Exception as e:
            print(f"[ERROR] OCR failed: {e}")
            return ""


    def analyze_with_gemini_text(self, text: str) -> dict:
        prompt = f"""
    Extract ONLY license/credential data from the document text.

    TEXT:
    {text}

    Return ONLY valid JSON:
    {{
    "registered_name": "",
    "license_number": "",
    "license_type": "",
    "issuing_authority": "",
    "issue_date": "",
    "expiry_date": "",
    "license_status": "",
    "registered_address": ""
    }}
    """

        MAX_RETRIES = 4

        for attempt in range(MAX_RETRIES):
            try:
                response = client.models.generate_content(
                    model=settings.GEMINI_TEXT_MODEL,
                    contents=prompt
                )

                output = response.text or ""
                start, end = output.find("{"), output.rfind("}")

                if start != -1 and end != -1:
                    return json.loads(output[start:end+1])

                return {
                    "error": "JSON not found",
                    "raw": output
                }

            except Exception as e:
                err = str(e)

                if "503" in err or "Service Unavailable" in err:
                    sleep_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                    print(f"⚠ Gemini busy (503). Retrying in {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                    continue

                return {"error": f"Gemini failed: {err}"}

        return {"error": "Gemini failed after retries"}

    def _read_json(self, path):
        if not os.path.exists(path):
            return {}
        try:
            return json.load(open(path, "r"))
        except:
            return {}

    def _write_json(self, path, data):
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def save_raw_text(self, provider_id, text):
        data = self._read_json(settings.RAW_PDF_TEXT_PATH)
        data[provider_id] = text
        self._write_json(settings.RAW_PDF_TEXT_PATH, data)

    def save_final_output(self, provider_id, result):
        data = self._read_json(settings.ENRICHED_JSON)
        data[provider_id] = result
        self._write_json(settings.ENRICHED_JSON, data)

    
    
    def run(self, pdf_path: str, provider_id: str) -> dict:

        text = self.extract_text(pdf_path)

        if len(text) < 20:
            print("⚠ No readable text → Running OCR (free)…")
            text = self.extract_ocr(pdf_path)

        self.save_raw_text(provider_id, text)

        result = self.analyze_with_gemini_text(text)

        self.save_final_output(provider_id, result)
        return result


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python pdf_vlm_extractor.py <PDF_PATH> <PROVIDER_ID>")
        exit()

    extractor = PDFVLMExtractor()
    output = extractor.run(sys.argv[1], sys.argv[2])

    print("\n===== FINAL RESULT =====\n")
    print(json.dumps(output, indent=2))
