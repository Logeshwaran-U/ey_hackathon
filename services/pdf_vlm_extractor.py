import os
import json
import pdfplumber
import pytesseract
import pypdfium2 as pdfium
from google import genai
from io import BytesIO
from PIL import Image

from config import settings

client = genai.Client(api_key=settings.GEMINI_API_KEY)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


class PDFVLMExtractor:

    # 1. TEXT EXTRACTION 
    def extract_text(self, pdf_path: str) -> str:
        text_data = ""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    text_data += (page.extract_text() or "") + "\n"
        except Exception as e:
            print(f"[ERROR] extract_text failed: {e}")

        return text_data.strip()

    # 2. OCR FALLBACK FOR SCANNED PDFs
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

    # 3. GEMINI TEXT MODEL (FREE)
    def analyze_with_gemini_text(self, text: str) -> dict:
        prompt = f"""
Extract structured healthcare provider data from the text below.

TEXT:
{text}

Return ONLY valid JSON:
{{
  "name": "",
  "qualifications": "",
  "clinic_address": "",
  "phone": "",
  "email": "",
  "specializations": "",
  "experience_years": "",
  "registration_number": ""
}}
"""

        try:
            response = client.models.generate_content(
                model=settings.GEMINI_TEXT_MODEL,
                contents=prompt
            )

            output = response.text
            start = output.find("{")
            end = output.rfind("}")

            if start == -1 or end == -1:
                return {"error": "JSON not found", "raw": output}

            return json.loads(output[start:end + 1])

        except Exception as e:
            return {"error": f"Text model failed: {e}"}

    # 4. JSON FILE HELPERS
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

    
    # 5. MAIN PIPELINE (FREE TIER SAFE)
    
    def run(self, pdf_path: str, provider_id: str) -> dict:

        # Step 1: text extraction
        text = self.extract_text(pdf_path)

        if len(text) < 20:
            print("⚠ No readable text → Running OCR (free)…")
            text = self.extract_ocr(pdf_path)

        self.save_raw_text(provider_id, text)

        # Step 2: process with Gemini TEXT model
        result = self.analyze_with_gemini_text(text)

        self.save_final_output(provider_id, result)
        return result


# CLI test
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python pdf_vlm_extractor.py <PDF_PATH> <PROVIDER_ID>")
        exit()

    extractor = PDFVLMExtractor()
    output = extractor.run(sys.argv[1], sys.argv[2])

    print("\n===== FINAL RESULT =====\n")
    print(json.dumps(output, indent=2))
