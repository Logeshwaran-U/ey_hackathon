# run_pipeline.py  (EXTRACTION + VALIDATION wired correctly)

import csv, asyncio, json, os
from services.pdf_vlm_extractor import PDFVLMExtractor
from agents.data_validation_agent import DataValidationAgent

INPUT_CSV = "data/input/dta.csv"
EXTRACTED_JSON = "data/processed/extracted_data.json"

def norm(r):
    return {
        "provider_id": r.get("provider_id") or r.get("Provider_ID"),
        "pdf_file": r.get("pdf_file") or r.get("Pdf"),
        "raw_csv": r
    }

async def process_row(r, ex):
    pid, pdf = r["provider_id"], r["pdf_file"]
    return {
        "provider_id": pid,
        "csv_data": r["raw_csv"],
        "pdf_data": await asyncio.to_thread(ex.run, pdf, pid)
        if pdf and os.path.exists(pdf) else None
    }

async def main():
    os.makedirs("data/processed", exist_ok=True)

    # -------- PHASE 1: CSV + PDF → extracted_data.json --------
    ex = PDFVLMExtractor()
    with open(INPUT_CSV, encoding="utf-8") as f:
        rows = [norm(r) for r in csv.DictReader(f)]

    extracted_results = await asyncio.gather(
        *[process_row(r, ex) for r in rows]
    )

    extracted_map = {x["provider_id"]: x for x in extracted_results}
    json.dump(extracted_map, open(EXTRACTED_JSON, "w"), indent=2)

    # -------- PHASE 2: extracted_data.json → validated_data.json --------
    validator = DataValidationAgent()

    for pid, payload in extracted_map.items():
        validator.run(
            provider_id=pid,
            csv_row=payload.get("csv_data"),
            pdf_row=payload.get("pdf_data")
        )

if __name__ == "__main__":
    asyncio.run(main())
