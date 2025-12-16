# run_pipeline.py
import csv, asyncio, json, os
from services.pdf_vlm_extractor import PDFVLMExtractor

def norm(r):
    return {
        "provider_id": r.get("provider_id") or r.get("Provider_ID"),
        "pdf_file": r.get("pdf_file") or r.get("Pdf"),
        "raw_csv": r
    }

async def process_row(r, ex):
    pid, pdf = r["provider_id"], r["pdf_file"]
    return {"provider_id": pid, "csv_data": r["raw_csv"],
            "pdf_data": await asyncio.to_thread(ex.run, pdf, pid) if pdf and os.path.exists(pdf) else None}

async def main():
    ex = PDFVLMExtractor()
    with open("data/input/dta.csv", encoding="utf-8") as f:
        rows = [norm(r) for r in csv.DictReader(f)]
    results = await asyncio.gather(*[process_row(r, ex) for r in rows])
    os.makedirs("data/processed", exist_ok=True)
    json.dump({x["provider_id"]: x for x in results}, open("data/processed/extracted_data.json","w"), indent=2)

if __name__ == "__main__":
    asyncio.run(main())
