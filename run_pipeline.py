#!/usr/bin/env python3
# run_pipeline.py
# EY Hackathon – FULL PIPELINE (ONE RUN)

import os
import csv
import json
import asyncio
from datetime import datetime, timezone

# ---------- SAFE PATH SETUP ----------
ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)

DATA_DIR = "data"
INPUT_DIR = os.path.join(DATA_DIR, "input")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

CSV_PATH = os.path.join(INPUT_DIR, "dta.csv")
EXTRACTED_JSON = os.path.join(PROCESSED_DIR, "extracted_data.json")
VALIDATED_JSON = os.path.join(PROCESSED_DIR, "validated_data.json")
ENRICHED_JSON = os.path.join(PROCESSED_DIR, "enriched_data.json")
QA_JSON = os.path.join(PROCESSED_DIR, "qa_results.json")

os.makedirs(PROCESSED_DIR, exist_ok=True)

# ---------- IMPORT AGENTS ----------
from services.pdf_vlm_extractor import PDFVLMExtractor
from agents.data_validation_agent import DataValidationAgent
from agents.enrichment_agent import run as run_enrichment
from agents.quality_assurance_agent import QualityAssuranceAgent

# ---------- HELPERS ----------
def utc_now():
    return datetime.now(timezone.utc).isoformat()

def atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

# ---------- PHASE 1: EXTRACT ----------
async def extract_phase():
    extractor = PDFVLMExtractor()
    extracted = {}

    with open(CSV_PATH, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    async def process(row):
        pid = row["Provider_ID"]
        pdf = row.get("Pdf")
        pdf_data = None

        if pdf and os.path.exists(pdf):
            pdf_data = await asyncio.to_thread(extractor.run, pdf, pid)

        extracted[pid] = {
            "provider_id": pid,
            "csv_data": row,
            "pdf_data": pdf_data
        }

    await asyncio.gather(*[process(r) for r in rows])
    atomic_write(EXTRACTED_JSON, extracted)
    print(f" Extraction completed ({len(extracted)})")

# ---------- PHASE 2: VALIDATION ----------
def validation_phase():
    validator = DataValidationAgent()

    extracted = json.load(open(EXTRACTED_JSON, encoding="utf-8"))
    for pid, payload in extracted.items():
        validator.run(
            provider_id=pid,
            csv_row=payload["csv_data"],
            pdf_row=payload["pdf_data"]
        )

    print(" Validation completed")

# ---------- PHASE 3: ENRICHMENT ----------
async def enrichment_phase():
    await run_enrichment()
    print("✅ Enrichment completed")

# ---------- PHASE 4: QUALITY ASSURANCE ----------
def qa_phase():
    enriched = json.load(open(ENRICHED_JSON, encoding="utf-8"))
    qa_agent = QualityAssuranceAgent()

    qa_results = {}
    for pid, record in enriched.items():
        qa_results[pid] = qa_agent.classify(record)

    atomic_write(QA_JSON, qa_results)
    print("✅ QA completed")

# ---------- MAIN ----------
async def main():
    print("\n🚀 EY PROVIDER PIPELINE STARTED\n")

    await extract_phase()
    validation_phase()
    await enrichment_phase()
    qa_phase()

    print("\n PIPELINE FINISHED SUCCESSFULLY")
    print(f" Extracted  : {EXTRACTED_JSON}")
    print(f" Validated  : {VALIDATED_JSON}")
    print(f" Enriched   : {ENRICHED_JSON}")
    print(f" QA Results : {QA_JSON}")

if __name__ == "__main__":
    asyncio.run(main())
