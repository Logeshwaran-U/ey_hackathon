import json
from agents.data_validation_agent import DataValidationAgent
from config import settings

EXTRACTED = settings.EXTRACTED_JSON_PATH       # extracted_data.json
VALIDATED = settings.VALIDATED_JSON            # validated_data.json


def main():
    # Load extracted structured info from PDFs
    extracted_all = json.load(open(EXTRACTED, "r", encoding="utf-8"))

    agent = DataValidationAgent(validated_json_path=VALIDATED)

    print("\n=== Running Batch Validation ===\n")

    for provider_id, extracted in extracted_all.items():
        print(f"🔍 Validating {provider_id} ...")
        agent.run(provider_id, csv_row=None, extracted_json=extracted)

    print("\n✅ Batch Validation Completed")
    print(f"✔ Output saved to: {VALIDATED}\n")


if __name__ == "__main__":
    main()
