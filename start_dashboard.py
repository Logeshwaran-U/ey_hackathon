import json
import os
import pandas as pd
import streamlit as st

QA_JSON = "data/processed/qa_results.json"
VALIDATED_JSON = "data/processed/validated_data.json"

st.set_page_config(
    page_title="EY Healthcare Provider Validation",
    layout="wide"
)

st.title("🏥 EY Healthcare Provider Validation Dashboard")

# ---------------- LOAD DATA ----------------
def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

qa_data = load_json(QA_JSON)
validated_data = load_json(VALIDATED_JSON)

if not qa_data:
    st.error("❌ QA results not found. Run pipeline first.")
    st.stop()

# ---------------- FLATTEN DATA ----------------
rows = []

for pid, qa in qa_data.items():
    base = validated_data.get(pid, {})
    norm = base.get("normalized", {})

    rows.append({
        "Provider ID": pid,
        "Name": norm.get("name", ""),
        "Phone": norm.get("phone", ""),
        "Address": norm.get("address", ""),
        "NPI": norm.get("npi", ""),
        "Validation Status": base.get("validation_status", ""),
        "Final Status": qa.get("final_status", ""),
        "Confidence": qa.get("combined_confidence", 0.0),
        "Issues": ", ".join(qa.get("issues", []))
    })

df = pd.DataFrame(rows)

# ---------------- METRICS ----------------
c1, c2, c3, c4 = st.columns(4)

c1.metric("Total Providers", len(df))
c2.metric("VERIFIED", (df["Final Status"] == "VERIFIED").sum())
c3.metric("NEEDS REVIEW", (df["Final Status"] == "NEEDS_REVIEW").sum())
c4.metric("FAIL / REJECTED", df["Final Status"].isin(["FAIL_QA", "REJECTED"]).sum())

st.divider()

# ---------------- FILTERS ----------------
status_filter = st.multiselect(
    "Filter by Final Status",
    options=df["Final Status"].unique().tolist(),
    default=df["Final Status"].unique().tolist()
)

filtered_df = df[df["Final Status"].isin(status_filter)]

# ---------------- TABLE ----------------
st.subheader("📋 Provider Results")

st.dataframe(
    filtered_df.sort_values("Confidence", ascending=False),
    use_container_width=True
)

# ---------------- CONFIDENCE CHART ----------------
st.subheader("📊 Confidence Distribution")

st.bar_chart(
    filtered_df.groupby("Final Status")["Confidence"].mean()
)

# ---------------- ISSUE BREAKDOWN ----------------
st.subheader("⚠️ Common Issues")

issue_series = (
    filtered_df["Issues"]
    .str.split(", ")
    .explode()
    .value_counts()
)

st.table(issue_series.reset_index().rename(
    columns={"index": "Issue", "Issues": "Count"}
))
