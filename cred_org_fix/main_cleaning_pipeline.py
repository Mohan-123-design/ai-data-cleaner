import pandas as pd
import time
import os
import json

from config import (
    INPUT_EXCEL,
    OUTPUT_EXCEL,
    PROGRESS_FILE,
    BATCH_SIZE,
    DELAY_BETWEEN_REQUESTS
)

from org_normalization_ai import normalize_org_safe

# ---------- CREDENTIAL RULES ----------
SPECIALTY_TO_CREDENTIAL = {
    "Internal Medicine": "MD",
    "Family Medicine": "MD",
    "Cardiology": "MD",
    "Dermatology": "MD",
    "Neurology": "MD",
    "Pediatrics": "MD",
    "Psychiatry": "MD",

    "Nurse Practitioner": "NP",
    "Family Nurse Practitioner": "NP",
    "Physician Assistant": "PA-C",

    "Dentistry": "DDS",
    "Optometry": "OD",
    "Chiropractic": "DC"
}

def fix_credentials(credential, specialty):
    if pd.isna(credential) or str(credential).strip() == "":
        return SPECIALTY_TO_CREDENTIAL.get(str(specialty).strip(), "UNKNOWN")
    return str(credential).strip()

# ---------- PROGRESS ----------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f).get("last_index", 0)
    return 0

def save_progress(index):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_index": index}, f)

# ---------- MAIN ----------
def main():
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    df = pd.read_excel(INPUT_EXCEL)

    # Audit columns
    df["Credential_Fixed"] = ""
    df["Organisation_Normalized"] = ""
    df["Org_Normalization_Status"] = ""
    df["Org_AI_Confidence"] = ""

    total = len(df)
    batch_start = load_progress()

    print(f"Total rows: {total}")
    print(f"Resuming from row: {batch_start}")

    while batch_start < total:
        batch_end = min(batch_start + BATCH_SIZE, total)

        user = input(
            f"\nProcess rows {batch_start} → {batch_end}? (y/n): "
        ).strip().lower()

        if user != "y":
            print("⛔ Stopped by user.")
            break

        for idx in range(batch_start, batch_end):
            row = df.iloc[idx]

            # ---- Credential Fix ----
            df.at[idx, "Credential_Fixed"] = fix_credentials(
                row["Credentials"], row["Specialty"]
            )

            # ---- Organisation Fix ----
            normalized, status, conf = normalize_org_safe(row["Organisation"])
            df.at[idx, "Organisation_Normalized"] = normalized
            df.at[idx, "Org_Normalization_Status"] = status
            df.at[idx, "Org_AI_Confidence"] = conf

            save_progress(idx + 1)

            # 🔐 SAVE EVERY ROW (NO DATA LOSS)
            df.to_excel(OUTPUT_EXCEL, index=False)

            time.sleep(DELAY_BETWEEN_REQUESTS)

        print(f"✔ Saved rows up to {batch_end}")
        batch_start = batch_end

    print("\n✅ PROCESS COMPLETED SAFELY")

if __name__ == "__main__":
    main()
