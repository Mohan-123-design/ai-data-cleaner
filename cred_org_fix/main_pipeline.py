import pandas as pd
import os, json, time, sys
from config import *
from npi_lookup import fetch_npi, extract_fields

# ---------------- PROGRESS ----------------
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f).get("last_row", 0)
    return 0

def save_progress(row):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"last_row": row}, f)

# ---------------- MAIN ----------------
def main():
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    df = pd.read_excel(INPUT_EXCEL)

    for col in [
        "Discovered_Credential",
        "Discovered_Organization",
        "Discovery_Status",
        "Confidence"
    ]:
        if col not in df.columns:
            df[col] = ""

    start = load_progress()
    total = len(df)

    print(f"\nTOTAL ROWS: {total}")
    print(f"RESUMING FROM ROW: {start}\n")

    try:
        for i in range(start, total):
            row = df.iloc[i]
            print("=" * 60)
            print(f"[ROW {i}]")
            print(f"NPI: {row['NPI']}")
            print(f"Name: {row['First Name']} {row['Last Name']}")
            print(f"Specialty: {row['Specialty']}")

            print("\n→ Fetching from NPI Registry...")
            data = fetch_npi(str(row["NPI"]).strip())

            if not data:
                print("❌ No NPI data found")
                df.at[i, "Discovery_Status"] = "NPI_NOT_FOUND"
                save_progress(i + 1)
                df.to_excel(OUTPUT_EXCEL, index=False)
                continue

            credential, org = extract_fields(data)

            print(f"✓ Credential found: {credential}")
            print(f"✓ Organization found: {org}")

            choice = input("\nApply updates for this row? (y/n): ").lower()
            if choice != "y":
                print("⏭ Skipped by user")
                df.at[i, "Discovery_Status"] = "SKIPPED_BY_USER"
            else:
                df.at[i, "Discovered_Credential"] = credential
                df.at[i, "Discovered_Organization"] = org
                df.at[i, "Discovery_Status"] = "APPLIED_FROM_NPI"
                df.at[i, "Confidence"] = 0.95
                print("💾 Saved")

            save_progress(i + 1)
            df.to_excel(OUTPUT_EXCEL, index=False)
            time.sleep(DELAY_BETWEEN_ROWS)

    except KeyboardInterrupt:
        print("\n\n⚠ USER INTERRUPT DETECTED")
        print("✔ Progress safely saved")
        sys.exit(0)

    print("\n✅ ALL ROWS PROCESSED SAFELY")

if __name__ == "__main__":
    main()
