import requests
import json
import time
import pandas as pd
import re

from config import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    MAX_RETRIES,
    RETRY_DELAY
)

# ---------- ADDRESS / GARBAGE DETECTION ----------
ADDRESS_PATTERN = re.compile(
    r"\b(st|street|ave|avenue|rd|road|blvd|suite|ste|#)\b",
    re.IGNORECASE
)

def is_not_organisation(value: str) -> bool:
    if value.isdigit():
        return True
    if ADDRESS_PATTERN.search(value):
        return True
    if any(char.isdigit() for char in value) and len(value.split()) <= 3:
        return True
    return False

# ---------- OLLAMA CALL ----------
def call_ollama_org_normalizer(org_name):
    prompt = f"""
You are standardizing healthcare organization names.

Rules:
- Normalize spelling and abbreviations
- Do NOT invent organizations
- If uncertain, return original name
- Output STRICT JSON ONLY

Input:
"{org_name}"

Output:
{{
  "canonical_name": "",
  "confidence": 0.0
}}
"""

    for _ in range(MAX_RETRIES):
        try:
            response = requests.post(
                f"{OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": OLLAMA_MODEL,
                    "prompt": prompt,
                    "temperature": 0.1,
                    "stream": False
                },
                timeout=60
            )

            if response.status_code == 200:
                raw = response.json().get("response", "").strip()
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        return parsed
                except json.JSONDecodeError:
                    pass

            time.sleep(RETRY_DELAY)

        except Exception:
            time.sleep(RETRY_DELAY)

    return {
        "canonical_name": str(org_name),
        "confidence": 0.0
    }

# ---------- HYBRID NORMALIZER ----------
def normalize_org_safe(org_name):
    if org_name is None or pd.isna(org_name):
        return "", "EMPTY", 0.0

    org_name_str = str(org_name).strip()
    if org_name_str == "":
        return "", "EMPTY", 0.0

    # ---- RULE FIRST ----
    if is_not_organisation(org_name_str):
        return org_name_str, "INVALID_ORG_ADDRESS", 0.0

    # ---- AI SECOND ----
    result = call_ollama_org_normalizer(org_name_str)

    canonical = str(result.get("canonical_name", org_name_str)).strip()

    try:
        confidence = float(result.get("confidence", 0.0))
    except Exception:
        confidence = 0.0

    if confidence >= 0.85 and canonical != "":
        return canonical, "AI_NORMALIZED", confidence

    return org_name_str, "UNCHANGED_LOW_CONF", confidence
