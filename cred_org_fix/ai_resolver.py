import requests
import json
from config import OLLAMA_BASE_URL, OLLAMA_MODEL

def resolve_with_ai(provider_name, specialty, org_candidates):
    prompt = f"""
You are resolving healthcare provider information.

Provider: {provider_name}
Specialty: {specialty}

Possible organization names:
{org_candidates}

Pick the MOST OFFICIAL organization.
Return JSON only.

{{
  "organization": "",
  "confidence": 0.0
}}
"""

    r = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "temperature": 0.1,
            "stream": False
        },
        timeout=60
    )

    try:
        return json.loads(r.json()["response"])
    except Exception:
        return {"organization": "", "confidence": 0.0}
