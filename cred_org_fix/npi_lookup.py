
import requests

NPI_API = "https://npiregistry.cms.hhs.gov/api/"

def fetch_npi(npi):
    params = {"number": npi, "version": "2.1"}
    try:
        r = requests.get(NPI_API, params=params, timeout=15)
        if r.status_code != 200:
            return None
        results = r.json().get("results")
        return results[0] if results else None
    except Exception:
        return None


def extract_fields(data):
    basic = data.get("basic", {})
    credential = basic.get("credential", "")
    org = basic.get("organization_name", "")

    if not org:
        for addr in data.get("addresses", []):
            if addr.get("address_purpose") == "LOCATION":
                org = addr.get("organization_name", "")
                break

    return credential, org
