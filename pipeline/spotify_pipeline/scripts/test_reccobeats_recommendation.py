import requests
import json
from datetime import datetime

BASE_URL = "https://api.reccobeats.com"
URL = f"{BASE_URL}/v1/track/recommendation"

headers = {"Accept": "application/json"}

r = requests.get(URL, headers=headers, params={"size":10}, timeout=20)

print("URL:", URL)
print("Status:", r.status_code)
print("Content-Type:", r.headers.get("Content-Type"))
print("Body preview:", (r.text[:600] if r.text else "<empty>"))

# Save response for debugging
out = {
    "tested_at": datetime.utcnow().isoformat() + "Z",
    "url": URL,
    "status": r.status_code,
    "headers": dict(r.headers),
    "body": None,
}

try:
    out["body"] = r.json()
except Exception:
    out["body"] = r.text

import os
os.makedirs("data/reccobeats_tests", exist_ok=True)
fname = "data/reccobeats_tests/reccobeats_recommendation_probe.json"
with open(fname, "w") as f:
    json.dump(out, f, indent=2)

print("Saved to:", fname)

# If it returned JSON, try to detect UUID-like ids
def looks_like_uuid(s: str) -> bool:
    import re
    return bool(re.match(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$", s or ""))

if isinstance(out["body"], dict) or isinstance(out["body"], list):
    body = out["body"]
    # search for any "id" fields that look like UUIDs
    ids_found = []

    def walk(x):
        if isinstance(x, dict):
            if "id" in x and isinstance(x["id"], str) and looks_like_uuid(x["id"]):
                ids_found.append(x["id"])
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(body)
    print("UUID-like ids found:", len(ids_found))
    if ids_found:
        print("Example UUID:", ids_found[0])
