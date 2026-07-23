import requests
from urllib.parse import quote

BASE_URL = "https://api.reccobeats.com"

# We'll search by the same query you used: RAPSTAR Polo G
query = "RAPSTAR Polo G"

# Probe a few common search endpoints used by APIs.
# We only care about finding one that returns track objects including a UUID "id".
candidates = [
    f"{BASE_URL}/v1/search?query={quote(query)}",
    f"{BASE_URL}/v1/search?q={quote(query)}",
    f"{BASE_URL}/v1/track/search?query={quote(query)}",
    f"{BASE_URL}/v1/track/search?q={quote(query)}",
    f"{BASE_URL}/v1/tracks/search?query={quote(query)}",
    f"{BASE_URL}/v1/tracks/search?q={quote(query)}",
    f"{BASE_URL}/v1/track?query={quote(query)}",
    f"{BASE_URL}/v1/tracks?query={quote(query)}",
]

headers = {"Accept": "application/json"}

for url in candidates:
    print("\nTrying:", url)
    try:
        r = requests.get(url, headers=headers, timeout=20)
        print("Status:", r.status_code)
        if r.text:
            print("Preview:", r.text[:250].replace("\n", " "))
        else:
            print("Preview: <empty>")
    except Exception as e:
        print("ERROR:", repr(e))
