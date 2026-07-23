import requests

BASE_URL = "https://api.reccobeats.com"

test_urls = [
    f"{BASE_URL}/",
    f"{BASE_URL}/docs",
]

for url in test_urls:
    try:
        r = requests.get(url, timeout=10)
        print("\nURL:", url)
        print("Status:", r.status_code)
        print("Content-Type:", r.headers.get("Content-Type"))
        print("Body preview:", r.text[:200].replace("\n", " "))
    except Exception as e:
        print("\nURL:", url)
        print("ERROR:", repr(e))
