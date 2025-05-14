import requests

url = "https://izvjskz707.execute-api.us-east-1.amazonaws.com/get-all-results"
resp = requests.get(url)
print(resp.status_code)
print(resp.json())
body = resp.json().get("body")
print(body)