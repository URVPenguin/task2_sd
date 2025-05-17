import requests

url = "https://zvvmbidtbf.execute-api.us-east-1.amazonaws.com/get-all-results"
resp = requests.get(url)
print(resp.json())