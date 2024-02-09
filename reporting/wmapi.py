import io
import sys
import json
import time
import logging
import requests
import pandas as pd

# SECTION 1: Call for an authentication token
type = 'application/json'
#Input parameters
url = "https://api.thetradedesk.com/v3/authentication"
payload = {
  "Login": "ttd_api_1rjrst3@liquidadvertising.com",    # Your platform username.
  "Password": "D^nDAUPX>b}L&A"  # Your platform password.
}
myResponse = requests.post(url, headers={"Content-Type": type}, json=payload)
if(myResponse.ok):
  jData = json.loads(str(myResponse.content, "utf-8"))
  authToken = jData["Token"]
  print(authToken)
else:
  #If response code is not ok (200), print the resulting HTTP error code with description
  myResponse.raise_for_status()

# SECTION 2: Call the API endpoint you want to test

#Input parameters
url = "https://api.thetradedesk.com/v3/partner/query"
payload = {
  "PageStartIndex": 0,
  "PageSize": 1
}
r = requests.post(url, json=payload)
print(r)
