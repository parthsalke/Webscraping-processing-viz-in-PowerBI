import requests
import pandas as pd

def push_to_power_bi(data_raw, REST_API_URL):
    # set the header record
    HEADER = ["Timestamp", "StockPrice", "Change"]

    data_df = pd.DataFrame(data_raw, columns=HEADER)
    data_json = data_df.to_json(orient='records')

    # Post the data on the Power BI API
    headers = {'Content-Type': 'application/json'}
    req = requests.post(REST_API_URL, data=data_json, headers=headers)

    print("Data posted in Power BI API")
