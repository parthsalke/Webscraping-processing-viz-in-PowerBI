import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import datetime
import re
from mysql_script_webscrape import save_to_mysql

def clean_text(text):
    return text.strip()  # Remove leading and trailing whitespaces

def getdata():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    url = 'https://www.moneycontrol.com/indian-indices/nifty-50-9.html'

    r = requests.get(url)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        price = clean_text(soup.find('div', {'class': 'inprice1'}).text)
        change = clean_text(soup.find('div', {'id': 'sp_ch_prch'}).text)

        timestamp = datetime.now().isoformat()  # ISO Date format

        #Convert decimal value to Integer
        input_text1 = price
        price1 = int(float(input_text1.replace(',', '')))

        #Convert % value to Integer
        def extract_value(change):
            try:
                return round(float(change.split()[0]))
            except (ValueError, IndexError):
                return None

        input_text = change
        result = extract_value(input_text)

        # Save data in MySQL
        save_to_mysql(timestamp, price1, result)
    
        #Save data in csv
        try:
            df = pd.read_csv('stock_data.csv')
        except FileNotFoundError:
            df = pd.DataFrame(columns=['Time', 'price1','result'])

        # Append new data to the DataFrame
        new_data = pd.DataFrame({'Time': [time.strftime('%H:%M:%S')],
                                 'price1': [price1],
                                 'result':[result]})
        df = pd.concat([df, new_data], ignore_index=True)

        # Save the updated DataFrame back to the CSV
        df.to_csv('stock_data.csv', index=False)

        print("Data scraped and saved successfully.")

        return [timestamp, price1, result]

    else:
        print(f"Failed to fetch data. Status code: {r.status_code}")


# Scrape and update data every 30 seconds
while True:
    getdata()
    time.sleep(30)  #30 seconds
    


###########################################################################



#Push real-time Data to Power BI via API
    if __name__ == '__main__':
        REST_API_URL = 'https://api.powerbi.com/beta/5d9a1b7a-74b2-4f0d-99bb-b230ca68169a/datasets/75d948f9-3f1c-4993-875c-ae91ade0699e/rows?redirectedFromSignup=1&experience=power-bi&key=baiVKcQh3RoSnn83HVPRcZOBgU5IgdRHZsnd%2FLr9Kt%2FNlLz0WsfA38cnBrhndemsKr44VzIH0ZKhYF9fyrD7Rw%3D%3D'

        while True:
            data_raw = []
            for i in range(1):
                row = getdata()
                if row:
                    data_raw.append(row)
                    print("Raw data - ", data_raw)

            # set the header record
            HEADER = ["Timestamp", "StockPrice", "Change"]

            data_df = pd.DataFrame(data_raw, columns=HEADER)
            data_json = data_df.to_json(orient='records')
            print("JSON dataset", data_json)

            # Post the data on the Power BI API
            headers = {'Content-Type': 'application/json'}
            req = requests.post(REST_API_URL, data=data_json, headers=headers)

            print("Data posted in Power BI API")
            time.sleep(30)

