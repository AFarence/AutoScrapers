import pandas as pd
import requests
from bs4 import BeautifulSoup

### For Google Sheets ###
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json

### For appending the date and time the scraper ran ###
import datetime
import pytz

# Get current date and time in Eastern Time (US)
et = pytz.timezone('US/Eastern')
now = datetime.datetime.now(et)

# Format date and time as a string
datetime_str = now.strftime('%Y-%m-%d %H:%M:%S')

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

scrape_dict = {"MiamiDade":"https://www.redfin.com/county/479/FL/Miami-Dade-County/filter/sort=hi-price,include=sold-1wk",
               "PalmBeach":"https://www.redfin.com/county/486/FL/Palm-Beach-County/filter/sort=hi-price,include=sold-1wk",
               "Broward":"https://www.redfin.com/county/442/FL/Broward-County/filter/sort=hi-price,include=sold-1wk",
               "CookCounty":"https://www.redfin.com/county/727/IL/Cook-County/filter/sort=hi-price,include=sold-1wk"}

scopes = [
'https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive'
]

def scrape_redfin(url,headers):

    try:
        response = requests.get(url,headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            price_soup = soup.find_all('span',{'class':'homecardV2Price'})
        except:
            price_soup = None
        try:
            address_soup = soup.find_all('span',{'class':'collapsedAddress primaryLine'})
        except:
            address_soup = None
        try:
            card_soup = soup.find('div',{'class':'HomeCardsContainer flex flex-wrap'})
            url_soup = card_soup.find_all('a')
        except:
            url_soup = None
    except Exception as e:
        print(f'{e} -- response fail!')
        return None

    # Get just the hyperlink
    url_list = []
    for x in url_soup:
        url_list.append(x['href'])  

    # Use a dictionary to keep track of elements we've already encountered
    seen = {}

    # Create a new list with duplicates removed
    new_list = []
    for item in url_list:
        if item not in seen:
            new_list.append(item)
            seen[item] = True

    data = {'Address':address_soup,'Price':price_soup,'URL':new_list}

    df = pd.DataFrame(data)

    df['URL'] = 'redfin.com' + df['URL'].astype(str)

    for i, r in df.iterrows():
        r['Price'] = r['Price'].text
        r['Address'] = r['Address'].text

    df['Price'] = df['Price'].str.replace('$','',regex=True)
    df['Price'] = df['Price'].str.replace(',','',regex=True)
    df['Price'] = pd.to_numeric(df['Price'])

    df = df.sort_values(by='Price', ascending=False)

    df.reset_index(inplace=True, drop=True)

    return df

def update_spreadsheet(spreadsheet, df):
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ.get('SERVICE_ACCOUNT_JSON')), scopes)
    file = gspread.authorize(credentials)
    sheet = file.open("RedfinFeed").worksheet(spreadsheet)

    # Clear existing data (optional)
    # sheet.clear()

    df['Scrape_Date'] = datetime_str

    header = df.columns.tolist()
    data = df.values.tolist()

    # Write new data at the top
    # sheet.update('A1', [header] + data)
    # write a blank row
    sheet.append_row([])
    sheet.insert_row(header, 2)
    sheet.insert_rows(data, 3)


for key in scrape_dict.keys():
    df = scrape_redfin(scrape_dict[key], headers)
    update_spreadsheet(key, df)

