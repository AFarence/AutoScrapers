import pandas as pd
import re
from datetime import date
from bs4 import BeautifulSoup
from playwright.sync_api import Playwright, sync_playwright, expect

### For Google Sheets ###
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json
import os
import time

### Set up start date, end date, and doc list

start_date = '08012023'
end_date = '09292023'

run_date = date.today().strftime('%b-%d-%Y')

doc_list = [
    'DEED',
    'DEIT',
    'QCD',
    'SPWD',
    'TRUD',
    'TEED',
    'WARD',
]

def page_scraper(page, page_counter, doc):

    dfs = pd.read_html(page)
    df = dfs[0]

    soup = BeautifulSoup(page, 'html.parser')

    link_suffix = 'https://crs.cookcountyclerkil.gov'

    deed_urls = []
    for link in soup.find_all('a', attrs={'href': re.compile('^/Document/Detail')}):
        parent = link.find_parent('td', class_='align-middle text-center')
        if parent is not None:
            page_link = link_suffix + link.get('href')
            deed_urls.append(page_link)

    df['deed_urls'] = deed_urls

    df['Consi. Amt.'] = df['Consi. Amt.'].str.replace('$','',regex=False)
    df['Consi. Amt.'] = df['Consi. Amt.'].str.replace(',','',regex=False)

    df['Consi. Amt.'] = pd.to_numeric(df['Consi. Amt.'])

    page_counter = page_counter + 1

    try:
        df.to_csv(f'{doc}_page{page_counter}_{start_date}_to_{end_date}.csv')
    except Exception as e:
        print(f'Error saving CSV file: {e}')
 
    return page_counter


def run(playwright: Playwright) -> None:
    for doc in doc_list:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()

        # Open new page
        page = context.new_page()

        # Go to https://crs.cookcountyclerkil.gov/Search/Additional
        page.goto("https://crs.cookcountyclerkil.gov/Search/Additional")

        # Click text=Document Type Search
        page.locator("text=Document Type Search").click()

        # Select Doc Type
        page.locator("select[name=\"DocumentType\"]").select_option(doc)

        # Click text=From Date * (mm/dd/yyyy) >> input[name="RecordedFromDate"]
        page.locator("text=From Date * (mm/dd/yyyy) >> input[name=\"RecordedFromDate\"]").click()

        page.locator("text=From Date * (mm/dd/yyyy) >> input[name=\"RecordedFromDate\"]").fill(start_date)

        # Click text=To Date * (mm/dd/yyyy) >> input[name="RecordedToDate"]
        page.locator("text=To Date * (mm/dd/yyyy) >> input[name=\"RecordedToDate\"]").click()

        page.locator("text=To Date * (mm/dd/yyyy) >> input[name=\"RecordedToDate\"]").fill(end_date)

        # Click input[name="LowerLimit"]
        page.locator("input[name=\"LowerLimit\"]").click()

        # Fill input[name="LowerLimit"]
        page.locator("input[name=\"LowerLimit\"]").fill("4000000")

        # Click text=Document Type Search Document Type * ABROGATION ACCEPTANCE ACCEPTANCE OF TRANFER >> button[name="submitButton"]
        page.locator("text=Document Type Search Document Type * ABROGATION ACCEPTANCE ACCEPTANCE OF TRANFER >> button[name=\"submitButton\"]").click()

        try:
        
            page.wait_for_selector("div[class=table-responsive]")

            counter = 0

            x = page.content()

            page_scraper(x, counter, doc)
            
            try:

                while True:
                    # Click the "next" button if it is present
                    next_button = page.locator("text=»")
                    if next_button.is_enabled():
                        next_button.click()
                        counter = counter + 1
                        x = page.content()
                        page_scraper(x, counter, doc)
                    else:
                        break

            except Exception as e:
                print(f'Navigation failed: {e}')
            
        except Exception as e:
            print(f'{doc}:{e}')

        # ---------------------
        context.close()
        browser.close()


with sync_playwright() as playwright:
    run(playwright)

# get a list of all files in the current directory
files = os.listdir()

# filter the list to include only CSV files
csv_files = [f for f in files if f.endswith('.csv')]

# read all CSV files into a single pandas dataframe
df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

# Remove NaNs
df = df.fillna('NA')

print(df.shape)

### For Google Sheets ###
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json

# set up credentials and authorize access
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# parse the JSON data and create credentials
credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ.get('SERVICE_ACCOUNT_JSON')), scopes)
file = gspread.authorize(credentials)

# Open the Google Sheet and specific worksheet
sheet = file.open("CCRecorder")
worksheet = sheet.worksheet("DeedInfo")

# Clear existing data (optional)
worksheet.clear()

# Write DataFrame to Google Sheet
worksheet.update([df.columns.values.tolist()] + df.values.tolist())

print(f'Sheet Updated! {df.columns.value_counts()}')
