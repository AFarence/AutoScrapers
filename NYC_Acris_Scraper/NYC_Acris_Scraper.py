import pandas as pd
import numpy as np
import re
from playwright.sync_api import Playwright, sync_playwright, expect
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import requests
import gspread
import os
import json

df_list = []

# Get the current location of the script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Initialize an empty list to collect scraped data
scraped_data_list = []

# Define your custom headers 
custom_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.4567.89 Safari/537.36'
}

# Define extra information list
deed_info_list = ['BOROUGH', 'BLOCK', 'LOT', 'PARTIAL', 'PROPERTY TYPE', 'EASEMENT',
    'AIR RIGHTS', 'SUBTERRANEAN RIGHTS', 'PROPERTY ADDRESS', 'UNIT',
    'REMARKS', 'IS_MULTI_SALE']

doc_type = [
    'DEED',  # Deed 
    'DEED, LE', # LIFE ESTATE DEED 
    # 'CORRD', # CORRECTION DEED -- Doesn't work
    # 'CORRD', # CORRECTION MORTGAGE -- Doesn't work
    # 'DEED, RC' # DEED WITH RESTRICTIVE COVENANT -- Doesn't work
    # 'DEEDO', # DEED, OTHER -- Doesn't work
    # 'DEEDP', # DEED, PRE RPT TAX -- Doesn't work
    # 'IDED', # IN REM DEED
    # 'MMRG', # MASTER MORTGAGE
    # 'MTGE', # MORTGAGE
    # 'M&CON', # MORTGAGE AND CONSOLIDATION
    'REIT', # REAL ESTATE TRUST DEED
]

no_records_text = 'No records were found that matched your search criteria.'

# Calculate yesterday's and today's dates
today = datetime.now()
yesterday = today - timedelta(days=1)

# Format the dates as strings in the required format
today_str = today.strftime("%m-%d-%Y")
yesterday_str = yesterday.strftime("%m-%d-%Y")

print(today)
print('-------')
print(yesterday)
print('-------')
print(today_str)
print('-------')
print(yesterday_str)
    
def scrape_data(url):
    try:
        response = requests.get(url, headers=custom_headers)
        response.raise_for_status()  # Raise an exception if the request was unsuccessful

        dfs = pd.read_html(response.content)
        df = dfs[22]
        parcels_column_list = dfs[21].columns
        df.columns = parcels_column_list
        df['IS_MULTI_SALE'] = np.where(df.count(axis=1) > 1, False, True)
        df = df.iloc[0]
        scraped_dict = df.to_dict()
        return scraped_dict
    except Exception as e:
        print(f"Error scraping data from {url}: {e}")
        # Return an empty dictionary with the same keys as your DataFrame
        return {key: None for key in deed_info_list}

def run(playwright: Playwright, doc, csv_name) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context(
        extra_http_headers=custom_headers
    )
    page = context.new_page()
    page.goto("https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentType")
    page.locator("select[name=\"cmb_date\"]").select_option("DR")
    # Fill the date fields with yesterday's and today's dates
    page.locator("input[name=\"edt_fromm\"]").fill(yesterday.strftime("%m"))
    page.locator("input[name=\"edt_fromd\"]").fill(yesterday.strftime("%d"))
    page.locator("input[name=\"edt_fromy\"]").fill(yesterday.strftime("%Y"))
    page.locator("input[name=\"edt_tom\"]").fill(today.strftime("%m"))
    page.locator("input[name=\"edt_tod\"]").fill(today.strftime("%d"))
    page.locator("input[name=\"edt_toy\"]").fill(today.strftime("%Y"))
    page.locator("select[name=\"combox_doc_doctype\"]").select_option(doc)
    page.locator("input[alt=\"Search\"]").click()


    locator = page.locator(f"text={no_records_text}")
    if locator.count() > 0:
        print(f'No records for {doc}')

    else:

        page.locator("select[name=\"com_maxrows\"]").select_option("99")
        page.wait_for_selector("text=View")

        # First page scraping
        dfs = pd.read_html(page.content())
        df = dfs[6]
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        df = df.reset_index(drop=True)
        df = df.shift(-1)

        # Selector for the input elements
        input_selector = 'input.detailandimagebutton[name="DET"]'
        page.wait_for_selector(input_selector)
        input_elements = page.query_selector_all(input_selector)

        # List to hold all the extracted numbers
        extracted_numbers = []

        # Loop through the input elements and extract the numbers
        for input_element in input_elements:
            onclick_value = input_element.get_attribute('onclick')
            
            # Use a regular expression to extract the number from the attribute value
            match = re.search(r'go_detail\("(\d+)"\)', onclick_value)
            if match:
                # Append the extracted number to the list
                extracted_numbers.append(match.group(1))

        # Add links to df
        df['DetailLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentDetail?doc_id=' + pd.Series(extracted_numbers)
        df['DocLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentImageView?doc_id=' + pd.Series(extracted_numbers)


        df_list.append(df)

        # Loop through pages
        while True:
            # Extract table data from the current page
            page.wait_for_selector("text=View")

            # Try to find the "next" hyperlink and click it if it exists
            next_button_selector = 'a:has-text("next")'
            next_button = page.locator(next_button_selector)
            if next_button.is_visible():
                next_button.click()
                page.wait_for_load_state('networkidle')
                page.wait_for_selector("text=View")

                dfs = pd.read_html(page.content())
                current_df = dfs[6]
                current_df.columns = current_df.iloc[0]  # Set the first row as the header
                current_df = current_df.drop(current_df.index[0])  # Drop the first row
                current_df = current_df.reset_index(drop=True)  # Reset the index
                current_df = current_df.shift(-1)

                # Extract numbers from the current page
                input_selector = 'input.detailandimagebutton[name="DET"]'
                input_elements = page.query_selector_all(input_selector)
                extracted_numbers = [
                    re.search(r'go_detail\("(\d+)"\)', input_element.get_attribute('onclick')).group(1)
                    for input_element in input_elements
                    if re.search(r'go_detail\("(\d+)"\)', input_element.get_attribute('onclick'))
                ]
            
                current_df['DetailLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentDetail?doc_id=' + pd.Series(extracted_numbers)
                current_df['DocLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentImageView?doc_id=' + pd.Series(extracted_numbers)


                # Append the current page's DataFrame to df_list
                df_list.append(current_df)
            else:
                break

    # Concatenate all DataFrames into one
    big_df = pd.concat(df_list, ignore_index=True)

    big_df = big_df.drop(columns='View')

    big_df['DOC_TYPE'] = doc

    big_df.to_csv(csv_name, index=False)

    # Clean up
    context.close()
    browser.close()

    return big_df

with sync_playwright() as playwright:
    for doc in doc_type:
        print(f'Starting Phase 1 for {doc}')
        csv_name = f'text_{doc}_ACRIS_{yesterday_str}_to_{today_str}.csv'
        run(playwright, doc, csv_name)

        print(f'Starting Phase 2 for {doc}')
        counter = 0

        df_without_scraped_info = pd.read_csv(csv_name)

        # Iterate through URLs and scrape data
        for index, row in df_without_scraped_info.iterrows():
            url = row['DetailLink']
            scraped_data = scrape_data(url)
            counter = counter + 1
            print(f'{counter} ... {doc}')

            # Append scraped data to scraped_data_list
            scraped_data_list.append(scraped_data)

        # Convert the list of dictionaries to a DataFrame
        scraped_df = pd.DataFrame(scraped_data_list)

        # Concatenate the scraped data DataFrame with your original DataFrame
        merged_df = pd.concat([df_without_scraped_info, scraped_df], axis=1)

        merged_df = merged_df.dropna(axis=1,thresh=4)

        # Define the output script location
        output_file = os.path.join(script_dir, f'text_{doc}_ACRIS_{yesterday_str}_to_{today_str}.csv')

        # Save the final DataFrame to a CSV file
        merged_df.to_csv(output_file, index=False)

# Your service account JSON key file
json_key_file = 'path/to/your/json-key.json'

# The URL of the Google Sheet you want to write to
sheet_url = 'your_google_sheet_url'

# Create a DataFrame (example)
df = pd.DataFrame({
'Column1': [1, 2, 3],
'Column2': ['A', 'B', 'C']
})

# Set up the credentials
scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name(json_key_file, scope)

client = gspread.authorize(creds)

# Open the Google Sheet
sheet = client.open_by_url(sheet_url).sheet1

# Convert the DataFrame to a list of lists
data = df.values.tolist()

# Update the sheet with the DataFrame data
sheet.update('A1', data, value_input_option='USER_ENTERED')
