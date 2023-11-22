import pandas as pd
import re
from playwright.sync_api import Playwright, sync_playwright, expect
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import os
import json

df_list = []

# Define your custom headers as a dictionary
custom_headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.4567.89 Safari/537.36'
}

def run(playwright: Playwright) -> None:
    # Calculate yesterday's and today's dates
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    # Format the dates as strings in the required format (e.g., "MM/DD/YYYY")
    today_str = today.strftime("%m/%d/%Y")
    yesterday_str = yesterday.strftime("%m/%d/%Y")

    browser = playwright.chromium.launch(headless=True)
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
    page.locator("select[name=\"combox_doc_doctype\"]").select_option("DEED")
    page.locator("input[alt=\"Search\"]").click()
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
        # Get the onclick attribute value
        onclick_value = input_element.get_attribute('onclick')
        
        # Use a regular expression to extract the number from the attribute value
        match = re.search(r'go_detail\("(\d+)"\)', onclick_value)
        if match:
            # Append the extracted number to the list
            extracted_numbers.append(match.group(1))

    # Assuming df has the same number of rows as extracted_numbers
    # And that you want to map each extracted number to the corresponding row in df
    df['DetailLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentDetail?doc_id=' + pd.Series(extracted_numbers)
    df['DocLink'] = 'https://a836-acris.nyc.gov/DS/DocumentSearch/DocumentImageView?doc_id=' + pd.Series(extracted_numbers)


    df_list.append(df)

    # Initialize an empty list to store the extracted numbers
    extracted_numbers_list = []

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

    # Save the final DataFrame to a CSV file
    big_df.to_csv('text.csv', index=False)

    # Clean up
    context.close()
    browser.close()

    big_df = big_df.fillna('NA')

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
    sheet = file.open("NYC")
    worksheet = sheet.worksheet("ACRIS")

    # Clear existing data (optional)
    worksheet.clear()

    # Write DataFrame to Google Sheet
    worksheet.update([big_df.columns.values.tolist()] + big_df.values.tolist())

    print(f'Sheet Updated! {big_df.columns.value_counts()}')

with sync_playwright() as playwright:
    run(playwright)