import requests
import pandas as pd
import re
import numpy as np
import os

from tqdm import tqdm
from bs4 import BeautifulSoup

from time import sleep

### For Google Sheets ###
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json

scopes = [
'https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive'
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

df = pd.read_csv('Agent_Scraper/2023_Brokerage_Dallas_Redfin.csv')

def agent_snagger(URL):
    try:
        response = requests.get(URL, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        print(response)
        
        # Extract agent info
        agent_soup = soup.find_all('div', {'class': 'agent-info-content'}) or None
        
        # Extract sold info
        list_soup = soup.find_all('div', {'class': 'listing-agent-item'}) or None
        
        # Extract bought info
        bought_soup = soup.find_all('div', {'class': 'buyer-agent-item'}) or None
        
        # Return results as a dictionary
        return {'agent_soup': agent_soup, 'list_soup': list_soup, 'bought_soup': bought_soup}
    except:
        # Return failure message as a dictionary
        return {'agent_soup': None, 'list_soup': None, 'bought_soup': None}
    

def split_agents_companies(row):
    agents = []
    companies = []
    for agent_company in row:
        if isinstance(agent_company, str):
            agent_company = agent_company.strip()
            if '•' in agent_company:
                parts = agent_company.split('•')
                agent = parts[0].strip()
                company = '•'.join(parts[1:]).strip()
                agents.append(agent)
                companies.append(company)
            else:
                agents.append(agent_company)
                companies.append('')
        else:
            agents.append('')
            companies.append('')
    return [agents, companies]


# Define the regex pattern
pattern = r"\b(DRE #\d+)|(\d{3}-\d{3}-\d{4})|(\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b)|•\s*([\w\s]+)\s*\(\s*agent\s*\)\s*\(\s*agent\s*\)"

# Define a function to clean the text in the desired column
def clean_text(text):
    if isinstance(text, str):
        return re.sub(pattern, "", text).strip()
    else:
        return text

# Define a function to write data to Google sheets
def update_spreadsheet(df):
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ.get('SERVICE_ACCOUNT_JSON')), scopes)
    file = gspread.authorize(credentials)
    sheet = file.open("DallasRedfinScraper").worksheet("ScrapedData")
    
    data = df.values.tolist()

    # Write new data and
    sheet.insert_rows(data, 1)

# Define variables for max retries, retry county, and the success boolean
max_retries = 3
retry_count = 0
success = False

# Figure out how to split the df so it always runs in chunks of 100

split_by = round(len(df) / 100)

# Split the dataframe into XX equal parts
df_list = np.array_split(df, split_by)

with tqdm(total=len(df)) as pbar:
    # Loop over each part of the dataframe
    for i, part in enumerate(df_list):
        results = []
        # Process each URL in the current part of the dataframe
        for url in part['URL']:
            result = agent_snagger(url)
            results.append(result)
            print(result)
            pbar.update(1)
            
        while retry_count < max_retries and not success:
            try:
                part['BROKER INFO'] = results
                part_results = pd.DataFrame(results)
                part['ALL INFO'] = part_results['agent_soup']
                part['LIST INFO'] = part_results['list_soup']
                part['BOUGHT INFO'] = part_results['bought_soup']
                part['ALL INFO CLEAN'] = part['ALL INFO'].apply(lambda x: [spoonful.text.strip() for spoonful in x] if isinstance(x, list) else None)
                part['LIST INFO CLEAN'] = part['LIST INFO'].apply(lambda x: [spoonful.text.strip() for spoonful in x] if isinstance(x, list) else None)
                part['BOUGHT INFO CLEAN'] = part['BOUGHT INFO'].apply(lambda x: [spoonful.text.strip() for spoonful in x] if isinstance(x, list) else None)
                part.reset_index(inplace=True, drop=True)
                # Apply the function to the SOLD INFO CLEAN column
                part['LIST INFO CLEAN'] = part['LIST INFO CLEAN'].fillna('')
                part['BOUGHT INFO CLEAN'] = part['BOUGHT INFO CLEAN'].fillna('')
                part[['LIST AGENTS', 'LIST COMPANIES']] = part['LIST INFO CLEAN'].apply(split_agents_companies).apply(pd.Series)
                part[['BOUGHT AGENTS', 'BOUGHT COMPANIES']] = part['BOUGHT INFO CLEAN'].apply(split_agents_companies).apply(pd.Series)
                part['LIST AGENTS'] = part['LIST AGENTS'].apply(lambda x: ', '.join(str(e) for e in x) if len(x) > 0 else float('nan'))
                part['LIST COMPANIES'] = part['LIST COMPANIES'].apply(lambda x: ', '.join(str(e) for e in x) if len(x) > 0 else float('nan'))
                part['BOUGHT AGENTS'] = part['BOUGHT AGENTS'].apply(lambda x: ', '.join(str(e) for e in x) if len(x) > 0 else float('nan'))
                part['BOUGHT COMPANIES'] = part['BOUGHT COMPANIES'].apply(lambda x: ', '.join(str(e) for e in x) if len(x) > 0 else float('nan'))
                part['LIST AGENTS'] = part['LIST AGENTS'].str.replace('Listed by','')
                part['BOUGHT AGENTS'] = part['BOUGHT AGENTS'].str.replace('Bought with','')
                # Apply the clean_text function to the desired column
                part['LIST COMPANIES'] = part['LIST COMPANIES'].apply(clean_text)
                part['BOUGHT COMPANIES'] = part['BOUGHT COMPANIES'].apply(clean_text)

                part['LIST COMPANIES'] = part['LIST COMPANIES'].str.replace('\(agent\)','',regex=True)
                part['LIST COMPANIES'] = part['LIST COMPANIES'].str.replace('•','',regex=True)

                part['BOUGHT COMPANIES'] = part['BOUGHT COMPANIES'].str.replace('•','',regex=True)

                print(part)
                update_spreadsheet(part)
                
                success = True
            except Exception as e:
                retry_count += 1
                print(e)
                sleep(4)  # Wait for 4 seconds before retrying

        if not success:
            print("Code failed after {} retries".format(max_retries))

        retry_count = 0
        success = False