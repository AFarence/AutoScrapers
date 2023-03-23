import requests
import pandas as pd
import re
import numpy as np
import random
import glob
import os

from tqdm import tqdm
from bs4 import BeautifulSoup

from time import sleep


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}

df = pd.read_csv('redfin_2023-03-21-15-19-02.csv')
df = df.head(100)

def agent_snagger(URL):
    try:
        response = requests.get(URL, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
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
    
max_retries = 3
retry_count = 0
success = False

max_retries = 3
retry_count = 0
success = False

# Split the dataframe into XX equal parts
df_list = np.array_split(df, 5)


with tqdm(total=len(df)) as pbar:
    # Loop over each part of the dataframe
    for i, part in enumerate(df_list):
        results = []
        # Process each URL in the current part of the dataframe
        for url in part['URL']:
            result = agent_snagger(url)
            results.append(result)
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
                success = True
            except Exception as e:
                retry_count += 1
                print(e)
                sleep(4)  # Wait for 1 second before retrying

        if not success:
            print("Code failed after {} retries".format(max_retries))
            
        retry_count = 0
        success = False

        # Write the current part of the dataframe to a CSV
        part.to_csv(f'part_{i}.csv', index=False)

# Get a list of all CSV files with 'part' in the name
file_list = glob.glob('part*.csv')

# Read in each CSV file and combine into one dataframe
df_final = pd.concat((pd.read_csv(f) for f in file_list), ignore_index=True)

# Apply the clean_text function to the desired column
df_final['LIST COMPANIES'] = df_final['LIST COMPANIES'].apply(clean_text)
df_final['BOUGHT COMPANIES'] = df_final['BOUGHT COMPANIES'].apply(clean_text)

df_final['LIST COMPANIES'] = df_final['LIST COMPANIES'].str.replace('\(agent\)','',regex=True)
df_final['LIST COMPANIES'] = df_final['LIST COMPANIES'].str.replace('•','',regex=True)

df_final['BOUGHT COMPANIES'] = df_final['BOUGHT COMPANIES'].str.replace('•','',regex=True)

df_final.to_csv('SF_Redfin_agent_data.csv')