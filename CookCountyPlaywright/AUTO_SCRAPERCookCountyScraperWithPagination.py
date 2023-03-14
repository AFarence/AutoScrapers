#!/usr/bin/env python
# coding: utf-8

# ## Imports

# In[30]:


import pandas as pd
import numpy as np
from playwright.sync_api import Playwright, sync_playwright, expect
from bs4 import BeautifulSoup
import re
import glob
import os
from datetime import date
import requests
import time

### For Google Sheets ###
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json


# ## Scraper

# In[3]:


### Find local path
path = os.getcwd()


# In[4]:


### Get a list of all CSV files in current directory
filelist = glob.glob(os.path.join(path,'*.csv'))


# In[5]:

### Remove any CSV files in current directory
for f in filelist:
    os.remove(f)


# Web link: https://crs.cookcountyclerkil.gov/Search/Additional

# ## Main Doc Scraper
# (Deeds & mortgages, those with a consideration amount)

# In[7]:


### Set up start date, end date, and doc list

start_date = '01012022'
end_date = '03132023'

run_date = date.today().strftime('%b-%d-%Y')

doc_list = [
    'DEED',
    'DEIT',
    'QCD',
    'SPWD',
    'TRUD',
    'TEED',
    'WARD',
    'MORT'
]


# Target website: https://crs.cookcountyclerkil.gov/Search/Additional

# In[8]:


def page_scraper(page, page_counter):

    dfs = pd.read_html(page)
    df = dfs[0]

    soup = BeautifulSoup(page, 'html.parser')

    link_suffix = 'https://crs.cookcountyclerkil.gov/'

    deed_urls = []
    for link in soup.find_all('a',attrs={'href': re.compile('^/Document/Detail')}):
        page = link_suffix + link.get('href')
        deed_urls.append(page)

    df['deed_urls'] = deed_urls

    df['Consi. Amt.'] = df['Consi. Amt.'].str.replace('$','',regex=False)
    df['Consi. Amt.'] = df['Consi. Amt.'].str.replace(',','',regex=False)

    df['Consi. Amt.'] = pd.to_numeric(df['Consi. Amt.'])

    page_counter = page_counter + 1

    df.to_csv(f'{doc}_page{page_counter}_{start_date}_to_{end_date}.csv')
    
    return page_counter


# In[9]:


### Run main doc scraper

def run(playwright: Playwright) -> None:

    browser = playwright.chromium.launch(headless = False)
    page = browser.new_page()

    # Go to https://crs.cookcountyclerkil.gov/Search/Additional
    page.goto("https://crs.cookcountyclerkil.gov/Search/Additional")

    # Click text=Document Type Search
    page.locator("text=Document Type Search").click()

    # Select DEED
    page.locator("text=Document Type * ABROGATION ACCEPTANCE ACCEPTANCE OF TRANFER ON DEATH INSTRUMEN A >> select[name=\"DocumentType\"]").select_option(doc)

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

        x = page.content()
        
        try:
        
            page_scraper(x, page_counter)
            
        except Exception as e:
            print(e)
            
        i = 1
        
        while i < 100:
            
            try:
                
                page.wait_for_selector("div[class=table-responsive]")

                page.locator("text=»").click()
                
                y = page.content()

                page_scraper(y, i)
                
                i += 1
                
            except Exception as e:
                
                i += 1000
                
                page.wait_for_selector("div[class=table-responsive]")
                
                y = page.content()

                page_scraper(y, i)
        
        browser.close()

    # ---------------------
        
    except Exception as e:
        print(e)
    
for doc in doc_list:
    
    page_counter = 0
    
    with sync_playwright() as playwright:
        run(playwright)
    

# ## Create Master CSV file

# In[13]:


### Join all created CSVs into one file

all_csvs = glob.glob(os.path.join(path,'*.csv'))

li = []

for filename in all_csvs:
    frame = pd.read_csv(filename, index_col=None, header=0)
    li.append(frame)
    
df = pd.concat(li, axis=0, ignore_index=True)


# In[14]:


### Clean master CSV and edit data types

df = df.drop(columns=['Unnamed: 0.1','Unnamed: 0','View Doc'])
df['Consi. Amt.'] = df['Consi. Amt.'].apply(lambda x : '${:,}'.format(x))

df['Doc Recorded'] = pd.to_datetime(df['Doc Recorded'])
df = df.sort_values(by='Doc Recorded', ascending=False)

df = df.drop_duplicates()

x = f'ALL_DEEDS_{start_date}_to_{end_date}_run_{run_date}.csv'
df.to_csv(x)


# In[21]:


df_deed = df.loc[df['Doc Type'] != 'MORTGAGE'].reset_index()
df_deed = df_deed.drop(columns=['level_0','index'])


# ## Google Sheets

# In[22]:


scopes = [
'https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive'
]

credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ.get('SERVICE_ACCOUNT_JSON')), scopes)
file = gspread.authorize(credentials)
sheet = file.open("CookCountyScraper")
sheet = sheet.sheet1


# In[23]:


df = pd.read_csv(x)


# In[24]:


df = df.drop(columns=['Unnamed: 0','index'])


# In[27]:


df = df.replace([pd.np.inf, -pd.np.inf, pd.np.nan], 'NA')


# In[28]:


# Clear existing data (optional)
sheet.clear()


# In[29]:


header = df.columns.tolist()
data = df.values.tolist()
sheet.insert_row(header, 1)
sheet.insert_rows(data, 2)

