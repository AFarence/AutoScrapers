#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests
from bs4 import BeautifulSoup

### For Google Sheets ###
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import json


# In[2]:


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
}


# In[3]:


url = 'https://www.redfin.com/county/486/FL/Palm-Beach-County/filter/sort=hi-price,include=sold-1wk'


# In[4]:


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

# In[6]:


data = {'Address':address_soup,'Price':price_soup,'URL':new_list}


# In[7]:


df = pd.DataFrame(data)


# In[8]:


df['URL'] = 'redfin.com' + df['URL'].astype(str)


# In[9]:


for i, r in df.iterrows():
    r['Price'] = r['Price'].text
    r['Address'] = r['Address'].text


# In[10]:


df['Price'] = df['Price'].str.replace('$','',regex=True)
df['Price'] = df['Price'].str.replace(',','',regex=True)
df['Price'] = pd.to_numeric(df['Price'])


# In[11]:


df = df.sort_values(by='Price', ascending=False)


# In[12]:


df.reset_index(inplace=True, drop=True)


# In[13]:


print(df)


# In[14]:


scopes = [
'https://www.googleapis.com/auth/spreadsheets',
'https://www.googleapis.com/auth/drive'
]

credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    json.loads(os.environ.get('SERVICE_ACCOUNT_JSON')), scopes)
file = gspread.authorize(credentials)
sheet = file.open("RedfinFeed").worksheet("PalmBeach")


# In[ ]:


# Clear existing data (optional)
sheet.clear()


# In[ ]:


header = df.columns.tolist()
data = df.values.tolist()
sheet.insert_row(header, 1)
sheet.insert_rows(data, 2)