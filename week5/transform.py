#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
df = pd.read_csv('books.csv')
df.head()


# In[2]:


df2 = df.drop(columns=['Edition Statement', 'Corporate Author', 'Former owner', 'Corporate Contributors', 'Engraver', 'Issuance type', 'Shelfmarks'])
df2.head(20)


# In[3]:


df3 = pd.read_csv('books.csv', usecols=['Date of Publication', 'Identifier', 'Edition Statement', 'Place of Publication', 'Date of Publication', 'Publisher', 'Title', 'Author', 'Contributors', 'Corporate Author', 'Flickr URL'])
df3.head()


# In[4]:


import numpy as np

df4 = df2
df4.loc[df4['Date of Publication'].astype(str).str.contains('\?|') | df4['Date of Publication'] == 'nan'] = np.nan
df4['Date of Publication'] = df4['Date of Publication'].str.extract(r'^(\d{4})', expand=False)
df4.head(30)


# In[5]:


import pandas as pd
df = pd.read_csv('uniplaces.txt', sep='\t')
df.head()


# In[33]:


df2 = df.fillna({ 'State': df['State'].ffill() })
df3 = df2[df2['City'].notna()]
df3.head(20)


# In[40]:


import re 
df4 = df3
df4['State'] = df4['State'].apply(lambda x: x.replace(r'[edit]', ''))

df5 = df4
df5['University'] = df5['City'].apply(lambda x: re.sub(r'^.*?\(|\).*?$', r'', x))
df5['City'] = df5['City'].apply(lambda x: re.sub(r' \(.*?$', r'', x))
df5.head()


# In[53]:


import pandas as pd
df = pd.read_csv('trimet_03411_2.csv')

df2 = df
df2 = df2.loc[df.index.repeat(df['OCCURRENCES'])].reset_index(drop=True)
df2[df2['OCCURRENCES'] > 1].head(20)


# In[54]:


df3 = df2
df3[df3['ARRIVE_TIME'].isna()].head()


# In[55]:


df3['ARRIVE_TIME'] = df3['ARRIVE_TIME'].interpolate()

df3[df3['VEHICLE_BREADCRUMB_ID'] == 4313659935].head()


# In[56]:


df3[150:160].head()

