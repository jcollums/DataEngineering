#!/usr/bin/env python
# coding: utf-8

# In[5]:


# load data
import pandas
import numpy as np
data = pandas.read_csv('original_data.csv')


# In[23]:


crash_ids = data['Crash ID'].unique()
print(len(crash_ids))

vehicles = data[data['Vehicle ID'].notnull()]
for id in crash_ids:
    exists = vehicles[data['Crash ID'] == id]
    if len(exists) == 0:
        print(f'Crash ID {id} does not have any vehicles associated with it')
        
participants = data[data['Participant ID'].notnull()]
for id in crash_ids:
    exists = participants[data['Crash ID'] == id]
    if len(exists) == 0:
        print(f'Crash ID {id} does not have any participants associated with it')


# In[7]:


lon = data[data['Longitude Degrees'].gt(180) | data['Longitude Degrees'].lt(-180)].filter(items=['Longitude Degrees'])
print(lon.head())

lat = data[data['Latitude Degrees'].gt(90) | data['Latitude Degrees'].lt(-90)].filter(items=['Latitude Degrees'])
print(lat.head())


# In[8]:


if len(participants) > len(vehicles):
    print('There are more participants than vehicles')


# In[24]:


data.groupby(['Crash Month']).count()['Crash ID'].plot.bar()


# In[34]:


crashes = data[data['Record Type'] == 1].dropna(axis=1, how='all')
vehicles = data[data['Record Type'] == 2].dropna(axis=1, how='all')
participants = data[data['Record Type'] == 3].dropna(axis=1, how='all')


# In[35]:


crashes.head()


# In[37]:


vehicles.head()


# In[ ]:





# In[38]:


participants.head()


# In[40]:


crashes.to_csv('crashes.csv')
vehicles.to_csv('vehicles.csv')
participants.to_csv('participants.csv')

