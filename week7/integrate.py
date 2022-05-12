#!/usr/bin/env python
# coding: utf-8

# In[20]:


import pandas as pd

data = pd.read_csv('acs2017_census_tract_data.csv')
df = pd.DataFrame({'County': [], 'Population': [], 'Poverty': [], 'PerCapitaIncome': []})
df.head()


# In[21]:


df['County'] = data['County'].apply(lambda x: x.replace(' County', ''))
df['County'] = df['County'].astype(str) + ', ' + data['State'].astype(str)
df['Population'] = data['TotalPop']
df['Poverty'] = data['Poverty']
df['PerCapitaIncome'] = data['IncomePerCap']

df.head()


# In[22]:


df2 = df

df2['CountyPopulation'] = df2.groupby(['County'])['Population'].transform('sum')

df2['PovertyPopulation'] = df2['Poverty'] * df2['Population']
df2['PovertyPopulation'] = df2.groupby(['County'])['PovertyPopulation'].transform('sum')
df2['PovertyPopulation'] = df2['PovertyPopulation'] / df2['CountyPopulation']
df2['Poverty'] = df2['PovertyPopulation']

df2['IncomePopulation'] = df2['PerCapitaIncome'] * df2['Population']
df2['IncomePopulation'] = df2.groupby(['County'])['IncomePopulation'].transform('sum')
df2['IncomePopulation'] = df2['IncomePopulation'] / df2['CountyPopulation']
df2['PerCapitaIncome'] = df2['IncomePopulation']

df2['Population'] = df2['CountyPopulation']

df2 = df2.drop(columns=['IncomePopulation', 'PovertyPopulation', 'CountyPopulation'])
df3 = df2.drop_duplicates(subset=['County'])
df3.head()


# In[23]:


print(df3[(df3['County'] == 'Loudoun, Virginia')])
print(df3[(df3['County'] == 'Washington, Oregon')])
print(df3[(df3['County'] == 'Harlan, Kentucky')])
print(df3[(df3['County'] == 'Malheur, Oregon')])
print(df3[df3['Population'] == df3['Population'].max()])
print(df3[df3['Population'] == df3['Population'].min()])


# In[25]:


acs = df3
acs.head()


# In[24]:


import re

data = pd.read_csv('COVID_county_data.csv')
df = pd.DataFrame({'County': [], 'Month': [], '# cases': [], '# deaths': []})
df.head()

df['County'] = data['county'].astype(str) + ', ' + data['state'].astype(str)
df['Month'] = data['date'].apply(lambda x: re.sub(r'-\d{2}$', '', x))
df['# cases'] = data['cases']
df['# deaths'] = data['deaths']

df.head()


# In[26]:


df2 = df

df2['# cases'] = df2.groupby(['County', 'Month'])['# cases'].transform('sum')
df2['# deaths'] = df2.groupby(['County', 'Month'])['# deaths'].transform('sum')

df3 = df2.drop_duplicates(subset=['County', 'Month'])

df3.head()


# In[27]:


print(df3[df3['County'] == 'Washington, Oregon'])
print(df3[df3['County'] == 'Malheur, Oregon'])
print(df3[df3['County'] == 'Washington, Oregon'])
print(df3[df3['County'] == 'Loudoun, Virginia'])
print(df3[df3['County'] == 'Harlan, Kentucky'])


# In[28]:


df4 = df3
df4['# cases'] = df3.groupby(['County'])['# cases'].transform('sum')
df4['# deaths'] = df3.groupby(['County'])['# deaths'].transform('sum')
df4 = df4.drop(columns=['Month'])
df5 = df4.drop_duplicates(subset=['County'])
df5.head()


# In[29]:


df6 = acs.merge(df5, on=['County'], how='outer')
df6.head()


# In[30]:


df6['TotalCasesPer100K'] = df6['# cases'] / 100000
df6['TotalDeathsPer100K'] = df6['# deaths'] / 100000
df6.head()


# In[31]:


print(df6[df6['County'] == 'Washington, Oregon'])
print(df6[df6['County'] == 'Malheur, Oregon'])
print(df6[df6['County'] == 'Loudoun, Virginia'])
print(df6[df6['County'] == 'Harlan, Kentucky'])


# In[35]:


dfo = df6[df6['County'].str.contains('Oregon')]
print(dfo['TotalCasesPer100K'].corr(dfo['Poverty']))
print(dfo['TotalDeathsPer100K'].corr(dfo['Poverty']))
print(dfo['TotalCasesPer100K'].corr(dfo['PerCapitaIncome']))
print(dfo['TotalDeathsPer100K'].corr(dfo['PerCapitaIncome']))


# In[36]:


print(df6['TotalCasesPer100K'].corr(df6['Poverty']))
print(df6['TotalDeathsPer100K'].corr(df6['Poverty']))
print(df6['TotalCasesPer100K'].corr(df6['PerCapitaIncome']))
print(df6['TotalDeathsPer100K'].corr(df6['PerCapitaIncome']))


# In[37]:


df6['TotalCasesPer100K'].corr(df6['Population'])


# In[39]:


df6['TotalCasesPer100K'].corr(df6['TotalDeathsPer100K'])


# In[42]:


ax1 = df6.plot.scatter(x='Population',
                      y='TotalCasesPer100K',
                      c='DarkBlue')

