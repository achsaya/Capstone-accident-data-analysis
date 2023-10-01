#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

# Load the dataset
df = pd.read_csv('US_Accidents.csv')


# In[4]:


df.shape


# In[11]:


# Select only the columns you need
desired_columns = ['ID', 'Source', 'Severity','Start_Time','End_Time','Start_Lat','Start_Lng','Street','City','State','Zipcode','Country'
,'Traffic_Signal','Weather_Condition']
filtered_df = df[desired_columns]



# In[12]:


num_rows_to_select = 4000000
random_sample_df = filtered_df.sample(n=num_rows_to_select, random_state=42)


# In[13]:


random_sample_df.to_csv('accident_new.csv', index=False)


# In[ ]:




