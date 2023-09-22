#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

# Load the dataset
df = pd.read_csv('US_Accidents_March23.csv')


# In[3]:


# Select only the columns you need
desired_columns = ['ID', 'Source', 'Severity','Start_Time','End_Time','Start_Lat','Start_Lng','End_Lat','End_Lng','Distance(mi)','Description','City','County','State','Zipcode','Country','Weather_Condition']
filtered_df = df[desired_columns]



# In[6]:


num_rows_to_select = 100000
random_sample_df = filtered_df.sample(n=num_rows_to_select, random_state=42)


# In[7]:


random_sample_df.to_csv('accident.csv', index=False)


# In[ ]:




