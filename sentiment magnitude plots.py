#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#pip install git+https://github.com/s/preprocessor


# In[ ]:


import mysql.connector ## database connection
import pandas as pd ## for pandas dataframe
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from matplotlib import dates as mpl_dates
import datetime
import time
from matplotlib import dates as mpl_dates


# In[ ]:


##### Retrieving the data from the database

db_connection = mysql.connector.connect(host='localhost', database='proc_comm1', user='root', password='password')

df = pd.read_sql('SELECT * from proc_netcom', con=db_connection)


# In[ ]:


backup_df = df.copy(deep=True)


# In[ ]:


x = 25 # set the community number here for plotting


# In[ ]:


df2 = df[df['community']== x]
df2.shape


# In[ ]:


df2.dtypes
df2['createdAt'] = pd.to_datetime(df2.createdAt).dt.time # dt.time part is actually extracting just the time part and excluding the date
plt.figure(figsize=(8,6))
plt.plot(df2['createdAt'],df2['sentimentmagnitude'],'b.') # first one is x and second is y axis list
plt.ylabel('magnitude of sentiments',fontsize=18)
plt.xlabel('timeline',fontsize=18)
plt.style.use('seaborn')
plt.tight_layout() # this kinda increased the size of the plot, supposedly adds padding
plt.xlim([datetime.time(hour=5, minute=0), datetime.time(hour=19, minute=30)])
plt.ylim(bottom=0,top=3.5)
plt.xticks(fontsize=12)
plt.gcf().autofmt_xdate()


labels = ["no.of tweets = "+ str(df2.shape[0])]
l2 = [df2.shape[0]]
plt.legend(labels,prop={'size': 16})

plt.show()

