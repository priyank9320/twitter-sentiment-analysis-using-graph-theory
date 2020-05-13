#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector ## database connection
import pandas as pd ## for pandas dataframe
from nltk.tokenize import RegexpTokenizer ## tokenizer


# In[ ]:


##### DATABASE CONNECTION
db_connection = mysql.connector.connect(host='localhost', database='tweet_data', user='root', password='password')


# In[ ]:


## import raw reduced adjacency list (SOURCE)
raw_reduc_adj = pd.read_sql('SELECT Source as raw_source from reduced_ex_adjacency', con=db_connection)


# In[ ]:


raw_reduc_adj.head(50)


# In[ ]:


## import prcocessed reduced adjacency list (non finlasied data) (SOURCE)
proc_reduc_adj = pd.read_sql('SELECT Source as proc_source from proc_ex_adjacency', con=db_connection)


# In[ ]:


proc_reduc_adj.head(50)


# In[ ]:


## import reduced MAIN DATA 
reduc_full = pd.read_sql('SELECT * from reduced_ex_full', con=db_connection)


# In[ ]:


reduc_full.head(50)


# In[ ]:


## join the data proc and raw adjacency into one , for easy comparision 
joined_proc_raw_adj = pd.concat([raw_reduc_adj, proc_reduc_adj], axis=1)
joined_proc_raw_adj.shape


# In[ ]:


joined_proc_raw_adj.head(50)


# In[ ]:


###### tokenize the MAIN DATA (this we are doing to convert the text into a list of words)

## tokenizer is converting into a list and stores it back in the hashtags column 
tokenizer = RegexpTokenizer(r'\w+')
reduc_full['hashtags']=reduc_full['hashtags'].apply(lambda x: tokenizer.tokenize(x))


# In[ ]:


## create a dictionary from SOURCE columns

joined_dict1 = joined_proc_raw_adj.set_index('raw_source').T.to_dict('record')[0]


# In[ ]:


joined_dict1


# In[ ]:


#joined_dict1['Gold']


# In[ ]:


## create dictionary from TARGET columns
## import raw reduced adjacency list
raw_reduc_adj2 = pd.read_sql('SELECT Target as raw_target from reduced_ex_adjacency', con=db_connection)
## import prcocessed reduced adjacency list (non finlasied data)
proc_reduc_adj2 = pd.read_sql('SELECT Target as proc_target from proc_ex_adjacency', con=db_connection)
## join them
joined_proc_raw_adj2 = pd.concat([raw_reduc_adj2, proc_reduc_adj2], axis=1)
## create dictionary
joined_dict2 = joined_proc_raw_adj2.set_index('raw_target').T.to_dict('record')[0]


# In[ ]:


joined_dict2


# In[ ]:


len(joined_dict1)


# In[ ]:


len(joined_dict2)


# In[ ]:


joined_dict1.update(joined_dict2)
len(joined_dict1)


# In[ ]:


reduc_full #before


# In[ ]:


#replace the values in MAIN DATA using DICTIONARIES

for x in range(reduc_full.shape[0]):
    for  y in range(len(reduc_full['hashtags'][x])):
        if reduc_full['hashtags'][x][y] in joined_dict1:
            reduc_full['hashtags'][x][y] = joined_dict1[reduc_full['hashtags'][x][y]]


# In[ ]:


reduc_full #after


# In[ ]:


#Joining the lists back to get a final string of processed hashtags
reduc_full['hashtags']=reduc_full['hashtags'].apply(lambda x: ' '.join(x))


# In[ ]:


reduc_full # final


# In[ ]:


#just converting everything to lower once
reduc_full["hashtags"] = reduc_full["hashtags"].str.lower()


# In[ ]:


reduc_full


# In[ ]:


##### FINAL STEP : storing the data back into mysql database
## inserting pandas data frame back to the mysql

from sqlalchemy import create_engine
import pymysql

connection = pymysql.connect(host='localhost',
                            user='root',
                            password='password',
                            db='tweet_data')

cursor=connection.cursor()

engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user = "root",
                              pw="password",
                              db="tweet_data"))


#insert the entire dataframe into mysql
# df is the name of our data frame

reduc_full.to_sql('proc_reduced_ex_full',con=engine,if_exists='append',chunksize=1000)
print("Success !! preprocessing is completed for the full data set ")


# In[ ]:





# In[ ]:




