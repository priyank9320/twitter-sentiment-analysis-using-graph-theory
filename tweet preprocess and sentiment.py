#!/usr/bin/env python
# coding: utf-8

# In[1]:


#pip install git+https://github.com/s/preprocessor # this will be performing the main preprocessing


# In[2]:


import mysql.connector
import preprocessor as p
import re
import pandas as pd


# In[3]:


import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\Priyank\\OneDrive\\Documents\\course material 2\\Digital media and social networks\\coursework\\key for google cloud services\\My First Project-bf127fc77e29.json"
os.environ


# In[4]:


##### Retrieving the data from the database

db_connection = mysql.connector.connect(host='localhost', database='tweet_data', user='root', password='password')

df = pd.read_sql('SELECT * from proc_reduced_ex_full', con=db_connection)

#backup_df = df.copy(deep=True)


# In[5]:


# df = df.head(10)
# df.head()
df.shape


# In[6]:


# this will remove the hashtags symbol from the tweets but maintain the hashtag word
df['tweet'] = df['tweet'].apply(lambda x : re.sub(r'#([^\s]+)', r'\1', x) ) 
df.head()


# In[7]:


#this does final cleaning by removing the links , RT , emojis
# the replace function is going to remove the colons which were not getting removed earlier
df['tweet'] = df['tweet'].apply(lambda x : p.clean(x).replace(':','')) 
df.head()


# In[8]:


# this is just a trial code
# text1 = 'RT : Preprocessor is #awesome 👍 https://github.com/s/preprocessor'
# text1 = re.sub(r'#([^\s]+)', r'\1', text1)
# text1 
# p.clean(text1).replace(':','')


# In[9]:


## first we create two new columns for this, fill them with 0.0 and then replace the values when calculating the sentiments
list1 =[0.0000]*df.shape[0]

df['sentimentscore'] = list1
df['sentimentmagnitude'] = list1
df['success'] = list1
df.shape

df.head(50)


# In[10]:


## delete after trial 

# df['tweet'][0] = 'أنا سعيد للغاية الآن بفوز فريقي بالميدالية الذهبية'
# df['tweet'][1] = "Sono così felice in questo momento con la mia squadra che ha vinto l'oro"
# df['tweet'][2] = "我现在很高兴我的团队获得金牌"
# df


# In[11]:


###### this part first translates the text into english and then performs the sentiment analysis on each tweet, tries 10 times on facing any error and then moves on
# we are first performing the trnslation becoz the sentiment analysis was only supoorting a limited number of languages(10) for sentiment analysis as of now

# Imports the Google Cloud client library
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
from google.cloud import translate_v2 as translate ## translation


import traceback

# Instantiates a client
client = language.LanguageServiceClient()
translate_client = translate.Client()
# The text to analyze

for x in range(df.shape[0]):
    print(f'count:{x}')
    text = df['tweet'][x]
    i = 0
    while i < 5:
        i+=1
        try:
            #print(df['tweet'][x])
            result = translate_client.translate(df['tweet'][x], target_language="en")
            df['tweet'][x] = result['translatedText']
            #print(df['tweet'][x])
            
            df['tweet'][x] = result['translatedText']
            text = df['tweet'][x] # new value
            
            document = types.Document(content=text, type=enums.Document.Type.PLAIN_TEXT)

            # Detects the sentiment of the text
            sentiment = client.analyze_sentiment(document=document).document_sentiment
            
            #store the sentiment scores and magnitude
            df['sentimentscore'][x] = sentiment.score
            df['sentimentmagnitude'][x] = sentiment.magnitude
            
            #display the results
            #print('Text: {}'.format(text))
            #print('Sentiment: score = {}, mag = {}'.format(df['sentimentscore'][x], df['sentimentmagnitude'][x]))
            
            #also store a marker indicating if the processing was done on it or not
            df['success'][x] = 1
        except:
            print('Error in record: {}'.format(x))
            print(traceback.format_exc())
            continue  
        break


# In[12]:


# # delete after trial
# # Imports the Google Cloud client library
# from google.cloud import language
# from google.cloud.language import enums
# from google.cloud.language import types

# import traceback

# # Instantiates a client
# client = language.LanguageServiceClient()

# # The text to analyze

# text = "فوز كبير"
# document = types.Document(content=text, type=enums.Document.Type.PLAIN_TEXT)
# sentiment = client.analyze_sentiment(document=document).document_sentiment
# print('Sentiment: score = {}, mag = {}'.format(sentiment.score, sentiment.magnitude))


# In[13]:


# final view at the data set
df


# In[14]:


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

df.to_sql('senti_proc_reduced_ex_full',con=engine,if_exists='append',chunksize=1000)
print("Success !! preprocessing is completed for the full data set ")

