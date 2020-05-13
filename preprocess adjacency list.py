#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# installed all these
# pip install mysql-connector
# pip install googletrans
# pip install google-cloud-translate==2.0.0
# pip install wordninja


# In[ ]:


# ALL THE IMPOORT STATEMENTS OVER HERE
import pandas as pd
import traceback ## for the exception handling


# In[ ]:


## this we do to set the environment variable, so that we can use the cloud service, the path refers to the location where we have saved the key 


import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\Priyank\\OneDrive\\Documents\\course material 2\\Digital media and social networks\\coursework\\key for google cloud services\\My First Project-bf127fc77e29.json"
os.environ


# In[ ]:


from  googletrans import Translator
import mysql.connector 
import pandas as pd


import nltk
from bs4 import BeautifulSoup
import string
from nltk.corpus import stopwords
#tokenizer
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer


# In[ ]:



db_connection = mysql.connector.connect(host='localhost', database='tweet_data', user='root', password='password')

df = pd.read_sql('SELECT * from reduced_ex_adjacency', con=db_connection)

#backup_df = df.copy(deep=True)


# In[ ]:


df.shape


# In[ ]:


df.head(50)


# In[ ]:


# PREP 1 = SPELLING ( this is very slow process , I ran this for around 18 hours and it couldnt complete the processing,
# thats why I am now reducing the data set, by removing the data which co-occured less than 10 time )
# the query used was : create table reduced_ad_adjacency select * from  tweet_data.sochiad_adjacency_list where weight > 9;

#@@@@@@@@@@@@@ CHANGES MADE

# spelling correction ( works very slow)
from spellchecker import SpellChecker

spell = SpellChecker()
spell.word_frequency.load_words(['sochi','beiber','canucks','yuna'])

df['Source'] = df['Source'].apply(lambda x : spell.correction(x)) # the rows are just SINGLE words, as this is the first step
print("Source column completed , now moving on to the Target column")
df['Target'] = df['Target'].apply(lambda x : spell.correction(x))



#@@@@@@@@@ reults of adding spelling correction in the beginning 
# words which dont have spaces dont have nay improevement or degradation
# different language words didnt show any improvement or degradation
# single words and even names got corrected
# apply() function is NOT inplace so we have assigned it properly now


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


# PREP 2 = TRANSLATION

###################################################
#using GCP google transalte service 


from google.cloud import translate_v2 as translate
translate_client = translate.Client()

for x in range(df.shape[0]): ## for number of rows  
    while True:
        try:    
            result1 = translate_client.translate(df['Source'][x], target_language="en")
            df['Source'][x] = result1['translatedText'].lower()
    
            result2 = translate_client.translate(df['Target'][x], target_language="en")
            df['Target'][x] = result2['translatedText'].lower()
        except:
            print('Error in record: {}'.format(x))
            print(traceback.format_exc())
            continue
        break
        


#@@@@@@@@@ foreign language with very wrong spelling didnt get translated


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


## since we have done the entire translation , taking a backup 
##%%%%%%%%%%%%%%%%%%%%%%%%%%%
backup_df = df.copy(deep=True)

#####DO NOT TOUCH THIS : LIVE TRANSLATED DATA HERE##############


# In[ ]:


#recovering the backup
#df = backup_df.copy(deep=True)


# In[ ]:


# PREP 3 = WORD SPLITTING

## word splitting ( CAREFUL as it DISCARDS THE CHINESE AND OTHER LANGUAGE WORDS, therefore we did translation before)
## this creates the requirement for variable z to reach the words, word = df['hashtags'][x][y][z]
import wordninja       
        
for x in range(df.shape[0]): ## for no. of rows
    df['Source'][x] = wordninja.split(df['Source'][x])
    df['Target'][x] = wordninja.split(df['Target'][x])

        


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


## download the corpora wordnet for lemmatizer

nltk.download('wordnet')


# In[ ]:


# PREP 4 = STEMMING

##### Stemming 
# Lemmatizer didnt work, as it doesn't it requires Part of speech tagging , on Kaggle it was written that is not possible for a single word , as we dont have context for tagging , and so everythinng is taken as a noun
stemmer = PorterStemmer()

for x in range(df.shape[0]):
    for y in range(len(df['Source'][x])): # so this we run for all the elements in the list
        df['Source'][x][y] = stemmer.stem(df['Source'][x][y])
        
for x in range(df.shape[0]): # this we run for all the elements in the list
    for y in range(len(df['Target'][x])):     
        df['Target'][x][y] = stemmer.stem(df['Target'][x][y])


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


# PREP 5 = STOP WORD REMOVAL

## removing the stop words
import nltk
nltk.download('stopwords')
stop_words = set(nltk.corpus.stopwords.words('english'))


# In[ ]:


## these words we need to exclude
exclude_words = set(('can','usa','all')) ## if we keep just one word it was not behaving properly, thus added usa 
new_stop_words = stop_words.difference(exclude_words)


# In[ ]:


def remove_stopwords(text):
    words=[w for w in text if w not in new_stop_words]
    return words


# In[ ]:


## this line acts as a checking line to see if stopword removal is working or not
remove_stopwords(['me','too','hi','all','can','we','i','the','for','is','a','be','when','if'])


# In[ ]:


for x in range(df.shape[0]):        
    
    cash = df['Source'][x] ## maintain value before removing words
    df['Source'][x] = remove_stopwords(df['Source'][x]) ## remove the words
    if len(df['Source'][x]) == 0: ## check if all the words were just stop words
        df['Source'][x] = cash ## if yes then REVERT BACK, if everything is just stop words, so that we dont loose data like me too
        
    cash = df['Target'][x]
    df['Target'][x]= remove_stopwords(df['Target'][x])
    if len(df['Target'][x]) == 0:
        df['Target'][x] = cash ## that is REVERT BACK, if everything is just stop words, so that we dont loose data like me too  
    
    
    
    
   


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


# PREP 6 = ALL LOWER CASE

#########tested this working fine now

## just to be safe we should lower() all the words again before joining them back

for x in range(df.shape[0]): ## for no. of rows
    for y in range(len(df['Source'][x])): ## length of the wrapper list
        df['Source'][x][y] = df['Source'][x][y].lower()
        #print(df['Source'][x][y][z])
            
            
for x in range(df.shape[0]): ## for no. of rows
    for y in range(len(df['Target'][x])): ## length of the wrapper list
        df['Target'][x][y] = df['Target'][x][y].lower()
            #print(df['Target'][x][y][z])


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


# PREP 7 = JOINING THE WORDS BACK WITHOUT SPACES

## joins the list items back into words without space
for x in range(df.shape[0]):
    df['Source'][x] = str(''.join(df['Source'][x]))
    #print(f"before : {df['Target'][x][0]}")
    df['Target'][x] = str(''.join(df['Target'][x]))
    #print(f"after : {df['Target'][x][0]}")


# In[ ]:


backup_df = df.copy(deep=True)
df.head(50)


# In[ ]:


#backup_final = df.copy(deep=True)


# In[ ]:


## recovering the backup 
#df = backup_final.copy(deep=True)


# In[ ]:


# FINAL STEP OF STROING THE RESULTS IN DATA BASE

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

df.to_sql('proc_ex_adjacency',con=engine,if_exists='append',chunksize=1000)
print("Success !! preprocessing is completed for the adjacency list ")


# In[ ]:




