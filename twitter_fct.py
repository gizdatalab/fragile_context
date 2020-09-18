from whatthelang import WhatTheLang
import pandas as pd
import re
import os
import matplotlib
import twint
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import nest_asyncio
nest_asyncio.apply()

from IPython.display import HTML
import requests

from googletransx import Translator
# ref.  - https://github.com/x0rzkov/py-googletrans#basic-usage

translator = Translator()

def show_tweet(link):
    '''Display the contents of a tweet. '''
    url = 'https://publish.twitter.com/oembed?url=%s' % link
    response = requests.get(url)
    html = response.json()["html"]
    display(HTML(html))

wtl = WhatTheLang()

# This function makes easy to handle exceptions (e.g. no text where text should be)
# not really needed but can be useful 

def detect_lang(text):
    try: 
        return wtl.predict_lang(text)
    except Exception:
        return 'exp'

def load(csv):
    df = pd.read_csv(csv)
    print('tweets:',len(df))
    df=df.sort_values('retweets_count',ascending=False) #retweets_count, replies_count

    df['date_time'] = pd.to_datetime(df['date'])
    d_max =  df['date_time'].max()
    d_min =  df['date_time'].min()
    if d_max == d_min:
        print('all tweets from',d_min)
    else:
        print('tweets per day:', round(len(df) / (d_max - d_min).days+1,2),'from',d_min,'to',d_max)
    df = df.set_index('id')
    df['lang'] = df['tweet'].map(lambda t: detect_lang(t))
    print(df['lang'].value_counts()[:3])
    return df

def load_folder(path):
    file_names = []
    for file in os.listdir(path):
        if file.endswith(".csv"):
            file_names.append(path+file)

    dfs = [pd.read_csv(fn) for fn in file_names]
    df = pd.concat(dfs)
    df = df.drop_duplicates('id')

    print('tweets:',len(df))
    df=df.sort_values('retweets_count',ascending=False) #retweets_count, replies_count

    df['date_time'] = pd.to_datetime(df['date'])
    d_max =  df['date_time'].max()
    d_min =  df['date_time'].min()
    if d_max == d_min:
        print('all tweets from',d_min)
    else:
        print('tweets per day:', round(len(df) / (d_max - d_min).days+1,2),'from',d_min,'to',d_max)

    df['lang'] = df['tweet'].map(lambda t: detect_lang(t))
    df = df.set_index('id')
    print(df['lang'].value_counts()[:3])
    return df

punktuation = ['.',',','!','/',':',';']
def repl_punkt(x):
    for punkt in punktuation:
        x = x.replace(punkt,'')
    return x

def mvh(df,top=5):
    hashtags = df['tweet'].str.extractall(r"(#\S+)")
    hashtags = hashtags.reset_index()
    hashtags = hashtags.set_index('id')
    hashtags['count'] = 1
    hashtags = hashtags.rename(columns={0: "hashtag"})
    hashtags['hashtag'] = hashtags['hashtag'].apply(lambda x: repl_punkt(x.lower())) #re.sub('[^a-zA-Z0-9 \n\.]', '', my_str)
    hashtags = hashtags.merge(df[['replies_count',	'retweets_count',	'likes_count']],how='left', left_index=True, right_index=True)
    hashtags['score'] = 5 * hashtags['retweets_count'] + 3* hashtags['replies_count']  + 1* hashtags['likes_count']
    hashtags_g = hashtags.groupby('hashtag')['score','count','retweets_count','replies_count','likes_count'].sum()
    hashtags_g = hashtags_g.sort_values('score',ascending=False)[:top]
    hashtags_g = hashtags_g.reset_index()
    hashtags_g['translation'] = hashtags_g['hashtag'].apply(lambda x: translator.translate(x.replace('#',''), dest='en').text)
    #print(hashtags_g[:5])
    return hashtags_g

def mvu(df,top=5):
    hashtags = df#['username'] #.str.extractall(r"(#\S+)")
    hashtags = hashtags.reset_index()
    hashtags = hashtags.set_index('id')
    hashtags['count'] = 1
    hashtags['score'] = 5 * hashtags['retweets_count'] + 3* hashtags['replies_count']  + 1* hashtags['likes_count']
    hashtags_g = hashtags.groupby('username')['score','count','retweets_count','replies_count','likes_count'].sum()
    hashtags_g = hashtags_g.sort_values('score',ascending=False)[:top]
    hashtags_g = hashtags_g.reset_index()
    return hashtags_g
    
def ht2var(df,text):
    return df['tweet'].str.contains(text)*1

from datetime import timedelta
from string import ascii_letters, digits
from os import mkdir, path

def clean_name(dirname):
    valid = set(ascii_letters + digits)
    return ''.join(a for a in dirname if a in valid)

def twint_search(searchterm, since, until, json_name,min_repost=5):
    '''
    Twint search for a specific date range.
    Stores results to json.
    '''
    c = twint.Config()
    c.Search = searchterm
    c.Since = since
    c.Until = until
    c.Min_repost = min_repost
    c.Hide_output = True
    #c.Store_json = True
    c.Store_csv = True
    c.Output = json_name
    c.Debug = True

    try:
        twint.run.Search(c)    
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print("Problem with %s." % since)


def twint_loop(searchterm, since, until,dirname,min_repost):

    try:
    # Create target Directory
        mkdir(dirname)
        print("Directory" , dirname ,  "Created ")
    except FileExistsError:
        print("Directory" , dirname ,  "already exists")
    #https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases
    daterange = pd.date_range(since, until, freq='MS')
    print(daterange)

    for start_date in daterange:

        since= start_date.strftime("%Y-%m-%d")
        until = (start_date + timedelta(days=30)).strftime("%Y-%m-%d")

        json_name = '%s.csv' % since
        json_name = path.join(dirname, json_name)

        print('Getting %s ' % since )
        twint_search(searchterm, since, until, json_name,min_repost)
