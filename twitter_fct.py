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

def load(csv,plot=False):
    df = pd.read_csv(csv)
    print('tweets:',len(df))
    df=df.sort_values('likes_count',ascending=False) #retweets_count, replies_count

    df['date_time'] = pd.to_datetime(df['date'])
    d_max =  df['date_time'].max()
    d_min =  df['date_time'].min()
    if d_max == d_min:
        print('all tweets from',d_min)
    else:
        print('tweets per day:', round(len(df) / (d_max - d_min).days+1,2),'from',d_min,'to',d_max)

    df['lang'] = df['tweet'].map(lambda t: detect_lang(t))
    print(df['lang'].value_counts()[:3])
    if plot:
        df.groupby('date')['id'].count().plot(figsize=(15,3))
        plt.xticks(rotation=45)
    return df

def load_folder(path,plot=False):
    file_names = []
    for file in os.listdir(path):
        if file.endswith(".csv"):
            file_names.append(path+file)

    dfs = [pd.read_csv(fn) for fn in file_names]
    df = pd.concat(dfs)

    print('tweets:',len(df))
    df=df.sort_values('likes_count',ascending=False) #retweets_count, replies_count

    df['date_time'] = pd.to_datetime(df['date'])
    d_max =  df['date_time'].max()
    d_min =  df['date_time'].min()
    if d_max == d_min:
        print('all tweets from',d_min)
    else:
        print('tweets per day:', round(len(df) / (d_max - d_min).days+1,2),'from',d_min,'to',d_max)

    df['lang'] = df['tweet'].map(lambda t: detect_lang(t))
    print(df['lang'].value_counts()[:3])
    if plot:
        df.groupby('date')['id'].count().plot(figsize=(15,3))
        plt.xticks(rotation=45)
    return df

punktuation = ['.',',','!','/',':',';']
def repl_punkt(x):
    for punkt in punktuation:
        x = x.replace(punkt,'')
    return x

def mvh(df):
    hashtags = df['tweet'].str.extractall(r"(#\S+)")
    hashtags = hashtags.reset_index()
    hashtags = hashtags.set_index('level_0')
    hashtags['count'] = 1
    hashtags = hashtags.rename(columns={0: "hashtag"})
    hashtags['hashtag'] = hashtags['hashtag'].apply(lambda x: repl_punkt(x.lower())) #re.sub('[^a-zA-Z0-9 \n\.]', '', my_str)
    hashtags = hashtags.merge(df[['replies_count',	'retweets_count',	'likes_count']],how='left', left_index=True, right_index=True)
    hashtags['score'] = 5 * hashtags['retweets_count'] + 3* hashtags['replies_count']  + 1* hashtags['likes_count']
    hashtags_g = hashtags.groupby('hashtag')['score','count','retweets_count','replies_count','likes_count'].sum()
    hashtags_g = hashtags_g.sort_values('score',ascending=False) 
    #print(hashtags_g[:5])
    return hashtags_g
    
def ht2var(df,text):
    return df['tweet'].str.contains(text)*1
