import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():

    api_key = "{Your api key}"
    api_secret = "{Your api secret}"
    acc_key = "{Your acc key}"
    acc_secret = "{Your acc secret}"

    # Twitter authentication
    auth = tweepy.OAuthHandler(api_key, api_secret)
    auth.set_access_token(acc_key, acc_secret)

    # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    list = []

    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)


    df = pd.DataFrame(list)
    df.to_csv('{your S3 address}')


run_twitter_etl()
