import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream

import time
import random

import os
from textblob import TextBlob



# Not sure if I will end up using redis, so..

import pdb

from config import *
from utils import _hs, _hi, clean
from datetime import datetime


class TestStreamListener(StreamListener):
    def on_status(self, status):
        if status.lang == "en":
            user, place, tweet, time, tweet_fact, user_fact = parse_status(status)
            #pdb.set_trace()
            if place != None:
                p = insert_place(place)
                if p:
                    if len(status.entities["hashtags"]) > 0:
                        hashtags = tuple([i["text"] for i in status.entities["hashtags"]])

                        print(hashtags)
                    u = insert_user(user)
                    t = insert_tweet(tweet)
                    ti = insert_time(time)
                    print(u, p, t, ti)
                    insert_tweet_fact(t, u, ti, p, tweet_fact)
                    #pdb.set_trace()
                    insert_user_fact(u, ti, user_fact)

    def on_error(self, error):
        pass

    def on_exception(self, exception):
        pass

def insert_tweet_fact(t, u, ti, p, tweet_fact):
    query = "insert into tweet_fact (tweet_dim_id, user_dim_id, time_dim_id, place_dim_id, retweet_count, favorite_count) values ({}, {}, {}, {}, {}, {})".format(t, u, ti, p, *tweet_fact)
    try:
        cursor.execute(query)
        db.commit()
    except Exception as e:
        print(e)


def insert_user_fact(u, ti, user_fact):
    query = "insert into user_fact (user_dim_id, time_dim_id, friends_count, followers_count, listed_count, statuses_count, favorites_count) values ({}, {}, {}, {}, {}, {}, {})".format(u, ti, *user_fact)
    print(query)
    try:
        cursor.execute(query)
        db.commit()
    except Exception as e:
        print(e)


auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)

def insert_user(user):
    query = "insert into user_dim (user_id, user_screen_name, user_name, user_description, user_language, user_created_at, user_time_zone, user_location) values ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}')".format(*user)

    cursor.execute(query)
    db.commit()

    query = "select user_dim_id from user_dim where user_id={}".format(user[0])
    cursor.execute(query)
    return cursor.fetchone()[0]


def insert_place(place):
    query = "insert into place_dim (place_id, place_country_code, place_country, place_name, place_full_name, place_type) values ('{}', '{}', '{}', '{}', '{}', '{}')".format(*place)
    try:
        cursor.execute(query)
        db.commit()
    except Exception as e:
        print(e)
    try:
        query = "select place_dim_id from place_dim where place_id='{}'".format(place[0])
        cursor.execute(query)
        return cursor.fetchone()[0]
    except:
        return 0


def insert_tweet(tweet):
    query = "insert into tweet_dim (tweet_id, tweet_text, tweet_created_at, tweet_source, tweet_language, tweet_polarity, tweet_reply_to_status_id, tweet_reply_to_user_id, tweet_reply_to_screen_name) values ({}, '{}', '{}', '{}', '{}', {}, {}, {}, '{}')".format(*tweet)


    try:
        cursor.execute(query)
        db.commit()
        query = "select tweet_dim_id from tweet_dim where tweet_id={}".format(tweet[0])
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        print(e)

def insert_time(time):
    query = "insert into time_dim (time_timestamp, time_seconds, time_minutes, time_hours, time_day, time_month, time_year) values ('{}', {}, {}, {}, {}, {}, {})".format(*time)

    cursor.execute(query)
    db.commit()

    query = "select time_dim_id from time_dim where time_timestamp='{}'".format(time[0])
    cursor.execute(query)
    return cursor.fetchone()[0]


def parse_status(s):

    user = (_hi(s.user.id),
            _hs(s.user.screen_name),
            clean(_hs(s.user.name)) ,
            clean(_hs(s.user.description)),
            _hs(s.user.lang),
            _hs(s.user.created_at),
            _hs(s.user.time_zone),
            _hs(s.user.location))



    try:
        place = (_hi(s.place.id),
                _hs(s.place.country_code),
                _hs(s.place.country),
                _hs(s.place.name),
                _hs(s.place.full_name),
                _hs(s.place.type))
    except Exception as e:
        place = (0, '', '', '', '', '')

    tweet = (s.id,
            clean(_hs(s.text)),
            _hs(s.created_at),
            _hs(s.source),
            _hs(s.lang),
            TextBlob(clean(s.text)).polarity,
            _hi(s.in_reply_to_status_id),
            _hi(s.in_reply_to_user_id),
            _hs(s.in_reply_to_screen_name))


    t = datetime.now().timestamp()

    time = (t,
            s.created_at.second,
            s.created_at.minute,
            s.created_at.hour,
            s.created_at.day,
            s.created_at.month,
            s.created_at.year)

    tweet_fact = (s.retweet_count, s.favorite_count)

    user_fact = (s.user.friends_count, s.user.followers_count, s.user.listed_count,
            s.user.statuses_count, s.user.favourites_count)

    return user, place, tweet, time, tweet_fact, user_fact


box_london = [-0.510375,51.28676,0.334015,51.691874]
geobox_us = [-125.14,30.28,-64.05,48.85]


if __name__ == '__main__':

    flag = True
    while(flag):

        try:
            listener = TestStreamListener()
            stream = Stream(auth, listener)
            print("here")
            stream.filter(track=["#TakeWarning", "#ALLCAPS", "Canes", "#bucciovertimechallenge", "#CARvsWSH"])
            time.sleep(10 * (1 + random.random()))

        except Exception as e:
            print(e)

