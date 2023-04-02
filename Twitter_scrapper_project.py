#Importing required libraries
import pandas as pd
import json
import streamlit as st
import pymongo
from pymongo import MongoClient
import snscrape.modules.twitter as sntwitter
from datetime import date, datetime

current_timestamp = datetime.now()

#Initializing empty dataframe and dictionary
tweets_df = pd.DataFrame()
tweets_dict = {}

#Function to scrape tweets
def scrape_tweets(keyword, start_date, end_date, limit):
    global scraped_data
    global tweets_df
    search_query = f"{keyword} since:{start_date} until:{end_date}"
    scraped_data = []
    for i, tweet in enumerate(sntwitter.TwitterSearchScraper(search_query).get_items()):
        if i >= limit:
            break
        data = {"date": tweet.date,
                "id": tweet.id,
                "url": tweet.url,
                "content": tweet.content,
                "user": tweet.user.username,
                "reply_count": tweet.replyCount,
                "retweet_count": tweet.retweetCount,
                "language": tweet.lang,
                "source": tweet.sourceLabel,
                "like_count": tweet.likeCount}
        scraped_data.append(data)

    tweets_df = pd.DataFrame(scraped_data)
    return tweets_df


#Setting up the streamlit app
st.title("Twitter Data Scrapper")
st.sidebar.title("Options")
keyword = st.sidebar.text_input("Enter a keyword to search")
start_date = st.sidebar.date_input("Start date")
end_date = st.sidebar.date_input("End date")
limit = st.sidebar.number_input("Maximum number of tweets to scrape", min_value=1, max_value=1000, value=100)
if st.button("scrape"):
    st.info("Scraping tweets....")
    #calling the scrape tweets function and displaying the dataframe in app
    tweets_df = scrape_tweets(keyword, start_date, end_date, limit)
    st.write(tweets_df)
    
#Function to convert dataframe to csv format
def convert_df(df):
    return df.to_csv().encode("utf-8")

#Checking if the dataframe is not empty and creating download buttons for csv and json
if not tweets_df.empty:
    csv = convert_df(tweets_df)
    csv_file_name = f"{keyword}_{start_date}_{end_date}_tweets.csv".replace(" ", "_")
    st.download_button(label="Download data as CSV", data=csv, file_name=csv_file_name, mime="text/csv")

    json_string = tweets_df.to_json(orient="records")
    json_file_name = f"{keyword}_{start_date}_{end_date}_tweets.json".replace(" ", "_")
    st.download_button(label="Download data as JSON", data=json_string, file_name=json_file_name, mime="application/json")

#Scrape tweets and convert them to dictionary
tweets_df = scrape_tweets(keyword, start_date, end_date, limit)    
tweets_dict = tweets_df.to_dict(orient='records')

#Uploading data to mongodb
if st.button("Upload to MongoDB"):
    if tweets_dict:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["twitter_scraping"]
        collection = db["scraped_data"]
        collection.delete_many({})
        
        #inserting each tweet as document in mongodb collection
        for tweet in tweets_dict:
            collection.insert_one(tweet)
            
        st.success('Successfully uploaded to MongoDB')
    else:
        st.warning('No tweets to upload')

    





