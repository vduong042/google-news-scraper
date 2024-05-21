import os
from pymongo.mongo_client import MongoClient

uri = os.environ['uri']

# Create a new client and connect to the server
client = MongoClient(uri)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

from keepalive import keepalive
from gnews import GNews
import requests
from bs4 import BeautifulSoup
import newspaper
import pandas as pd
import time
import schedule
from datetime import datetime


def main():
    # Scraping news data
    # Configurations
    language = 'vi'
    country = 'Vietnam'
    period = '24h'
    max_results = 100

    gg_news = GNews(language=language,
                    country=country,
                    period=period,
                    max_results=max_results)

    # Get top news
    news_data = gg_news.get_top_news()

    # Get the final URL of the webpage
    def get_website_url(url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.url
            else:
                return None
        except Exception as e:
            return None

    # Get the description of the webpage
    def get_website_description(url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                if 'youtube.com' in response.url:
                    return None

                soup = BeautifulSoup(response.content, 'html.parser')

                search_targets = [('meta', 'name', 'description'),
                                  ('meta', 'property', 'og:description'),
                                  ('div', 'class', 'description')]

                for tag, attr, value in search_targets:
                    description_tag = soup.find(tag, attrs={attr: value})
                    if description_tag:
                        if tag == 'meta' and 'content' in description_tag.attrs:
                            return description_tag['content']
                        else:
                            return description_tag.get_text(strip=True)

                return ('Không có mô tả')

            else:
                return None

        except Exception as e:
            return None

    # Lists to store information of articles after parsing
    title = []
    description = []
    url = []
    content = []
    provider = []
    datePublished = []

    # Process each article in the lists
    for article in news_data:
        try:
            article_url = get_website_url(article["url"])
            article_description = get_website_description(article["url"])
            # print(article_url)

            if article_url != None and article_description != None:

                # Extract information from the dictionary
                title.append(article["title"])
                description.append(article_description)
                url.append(article_url)
                content.append(gg_news.get_full_article(article["url"]).text)
                provider.append(article['publisher']['title'])
                datePublished.append(article['published date'])

        except Exception as e:
            print(f"Error processing JSON object: {e}")

    # Combine the lists
    data = {
        'title': title,
        'description': description,
        'content': content,
        'url': url,
        'provider': provider,
        'datePublished': datePublished
    }

    # Create DataFrame
    df = pd.DataFrame(data)

    # Create or select the database
    db = client["news_database"]

    # Create or select the collection
    collection = db["news_collection"]
    scraper_info = db["scraper_info"]

    # Convert DataFrame to list of dictionaries and insert into MongoDB
    records = df.to_dict('records')
    total_article_count = 0
    duplicate_count = 0
    status = "Done"

    try:
        for record in records:
            total_article_count += 1

            result = collection.update_one({"url": record["url"]},
                                           {"$setOnInsert": record},
                                           upsert=True)

            if result.matched_count > 0:
                duplicate_count += 1

    except Exception as e:
        status = "Failed"

    scraper_info.insert_one({
        "Time": datetime.now(),
        "Total articles": total_article_count,
        "Duplicate articles": duplicate_count,
        "Status": status
    })

    print(f"{status}\nData scraping will continue in 3 hours.")


keepalive()

# "Scheduling the main function
schedule.every(3).hours.do(main)

# Start the loop to run on schedule.
while True:
    schedule.run_pending()
    time.sleep(1)
