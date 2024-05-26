import os
from pymongo.mongo_client import MongoClient


# MongoDB URI từ biến môi trường
uri = os.environ['uri']

# Tạo client và kết nối tới server
client = MongoClient(uri)

# Gửi lệnh ping để xác nhận kết nối thành công
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)



from keepalive import keepalive
from gnews import GNews
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import schedule
from datetime import datetime
import pytz


def get_website_url(url):
    try:
        response = requests.get(url, timeout=15, verify=False)
        response.raise_for_status()  # Kiểm tra nếu có lỗi HTTP
        return response.url
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the website URL: {e}")
        return None


def get_website_description(url):
    try:
        response = requests.get(url, timeout=15, verify=False)
        response.raise_for_status()  # Kiểm tra nếu có lỗi HTTP
        soup = BeautifulSoup(response.content, 'html.parser')

        # Các mục tiêu tìm kiếm mô tả
        search_targets = [
            ('meta', 'name', 'description'),
            ('meta', 'property', 'og:description'),
            ('div', 'class', 'description')
        ]

        # Tìm kiếm mô tả trong các mục tiêu
        for tag, attr, value in search_targets:
            description_tag = soup.find(tag, attrs={attr: value})
            if description_tag:
                if tag == 'meta' and 'content' in description_tag.attrs:
                    return description_tag['content']
                else:
                    return description_tag.get_text(strip=True)

        return 'Không có mô tả'
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the article: {e}")
        return None

def main():
    # Cấu hình scraping tin tức
    language = 'vi'
    country = 'Vietnam'
    period = '24h'
    max_results = 100

    gg_news = GNews(language=language,
                    country=country,
                    period=period,
                    max_results=max_results)

    # Lấy tin tức hàng đầu
    news_data = gg_news.get_top_news()

    # Danh sách để lưu thông tin bài báo sau khi phân tích
    title = []
    description = []
    url = []
    content = []
    provider = []
    datePublished = []

    # Xử lý từng bài báo trong danh sách
    for article in news_data:
        try:
            article_url = get_website_url(article["url"])
            article_description = get_website_description(article["url"])

            if article_url and article_description:
                title.append(article["title"])
                description.append(article_description)
                url.append(article_url)
                content.append(gg_news.get_full_article(article["url"]).text)
                provider.append(article['publisher']['title'])
                datePublished.append(article['published date'])

        except Exception as e:
            print(f"Error processing JSON object: {e}")

    # Kết hợp danh sách
    data = {
        'title': title,
        'description': description,
        'content': content,
        'url': url,
        'provider': provider,
        'datePublished': datePublished
    }

    # print(data)

    # Tạo DataFrame
    df = pd.DataFrame(data)

    # Tạo hoặc chọn database
    db = client["news_database"]

    # Tạo hoặc chọn collection
    collection = db["news_collection"]
    scraper_info = db["scraper_info"]

    # Chuyển DataFrame thành danh sách dictionary và chèn vào MongoDB
    records = df.to_dict('records')
    total_article_count = 0
    duplicate_count = 0
    status = "Done"

    # Định nghĩa múi giờ GMT +7
    gmt_plus_7 = pytz.timezone('Asia/Bangkok')

    # Chuyển đổi thời gian hiện tại sang múi giờ GMT +7
    now_gmt_plus_7 = datetime.now().astimezone(gmt_plus_7)

    # Định dạng chuỗi chỉ chứa thời gian
    formatted_time = now_gmt_plus_7.strftime('%Y-%m-%d %H:%M:%S')

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
        print(f"Error inserting into MongoDB: {e}")

    scraper_info.insert_one({
        "Time": formatted_time,
        "Total articles": total_article_count,
        "Duplicate articles": duplicate_count,
        "Status": status
    })

    print(f"{status}\nData scraping will continue in 1 hours.")


keepalive()

# Lên lịch chạy hàm main mỗi 1 giờ
schedule.every(1).hour.do(main)

# Bắt đầu vòng lặp để chạy theo lịch trình
while True:
    schedule.run_pending()
    time.sleep(1)
