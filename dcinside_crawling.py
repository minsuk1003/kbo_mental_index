import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine
import pymysql
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# 데이터베이스 연결 설정
db_config = {
    'user': 'admin',
    'password': 'incheon29!',
    'host': 'db-baseball.c3qkocogec5f.us-east-2.rds.amazonaws.com',
    'database': 'baseball'
}
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")


# 특정 연도와 팀의 데이터를 가져오는 함수
def fetch_dcinside_data(post_id):
    url = f"https://gall.dcinside.com/board/view/?id=skwyverns&no={post_id}"
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Handle <br> tags to treat them as new lines
    for br in soup.find_all("br"):
        br.replace_with("\n")
    
    # Extract the desired information
    match = re.search(r'no=(\d+)', url)
    id = match.group(1)
    team = 'SK_SSG'
    datetime = soup.find('span', {'class': 'gall_date'}).text.strip()
    title = soup.find('span', {'class': 'title_subject'}).text.strip()
    content_div = soup.find('div', {'class': 'write_div'})
    if content_div:
        # Remove the specific span
        span_to_remove = content_div.find('span', {'id': 'dcappheader'})
        if span_to_remove:
            span_to_remove.decompose()
        content = content_div.text.strip()
    else:
        content = ""    
    views = soup.find('span', {'class': 'gall_count'}).text[3:].strip()
    likes = soup.find('p', {'class': 'up_num font_red'}).text.strip()
    dislikes = soup.find('p', {'class': 'down_num'}).text.strip()
    comment_counts = soup.find('span', {'class': 'gall_comment'}).text[3:].strip()
    
    # Insert data into the database
    with engine.connect() as connection:
        connection.execute(
            """
            INSERT INTO your_table (id, team, datetime, title, content, views, likes, dislikes, comment_counts)
            VALUES (:id, :team, :datetime, :title, :content, :views, :likes, :dislikes, :comment_counts)
            ON DUPLICATE KEY UPDATE
            team=VALUES(team),
            datetime=VALUES(datetime),
            title=VALUES(title),
            content=VALUES(content),
            views=VALUES(views),
            likes=VALUES(likes),
            dislikes=VALUES(dislikes),
            comment_counts=VALUES(comment_counts)
            """,
            {
                'id': id,
                'team': team,
                'datetime': datetime, 
                'title': title, 
                'content': content, 
                'views': views, 
                'likes': likes, 
                'dislikes': dislikes, 
                'comment_counts': comment_counts
            }
        )

    return id

def collect_dcinside_data(start_id, num_posts):
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_post_id = {executor.submit(fetch_dcinside_data, post_id): post_id for post_id in range(start_id, start_id - num_posts, -1)}
        
        for future in as_completed(future_to_post_id):
            post_id = future_to_post_id[future]
            try:
                data_id = future.result()
                print(f"Successfully fetched and inserted data for post id {data_id}")
            except Exception as e:
                print(f"Error fetching data for post id {post_id}: {e}")

# 예시: 시작 ID가 10836209이고 10개의 포스트를 수집하는 경우
start_id = 10836209
num_posts = 1000000

collect_dcinside_data(start_id, num_posts)