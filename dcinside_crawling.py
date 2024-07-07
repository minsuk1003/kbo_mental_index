import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, text
import pymysql
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

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
            text("""
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
            """),
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

def collect_dcinside_data(start_id, num_posts, batch_size, sleep_time):
    total_collected = 0
    total_time = 0
    batch_count = 0
    
    while total_collected < num_posts:
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_post_id = {executor.submit(fetch_dcinside_data, post_id): post_id for post_id in range(start_id, start_id - batch_size, -1)}
            
            for future in as_completed(future_to_post_id):
                post_id = future_to_post_id[future]
                try:
                    data_id = future.result()
                    print(f"Successfully fetched and inserted data for post id {data_id}")
                except Exception as e:
                    print(f"Error fetching data for post id {post_id}: {e}")
        
        end_time = time.time()
        batch_time = end_time - start_time
        total_time += batch_time
        batch_count += 1
        total_collected += batch_size
        start_id -= batch_size
        
        print(f"Batch {batch_count} completed in {batch_time:.2f} seconds. Total time so far: {total_time:.2f} seconds.")
        print(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)
    
    return total_time, batch_count
        
        
# 예시: 시작 ID가 10836209이고 1000000개의 포스트를 수집하는 경우, 한 번에 50000개씩 수집하고 100초간 중단
start_id = 10836209
num_posts = 3500000
batch_size = 50000
sleep_time = 100

total_time, batch_count = collect_dcinside_data(start_id, num_posts, batch_size, sleep_time)

# 총 실행 시간 추정
average_batch_time = total_time / batch_count
estimated_total_time = total_time + (batch_count * sleep_time)
print(f"Estimated total time for {batch_count} batches: {estimated_total_time / 3600:.2f} hours")