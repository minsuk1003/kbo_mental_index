import requests
from bs4 import BeautifulSoup

def crawl_statiz_boxscore(url):
    # HTTP GET 요청을 보내고 응답 받기
    response = requests.get(url)
    
    # 요청이 성공했는지 확인
    if response.status_code != 200:
        raise Exception(f"Failed to load page {url}")

    # BeautifulSoup을 사용하여 HTML 파싱
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 필요한 데이터 추출 (예: 경기 정보, 선수 기록 등)
    game_info = {}
    game_info['date'] = soup.select_one('div.game-title > h4').text.strip()
    game_info['teams'] = soup.select_one('div.game-title > h5').text.strip()
    
    # 선수 기록 테이블 크롤링
    tables = soup.select('table')
    players_data = []

    for table in tables:
        headers = [th.text.strip() for th in table.select('thead th')]
        for row in table.select('tbody tr'):
            player_data = {}
            for idx, td in enumerate(row.select('td')):
                player_data[headers[idx]] = td.text.strip()
            players_data.append(player_data)

    return game_info, players_data

# 사용 예시
url = "https://statiz.sporki.com/schedule/?m=boxscore&s_no=20230101"
game_info, players_data = crawl_statiz_boxscore(url)

print("Game Info:", game_info)
print("Players Data:", players_data)