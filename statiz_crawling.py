import requests
from bs4 import BeautifulSoup

# URL 설정
url = "https://statiz.sporki.com/schedule/?m=boxscore&s_no=20230001"

# 요청 보내기
response = requests.get(url)

# 응답의 HTML을 파싱
soup = BeautifulSoup(response.text, 'html.parser')

# 박스스코어 데이터 추출
player_columns = soup.find_all('thead')  # 타자 박스스코어 컬럼
print(player_columns[0])