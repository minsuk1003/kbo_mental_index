import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

url = 'https://statiz.sporki.com/' + '/player/?m=playerinfo&p_no=11190' + '&m=income'

response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

cols = soup.find_all('th')
data = soup.find_all('td')

# 리스트를 청크로 나누는 함수
def chunk_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

salary_data = chunk_list([batter.text for batter in data], 3)

print(pd.DataFrame(salary_data, columns=['Year', '연봉(만원)', 'WAR']))