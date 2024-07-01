import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

# 연도 목록과 팀 사전 정의
year_list = [2018, 2019, 2020, 2021, 2022, 2023, 2024]
team_dict = {
    1: '삼성', 2: 'KIA', 3: '롯데',
    5: 'LG', 6: '두산', 7: '한화',
    9: 'SK_SSG', 11: '넥센_키움', 12: 'NC', 13: 'KT'
}

# 리스트를 청크로 나누는 함수
def chunk_list(lst, chunk_size):
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

# 각 하위 리스트의 앞에 원소를 추가하는 함수
def add_elements_to_sublist(lst, elements):
    for sublist in lst:
        for i, element in enumerate(elements):
            sublist.insert(i, element)
    return lst

# 특정 연도와 팀의 데이터를 가져오는 함수
def fetch_seasonal_batter_data(year, team):
    url = f"https://statiz.sporki.com/stats/?m=main&m2=batting&m3=default&so=WAR&ob=DESC&year={year}&sy=&ey=&te={team}&po=&lt=10100&reg=P70&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=50&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    columns = ['Year', 'Team', 'Rank', 'Name', 'Pos', 'WAR', 'oWAR', 'dWAR', 'G', 'PA', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF', 'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+', 'WAR2']

    batter_datas = soup.find_all('td')
    chunked_batter_data = chunk_list([batter.text for batter in batter_datas], 33)
    
    new_elements = [year, team_dict[team]]
    batter_data = add_elements_to_sublist(chunked_batter_data, new_elements)
    
    df = pd.DataFrame(batter_data, columns=columns)
    url_list = [batter.find('a')['href'] for i, batter in enumerate(batter_datas) if i%33 == 1]
    
    df['url'] = url_list
    return df

# 특정 연도와 팀의 데이터를 가져오는 함수
def fetch_seasonal_pitcher_data(year, team):
    url = f"https://statiz.sporki.com/stats/?m=main&m2=pitching&m3=default&so=WAR&ob=DESC&year={year}&sy=&ey=&te={team}&po=&lt=10100&reg=P70&pe=&ds=&de=&we=&hr=&ha=&ct=&st=&vp=&bo=&pt=&pp=&ii=&vc=&um=&oo=&rr=&sc=&bc=&ba=&li=&as=&ae=&pl=&gc=&lr=&pr=50&ph=&hs=&us=&na=&ls=&sf1=&sk1=&sv1=&sf2=&sk2=&sv2="
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    columns = ['Year', 'Team', 'Rank', 'Name', 'Pos', 'WAR', 'G', 'GS', 'GR', 'GF', 'CG', 'SHO', 'W', 'L', 'S', 'HD', 'IP', 'ER', 'R', 'rRA', 'TBF', 'H', '2B', '3B', 'HR', 'BB', 'HP', 'IB', 'SO', 'ROE', 'BK', 'WP', 'ERA', 'RA9', 'rRA9', 'rRA9pf', 'FIP', 'WHIP', 'WAR2']

    pitcher_datas = soup.find_all('td')
    chunked_pitcher_data = chunk_list([pitcher.text for pitcher in pitcher_datas], 37)
    
    new_elements = [year, team_dict[team]]
    pitcher_data = add_elements_to_sublist(chunked_pitcher_data, new_elements)
    
    df = pd.DataFrame(pitcher_data, columns=columns)
    url_list = [pitcher.find('a')['href'] for i, pitcher in enumerate(pitcher_datas) if i%37 == 1]
    
    df['url'] = url_list
    return df

# 데이터를 수집하고 데이터프레임으로 만드는 함수
def collect_seasonal_data(year_list, team_dict):
    batter_df = None
    pitcher_df = None
    for year in tqdm(year_list):
        for team in team_dict.keys():
            new_batter_df = fetch_seasonal_batter_data(year, team)
            new_pitcher_df = fetch_seasonal_pitcher_data(year, team)
            if batter_df is None:
                batter_df = new_batter_df
            else:
                batter_df = pd.concat([batter_df, new_batter_df], ignore_index=True)
            if pitcher_df is None:
                pitcher_df = new_pitcher_df
            else:
                pitcher_df = pd.concat([pitcher_df, new_pitcher_df], ignore_index = True)
    
    # batter_url_list = []
    # for year in tqdm(year_list):
    #     for team in team_dict.keys():
    #         batter_url_list += batter_url_data(year, team)
    # batter_df['url'] = batter_url_list
    
    # pitcher_url_list = []
    # for year in tqdm(year_list):
    #     for team in team_dict.keys():
    #         pitcher_url_list += pitcher_url_data(year, team)
    # pitcher_df['url'] = pitcher_url_list
       
    batter_df = batter_df.drop('WAR2', axis=1)
    pitcher_df = pitcher_df.drop('WAR2', axis=1)
    return batter_df, pitcher_df

# 특정 연도와 팀의 데이터를 가져오는 함수
def fetch_batter_game_data(seasonal_df):
    df = None
    for i in tqdm(range(len(seasonal_df))):
        url = f"https://statiz.sporki.com/{seasonal_df.loc[i,'url']}&year={seasonal_df.loc[i,'Year']}&m=day"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
    
        columns = ['Year', 'Team', 'Name', 'Date', 'Opponent', 'Result', 'Order', 'Pos', 'GS', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF', 'accumulated_AVG', 'accumulated_OBP', 'accumulated_SLG', 'accumulated_OPS', 'NP', 'avLI', 'RE24', 'WPA']

        batter_datas = soup.find_all('td')
        chunked_batter_data = chunk_list([batter.text for batter in batter_datas], len(columns)-3)
        
        new_elements = [seasonal_df.loc[i,'Year'], seasonal_df.loc[i, 'Team'], seasonal_df.loc[i, 'Name']]
        batter_data = add_elements_to_sublist(chunked_batter_data, new_elements)
        
        new_df = pd.DataFrame(batter_data, columns=columns)
        if df is None:
            df = new_df
        else:
            df = pd.concat([df, new_df], ignore_index=True)
    return df

# 특정 연도와 팀의 데이터를 가져오는 함수
def fetch_pitcher_game_data(seasonal_df):
    df = None
    for i in tqdm(range(len(seasonal_df))):
        url = f"https://statiz.sporki.com/{seasonal_df.loc[i,'url']}&year={seasonal_df.loc[i,'Year']}&m=day"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
    
        columns = ['Year', 'Team', 'Name', 'Date', 'Opponent', 'Result', 'GS', 'IP', 'R', 'ER', 'rRA', 'TBF', 'AB', 'H', '2B', '3B', 'HR', 'BB', 'IB', 'HP', 'SO', 'NP', 'WHIP', 'AVG', 'OBP', 'OPS', 'ERA', 'avLI', 'RE24', 'WPA', 'GSC', 'DEC', 'Int']

        pitcher_datas = soup.find_all('td')
        chunked_pitcher_data = chunk_list([pitcher.text for pitcher in pitcher_datas], len(columns)-3)
        
        new_elements = [seasonal_df.loc[i,'Year'], seasonal_df.loc[i, 'Team'], seasonal_df.loc[i, 'Name']]
        pitcher_data = add_elements_to_sublist(chunked_pitcher_data, new_elements)
        
        new_df = pd.DataFrame(pitcher_data, columns=columns)
        if df is None:
            df = new_df
        else:
            df = pd.concat([df, new_df], ignore_index=True)
    return df


def fetch_player_salary(url, team, name):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    columns=['Team', 'Name', 'Year', '연봉(만원)', 'WAR']
    
    data = soup.find_all('td')
    chunked_salary_data = chunk_list([salary.text for salary in data], 3)
    
    new_elements = [team, name]
    salary_data = add_elements_to_sublist(chunked_salary_data, new_elements)
    
    return pd.DataFrame(salary_data, columns=columns)

def collect_player_salary(seasonal_df):
    df = None
    for i in tqdm(range(len(seasonal_df))):
        url = 'https://statiz.sporki.com/' + seasonal_df.loc[i, "url"] + '&m=income'
        new_df = fetch_player_salary(url, seasonal_df.loc[i, 'Team'], seasonal_df.loc[i, 'Name'])
        if df is None:
            df = new_df
        else:
            df = pd.concat([df, new_df], ignore_index=True)
                    
    return df

# 데이터 수집 실행
seasonal_batter_df, seasonal_pitcher_df = collect_seasonal_data(year_list, team_dict)
game_batter_df = fetch_batter_game_data(seasonal_batter_df)
game_pitcher_df = fetch_pitcher_game_data(seasonal_pitcher_df)
salary_df = collect_player_salary(pd.concat([seasonal_batter_df[['Name', 'Team', 'url']], seasonal_pitcher_df[['Name', 'Team', 'url']]]).drop_duplicates(subset='url', keep='first', ignore_index=True))

# 데이터프레임 출력
print(seasonal_batter_df.iloc[:, :10])
print(seasonal_batter_df.iloc[:, 10:20])
print(seasonal_batter_df.iloc[:, 20:30])
print(seasonal_batter_df.iloc[:, 30:])

print(seasonal_pitcher_df.iloc[:, :10])
print(seasonal_pitcher_df.iloc[:, 10:20])
print(seasonal_pitcher_df.iloc[:, 20:30])
print(seasonal_pitcher_df.iloc[:, 30:])

print(game_batter_df.iloc[:, :10])
print(game_batter_df.iloc[:, 10:20])
print(game_batter_df.iloc[:, 20:30])
print(game_batter_df.iloc[:, 30:])

print(game_pitcher_df.iloc[:, :10])
print(game_pitcher_df.iloc[:, 10:20])
print(game_pitcher_df.iloc[:, 20:30])
print(game_pitcher_df.iloc[:, 30:])
print(salary_df)