import pandas as pd
from sqlalchemy import create_engine

# MySQL 데이터베이스 연결 문자열 (사용자의 MySQL 설정에 맞게 수정)
db_connection_str = 'mysql+pymysql://admin:incheon29!@db-baseball.c3qkocogec5f.us-east-2.rds.amazonaws.com:3306/baseball'

# SQLAlchemy 엔진 생성
engine = create_engine(db_connection_str)

# CSV 파일들을 읽어서 데이터프레임으로 변환
seasonal_batter_df = pd.read_csv('seasonal_batter.csv')
seasonal_pitcher_df = pd.read_csv('seasonal_pitcher.csv')
game_batter_df = pd.read_csv('game_batter.csv')
game_pitcher_df = pd.read_csv('game_pitcher.csv')
salary_df = pd.read_csv('player_salary.csv')

# 데이터프레임을 MySQL 테이블에 저장하는 함수
def save_to_mysql(df, table_name, engine):
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

# 데이터 저장 실행
save_to_mysql(seasonal_batter_df, 'seasonal_batter', engine)
save_to_mysql(seasonal_pitcher_df, 'seasonal_pitcher', engine)
save_to_mysql(game_batter_df, 'game_batter', engine)
save_to_mysql(game_pitcher_df, 'game_pitcher', engine)
save_to_mysql(salary_df, 'salary', engine)

print("Data has been saved to MySQL database.")