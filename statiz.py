import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

columns = ['Year', 'Team', 'Rank', 'Name', 'Pos', 'WAR', 'oWAR', 'dWAR', 'G', 'PA', 'ePA', 'AB', 'R', 'H', '2B', '3B', 'HR', 'TB', 'RBI', 'SB', 'CS', 'BB', 'HP', 'IB', 'SO', 'GDP', 'SH', 'SF', 'AVG', 'OBP', 'SLG', 'OPS', 'R/ePA', 'wRC+', 'WAR2']
print(len(columns))