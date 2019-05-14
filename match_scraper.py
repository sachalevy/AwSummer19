from bs4 import BeautifulSoup
import selenium
from selenium import webdriver
import time 
import re
import requests
from tennis_db import players, games, init, get_session, update_player_stats, add_match, create_hashID
from player_scraper import scrape_players
from sklearn.externals import joblib
from sklearn.metrics import mean_absolute_error
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd 


# global url for simplification purposes
URL = "https://www.unibet.fr/sport/tennis/"

class game():

	def __init__(self, match_id, match_date, match_time, player_1_id, 
		player_1_name, player_1_odd, player_2_id, player_2_name, player_2_odd):
		
		self.match_id = match_id
		self.match_date = match_date
		self.match_time = match_time
		self.player_1_id = player_1_id
		self.player_1_name = player_1_name
		self.player_1_odd = player_1_odd
		self.player_2_id = player_2_id
		self.player_2_name = player_2_name
		self.player_2_odd = player_2_odd

		self.player_1_ace = 0
		self.player_1_df = 0
		self.player_1_svpt = 0
		self.player_1_1stIn = 0
		self.player_1_1stWon = 0
		self.player_1_2ndWon = 0
		self.player_1_SvGms = 0
		self.player_1_bpf = 0
		self.player_1_bps = 0

		self.player_2_ace = 0
		self.player_2_df = 0
		self.player_2_svpt = 0
		self.player_2_1stIn = 0
		self.player_2_1stWon = 0
		self.player_2_2ndWon = 0
		self.player_2_SvGms = 0
		self.player_2_bpf = 0
		self.player_2_bps = 0

def create_driver():
	driver = webdriver.Firefox()
	return driver

# returns a list of game instances 
def get_headlines(url, session):
	driver = webdriver.Firefox()
	driver.get(url)
	# get all the games with players names concatenated as keys
	_games = dict()
	
	# click to deploy tennis competition table
	element = driver.find_by_css_selector('li.SPORT_TENNIS:nth-child(2) > a:nth-child(1) > span:nth-child(2)')
	element.click()
	games_ = []
	# determine how many competition are interesting, knowing ATP will be first in the bets proposal
	n = 1
	while(True):
		element = driver.find_by_css_selector('.level1 > li:nth-child('+str(n)+') > a:nth-child(1) > span:nth-child(1)')
		if (element.get_attribute('innerHTML').split()[0] == 'ATP'):
			n += 1
			element.click()
			i = 1

			while(True):
				try:
					date = driver.find_by_css_selector('.bettingbox-title > span:nth-child('+str(i)+')').get_attribute('innerHTML')
					day = date.split()[1]
					# get the number of the month
					month = date.split()[2]
					# overriding the date definition normal ?
					date = '2019-'+month+'-'+day

					i +=1
					k = 1
					while(True):
						try:
							element_ = driver.find_by_css_selector('div.had-market:nth-child('+str(k)+') > div:nth-child(2) > div:nth-child(1) > div:nth-child(2) > div:nth-child(1) > a:nth-child(1)').get_attribute('innerHTML')
							time = driver.find_by_css_selector('div.had-market:nth-child('+str(k)+') > div:nth-child(2) > div:nth-child(1) > div:nth-child(2) > div:nth-child(2) > span:nth-child(1)').get_attribute('innerHTML')
							k += 1
							names = element_.split('-')
							lastname_1 = names[1].replace(' %s.', '').lower()
							lastname_2 = names[2].replace(' %s.', '').lower()
							# key corresponding to the name of the two players
							_games[lastname_1+'_'+lastname_2] = {}

							odd_1 = driver.find_by_css_selector('div.had-market:nth-child('+str(k)+') > div:nth-child(3) > div:nth-child(1) > div:nth-child(2) > span:nth-child(1) > span:nth-child(1) > span:nth-child(3)').get_attribute('innerHTML')
							odd_2 = driver.find_by_css_selector('div.had-market:nth-child('+str(k)+') > div:nth-child(3) > div:nth-child(1) > div:nth-child(2) > span:nth-child(2) > span:nth-child(1) > span:nth-child(3)').get_attribute('innerHTML')
							
							_games[lastname_1+'_'+lastname_2] = {lastname_1 : odd_1, lastname_2 : odd_2}

							# use dictionnary
							game = games(match_date = create_hashID(date+lastname_1+lastname_2), player_1_id = create_hashID(lastname_1), 
								player_1_name = lastname_1, player_2_id = create_hashID(lastname_2), player_2_name = lastname_2, match_time = time,
								player_1_odd = float(odd_1), player_2_odd = float(odd_2))

							game_ = game(match_date = create_hashID(date+lastname_1+lastname_2), player_1_id = create_hashID(lastname_1), 
								player_1_name = lastname_1, player_2_id = create_hashID(lastname_2), player_2_name = lastname_2, match_time = time,
								player_1_odd = float(odd_1), player_2_odd = float(odd_2))

							session.add(games)
							session.commit()

							games.append(game_)

						except selenium.common.exceptions.NoSuchElementException:
							break
				except selenium.common.exceptions.NoSuchElementException:
					break
		else:
			break
	# games being a list of games instances & _games the dictionnary containing names & odds
	return games, _games

def build_df(games, stats):
	for game in games:
		game.player_1_ace = stats[player_1_name+'_ace']
		game.player_1_bps = stats[player_1_name+'_bps']
		game.player_1_bpf = stats[player_1_name+'_bpf']
		game.player_1_df = stats[player_1_name+'_df']
		game.player_1_svpt = stats[player_1_name+'_svpt']
		game.player_1_SvGms = stats[player_1_name+'_svgms']
		game.player_1_1stIn = stats[player_1_name+'_1stIn']
		game.player_1_2ndWon = stats[player_1_name+'_2ndWon']
		game.player_1_1stWon = stats[player_1_name+'_1stWon']

		game.player_2_ace = stats[player_2_name+'_ace']
		game.player_2_bps = stats[player_2_name+'_bps']
		game.player_2_bpf = stats[player_2_name+'_bpf']
		game.player_2_df = stats[player_2_name+'_df']
		game.player_2_svpt = stats[player_2_name+'_svpt']
		game.player_2_SvGms = stats[player_2_name+'_svgms']
		game.player_2_1stIn = stats[player_2_name+'_1stIn']
		game.player_2_2ndWon = stats[player_2_name+'_2ndWon']
		game.player_2_1stWon = stats[player_2_name+'_1stWon']

	# should yield a dataframe containing all necessary informations about the games
	# otherwise can just dump stats in dataset => easier, since here will have a df 
	# containing objects not easy to use, basically useless
	df = pd.DataFrame(games)
	return df


def main():
	# url = input("Enter the unitbet tennis competition url: ")
	database = "tennis_bets.db"
	
	connection = init(database)
	session = get_session()
	# get the headlines from the betting website, load the db with the games
	url = 'https://www.unibet.fr/sport/tennis'
	games, _games = get_headlines(url, session)
	
	players = []
	for game in games:
		players.append(game.player_1_name.lower(), game.player_2_name.lower())
	# => useless
	# get stats of the players over their last five games
	stats = scrape_players(_games, session)
	
	# returns dataframe to predict
	game_pred = build_df(_games, stats)
	game_pred_ = game_pred.to_numpy()
	
	# load the model from the database
	model = joblib.load('model_n1.joblib')

	predicted = model.predict(game_pred_)

	session.close()
	conn.close()