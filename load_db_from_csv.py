import json
import time
import pandas as pd
import numpy as np
import random
import selenium
from selemenium import webdriver
from tennis_db import players, games, init, get_session, create_hashID, update_player_stats, add_match

URL = 'http://www.tennisabstract.com/'

def read_csvs(path):
	train_df = pd.DataFrame()

	# read every file in the directory
	for file in os.listdir(directory_path):
		df = pd.read_csv(directory_path+file)
		df.drop(["draw_size", "match_num",
			"winner_id", "winner_seed", "winner_entry", "winner_hand", "loser_id", 
			"loser_seed", "loser_entry",  "loser_hand", "round", "best_of", 
			], axis = 1, inplace = True)
		# check how many rows remain when drop null values with those selected columns
		print(df.shape)
		df.dropna(how = 'any', inplace = True)
		print(df.shape)

		# create the target for the given game: 1 for the first value is the winner
		df.insert(0, 'target', 1)
		# either associate to the best individual or random

		for index, row in df.iterrows():
			if random.uniform(0,1) > 0.5:
				# here player 1 won 
				# player 2 lost
				# 1 indicates that player 1 won
				pass
			else:
				df.at[index, 'target'] = 0
				# swap everything
				# swap the two players, player 1 lost
				# player 2 won, 0 indicates that player 2 won
				temp_p1_name = df.at[index, 'loser_name']
				temp_p1_height = df.at[index, 'loser_ht']
				temp_p1_ioc = df.at[index, 'loser_ioc']
				temp_p1_rank = df.at[index, 'loser_rank']
				temp_p1_rank_points = df.at[index, 'loser_rank_points']
				temp_p1_ace = df.at[index, 'l_ace']
				temp_p1_df = df.at[index, 'l_df']
				temp_p1_svpt = df.at[index, 'l_svpt']
				temp_p1_1stIn = df.at[index, 'l_1stIn']
				temp_p1_1stWon = df.at[index, 'l_1stWon']
				temp_p1_2ndWon = df.at[index, 'l_2ndWon']
				temp_p1_SvGms = df.at[index, 'l_SvGms']
				temp_p1_bpSaved = df.at[index, 'l_bpSaved']
				temp_p1_bpFaced = df.at[index, 'l_bpFaced']

				df.at[index, 'loser_name'] = df.at[index, 'winner_name']
				df.at[index, 'loser_ht'] = df.at[index, 'winner_ht']
				df.at[index, 'loser_ioc'] = df.at[index, 'winner_ioc']
				df.at[index, 'loser_rank'] = df.at[index, 'winner_rank']
				df.at[index, 'loser_rank_points'] = df.at[index, 'winner_rank_points']
				df.at[index, 'l_ace'] = df.at[index, 'w_ace']
				df.at[index, 'l_df'] = df.at[index, 'w_df']
				df.at[index, 'l_svpt'] = df.at[index, 'w_svpt']
				df.at[index, 'l_1stIn'] = df.at[index, 'w_1stIn']
				df.at[index, 'l_1stWon'] = df.at[index, 'w_1stWon']
				df.at[index, 'l_2ndWon'] = df.at[index, 'w_2ndWon']
				df.at[index, 'l_SvGms'] = df.at[index, 'w_SvGms']
				df.at[index, 'l_bpSaved'] = df.at[index, 'w_bpSaced']
				df.at[index, 'l_bpFaced'] = df.at[index, 'w_bpFaced']

				df.at[index, 'winner_name'] = temp_p1_name
				df.at[index, 'winner_ht'] = temp_p1_height
				df.at[index, 'winner_ioc'] = temp_p1_ioc
				df.at[index, 'winner_rank'] = temp_p1_rank
				df.at[index, 'winner_rank_points'] = temp_p1_rank_points
				df.at[index, 'w_ace'] = temp_p1_ace
				df.at[index, 'w_df'] = temp_p1_df
				df.at[index, 'w_svpt'] = temp_p1_svpt
				df.at[index, 'w_1stIn'] = temp_p1_1stIn
				df.at[index, 'w_1stWon'] = temp_p1_1stWon
				df.at[index, 'w_2ndWon'] = temp_p1_2ndWon
				df.at[index, 'w_SvGms'] = temp_p1_SvGms
				df.at[index, 'w_bpSaced'] = temp_p1_bpSaved
				df.at[index, 'w_bpFaced'] = temp_p1_bpFaced

		train_df = train_df.append(df)
	train_df.reset_index(inplace = True)
	return train_df

# do by indexes instead of the exact date, time efficient
def find_game_date(p1_name, p2_name):
	driver = webdriver.Firefox()
	global URL
	page = driver.get(URL)
	box1 = driver.find_by_xpath('//*[@id="head1"]')
	box2 = driver.find_by_xpath('//*[@id="head2"]')
	box1.send_keys('(M) '+ p1_name)
	box2.send_keys('(M)' + p2_name)

def get_stats_l5(p1_name, p2_name, match_date, df, index, max_):
	# search in the df for the the given match, then all matches are before this one
	# extract sub dataframes for both players, with only their games
	# use dicts instead of dfs ?
	p1_values = {}
	p2_values = {}
	count_1 = 0
	count_2 = 0

	# init all these fields to lists
	p1_values['aces'] = []
	p1_values['dfs'] = []
	p1_values['bps'] = []
	p1_values['bpf'] = []
	p1_values['svpt'] = []
	p1_values['1stServe'] = []
	p1_values['1stIn'] = []
	p1_values['2ndWon'] = []
	p1_values['Svgms'] = []

	p2_values['aces'] = []
	p2_values['dfs'] = []
	p2_values['bps'] = []
	p2_values['bpf'] = []
	p2_values['svpt'] = []
	p2_values['1stServe'] = []
	p2_values['1stIn'] = []
	p2_values['2ndWon'] = []
	p2_values['Svgms'] = []

	# direction of the dataset would be useful ... => optimize the travel toward the last 5
	for index, row in df[:index].iterrows():
		
		# if the row is about the player 1 append
		if df.at[index, 'winner_name'] == p1_name: 
			p1_values['aces'].append(df.at[index, 'w_ace'])
			p1_values['dfs'] = []
			p1_values['bps'] = []
			p1_values['bpf'] = []
			p1_values['svpt'] = []
			p1_values['1stServe'] = []
			p1_values['1stIn'] = []
			p1_values['2ndWon'] = []
			p1_values['Svgms'] = []

		elif df.at[index, 'loser_name'] == p1_name:
			p1_values['aces'] = []
			p1_values['dfs'] = []
			p1_values['bps'] = []
			p1_values['bpf'] = []
			p1_values['svpt'] = []
			p1_values['1stServe'] = []
			p1_values['1stIn'] = []
			p1_values['2ndWon'] = []
			p1_values['Svgms'] = []

		# if the row is about the player 2 append
		elif df.at[index, 'winner_name'] == p2_name:
			if count_2 > max_: break
			# append the corresponding values
			# need to know if t
			p2_values['result'].append(1)
			p2_values['aces'].append(df.at[index, 'w_ace'])
			p2_values['dfs'] = []
			p2_values['bps'] = []
			p2_values['bpf'] = []
			p2_values['svpt'] = []
			p2_values['1stServe'] = []
			p2_values['1stIn'] = []
			p2_values['2ndWon'] = []
			p2_values['Svgms'] = []
			
			count_2 += 1
		
		elif df.at[index, 'loser_name'] == p2_name:
			if count_2 > max_: break
			
			p2_values['result'].append(0)
			p2_values['aces'].append(df.at[index, 'l_ace'])
			p2_values['dfs']
			p2_values['bps'] = []
			p2_values['bpf'] = []
			p2_values['svpt'] = []
			p2_values['1stServe'] = []
			p2_values['1stIn'] = []
			p2_values['2ndWon'] = []
			p2_values['Svgms'] = []
			
			count_2 += 1
		else:
			pass

	print(df_1.head())
	print(df_2.head())
	# from only the last 5 games
	stats_1, stats_2 = get_full_stats(p1_values, p2_values)
	return stats_1, stats_2
# input two dicts, output dicts containing statistics
def get_full_stats(p1, p2):


def load_db(session, df):
	for index, row in df.iterrows():
		match_id = create_hashID(df.at[index, 'winner_name']+df.at[index, 'loser_name']+df.at[index, 'tourney_date'])
		game_inDB = session.query(games).filter_by()

		if game_inDB:
			pass
		else:
			p1_id = create_hashID(df.at[index, 'winner_name'])
			p2_id = create_hashID(df.at[index, 'loser_name'])
			match_date = df.at[index, 'tourney_date']
			# update everything to take into account the last 5 five games, with mean, std, last value & average last results
			stats_1, stats_2 = get_stats_l5(df.at[index, 'winner_name'], df.at[index, 'loser_name'], match_date, df, index, 5)

			game = games(match_id = match_id, match_date = match_date,tourney_id = df.at[index, 'tourney_id'], 
				tourney_name = df.at[index, 'tourney_name'],
				minutes = df.at[index, 'minutes'], score = df.at[index, 'score'], 
				surface = df.at[index, 'surface'], player_1_id = p1_id, player_2_id = p2_id,
				player_1_name = df.at[index, 'winner_name'], player_2_name = df.at[index, 'loser_name'],
				player_1_rank = df.at[index, 'winner_rank'], player_2_rank = df.at[index, 'loser_rank'],
				player_1_rank_points = df.at[index, 'winner_rank_points'],
				player_2_rank_points = df.at[index, 'loser_rank_points'], 
				player_1_ioc = df.at[index, 'winner_ioc'], player_2_ioc = df.at[index, 'loser_ioc'],
				player_1_ace = df.at[index, 'w_ace'], player_2_ace = df.at[index, 'l_ace'],
				player_1_df = df.at[index, 'w_df'], player_2_df = df.at[index, 'l_df'], 
				player_1_svpt = df.at[index, 'w_svpt'], player_2_svpt =  df.at[index, 'l_svpt'],
				player_1_1stIn =  df.at[index, 'w_1stIn'], player_2_1stIn =  df.at[index, 'l_1stIn'],
				player_1_1stWon =  df.at[index, 'w_1stWon'], player_2_1stWon =  df.at[index, 'l_1stWon'],
				player_1_2ndWon =  df.at[index, 'w_2ndWon'], player_2_2ndWon =  df.at[index, 'l_2ndWon'],
				player_1_SvGms =  df.at[index, 'w_SvGms'], player_2_SvGms =  df.at[index, 'l_SvGms'],
				player_1_bpf =  df.at[index, 'w_bpFaced'], player_2_bpf =  df.at[index, 'l_bpFaced'],
				player_1_bps =  df.at[index, 'w_bpSaved'], player_2_bps = df.at[index, 'l_bpSaved'], 
				target =  df.at[index, 'target'])
			
			session.add(game)
			session.commit()

def load_db(directory_path, database):
	df = read_csvs(directory_path)
	conn = init(database)
	session = get_session()
	load_db(session, df)
