import json
import time
import pandas as pd
import numpy as np
import random
import os, sys
import selenium
from selenium import webdriver
from tqdm import tqdm
from tennis_db import players, games, init, get_session, create_hashID, update_player_stats, add_match

# user LOWERCASE for names

URL = 'http://www.tennisabstract.com/'
"""
Store work related to the db handling part of the pre processing:
________ goes into the load_db_df portion of the code, check if a game is in the database if not adds it with stats
		# create a local match id based on the player's name and the date of the game
		match_id = create_hashID(df.at[index, 'winner_name']+df.at[index, 'loser_name']+df.at[index, 'tourney_date'])
		# look up to see if game is in db
		game_inDB = session.query(games).filter_by()

		# ALSO add the statistiscs about the dataframe to the db
		if game_inDB:
			pass

		# if the game is not in db, then add it
		else:
			# create players' ids (same for players since their names should be the same)
			p1_id = create_hashID((df.at[index, 'winner_name'])).lower()
			p2_id = create_hashID((df.at[index, 'loser_name'])).lower()
			

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

"""
# also make a csv file with the pre-processed game
# method to read & compile the csv files containing the data of the games
def read_csvs(path):
	train_df = pd.DataFrame()
	# read every file in the directory
	for file in tqdm(os.listdir(path)):
		
		df = pd.read_csv(path+file)
		
		df.drop(["draw_size", "match_num",
			"winner_id", "winner_seed", "winner_entry", "winner_hand", "loser_id", 
			"loser_seed", "loser_entry",  "loser_hand", "round", "best_of", 
			], axis = 1, inplace = True)
		
		# check how many rows remain when drop null values with those selected columns
		df.dropna(how = 'any', inplace = True)
		# create the target for the given game: 1 for the first value is the winner
		df.insert(0, 'target', 1)
		# if the first player is the winner then it is a one otherwise a zero
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
				df.at[index, 'l_bpSaved'] = df.at[index, 'w_bpSaved']
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
				df.at[index, 'w_bpSaved'] = temp_p1_bpSaved
				df.at[index, 'w_bpFaced'] = temp_p1_bpFaced

		train_df = train_df.append(df)
	
	# transform the dataframes in a good format 
	train_df.sort_values('tourney_date', inplace = True, ascending = False)
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

# wrapper method to get the data for the players game at a given date,
# implement filtering to erase players and games at the bottom of the dataset
def get_stats(p1_name, p2_name, match_date, df, index_, max_):
	# search in the df for the the given match, then all matches are before this one
	# extract sub dataframes for both players, with only their games
	p1_values = {}
	p2_values = {}
	# counts for the number of values to be evaluated, keep 7 into the dicts
	count_1 = 0
	count_2 = 0

	# init all these fields to lists
	p1_values['results'] = []
	p1_values['rank_points'] = []
	p1_values['aces'] = []
	p1_values['dfs'] = []
	p1_values['bps'] = []
	p1_values['bpf'] = []
	p1_values['svpt'] = []
	p1_values['1stWon'] = []
	p1_values['1stIn'] = []
	p1_values['2ndWon'] = []
	p1_values['Svgms'] = []

	p2_values['results'] = []
	p2_values['rank_points'] = []
	p2_values['aces'] = []
	p2_values['dfs'] = []
	p2_values['bps'] = []
	p2_values['bpf'] = []
	p2_values['svpt'] = []
	p2_values['1stWon'] = []
	p2_values['1stIn'] = []
	p2_values['2ndWon'] = []
	p2_values['Svgms'] = []

	# direction of the dataset would be useful ... => optimize the travel toward the last 7
	for index, row in df.loc[index_:].iterrows():
		# if the counts exceed 7 then break
		if count_1 >= max_ and count_2 >= max_:
			break
		# if the row is about the player 1 append
		if df.at[index, 'winner_name'] == p1_name:
			if count_1 >= max_:
				pass
			else:
				p1_values['results'].append(df.at[index, 'target'])
				p1_values['rank_points'].append(df.at[index, 'winner_rank_points'])
				p1_values['aces'].append(df.at[index, 'w_ace'])
				p1_values['dfs'].append(df.at[index, 'w_df'])
				p1_values['bps'].append(df.at[index, 'w_bpSaved'])
				p1_values['bpf'].append(df.at[index, 'w_bpFaced'])
				p1_values['svpt'].append(df.at[index, 'w_svpt'])
				p1_values['1stWon'].append(df.at[index, 'w_1stWon'])
				p1_values['1stIn'].append(df.at[index, 'w_1stIn'])
				p1_values['2ndWon'].append(df.at[index, 'w_2ndWon'])
				p1_values['Svgms'].append(df.at[index, 'w_SvGms'])
				count_1 += 1

		elif df.at[index, 'loser_name'] == p1_name:
			if count_1 >= max_:
				pass
			else:
				if df.at[index, 'target'] == 1:
					p1_values['results'].append(0)
				else:
					p1_values['results'].append(1)

				p1_values['rank_points'].append(df.at[index, 'loser_rank_points'])
				p1_values['aces'].append(df.at[index, 'l_ace'])
				p1_values['dfs'].append(df.at[index, 'l_df'])
				p1_values['bps'].append(df.at[index, 'l_bpSaved'])
				p1_values['bpf'].append(df.at[index, 'l_bpFaced'])
				p1_values['svpt'].append(df.at[index, 'l_svpt'])
				p1_values['1stWon'].append(df.at[index, 'l_1stWon'])
				p1_values['1stIn'].append(df.at[index, 'l_1stIn'])
				p1_values['2ndWon'].append(df.at[index, 'l_2ndWon'])
				p1_values['Svgms'].append(df.at[index, 'l_SvGms'])
				count_1 += 1

		# if the row is about the player 2 append
		elif df.at[index, 'winner_name'] == p2_name:
			if count_2 >= max_:
				pass
			else:
				p2_values['results'].append(df.at[index, 'target'])
				p2_values['rank_points'].append(df.at[index, 'winner_rank_points'])
				p2_values['aces'].append(df.at[index, 'w_ace'])
				p2_values['dfs'].append(df.at[index, 'w_df'])
				p2_values['bps'].append(df.at[index, 'w_bpSaved'])
				p2_values['bpf'].append(df.at[index, 'w_bpFaced'])
				p2_values['svpt'].append(df.at[index, 'w_svpt'])
				p2_values['1stWon'].append(df.at[index, 'w_1stWon'])
				p2_values['1stIn'].append(df.at[index, 'w_1stIn'])
				p2_values['2ndWon'].append(df.at[index, 'w_2ndWon'])
				p2_values['Svgms'].append(df.at[index, 'w_SvGms'])
				count_2 += 1
		
		elif df.at[index, 'loser_name'] == p2_name:
			if count_2 >= max_:
				pass
			else:
				if df.at[index, 'target'] == 1:
					p2_values['results'].append(0)
				else:
					p2_values['results'].append(1)
				
				p2_values['rank_points'].append(df.at[index, 'loser_rank_points'])
				p2_values['aces'].append(df.at[index, 'l_ace'])
				p2_values['dfs'].append(df.at[index, 'l_df'])
				p2_values['bps'].append(df.at[index, 'l_bpSaved'])
				p2_values['bpf'].append(df.at[index, 'l_bpFaced'])
				p2_values['svpt'].append(df.at[index, 'l_svpt'])
				p2_values['1stWon'].append(df.at[index, 'l_1stWon'])
				p2_values['1stIn'].append(df.at[index, 'l_1stIn'])
				p2_values['2ndWon'].append(df.at[index, 'l_2ndWon'])
				p2_values['Svgms'].append(df.at[index, 'l_SvGms'])	
				count_2 += 1
		else:
			pass

	df_1 = pd.DataFrame(p1_values)
	df_2 = pd.DataFrame(p2_values)

	#print(df_1.head(5))
	#print(df_2.head(5))
	return get_stats_(p1_values, p2_values, df_1, df_2, list(p1_values.keys()))

# input two dicts, output dicts containing statistics
def get_stats_(p1_values, p2_values, df_1, df_2, metrics):
	# include the result metrics
	stats_1 = {}
	stats_2 = {}

	#std_1_3 = df_1[:3].std(axis = 0)
	#std_1_7 = df_1[:7].std(axis = 0)

	#std_2_3 = df_2[:3].std(axis = 0)
	#std_2_7 = df_2[:7].std(axis = 0)

	#avg_1_3 = df_1[:3].mean(axis = 0)
	#avg_1_7 = df_1[:7].mean(axis = 0)

	#avg_2_3 = df_2[:3].mean(axis = 0)
	#avg_2_7 = df_2[:7].mean(axis = 0)

	i_s = [1, 3, 7]
	stats_ = ['avg', 'std', 'perc']
	
	if len(p1_values[metrics[0]]) >= 5 and len(p2_values[metrics[0]]) >= 5:

		for i in i_s:
			for stat in stats_:
				for element in metrics:
					if i == 1:
						# simply assign last element for the last element to deal with
						stats_1['player_1_'+str(element)+'_l'] = p1_values[element][0]
						stats_2['player_2_'+str(element)+'_l'] = p2_values[element][0]
					else:

						# compute for the standard deviation
						if stat == 'std':
							stats_1['player_1_'+str(element)+'_'+str(stat)+'_l'+str(i)] = df_1[:i].std(axis = 0)[element]
							stats_2['player_2_'+str(element)+'_'+str(stat)+'_l'+str(i)] = df_2[:i].std(axis = 0)[element]
						
						# compute for the mean of those dataframes
						elif stat == 'avg':
							stats_1['player_1_'+str(element)+'_'+str(stat)+'_l'+str(i)] = df_1[:i].mean(axis = 0)[element]
							stats_2['player_2_'+str(element)+'_'+str(stat)+'_l'+str(i)] = df_2[:i].mean(axis = 0)[element]

						elif stat == 'perc':
							
							if i == 7:
								for k in range(25, 100, 25):
									stats_1['player_1_'+str(element)+'_'+str(stat)+'_l'+str(i)+'_p'+str(k)] = np.percentile(df_1[:i][element], k, axis = 0)
									stats_2['player_2_'+str(element)+'_'+str(stat)+'_l'+str(i)+'_p'+str(k)] = np.percentile(df_2[:i][element], k, axis = 0)
							else:
								pass

						else:
							print('ERROR: There was an error in the loading process.')
	else:
		pass

	stats_2.update(stats_1)
	# return the dictionnary containing all the informations about the dataframes being processed
	return stats_2

def load_db_df(session, df):
	# load the games into a db for cleaner updates
	
	# detailed statistics about last, 3, seventh mgames

	# but also make a dataframe for ready to use informations => run algorithm
	processed_df = pd.DataFrame()
	n = 0
	for index, row in tqdm(df.iterrows(), total=df.shape[0]):
	#for index, row in df.iterrows():
		match_date = df.at[index, 'tourney_date']
		# at each row update the dataframe & the database
		# update everything to take into account the last 5 five games, with mean, std, last value & average last results
		tmp_stats = get_stats(df.at[index, 'winner_name'], df.at[index, 'loser_name'], match_date, df, index, 7)
		
		if any(tmp_stats) == True:
			# add automatic saving to check progess of the program
			if n > 5000:
				processed_df.to_csv('./pre_processed_df_.csv')
				print(index)
				n =0

			n +=1
			#print(row)
			# define temporary df as row from the dict above
			tmp_df = pd.DataFrame(tmp_stats, index = [0])
			#df_ = df.loc[index, 'target':'minutes']
			#print(df_.shape)

			#df_ = df_.transpose()
			#tmp_dfs = [tmp_df, df_]
			#print(df.loc[index, 'target':'minutes'].transpose().head())

			#tmp_df = pd.concat(tmp_dfs, axis = 1)

			tmp_df.insert(0, 'target', df.at[index, 'target'])
			tmp_df.insert(1, 'p1_name', df.at[index, 'winner_name'])
			tmp_df.insert(2, 'p2_name', df.at[index, 'loser_name'])
			tmp_df.insert(3, 'p1_height', df.at[index, 'winner_ht'])
			tmp_df.insert(4, 'p2_height', df.at[index, 'loser_ht'])
			tmp_df.insert(5, 'p1_age', df.at[index, 'winner_age'])
			tmp_df.insert(6, 'p2_age', df.at[index, 'loser_age'])
			tmp_df.insert(7, 'p1_rank', df.at[index, 'winner_rank'])
			tmp_df.insert(8, 'p2_rank', df.at[index, 'loser_rank'])
			tmp_df.insert(9, 'score', df.at[index, 'score'])
			tmp_df.insert(10, 'minutes', df.at[index, 'minutes'])
			tmp_df.insert(11, 'tourney_date', df.at[index, 'tourney_date'])
			tmp_df.insert(12, 'tourney_name', df.at[index, 'tourney_name'])
			tmp_df.insert(13, 'tourney_level', df.at[index, 'tourney_level'])
			tmp_df.insert(14, 'p1_ioc', df.at[index, 'winner_ioc'])
			tmp_df.insert(15, 'p2_ioc', df.at[index, 'loser_ioc'])

			# add this row to the dataset keeping the index growing
			processed_df = processed_df.append(tmp_df, ignore_index = True)
			#print(processed_df)
			#print('\n\n')	
		else:
			pass
			

	processed_df.reset_index(inplace = True)
	processed_df.to_csv('./pre_processed_df_stats.csv')

# read the data stored in the pre_processed file to prepare for training
def load_data():
	data = pd.DataFrame()

	with open('./pre_processed_df_stats.csv', 'r') as in_file:
		data = read_csv(in_file)

	return data

def load_db(directory_path, database):
	# read the files & clean them
	df = read_csvs(directory_path)
	# dump the pre processed data into a csv file
	df.to_csv('./processed_data.csv')

	# init the database
	conn = init(database)
	session = get_session()

	load_db_df(session, df)

if __name__ == '__main__':
	load_db('./atp-matches-dataset/', 'hello')