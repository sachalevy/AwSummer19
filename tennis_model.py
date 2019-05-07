import sklearn
import time
from sklearn import preprocessing
import pandas as pd 
import numpy as np
from tqdm import tqdm
from sklearn.preprocessing import StandardScaler
from sklearn.svm import NuSVR
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import os, sys
import warnings
import tensorflow
from sklearn.neural_network import MLPRegressor
from sklearn.neural_network import MLPClassifier
from sklearn.externals import joblib
warnings.simplefilter('ignore')
"""
TODO: 
-> Centralize all the data in the sqlite db: so have players, games all in db
 - 1. headlines & stats scraper: automate scrapping process for the bet headlines
 - 3. get betting odds corresponding games in csvs: transfer all csv file to database + scrape the odds for these games
 - 2. set up environment for rl: actions, states, agent, reward function
"""
def read_file(directory_path):
	train_df = pd.DataFrame()

	for file in os.listdir(directory_path):
		# read file in raw manner
		df = pd.read_csv(directory_path+file)
		#df.dropna()
		#print(df.shape)
		#df_ = pd.read_csv(directory_path+file, dtype = {"tourney_id": object, "tourney_name": object, "surface": object, 
		#	"draw_size": np.int32, "tourney_level": object, "tourney_date": np.int32, "match_num": np.int32, 
		#	"winner_id": np.int32, "winner_seed": np.float32, "winner_entry": object, "winner_name": object, 
		#	"winner_hand": object, "winner_ht": np.float32, "winner_ioc": object, "winner_age": np.float64, 
		#	"winner_rank": np.float32, "winner_rank_points": np.float32, "loser_id": np.int32, 
		#	"loser_seed": np.float32,"loser_entry": object, "loser_name": object, "loser_hand": object, "loser_ht": np.float32,
		#	"loser_ioc": object,"loser_age": np.float64, "loser_rank": np.float32, "loser_rank_points": np.float32,
		#	"score": object, "best_of": np.int32, "round": object, "minutes": np.float32, "w_ace": np.float32, 
		#	"w_df": np.float32, "w_svpt": np.float32,"w_1stIn": np.float32, "w_1stWon": np.float32, 
		#	"w_2ndWon": np.float32, "w_SvGms": np.float32, "w_bpSaved": np.float32, "w_bpFaced": np.float32,
		#	"l_ace": np.float32, "l_df": np.float32, "l_svpt": np.float32,"l_1stIn": np.float32, "l_1stWon": np.float32, 
		#	"l_2ndWon": np.float32, "l_SvGms": np.float32, "l_bpSaved": np.float32, "l_bpFaced": np.float32})
		#df_.append(df)
	#	df = pd.read_csv(directory_path+file, dtype = {"tourney_id": str, "tourney_name": str, "surface": str, 
	#		"draw_size": np.float16, "tourney_level": str, "tourney_date": str, "match_num": np.float16, 
	#		"winner_id": np.float16, "winner_seed": np.float16, "winner_entry": str, "winner_name": str, 
	#		"winner_hand": str, "winner_ht": np.float16, "winner_ioc": str, "winner_age": np.float16, 
	#		"winner_rank": np.float16, "winner_rank_points": np.float16, "loser_id": np.float16, 
	#		"loser_seed": np.float16,"loser_entry": str, "loser_name": str, "loser_hand": str, "loser_ht": np.float16,
	#		"loser_ioc": str,"loser_age": np.float16, "loser_rank": np.float16, "loser_rank_points": np.float16,
	#		"score": str, "best_of": np.float16, "round": str, "minutes": np.float16, "w_ace": np.float16, 
	#		"w_df": np.float16, "w_svpt": np.float16,"w_1stIn": np.float16, "w_1stWon": np.float16, 
	#		"w_2ndWon": np.float16, "w_SvGms": np.float16, "w_bpSaved": np.float16, "w_bpFaced": np.float16,
	#		"l_ace": np.float16, "l_df": np.float16, "l_svpt": np.float16,"l_1stIn": np.float16, "l_1stWon": np.float16, 
	#		"l_2ndWon": np.float16, "l_SvGms": np.float16, "l_bpSaved": np.float16, "l_bpFaced": np.float16})
		
		# drop columns that are not intereseting to analysis
		df.drop(["tourney_id", "tourney_name", "surface", "draw_size", "tourney_level", "tourney_date", "match_num",
			"winner_id", "winner_seed", "winner_entry", "winner_hand", "winner_name", "winner_ioc", "loser_id", 
			"loser_seed", "loser_entry", "loser_name",  "loser_hand", "loser_ioc", "score", "round", "best_of", 
			"minutes"], axis = 1, inplace = True)

		# drop columns containing null values
		df.dropna(how = 'any', inplace = True)
		# insert a target column: we try to predict if the best ranked player will win the game
		df.insert(0, 'target', 0)


		for index, row in df.iterrows():
			if(row['winner_rank'] > row['loser_rank']):
				df.at[index, 'target'] = 1
			else:
				df.at[index, 'target'] =0

		# add this dataframe portion to the current dataframe
		train_df = train_df.append(df)

	# reset the index of the dataframe so continuous indexing
	train_df.reset_index(inplace = True)

	return train_df


def main():
	directory_path = "/users/sachalevy/tennis_data/atp-matches-dataset/"
	file_type = ".csv"
	
	train_df = read_file(directory_path)
	print(train_df.shape)
	
	train_X, test_X, train_Y, test_Y = train_test_split(train_df.loc[:,'winner_ht':'l_bpFaced'],train_df.loc[:,'target'], test_size = 0.1, random_state = 3, shuffle = False)
	
	scaler = StandardScaler()
	scaler.fit(train_X)
	scaler.fit(test_X)
	train_X = pd.DataFrame(scaler.transform(train_X), columns = train_X.columns)
	test_X = pd.DataFrame(scaler.transform(test_X), columns = test_X.columns)

	print(test_X.head(10))
	print(test_Y.head(10))
	print(train_X.head(10))
	print(train_Y.head(10))

	
	batch_size = 100
	learning_rate = 0.001
	epochs = 300
	random_state = 3
	momentum = 0.9

	# change model to be player oriented => so learns relationships between players
	# if the MLP regressor shuffles the data, does that account for shuffling the fields

	model = MLPRegressor(hidden_layer_sizes = 1000, activation = 'tanh', solver = 'sgd', 
		alpha = 0.0001, batch_size = batch_size,
		max_iter = epochs, shuffle = True, 
		random_state = random_state, verbose = True, momentum = momentum, 
		nesterovs_momentum = True, early_stopping = True)
	
	model.fit(train_X, train_Y)
	print(model.score(test_X, test_Y))
	joblib.dump(model, 'model_n1.joblib')

	n = 0
	predicted = model.predict(test_X)
	
	for row in predicted:
		if(n>20): break
		i = n + len(train_X)
		print('Target: {0}  --- Model prediction: {1:.2f}'.format(test_Y.at[i],row))
		n = n+1

if __name__ == '__main__':
    main()