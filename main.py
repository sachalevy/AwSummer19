import tensorflow as tf
import pandas as pd
import numpy as np

import os, sys
import argparse
from tqdm import tqdm
import json
import csv
import time

# program must:
# 1. get the headlines of the day for the tennis games
# 2. get the stats & values for the players corresponding to these games
# 3. load latest model and predict for the given data

def main():
	url = 'https://www.unibet.fr/sport/tennis'

	games = get_headlines(url, session)

	players = []
	for game in games:
		players.append(game.player_1_name.lower(), game.player_2_name.lower())

	stats = scrape_players(players, session)
	# to predict array to be defined
	to_predict = build_df(games, stats)

	# load the latest model
	model = keras.models.load_model('./model_v2.h5')
	# recompile the model => weights already initialized, redefine the optimizer
	model.compile(
		optimizer = tf.train.RMSPropOptimizer(0.0001),
		loss = 'categorical_crossentropy',
		metrics = ['accuracy'])

	# get a summary of the model's shape from the last runs
	model.summary()
	predicted = model.predict(to_predict)

	# now display the results of the prediction
	for i in range(len(predicted)):
		print('Game {}: {} : {} VS {3:.2f} : {4:.2f}'.format(i,
			games[i][players][0], games[i][players][1], predicted[i][0], predicted[i][1]) )




if __name__ == '__main__':
	main()