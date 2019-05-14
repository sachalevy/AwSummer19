import json
import csv
import time
import argparse
import os, sys
import tensorflow as tf
from tensorflow.keras import layers
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

import warnings
from tqdm import tqdm
import pandas as pd
import numpy as np
import math

# include confidence coefficient for prediction


def read_files(files):
	train_df = pd.DataFrame()

	# full path is necessary for those files
	for file in files:
		df = pd.read_csv(file)
		train_df = train_df.append(df)
	
	# train_df = train_df.loc[0:int(0.2*len(train_df)), :]
	# train_df.reset_index(inplace = True, drop = True)

	# shuffle all the data from the dataset
	# train_df = train_df.sample(frac=1).reset_index(drop=True)
	
	if 'Unnamed: 0' in train_df.columns:
		train_df.drop('Unnamed: 0', axis = 1, inplace = True)

	# drop all non necessary columns to training
	train_df.drop(['index', 'minutes', 'p1_ioc', 'p2_ioc', 'p1_name', 'p2_name',
		'player_1_rank_points_avg_l3', 'player_1_rank_points_l', 'player_1_rank_points_perc_l7_p25',
		'player_1_rank_points_perc_l7_p50', 'player_1_rank_points_perc_l7_p75', 'player_1_rank_points_std_l3',
		'player_2_rank_points_avg_l3', 'player_2_rank_points_l', 'player_2_rank_points_perc_l7_p25',
		'player_2_rank_points_perc_l7_p50', 'player_2_rank_points_perc_l7_p75', 'player_2_rank_points_std_l3',
		'score', 'tourney_date', 'tourney_level', 'tourney_name'],
       axis = 1, inplace = True)

	train_df.rename(columns = {'target_': 'target_0'}, inplace = True)
	train_df.insert(0, 'target_1', 0)
	train_df.insert(1, 'target_0', 0)

	# get the columns in the right order so that we can process them easily
	for index, row in train_df.iterrows():
		
		train_df.at[index, 'target_1'] = train_df.at[index, 'target']
		
		if train_df.at[index, 'target'] == 1:
			train_df.at[index, 'target_0'] = 0
		else:
			train_df.at[index, 'target_0'] = 1

	train_df.drop('target', axis = 1, inplace = True)
	train_df.dropna(how = 'any', inplace = True)

	# normalize the dataset 
	# => use min max scaler with positive values since can't learn from 0 and 1 standardized => maps to 0
	#scaler = StandardScaler()
	#scaler.fit(train_df)
	#train_df = pd.DataFrame(scaler.transform(train_df), columns = train_df.columns)

	# seems to show same weird attitude => accuracy goes to one very quickly, linked to binary target 0/1
	#scaler = MinMaxScaler((0,10))
	#scaler.fit(train_df)
	#train_df = pd.DataFrame(scaler.transform(train_df), columns = train_df.columns)

	# quick patch to limit damages done by non limiting the values returned by the stats => which values

	# shifting the number of values in the datasets seems to highly influence the accuracy of the prediction
	# and also influences the change rate of this solutions => should be good to include only the valid data 0.2

	train, test = split(train_df)

	train_Y = train.loc[:, 'target_1':'target_0']
	train_X = train.loc[:, 'p1_age':'player_2_svpt_std_l7']

	test_Y = test.loc[:, 'target_1':'target_0']
	test_X = test.loc[:, 'p1_age':'player_2_svpt_std_l7']
	
	print(train_X.head())
	print(train_X.shape)
	print('\n -------------------------------- \n')
	print(train_Y.head())
	print(train_Y.shape)
	print('\n -------------------------------- \n')
	print(test_X.head())
	print(test_X.shape)
	print('\n -------------------------------- \n')
	print(test_Y.head())
	print(test_Y.shape)

	train_X = train_X.to_numpy()
	train_Y = train_Y.to_numpy()

	test_X = test_X.to_numpy()
	test_Y = test_Y.to_numpy()

	#time.sleep(5)

	return train_X, train_Y, test_X, test_Y

def split(df):

	train = df.iloc[:int(0.8*len(df)), :]
	test = df.iloc[int(0.8*len(df)):, :]

	return train, test

def init_parser():

	parser = argparse.ArgumentParser(description='inputdata')
	
	parser.add_argument(
        '-o', '--output',
        required=False,
        default='./model_v2.h5',
        type=str,
        help='Output path for model',
        dest='output_path')
	
	parser.add_argument(
        '-i', '--input',
        required=True,
        dest='input_list',
        metavar="CSV-FILE",
        nargs='+',
        type=str,
        help="CSV files")
	args = parser.parse_args()

	return args

def build_model():
	model = tf.keras.Sequential()
	model.add(layers.Dense(64, activation='relu'))
	model.add(layers.Dense(32, activation='relu'))
	model.add(layers.Dense(16, activation='sigmoid'))
	model.add(layers.Dense(32, activation='relu'))
	model.add(layers.Dense(64, activation='tanh'))
	model.add(layers.Dense(2, activation='softmax'))

	model.compile(
		optimizer = tf.train.RMSPropOptimizer(0.0001),
		loss = 'categorical_crossentropy',
		metrics = ['accuracy'])

	return model

def main():
	# give the files through terminal arguments & read em
	args = init_parser()
	# read the input files and get numpy arrays with input and outputs
	train_X, train_Y, test_X, test_Y = read_files(args.input_list)

	# build model using tensorflow's keras
	model = build_model()
	# get a summary of how the model is composed
	
	# fit the model, seems like a bug in the model => optimal batch_size seems arbitrary
	# also optimize using hyperparameter optimization algorithm => 1000 not good,
	# does not make good decisions => should use batch size near the application size of the dataste
	model.fit(train_X, train_Y, epochs = 300, batch_size = 10)
	model.summary()
	print('\n\n--------------------------------------------')

	# evaluate the model using the tests samples
	model.evaluate(test_X, test_Y, batch_size=10)
	print('--------------------------------------------\n\n')
	
	# used batch size realistic relatively to the size of the data which will be predicted in application
	X_predicted = model.predict(test_X[0:1000], batch_size = 10)
	Y_target = test_Y[0:1000]
	# custom estimators for observation of the model's performance
	very_good = []
	good = []
	notbad = []
	for i in range(1000):
		#print('Prediction: {0:.2f} : {1:.2f} ---- Actual target: {2} : {3}.'.format(
		#	X_predicted[i][0], X_predicted[i][1], Y_target[i][0], Y_target[i][1]))
		# print('Result : {} \n'.format(abs(Y_target[i][0] - X_predicted[i][0])))
		
		if 0.05 > abs(Y_target[i][0] - X_predicted[i][0]):
			very_good.append(X_predicted[i])	
		elif 0.10 > abs(Y_target[i][0] - X_predicted[i][0]):
			good.append(X_predicted[i])
		elif 0.20 > abs(Y_target[i][0] - X_predicted[i][0]):
			notbad.append(X_predicted[i])

	
	print('\n\n--------------------------------------------')
	print('Over 1000 games we have {} not bad, {} good and {} very good results.'.format(len(notbad), len(good), len(very_good)))
	print('--------------------------------------------\n\n')
	
	# is right 9/10 but gets to really miss the target when does => extreme decisions

	# save the model for later use
	model.save(args.output_path)
	print('\nThe model was saved at {}.'.format(args.output_path))

if __name__ == '__main__':
	main()