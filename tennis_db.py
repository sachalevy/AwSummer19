import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Column, Integer, String, ARRAY, Boolean, Float
from sqlalchemy import inspect
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy import ForeignKey
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy.orm import mapper
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import hashlib
import sqlite3
import sys
import time
import hashlib

CONN = None 
ENGINE = None
SESSION = None

def init_session(engine):

	global ENGINE 
	ENGINE = engine
	global SESSION
	if (SESSION == None):
		Session = sessionmaker(bind=engine)
		session = Session()
		SESSION = session

def get_session():
	# returns SESSION global variable 
	return SESSION


def Conn(database):
	global CONN 
	if database:
		print("[+] Inserting into Homemade Database: " + str(database))
		# avoid creating multiple connection to the database 
		if(CONN == None):
			conn = init(database)
			CONN = conn 
		else: 
			conn = CONN 

		if isinstance(conn, str):
			print(str)
			sys.exit(1)
	else:
		conn = ""

	return conn

# method from twatt project 
def init(db):

	try:
		# init an interface to the database
		database_link = "sqlite:///" + str(db)
		# why echo twice on every item ? 
		engine = create_engine(database_link, echo = False)

		# generate metadata for all tables in base model
		Base.metadata.create_all(bind=engine)
		init_session(engine)

		# or work with conn to allow connection and sql langage execute statement
		conn = engine.raw_connection()

		return conn
	except Exception as e:
		return str(e)

# defining templates for tables to create when init
Base = declarative_base()

class MutableList(Mutable, list):
	def append(self, value):
		list.append(self, value)
		self.changed()

	@classmethod
	def coerce(cls, key, value):
		if not isinstance(value, MutableList):
			if isinstance(value, list):
				return MutableList(value)
			return Mutable.coerce(key, value)
		else:
			return value

class waitlist(Base):
	__tablename__ = 'waitlist'

	player_hash_id = Column(Integer, primary_key = True, nullable = False)
	player_name = Column(String, nullable = False)
	player_aces = Column(Integer)
	player_dfs = Column(Integer)
	scraped = Column(Boolean, nullable = False, default = False)

class players(Base):
	__tablename__ = 'players'
	last_update = Column(String, nullable = False)
	player_id = Column(Integer, primary_key = True)
	player_hash_id = Column(Integer, primary_key = True, nullable = False)
	player_name = Column(String, nullable = False)
	player_height = Column(Float)
	player_rank = Column(Float)
	player_age = Column(Float)

	# stats for last games
	ace = Column(Float)
	df = Column(Float)
	svpt = Column(Float)
	firstIn = Column(Float)
	firstWon = Column(Float)
	sndWon = Column(Float)
	bpSaved = Column(Float)
	bpFaced = Column(Float)
	SvGms = Column(Float)

	# dimensions of the array ?
	#aces = Column(MutableList.as_mutableARRAY(Integer))
	#dfs = Column(MutableList.as_mutableARRAY(Integer))
	#svpts = Column(MutableList.as_mutableARRAY(Integer))
	#firstIns = Column(MutableList.as_mutableARRAY(Integer))
	#firstWons = Column(MutableList.as_mutableARRAY(Integer))
	#sndWons = Column(MutableList.as_mutableARRAY(Integer))
	#bpSaveds = Column(MutableList.as_mutableARRAY(Integer))
	#bpFaceds = Column(MutableList.as_mutableARRAY(Integer))
	#SvGms_s = Column(MutableList.as_mutableARRAY(Float))

	def __init__(self, last_update, player_id, player_hash_id, player_name, 
		player_height, player_rank, player_age, ace, df, svpt, firstIn, 
		firstWon, sndWon, bpSaved, bpFaced, SvGms):
		self.last_update = last_update
		self.player_id = player_id
		self.player_hash_id = player_hash_id
		self.player_name = player_name
		self.player_height = player_height
		self.player_age = player_age
		self.ace = ace
		self.df = df
		self.svpt = svpt
		self.firstIn = firstIn
		self.firstWon = firstWon
		self.sndWon = sndWon
		self.bpSaved = bpSaved
		self.bpFaced = bpFaced
		self.SvGms = SvGms

class games(Base):
	__tablename__ = 'games'
	
	match_id = Column(Integer, primary_key = True, nullable = False)
	match_date = Column(String, nullable = False)
	match_time = Column(String)
	# created using a hashid local
	tourney_id = Column(String)
	tourney_name = Column(String)
	surface = Column(String)
	score = Column(String)
	minutes = Column(Float)

	# we consider player_1 to be the highest ranked player ?
	player_1_id = Column(Integer, nullable = False)
	player_1_name = Column(String, nullable = False)
	player_1_rank = Column(Integer, nullable = False)
	player_1_rank_points = Column(Integer)
	player_1_ioc = Column(String)
	player_1_ace = Column(Integer)
	player_1_df = Column(Integer)
	player_1_svpt = Column(Integer)
	player_1_1stIn = Column(Integer)
	player_1_1stWon = Column(Integer)
	player_1_2ndWon = Column(Integer)
	player_1_SvGms = Column(Integer)
	player_1_bpf = Column(Integer)
	player_1_bps = Column(Integer)

	# and by default player_2 to be the least ranked
	player_2_id = Column(Integer, nullable = False)
	player_2_name = Column(String, nullable = False)
	player_2_age = Column(Float)
	player_2_height = Column(Float)
	player_2_rank = Column(Integer, nullable = False)
	player_2_rank_points = Column(Integer)
	player_2_ioc = Column(String)
	player_2_ace = Column(Integer)
	player_2_df = Column(Integer)
	player_2_svpt = Column(Integer)
	player_2_1stIn = Column(Integer)
	player_2_1stWon = Column(Integer)
	player_2_2ndWon = Column(Integer)
	player_2_SvGms = Column(Integer)
	player_2_bpf = Column(Integer)
	player_2_bps = Column(Integer)

	# stats for the given player over the last five games
	l5_player_1_ace = Column(Integer)
	l5_player_1_df = Column(Integer)
	l5_player_1_svpt = Column(Integer)
	l5_player_1_1stIn = Column(Integer)
	l5_player_1_1stWon = Column(Integer)
	l5_player_1_2ndWon = Column(Integer)
	l5_player_1_SvGms = Column(Integer)
	l5_player_1_bpf = Column(Integer)
	l5_player_1_bps = Column(Integer)

	# stats for the given player over the last five games
	l5_player_2_ace = Column(Integer)
	l5_player_2_df = Column(Integer)
	l5_player_2_svpt = Column(Integer)
	l5_player_2_1stIn = Column(Integer)
	l5_player_2_1stWon = Column(Integer)
	l5_player_2_2ndWon = Column(Integer)
	l5_player_2_SvGms = Column(Integer)
	l5_player_2_bpf = Column(Integer)
	l5_player_2_bps = Column(Integer)

	# for the training games, where outcome of the game is known
	# 1 for the player 1, 0 for the player 2
	target = Column(Integer)
	player_1_odd = Column(Integer)
	player_2_odd = Column(Integer)
	# % for player_1 to win
	# for the record
	prediction_ = Column(Integer)
	# explicit the prediction of the algorithm
	prediction_player = Column(String)

	# the bet registered by the algorithm for this match
	# decision made by the algorithm
	bet = Column(Integer)
	# outcome of the bet: if predicted player won, otherwise false
	bet_result = Column(Integer)

	def __init__(self, match_id, match_date, match_time, player_1_id, 
		player_1_name, player_1_odd, player_2_id, player_2_name, player_2_odd):
		
		# add values for this game
		# add the statistics for the last five games of each players
		self.match_id = match_id
		self.match_date = match_date
		self.match_time = match_time
		self.player_1_id = player_1_id
		self.player_1_name = player_1_name
		self.player_2_id = player_2_id
		self.player_2_name = player_2_name

		self.player_1_odd = player_1_odd
		self.player_2_odd = player_2_odd

def create_hashID(search):
	new_id = int(hashlib.sha256(search.encode('utf-8')).hexdigest(), 16) % 10**8
	return new_id

# update the player profile
def update_player_stats(player):
	session = get_session()
	players_inDb = session.query(players.player_id).all()

	if player.player_id not in players_inDb:
		player = players(last_update = time.time(), player_id = player.player_id,
			player_name = player.player_name, player_height = player.player_height,
			player_age = player.player_age, ace = player.ace, df = player.df,
			svpt = player.svpt, firstIn = player.firstIn, firstWon = player.firstWon,
			sndWon = player.sndWon, bpSaved = player.bpSaved, bpFaced = player.bpFaced)
		session.add(player)
		session.commit()

	if player.player_id in players_inDb:
		player_ = session.query(players).filter_by(player_id = player.player_id).first()
		player_.last_update = time.time()
		player_.player_age = player.player_age
		player_.ace = player.ace
		player_.df = player.df
		player_.svpt = player.svpt
		player_.firstIn = player.firstIn
		player_.firstWon = player.firstWon
		player_.sndWon = player.sndWon
		player_.bpSaved = player.bpSaved
		player_.bpFaced = player.bpFaced
		session.commit()

def add_match(match):
	session = get_session()
	games_inDb = session.query(games.match_id).all()
	
	if match.match_id not in games_inDb:
		game = games(match_id = match.match_id, match_date = match.match_date,
			match_time = match.match_time, player_1_id = match.player_1_id, 
			player_1_name = match.player_1_name, player_1_odd = match.player_1_odd, 
			player_2_id = match.player_2_id, player_2_name =match.player_2_name, 
			player_2_odd = match.player_2_odd)
		session.add(game)
		session.commit()

	# either precompute ids in scraper program or craft it here
	players_inDb = session.query(players.player_id).all()
	
	if match.player_1_id not in players_inDb:
		player = players(last_update = time.time(), player_id = match.player_1_id,
			player_name = match.player_1_name)
		session.add(player)
		session.commit()

	if match.player_2_id not in players_inDb:
		player = players(last_update = time.time(), player_id = match.player_2_id,
			player_name = match.player_2_name)
		session.add(player)
		session.commit()