import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import json
import time
import numpy as np
import requests
import re
import math
import statistics
import sqlalchemy
from tennis_db import players, games, init, get_session, update_player_stats, add_match, create_hashID, waitlist
import os, sys, logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fractions import Fraction as fr 
# set up a logging environment

class Logger(): 
	
	def __init__(self):
		self.logger = logging.getLogger(__name__)
		self.logger.setLevel(logging.DEBUG)

		handler = logging.FileHandler('logs.log')

		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		handler.setFormatter(formatter)

		self.logger.addHandler(handler)
		self.logger.addHandler(logging.StreamHandler(sys.stdout))

# input: players list to scrape
# will probably have to use selenium to scrape the players
class player():
	def __init__(self, start_time, name_hash, firstname, lastname, rank):
		self.start_time = start_time
		self.firstname = firstname
		self.lastname = lastname
		self.name_hash = name_hash
		self.rank = rank
		self.aces = []
		self.dfs = []
		self.svgms = []
		self.bps = []
		self.bpf = []
		self.svpt = []
		self.firstserves = []
		self.firstserveswon = []
		self.sndserveswon = []

BASE_URL = 'https://www.ultimatetennisstatistics.com/playerProfile?name='
URL_2 = 'http://www.tennisabstract.com/cgi-bin/player.cgi?p='
URL_3 = 'https://www.scoreboard.com/tennis/rankings/atp/'

def get_last5(elements, player_, driver, session):

	url = driver.current_url

	window_before = driver.window_handles[0]
	count = 0
	for name in list(elements.keys()):
		if count >= 5: 
			break
		elements[name].click()
		# have selenium switch to this tab
		# then have it switch back to the main tab to find the rest of the information
		window_after = driver.window_handles[1]
		active_element = driver.switch_to.window(window_after)
		time.sleep(5)
		# if p is equal to player lastname then look on the left column otherwise look on the right column
		if name.lower() != player_.lastname.lower():
			try:
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(3) > div:nth-child(2)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(1) > div:nth-child(1)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(1) > div:nth-child(1)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > div:nth-child(1)
				aces_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				dfs_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				firstserves_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(4) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				firstserveswon_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(5) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				sndserveswon_ =  driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(6) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				bps_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(7) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				svpt_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(14) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				svgms_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(19) > td:nth-child(3) > div:nth-child(2)').get_attribute('innerHTML')
				#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(3) > div:nth-child(2)
				
				firstserves = firstserves_.replace('%', '')
				firstserveswon_ = firstserveswon_.split()[1].replace('(', '')
				firstserveswon_ = firstserveswon_.replace(')', '')

				temp_1, temp_2 = firstserveswon_.split('/')
				firstserves = math.ceil((int(temp_2)*100)/74)

				sndserveswon_ = sndserveswon_.split()[1].replace('(', '')
				sndserveswon_ = sndserveswon_.replace(')', '')
				sndwon, temp_3 = sndserveswon_.split('/')
				
				bps_ = bps_.split()[1].replace('(', '')
				bps_ = bps_.replace(')', '')
				bps, bpf = bps_.split('/')

				svpt_ = svpt_.split()[1].replace('(', '')
				svpt_ = svpt_.replace(')', '')
				temp_5, svpt = svpt_.split('/')

				svgms_ = svgms_.split()[1].replace('(', '')
				svgms_ = svgms_.replace(')', '')
				temp4, svgms = sndserveswon_.split('/')

				print('aces: {} dfs: {} bps: {} firstserves: {} service games: {}'.format(aces_.split()[0], dfs_.split()[0], bps, firstserves, svgms))
				player_.firstserves.append(int(firstserves))
				player_.aces.append(int(aces_.split()[0]))
				player_.dfs.append(int(dfs_.split()[0]))
				player_.svpt.append(int(svpt))
				player_.firstserveswon.append(int(temp_1))
				player_.sndserveswon.append(int(sndwon))
				player_.bpf.append(int(bpf))
				player_.bps.append(int(bps))
				player_.svgms.append(int(svgms))
				count += 1
			except selenium.common.exceptions.NoSuchElementException:
				pass
		else:
			try:
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(3) > div:nth-child(2)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(1) > div:nth-child(1)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(1) > div:nth-child(1)
								#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > div:nth-child(1)
				aces_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				dfs_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(3) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				firstserves_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(4) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				firstserveswon_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(5) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				sndserveswon_ =  driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(6) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				bps_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(7) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				svpt_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(14) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')
				svgms_ = driver.find_element_by_css_selector('#tab-statistics-0-statistic > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(19) > td:nth-child(1) > div:nth-child(1)').get_attribute('innerHTML')

				firstserves = firstserves_.replace('%', '')
				firstserveswon_ = firstserveswon_.split()[1].replace('(', '')
				firstserveswon_ = firstserveswon_.replace(')', '')

				temp_1, temp_2 = firstserveswon_.split('/')
				firstserves = math.ceil((int(temp_2)*100)/74)

				sndserveswon_ = sndserveswon_.split()[1].replace('(', '')
				sndserveswon_ = sndserveswon_.replace(')', '')
				sndwon, temp_3 = sndserveswon_.split('/')
				
				bps_ = bps_.split()[1].replace('(', '')
				bps_ = bps_.replace(')', '')
				bps, bpf = bps_.split('/')

				svpt_ = svpt_.split()[1].replace('(', '')
				svpt_ = svpt_.replace(')', '')
				temp_5, svpt = svpt_.split('/')

				svgms_ = svgms_.split()[1].replace('(', '')
				svgms_ = svgms_.replace(')', '')
				temp4, svgms = svgms_.split('/')

				print('aces: {} dfs: {} bps: {} firstserves: {} service games: {}'.format(aces_.split()[0], dfs_.split()[0], bps, firstserves, svgms))
				player_.firstserves.append(int(firstserves))
				player_.aces.append(int(aces_.split()[0]))
				player_.dfs.append(int(dfs_.split()[0]))
				player_.svpt.append(int(svpt))
				player_.firstserveswon.append(int(temp_1))
				player_.sndserveswon.append(int(sndwon))
				player_.bpf.append(int(bpf))
				player_.bps.append(int(bps))
				player_.svgms.append(int(svgms))
				count += 1
			except selenium.common.exceptions.NoSuchElementException:
				pass
		
		driver.close()
		active_element = driver.switch_to.window(window_before)
		
	update_db(player_, session)

def scrape_stats_2(players, driver, session):
	driver.get(URL_3)

	with requests.Session() as res:
		page = res.get(URL_3)
	soup = BeautifulSoup(page.content, 'html.parser')
	rows_even = soup.find_all("tr", {"class": "rank-row even"})
	rows_odd = soup.find_all("tr", {"class": "rank-row odd"})
	
	ranks = {}

	for row in rows_even:
		p = row.find("td", {"class": "rank-column-player"})
		ranks[(p.find("a").get_text(strip=True)).split()[0].lower()] = int(float(
			row.find("td", {"class": "rank-column-rank"}).get_text(strip=True)+str(0)))

	for row in rows_odd:
		p = row.find("td", {"class": "rank-column-player"})
		ranks[(p.find("a").get_text(strip=True)).split()[0].lower()] = int(float(
			row.find("td", {"class": "rank-column-rank"}).get_text(strip=True)+str(0)))
	
	for player_ in players:
		firstname, lastname = get_names(player_)
		player_hashname = create_hashID(player_)
		start_time = time.time()
		_player_ = player(start_time, player_hashname, firstname, lastname, ranks[(lastname).lower()])
		element = driver.find_element_by_css_selector('tr.rank-row:nth-child('
			+str(ranks[(lastname).lower()])+') > td:nth-child(2) > a:nth-child(2)')
		element.click()
		count = 0
		elements = {}
		for i in range(1,4):
			for j in range(1,8):
				try:
					time.sleep(1)
					element = driver.find_element_by_xpath('/html/body/div[2]/div[3]/div[2]/div[1]/div[5]/div[3]/div[5]/div[2]/table['
						+str(i)+']/tbody/tr['+str(j)+']/td[3]')
					p = element.get_attribute('innerHTML')
					p = p.replace('<span class="padl">', '')
					p = p.replace('</span>', '')
					p = p.split()[0]
					print(p)

					elements[p]= element
				except selenium.common.exceptions.NoSuchElementException:
					break
		get_last5(elements, _player_, driver, session)
		driver.get(URL_3)

def update_db(player, session):
	# get the player profile in the db
	stats = get_stats(player)

	player_ = session.query(players).filter_by(player_hash_id = player.name_hash).first()
	if player_:
		# reset the last update to now
		player_.lastupdate = time.time()
		player_.ace = stats[player.player_name +'_ace']
		player_.df = stats[player.player_name +'_df']
		player_.bpf = stats[player.player_name +'_bpf']
		player_.bps = stats[player.player_name +'_bps']
		player_.svpt = stats[player.player_name +'_svpt']
		player_.firstIn = stats[player.player_name +'_firstIn']
		player_.firstWon = stats[player.player_name +'_firstWon']
		player_.sndWon = stats[player.player_name +'_sndWon']
		player_.SvGms = stats[player.player_name +'_SvGms']
		session.commit()

	else:
		# add player to the list of players to add to the db
		# will need to setup an async scraper to retrieve broader information for those players
		player_new = players(last_update = time.time(), player_id = 0, player_hash_id = player.name_hash, 
			player_name = player.firstname +' '+ player.lastname, player_height = 0, player_age = 0, player_rank = player.rank, 
			ace = stats[player.player_name +'_ace'], df = stats[player.player_name +'_df'], svpt = stats[player.player_name +'_svpt'], 
			firstIn = stats[player.player_name +'_firstIn'], firstWon = stats[player.player_name +'_firstWon'], 
			sndWon = stats[player.player_name +'_sndWon'],bpSaved = stats[player.player_name +'_bps'], 
			bpFaced = stats[player.player_name +'_bpf'], SvGms = stats[player.player_name +'_SvGms']
			)
		session.add(player_new)
		session.commit()

	# log to indicate end of scrapping for given player, in ___ amount of time
	#logger.info('Finished scrapping for {} {} in {}.'.format(player.firstname, player.lastname, time.time()-player.start_time))
	
def get_stats(player):
	# compute stats for the last five games of the player
	stats = {}

	stats[player.player_name +'_ace'] = statistics.mean(player.aces)
	stats[player.player_name +'_df'] = statistics.mean(player.dfs)
	stats[player.player_name +'_SvGms'] = statistics.mean(player.svgms)
	stats[player.player_name +'_svpt'] = statistics.mean(player.svpt)
	stats[player.player_name +'_firstIn'] = statistics.mean(player.firstserves)
	stats[player.player_name +'_bpf'] = statistics.mean(player.bpf)
	stats[player.player_name +'_bps'] = statistics.mean(player.bps)
	stats[player.player_name +'_firstWon'] = statistics.mean(player.firstserveswon)
	stats[player.player_name +'_sndWon'] = statistics.mean(player.sndserveswon)
	print(stats)
	return stats

# might need to complexify the process
def create_driver():
	driver = webdriver.Firefox()
	return driver

# here we assume player is a string containing first & last name
# useful method since will need more complex processing
def get_names(player):
	names = player.split()
	return names[0], names[1]

def scrape_players(players, session):
	driver = create_driver()
	return scrape_stats_2(players, driver, session)

def main():
	database = "tennis_bets.db"
	conn = init(database)
	session = get_session()
	players = ['Roger Federer', 'Kei Nishikori']
	scrape_players(players, session)
	# for all players in the headlines => scrape them
	session.close()
	conn.close()

if __name__ == '__main__':
	main()