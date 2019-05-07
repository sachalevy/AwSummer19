import selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
URL = 'http://www.tennisabstract.com/'
import time

# too long method
def find_game_date(p1_name, p2_name):
	driver = webdriver.Firefox()
	global URL
	page = driver.get(URL)
	
	box1 = driver.find_element_by_xpath('//*[@id="head1"]')
	box2 = driver.find_element_by_xpath('//*[@id="head2"]')
	box1.send_keys('(M) '+ p1_name, Keys.ENTER)
	box2.send_keys('(M) '+ p2_name, Keys.ENTER)

if __name__ == '__main__':
	find_game_date('Roger Federer', 'Kei Nishikori')