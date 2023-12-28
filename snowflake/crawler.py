import time
import os
import random
import json
import re
import math
import requests
from bs4 import BeautifulSoup
import concurrent
from concurrent import futures
from tqdm import tqdm
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from categories import baseUrl, categoryIdMap, categoryBrowseLinkMap
from fullfillment import ff
from markdownify import MarkdownConverter
from markdownify import markdownify as md
from aws.crawler import categories, Get_Categories
from account import username, password


def Initialize_Driver():
    chromedriver_path = os.path.join(os.path.dirname(__file__), "../chrome_driver/chromedriver")
    # print(chromedriver_path)
    service = webdriver.chrome.service.Service(chromedriver_path)
    chrome_options = webdriver.ChromeOptions()
   
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation', 'disable-component-update','ignore-certificate-errors'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    # chrome_options.add_argument("--headless")
  
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1400, 750)
    driver.set_page_load_timeout(20)
    return driver

class Snowflake_Db_Crawler:
    pass