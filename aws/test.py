import bs4
import markdownify
from bs4 import BeautifulSoup
import requests
from markdownify import MarkdownConverter
from markdownify import markdownify as md
from selenium import webdriver
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from categories import baseUrl, categoryIdMap, categoryBrowseLinkMap
from fullfillment import ff

# url = "https://aws.amazon.com/marketplace/pp/prodview-3b32sjummof5s?sr=0-1&ref_=beagle&applicationId=AWSMPContessa"


# browser = webdriver.Firefox()
# browser.get(url)  # replace with your URL
# WebDriverWait(browser, 200).until(  # wait for desired element to load
#     EC.presence_of_element_located((By.CLASS_NAME, "custom-markdown-viewer"))
# )
# html = browser.page_source



# soup = BeautifulSoup(html, 'html.parser')
# htmlText = soup.find('div', class_='custom-markdown-viewer')
# with open("./test.html", "w", encoding="utf-8") as html_file:
#     html_file.write(str(soup))


# print(soup)
# print (md(str(htmlText)))

print(type("123"))