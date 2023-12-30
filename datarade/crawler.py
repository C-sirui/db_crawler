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
import traceback
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from markdownify import MarkdownConverter
from markdownify import markdownify as md
from account import username, password
from selenium.common.exceptions import TimeoutException

links_per_page = 10

def Initialize_Driver():
    chromedriver_path = os.path.join(os.path.dirname(__file__), "../chrome_driver/chromedriver")
    # print(chromedriver_path)
    service = webdriver.chrome.service.Service(chromedriver_path)
    chrome_options = webdriver.ChromeOptions()
    
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation', 'disable-component-update','ignore-certificate-errors'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--headless")
  
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1400, 750)
    driver.set_page_load_timeout(60)
    return driver

class Dataraide_Db_Crawler:
    def __init__(self):
        self.driver = Initialize_Driver()
        self.failed_count = 0
        self.total_count = 0
        self.wait = WebDriverWait(self.driver, 20)
        
    def login(self):
        self.driver.get('https://datarade.ai/users/sign-in')
        # Wait for the page to load
        username_box = self.driver.find_element(By.ID, 'user__email')
        password_box = self.driver.find_element(By.ID, 'user__password')
        username_box.send_keys(username)
        password_box.send_keys(password)
        password_box.send_keys(Keys.RETURN)
        # Wait for the next page to load or for login to complete
        time.sleep(5)
        
    def get_search_link(self, pageNum, categoryName):
        searchUrl = f'https://datarade.ai/search/products?search_context=products&search_type=navbar&'
        searchVariable = f'keywords={categoryName}&page={pageNum}'
        return searchUrl + searchVariable
    
    def get_all_categories(self):
        ans = []
        self.driver.get('https://datarade.ai/data-categories')
        categoriesDiv = self.driver.find_element(By.CLASS_NAME, 'categories')
        categories = categoriesDiv.find_elements(By.TAG_NAME, 'a')
        categories = [category for category in categories if 
                      self.driver.execute_script("return arguments[0].children.length;", category) 
                      == 1]
        # categories = [category.find_element(By.CLASS_NAME, "categories__list-item__title")
        #               for category in categories]
        for category in categories[:22]:
            ans.append(category.find_element(By.XPATH, "./*[1]").text)
        return ans
    
    def open_link_in_new_tab(self, url):
        # Open the link in a new tab
        self.driver.execute_script(f"window.open('{url}', '_blank');")

        # Switch to the new tab
        self.driver.switch_to.window(self.driver.window_handles[-1])

    def close_current_tab(self):
        # Close the current tab
        self.driver.close()

        # Switch back to the first tab
        self.driver.switch_to.window(self.driver.window_handles[0])
        time.sleep(2)
              
    def crawl_by_categories(self, categoryName):
        # self.driver.implicitly_wait(100)  # Set implicit wait to 10 seconds
        self.driver.set_page_load_timeout(15)  # Set page load timeout to 30 seconds
        page1 = self.get_search_link(1, categoryName)
        
        try:
            self.driver.get(page1)
        except TimeoutException:
            pass
    
        # storage path
        htmlStorePath = os.path.join(os.getcwd(), f"htmls/{categoryName}")
        if not os.path.exists(htmlStorePath):
            os.makedirs(htmlStorePath)
        dataStorePath = os.path.join(os.getcwd(), f"metadata/{categoryName}")
        if not os.path.exists(dataStorePath):
            os.makedirs(dataStorePath)
        
        # check number of pages:
        paginations = self.driver.find_element(By.CLASS_NAME, 'search__info')
        paginations = paginations.find_element(By.XPATH, "./*[1]").text
        totalDbCount = int(re.search(r'(\d+) product results', paginations).group(1))
        totalPageCount = math.ceil(totalDbCount / links_per_page)
        
        # setup progress bar
        outer_range = range(totalPageCount)
        outer_pbar = tqdm(total=len(outer_range), desc=f"{categoryName} has {totalPageCount} Pages")
        
        ## canot be used on nested elements
        def get_element_with_text(self, soup, type, text):
            return soup.find(lambda tag:tag.name==type and text in tag.text)
        
        # iterate every page
        try: # for page
            for pageNum in range(1, totalDbCount+1):
                curPageNum = pageNum
                try:
                    self.driver.get(self.get_search_link(pageNum, categoryName))
                except TimeoutException:
                    pass
                self.wait.until(
                    EC.presence_of_element_located((By.XPATH, "//*[starts-with(@alt, 'Logo for')]")),
                )
                dbs = {}
                # Find the all urls on the page
                elements = self.driver.find_elements(By.XPATH, "//*[starts-with(@alt, 'Logo for')]")
                for element in elements:
                    dbs[element.get_attribute("alt").replace("Logo for ", "")] = element.find_element(By.XPATH, "..").get_attribute("href")
                     
                # update progress bar
                outer_pbar.update(1)
                inner_pbar = tqdm(total=len(dbs), desc=f"On page {curPageNum} for {categoryName}", leave=False)
                
                # each database in the page
                for dbName, dbUrl in dbs.items():
                    try: 
                        # save the url    
                        safeDbName = dbName.replace(" ", "_").replace('/', '\\')  # adjust as needed
                        # check status
                        self.open_link_in_new_tab(dbUrl)
                        self.wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
                        self.wait.until(
                            EC.presence_of_all_elements_located((By.CLASS_NAME, 'header'))
                        )
                        self.wait.until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'data-dictionary-container'))
                        )
                        self.wait.until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'table--dataset'))
                        )
                        self.wait.until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'provider'))
                        )
                        self.wait.until(
                            EC.presence_of_element_located((By.CLASS_NAME, 'product-content__pricing'))
                        )
                        
                        tag = "success" if  self.driver.current_url == dbUrl else "fail"
                        status = 200 if tag == "success" else None
                
                        with open(os.path.join(htmlStorePath, f"{tag}_{safeDbName}.html"), "w", encoding="utf-8") as html_file:
                            if status == 200:
                                
                                body = self.driver.find_element(By.ID, "app")
                                html_file.write(body.get_attribute('outerHTML'))

                                # save the metadata
                                savedData = {
                                    "dataset_name": "",
                                    "short_description": "",
                                    "data_vendor_name": "",
                                    "data_vendor_email": "",
                                    "data_vendor_link":"",
                                    "price":"",
                                    "metadata":"",
                                    "sample_data":"",
                                    "data_exchange":"datarade" # platform
                                }
                                
                                # get dataset_name
                                savedData["dataset_name"] = dbName
                                # get short_description
                                sd = body.find_element(By.XPATH, "//h2[text()='Description']").find_element(By.XPATH, "..")
                                sd = md(str(sd.get_attribute('outerHTML')))
                                savedData["short_description"] = sd
                                # get data_vendor_name
                                dvn = body.find_element(By.CLASS_NAME, "provider")
                                savedData["data_vendor_name"] = dvn.text
                                # get data_vendor_link
                                savedData["data_vendor_link"] = dvn.get_attribute("href")
                                # get data_vendor_email NOT APPLICABLE
                                savedData["data_vendor_email"] = None
                                # get price
                                p = body.find_element(By.CLASS_NAME, "product-content__pricing")
                                savedData["price"] = md(str(p.get_attribute('outerHTML')))
                                # get meta data
                                try:
                                    _md = body.find_element(By.CLASS_NAME, "data-dictionary-container")
                                    savedData["metadata"] = md(str(_md.get_attribute('outerHTML')))
                                except:
                                    savedData["metadata"] = None
                                # get sample data
                                try:  # if data sample is blurred
                                    sd = body.find_element(By.XPATH, "//a[text()='Request Data Sample']")
                                except:
                                    sd = body.find_element(By.CLASS_NAME, "table--dataset")
                                    savedData["sample_data"] = md(str(sd.get_attribute("outerHTML")))
                                else:
                                    sd = body.find_element(By.CLASS_NAME, "table--dataset")
                                    soup = BeautifulSoup(sd.get_attribute("innerHTML"), 'html.parser')
                                    for tr in soup.find_all('tr'):
                                        if tr.find('td', class_='blur-text-6px'):
                                            tr.decompose()
                                    if len(soup.find_all('tr')) == 0:
                                        savedData["sample_data"] = None
                                    else:
                                        savedData["sample_data"] = md(str(soup))
                                    
                                            
                                # write meta data to file
                                with open(os.path.join(dataStorePath, f"{tag}_{safeDbName}.html"), "w", encoding="utf-8") as html_file:
                                    html_file.write(json.dumps(savedData))
                                        
                            else:
                                html_file.write("")
                                with open(os.path.join(dataStorePath, f"{tag}_{safeDbName}.html"), "w", encoding="utf-8") as html_file:
                                    html_file.write("")
                        self.close_current_tab()
                        inner_pbar.update(1)
                        
                    except Exception as e:
                        msg = f"On page {curPageNum}, error {dbUrl}:\n {str(e)}"
                        print(msg)
                        trace = traceback.print_exc()
                        print(trace)
                        
            inner_pbar.close()      
                
        except Exception as e:
                    msg = f"Error on reading page {curPageNum} of {categoryName}"
                    print(msg)
                    trace = traceback.print_exc()
                    print(trace)
                    
                    
        outer_pbar.close()
        return f"Sucess on category: {categoryName}"
        
        
        

    
    
    
crawler = Dataraide_Db_Crawler()
crawler.login()
categories = crawler.get_all_categories()
for category in categories:
    crawler.crawl_by_categories(category)
        