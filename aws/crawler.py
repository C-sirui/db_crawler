import time
import os
import random
import json
import re
import math
import requests
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


### Macros
max_wait_time = 20
max_workers = 8

def Initialize_Driver():
    chromedriver_path = os.path.join(os.path.dirname(__file__), "../chrome_driver/chromedriver")
    # print(chromedriver_path)
    service = webdriver.chrome.service.Service(chromedriver_path)
    options = webdriver.ChromeOptions()
    # # Loading profile
    # options.add_argument('user-data-dir=/Users/ryanchen/Library/Application Support/Google/Chrome')
    # options.add_argument('profile-directory=Profile 3')
    options.add_argument('--disable-extensions')
    # Adding argument to disable the AutomationControlled flag
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument('disable-infobars')
    options.add_argument('--disable-gpu')
    # options.page_load_strategy = 'eager'

    # Exclude the collection of enable-automation switches
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    # Turn-off userAutomationExtension
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_window_size(1400, 750)
    driver.set_page_load_timeout(20)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def Get_Categories():
    category2link = {}
    for key, value in categoryIdMap.items():
        category2link[key] = baseUrl + categoryBrowseLinkMap[value]
    return category2link

### single crawler
class Aws_Db_Crawler:
    def __init__(self):
        self.driver = Initialize_Driver()
        self.categories = Get_Categories()
        self.failed_count = 0
        self.total_count = 0
        self.fullfills = ff
    
    def get_fullfillment_option_filter(self):
        fullfillmentIdMap = {}
        self.driver.implicitly_wait(100)  # Set implicit wait to 10 seconds
        self.driver.set_page_load_timeout(300)  # Set page load timeout to 30 seconds
        self.driver.get("https://aws.amazon.com/marketplace/search")
        optionBox = self.driver.find_element(By.CLASS_NAME, "FULFILLMENT_OPTION_TYPEOptions")
        options = optionBox.find_elements(By.CLASS_NAME, "awsui_wrapper_1wepg_12w0t_110")  # This may change
        
        for option in options:
            metaData = option.get_attribute("data-metric-meta-data")
            metaData = json.loads(metaData)
            fullfillmentIdMap[metaData["SubComponent"]] = metaData["ComponentId"]
        
        self.fullfills = fullfillmentIdMap
    
    def crawl_url_from_category(self, categoryName):
        self.driver.implicitly_wait(100)  # Set implicit wait to 10 seconds
        self.driver.set_page_load_timeout(300)  # Set page load timeout to 30 seconds
        
        # get total page num
        self.driver.get(self.categories[categoryName]+"&pageSize=50")
        numBox = self.driver.find_element(By.CSS_SELECTOR, '[data-test-selector="availableProductsCountMessage"]')
        totalUrlCount = int(re.search(r'\d+', numBox.text).group())
        totalFailCount = 0
        totalPageCount = math.ceil(totalUrlCount / 50)
        
        curUrl = ""
        curPageNum = 1
        try:
            # create folder for data
            htmlStorePath = os.path.join(os.getcwd(), f"./htmls/{categoryName}")
            if not os.path.exists(htmlStorePath):
                os.makedirs(htmlStorePath)
                
            # setup progress bar
            outer_range = range(totalPageCount)
            outer_pbar = tqdm(total=len(outer_range), desc=f"{totalPageCount} Pages")
            
            # iteratively crawl on each page
            for pageNum in range(1,totalPageCount+1):
                curPageNum = pageNum
                
                # get all urls
                urls = {}   
                WebDriverWait(self.driver, max_wait_time).until(  # wait for desired element to load
                    EC.presence_of_element_located((By.CLASS_NAME, "awsui_has-header_wih1l_1l1xk_168"))
                )
                body_element = self.driver.find_element("tag name", "body")
                dbBox = body_element.find_element(By.CLASS_NAME, "awsui_has-header_wih1l_1l1xk_168")
                dbUrls = dbBox.find_elements(By.CSS_SELECTOR, '[data-metric-name="srchRsltCl"]')
                dbUrls = [dbUrls[i] for i in range(len(dbUrls)) if i % 2 != 0] # remove duplicated ones
                for dbUrl in dbUrls:
                    href = dbUrl.get_attribute("href")
                    urls[dbUrl.text] = href
                    
                # update progress bar
                outer_pbar.update(1)
                inner_pbar = tqdm(total=len(urls), desc=f"{len(urls)} databases")
                
                # save htmls from url
                for urlName, urlAddr in urls.items():
                    response = requests.get(urlAddr)
                    # Check if the request was successful (status code 200)
                    urlName = urlName.replace(" ", "_")
                    with open(os.path.join(htmlStorePath, f"./success_{urlName}.html"), "a+", encoding="utf-8") as html_file:
                        if response.status_code == 200:
                            html_file.write(response.text)
                        else:
                            totalFailCount += 1
                            html_file.write("")
                    inner_pbar.update(1)
                inner_pbar.close()
                        
                # move to next page
                if curPageNum != totalPageCount:
                    nextPageButton = self.driver.find_element(By.CSS_SELECTOR, '[aria-label="Next page"]')
                    nextPageButton.click()
                    
            outer_pbar.close()
                
        except Exception as e:
            print(f"On page {curPageNum}, ab error occurred on {curUrl}:\n {str(e)}")


### multi thraeding
categories = list(Get_Categories().keys())
totalTask = len(categories)
with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
    futures = []

    for task_id in range(max_workers):
        crawler = Aws_Db_Crawler()
        future = executor.submit(crawler.crawl_url_from_category, categories[task_id])
        futures.append(future)
    
    # Continue submitting tasks as threads become available
    for task_id in range(max_workers, total_tasks):
        completed_future = concurrent.futures.wait(
            futures, return_when=concurrent.futures.FIRST_COMPLETED
        ).done.pop()
        
        # Process the completed task and remove it from the list
        print(completed_future.result())
        futures.remove(completed_future)
        
        # Submit the next task
        future = executor.submit(worker_function, task_id)
        futures.append(future)
    
    # Wait for the remaining tasks to complete
    concurrent.futures.wait(futures)



    
    