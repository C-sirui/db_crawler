import time
import os
import random
import traceback
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

### Macros
max_wait_time = 20
max_workers = 8
link_per_page = 50

def Initialize_Driver():
    chromedriver_path = os.path.join(os.path.dirname(__file__), "../chrome_driver/chromedriver")
    # print(chromedriver_path)
    service = webdriver.chrome.service.Service(chromedriver_path)
    chrome_options = webdriver.ChromeOptions()
   
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_experimental_option("excludeSwitches", ['enable-automation', 'disable-component-update','ignore-certificate-errors'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--headless")
  
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1400, 750)
    driver.set_page_load_timeout(20)
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
        self.wait = WebDriverWait(self.driver, 30)
        # self.fullfills = ff
    
    
    # ### This is dynamically fetching filters at run time
    # ### just writing for a taste of the wired way
    # def get_fullfillment_option_filter(self):
    #     fullfillmentIdMap = {}
    #     self.driver.implicitly_wait(100)  # Set implicit wait to 10 seconds
    #     self.driver.set_page_load_timeout(300)  # Set page load timeout to 30 seconds
    #     self.driver.get("https://aws.amazon.com/marketplace/search")
    #     optionBox = self.driver.find_element(By.CLASS_NAME, "FULFILLMENT_OPTION_TYPEOptions")
    #     options = optionBox.find_elements(By.CLASS_NAME, "awsui_wrapper_1wepg_12w0t_110")  # This may change
        
    #     for option in options:
    #         metaData = option.get_attribute("data-metric-meta-data")
    #         metaData = json.loads(metaData)
    #         fullfillmentIdMap[metaData["SubComponent"]] = metaData["ComponentId"]
        
    #     self.fullfills = fullfillmentIdMap
    
    def open_link_in_new_tab(self, url):
        # Open the link in a new tab
        self.driver.execute_script(f"window.open('{url}', '_blank');")

        # Switch to the new tab
        self.driver.switch_to.window(self.driver.window_handles[-1])
        time.sleep(2)

    def close_current_tab(self):
        # Close the current tab
        self.driver.close()

        # Switch back to the first tab
        self.driver.switch_to.window(self.driver.window_handles[0])
        time.sleep(2)
        
    ## canot be used on nested elements
    def get_element_with_text(self, soup, type, text):
        return soup.find(lambda tag:tag.name==type and text in tag.text)
    
    def crawl_url_from_category(self, categoryName):
        self.driver.implicitly_wait(100)  # Set implicit wait to 10 seconds
        self.driver.set_page_load_timeout(300)  # Set page load timeout to 30 seconds
        
        # get total page num
        self.driver.get(self.categories[categoryName]+f"&pageSize={link_per_page}")
        numBox = self.driver.find_element(By.CSS_SELECTOR, '[data-test-selector="availableProductsCountMessage"]')
        totalUrlCount = int(re.search(r'\d+', numBox.text).group())
        totalFailCount = 0
        totalPageCount = math.ceil(totalUrlCount / link_per_page)
        
        curPageNum = 1
        # create folder for html file
        htmlStorePath = os.path.join(os.getcwd(), f"htmls/{categoryName}")
        if not os.path.exists(htmlStorePath):
            os.makedirs(htmlStorePath)
        
        # create folder for metadata
        dataStorePath = os.path.join(os.getcwd(), f"metadata/{categoryName}")
        if not os.path.exists(dataStorePath):
            os.makedirs(dataStorePath)
            
        # setup progress bar
        outer_range = range(totalPageCount)
        outer_pbar = tqdm(total=len(outer_range), desc=f"{categoryName} has {totalPageCount} Pages")
        
        # iteratively crawl on each page
        for pageNum in range(1,totalPageCount+1):
        # for pageNum in range(1,2):
            curPageNum = pageNum
            
            # get all urls
            urls = {}   
            WebDriverWait(self.driver, max_wait_time).until(  # wait for desired element to load
                EC.presence_of_element_located((By.CLASS_NAME, "awsui_has-header_wih1l_1l1xk_168"))
            )
            body_element = self.driver.find_element(By.TAG_NAME, "body")
            dbBox = body_element.find_element(By.CLASS_NAME, "awsui_has-header_wih1l_1l1xk_168")
            dbUrls = dbBox.find_elements(By.CSS_SELECTOR, '[data-metric-name="srchRsltCl"]')
            dbUrls = [dbUrls[i] for i in range(len(dbUrls)) if i % 2 != 0] # remove duplicated ones
            for dbUrl in dbUrls:
                href = dbUrl.get_attribute("href")
                urls[dbUrl.text] = href
                
            # update progress bar
            outer_pbar.update(1)
            inner_pbar = tqdm(total=len(urls), desc=f"On page {curPageNum} for {categoryName}", leave=False)
            
            # save htmls from url
            for urlName, urlAddr in urls.items():
                try:
                    # urlAddr = 'https://aws.amazon.com/marketplace/pp/prodview-za7cjvzrcrnk2?sr=0-10&ref_=beagle&applicationId=AWSMPContessa#overview'
                    response = requests.get(urlAddr)
                    
                    # save html
                    safeUrlName = urlName.replace(" ", "_").replace('/', '\\')  # adjust as needed
                    
                    # check fully loaded
                    self.open_link_in_new_tab(urlAddr)
                    self.wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
                    tag = "success" if  self.driver.current_url == urlAddr else "fail"
                    status = 200 if tag == "success" else None
                    
                    # write to html file
                    renderedHtml = ""
                    with open(os.path.join(htmlStorePath, f"{tag}_{safeUrlName}.html"), "w", encoding="utf-8") as html_file:
                        if status == 200:
                            self.wait.until(
                                EC.presence_of_all_elements_located((By.CLASS_NAME, 'content'))
                            )
                            self.wait.until(
                                EC.presence_of_all_elements_located((By.CLASS_NAME, 'awsui_content-wrapper_14iqq_1yco7_189')),
                            )
                            self.wait.until(
                                EC.presence_of_all_elements_located((By.CLASS_NAME, 'header-content__description'))
                            )
                     
                            temp = self.driver.find_element(By.CLASS_NAME, "content")
                            renderedHtml = temp.get_attribute('outerHTML')
                            html_file.write(renderedHtml)
                            self.close_current_tab()
                            
                            # save meta data
                            renderedSoup = BeautifulSoup(renderedHtml, 'html.parser')
                            savedData = {
                                "dataset_name": "",
                                "short_description": "",
                                "data_vendor_name": "",
                                "data_vendor_email": "",
                                "data_vendor_link":"",
                                "price":"",
                                "metadata":None,
                                "sample_data":None,
                                "data_exchange":"aws" # platform
                            }
                            # page are arranged in two ways depending on contract type: 
                            # standard data subscription agreement or open data license
                            contractType = "DSA" if "Usage information" in renderedHtml else "ODL"

                            # get dataset_name
                            savedData["dataset_name"] = urlName
                            
                            # get short_description
                            vendorName = None
                            if contractType == "DSA":
                                htmlOverview = self.get_element_with_text(renderedSoup, "h2", "Overview")
                                htmlOverview = htmlOverview.find_parent('div', class_='awsui_content-wrapper_14iqq_1yco7_189')
                            elif contractType == "ODL":
                                htmlOverview = self.get_element_with_text(renderedSoup, "h2", "Description")
                                htmlOverview = htmlOverview.find_parent('div', class_='awsui_content-wrapper_14iqq_1yco7_189')
                            else:
                                print(f"unknwn conrtact type for dataset_name on {urlAddr}")
                            savedData["short_description"] = md(str(htmlOverview))
                            
                            # get data_vendor_name
                            elements = renderedSoup.find_all(class_='header-content__description')
                            vendorName = elements[1].find('h5')
                            savedData["data_vendor_name"] = vendorName.text
                            
                            # get data_vendor_email
                            vendorEmail = None
                            if contractType == "DSA":
                                vendorEmail = renderedSoup.find(attrs={"data-testid": 'support-information'})
                                vendorEmail = vendorEmail.find(attrs={"data-testid": "awsui-value"})
                                vendorEmail = vendorEmail.find() if vendorEmail is not None else None
                                vendorEmail = vendorEmail["href"] if vendorEmail is not None else None
                            elif contractType == "ODL":       
                                vendorEmail = self.get_element_with_text(renderedSoup, "p", "Managed by:")  
                                # For the first child element
                                vendorEmail = vendorEmail.find('a') if vendorEmail is not None else None
                                vendorEmail = vendorEmail["href"] if vendorEmail is not None else None
                                vendorEmail = "".join(vendorEmail.split(" ")[1:]) if vendorEmail is not None else None
                            else:
                                print(f"unknwn conrtact type for dataset_name on {urlAddr}")
                            if isinstance(vendorEmail, str):
                                if "mailto:" in vendorEmail: vendorEmail.replace("mailto:", "")
                            savedData["data_vendor_email"] = vendorEmail
                            
                            # get data_vendor_link
                            # first get aws summary link
                            temp = vendorName.find('a')
                            temp = temp["href"] if temp is not None else None  
                            vendorLink = None
                            if contractType == "DSA":            
                                vendorLink = renderedSoup.find(attrs={"data-testid": 'support-information'})
                                vendorLink = vendorLink.find_all(attrs={"data-testid": "awsui-value"})[1]
                                vendorLink = vendorLink.find() if vendorLink is not None else None
                                vendorLink = vendorLink["href"] if vendorLink is not None else None
                            elif contractType == "ODL":
                                elements = renderedSoup.find_all(class_='custom-markdown-viewer')
                                vendorLink = elements[6]
                                vendorLink = vendorLink.find('a') if vendorLink is not None else None
                                vendorLink = vendorLink["href"] if vendorLink is not None else None
                            else:
                                print(f"unknwn conrtact type for dataset_name on {urlAddr}")
                            savedData["data_vendor_link"] = f'Summary Link: https://aws.amazon.com{temp}, Original Link: {vendorLink}'

                            # get price
                            price = None
                            if contractType == "DSA":            
                                price = self.get_element_with_text(renderedSoup, "p", "The following offers are ")
                                price = price.find_parent('div', class_='awsui_content-wrapper_14iqq_1yco7_189')
                                price = md(str(price))
                                
                                refund = renderedSoup.find(attrs={"data-testid": 'support-information'})
                                refund = refund.find_all(attrs={"data-testid": "awsui-value"})[2].text
                                price = price + refund
                            elif contractType == "ODL":
                                price="free(Open Data License)"
                            else:
                                print(f"unknwn conrtact type for dataset_name on {urlAddr}")
                            savedData["price"] = price
                            
                            # get metadata
                            ### to do ###
                            
                            # get sample_data
                            ### to do
                            
                            with open(os.path.join(dataStorePath, f"success_{safeUrlName}.html"), "w", encoding="utf-8") as html_file:
                                html_file.write(json.dumps(savedData))
                                    
                        else:
                            self.close_current_tab()
                            totalFailCount += 1
                            html_file.write("")
                            with open(os.path.join(dataStorePath, f"{tag}_{safeUrlName}.html"), "w", encoding="utf-8") as html_file:
                                html_file.write("")

                    inner_pbar.update(1)
                    
                except Exception as e:
                    msg = f"On page {curPageNum}, ab error occurred on {urlAddr}:\n {str(e)}"
                    print(msg)
                    trace = traceback.print_exc()
                    print(trace)
                    
                    # change succes to fail
                    htmlPath = os.path.join(htmlStorePath, f"{tag}_{safeUrlName}.html")
                    metadataPath = os.path.join(dataStorePath, f"{tag}_{safeUrlName}.html")
                    if os.path.exists(htmlPath):
                        os.remove(htmlPath)
                    if os.path.exists(metadataPath):
                        os.remove(metadataPath)
                    with open(os.path.join(dataStorePath, f"fail_{safeUrlName}.html"), "w", encoding="utf-8") as html_file:
                        html_file.write(f"{msg}\n{trace}")
                    with open(os.path.join(htmlStorePath, f"fail_{safeUrlName}.html"), "w", encoding="utf-8") as html_file:
                        html_file.write(f"{msg}\n{trace}")    
            inner_pbar.close()
                    
            # move to next page
            if curPageNum != totalPageCount:
                nextPageButton = self.driver.find_element(By.CSS_SELECTOR, '[aria-label="Next page"]')
                nextPageButton.click()
                
        outer_pbar.close()
            
        return f"Sucess on category: {categoryName}"


# ### multi thraeding
# categories = list(Get_Categories().keys())
# categories = ["financial_services_data", "healthcare_and_life_sciences_data", "media_and_entertainment_data", 
#               "telecommunications_data", "gaming_data", "automotive_data", "manufacturing_data", "resources_data", 
#               "retail_location_and_marketing_data", "public_sector_data", "environmental_data"]
# totalTask = len(categories)
# with concurrent.futures.ThreadPoolExecutor(max_workers) as executor:
#     futures = []

#     for task_id in range(max_workers):
#         crawler = Aws_Db_Crawler()
#         future = executor.submit(crawler.crawl_url_from_category, categories[task_id])
#         futures.append(future)
    
#     # Continue submitting tasks as threads become available
#     for task_id in range(max_workers, totalTask):
#         completed_future = concurrent.futures.wait(
#             futures, return_when=concurrent.futures.FIRST_COMPLETED
#         ).done.pop()
        
#         # Process the completed task and remove it from the list
#         print(completed_future.result())
#         futures.remove(completed_future)
        
#         # Submit the next task
#         crawler = Aws_Db_Crawler()
#         future = executor.submit(crawler.crawl_url_from_category, categories[task_id])
#         futures.append(future)
    
#     # Wait for the remaining tasks to complete
#     concurrent.futures.wait(futures)

### single
### done categories: "gaming_data", "healthcare_and_life_sciences_data", "media_and_entertainment_data", 
              #      "telecommunications_data", "financial_services_data", "automotive_data", "manufacturing_data", "resources_data", "retail_location_and_marketing_data",
categories = [ "public_sector_data", "environmental_data"]


crawler = Aws_Db_Crawler()
for category in categories:
    crawler.crawl_url_from_category(category)



    
    