from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

chrome_option = Options()
chrome_option.add_argument("--incognito")

driver = webdriver.Chrome(options = chrome_option)
driver.get("https://poll.fm/13306280") #Replace the URL 
k = driver.find_element(By.ID, 'PDI_answer59586106')
v = driver.find_element(By.ID, 'pd-vote-button13306280')
try :
    for i in range(100):
        print("Voted!")
        k.click()
        v.click()
except :
    for i in range(100):
        print("Voted!")
        k.click()
        v.click()
