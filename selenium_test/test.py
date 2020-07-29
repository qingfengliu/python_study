from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import time
lujiang={"集团总览":[1,1]}
def jiaojian_log(browser):
    browser.get('')
    time.sleep(5)
    browser.maximize_window()
    browser.find_element_by_id('username').send_keys('')
    browser.find_element_by_id('password').send_keys('')
    browser.find_element_by_css_selector('.four').click()
    time.sleep(5)

def goto_son_page(browser,weizhi1,weizhi2):
    browser.find_element_by_xpath('//div[@class="menu"]//li[%s]' %weizhi1).click()    #集团总览,
    time.sleep(3)
    browser.find_element_by_xpath('//div[contains(@class,"second_menu_one")]//span[%s]' %weizhi2).click()
    time.sleep(5)



browser = webdriver.Chrome(executable_path=r'D:\gongju\chromedriver.exe')
jiaojian_log(browser)

item={}
for k in lujiang:
    item[k]={}
    goto_son_page(browser,lujiang[k][0],lujiang[k][1])
    browser.switch_to.frame("iframe1")
    zhi=browser.find_element_by_xpath('//div[@id="REPORT1"]//span[contains(text(), "完成额")]/following-sibling::'
                                  'div[1]/span[1]').text
    qiwang=browser.find_element_by_xpath('//div[@id="REPORT1"]//span[contains(text(), "年度计划")]/following-sibling::'
                                  'div[1]/span[1]').text
    tongbi=browser.find_element_by_xpath('//div[@id="REPORT1"]//span[contains(text(), "同比")]/following-sibling::'
                                  'div[1]/span[1]').text
    item['集团总览'].update({'新签合同额':{'完成额':zhi,'年度计划':qiwang,'同比':tongbi}})
    zhi=browser.find_element_by_xpath('//div[@id="REPORT2"]//span[contains(text(), "完成额")]/following-sibling::'
                                  'div[1]/span[1]').text
    qiwang=browser.find_element_by_xpath('//div[@id="REPORT2"]//span[contains(text(), "年度计划")]/following-sibling::'
                                  'div[1]/span[1]').text
    tongbi=browser.find_element_by_xpath('//div[@id="REPORT2"]//span[contains(text(), "同比")]/following-sibling::'
                                  'div[1]/span[1]').text
    item['集团总览'].update({'营业收入':{'完成额':zhi,'年度计划':qiwang,'同比':tongbi}})
    zhi=browser.find_element_by_xpath('//div[@id="REPORT3"]//span[contains(text(), "完成额")]/following-sibling::'
                                  'div[1]/span[1]').text
    qiwang=browser.find_element_by_xpath('//div[@id="REPORT3"]//span[contains(text(), "年度计划")]/following-sibling::'
                                  'div[1]/span[1]').text
    tongbi=browser.find_element_by_xpath('//div[@id="REPORT3"]//span[contains(text(), "同比")]/following-sibling::'
                                  'div[1]/span[1]').text
    item['集团总览'].update({'利润额':{'完成额':zhi,'年度计划':qiwang,'同比':tongbi}})
    print( browser.find_element_by_xpath("//div[@id='REPORT4']//td[matchs(@id,'A3-0')]").text)
    browser.close()
    print(item)
    exit()

time.sleep(5)
browser.switch_to.default_content()
browser.close()
