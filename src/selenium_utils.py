import time

# to ensure that page is loaded and we may read it
# http://www.obeythetestinggoat.com/how-to-get-selenium-to-wait-for-page-load-after-a-click.html
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import \
    staleness_of
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver as WebDriverClass
from selenium.webdriver.chrome.options import Options

from contextlib import contextmanager

from .vbulletin_utils import driver2soup

from credentials import username, password

import imgkit


def read_html_with_webdriver(url_to_read, wait_sec=7, headless=True,
                             disable_extensions=True, close_page=False):
    if headless or disable_extensions:
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        if disable_extensions:
            chrome_options.add_argument("--disable-extensions")
        # chrome_options.add_argument("--disable-gpu")
        driver = webdriver.Chrome(options=chrome_options)
    else:
        driver = webdriver.Chrome()

    driver.get(url_to_read)
    time.sleep(wait_sec)

    soup = driver2soup(driver)

    if close_page:
        driver.close()
        return soup

    return soup, driver


@contextmanager
def wait_for_page_load(driver, timeout=30):
    old_page = driver.find_element_by_tag_name('html')
    yield
    WebDriverWait(driver,
                  timeout,  # Number of seconds before timing out
                  poll_frequency=0.1).until(staleness_of(old_page))


def move_to_next_page(driver: WebDriverClass):
    # print('moving from page', driver.find_element_by_tag_name('html').id)
    try:
        with wait_for_page_load(driver):
            driver.find_element_by_xpath('//*[@rel="next"]').click()
        # print('moved to', driver.find_element_by_tag_name('html').id)
    except Exception as e:
        print('Unable to get next page')
        print(e)
        try:
            log_in(driver)
        except Exception as e:
            print('Unable to get next page')
            print(e)


def log_in(driver: WebDriverClass, wait_sec=7):
    # print('trying to log_in\n', driver2soup(driver))
    try:
        with wait_for_page_load(driver):
            driver.find_element_by_id('navbar_username').send_keys(username)
            driver.find_element_by_id('navbar_password_hint').send_keys(password)
            driver.find_element_by_class_name('loginbutton').click()
            time.sleep(wait_sec)
    except Exception as e:
        print('Unable to log in')
        raise
