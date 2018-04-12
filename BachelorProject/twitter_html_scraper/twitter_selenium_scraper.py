#!/usr/bin/env python
# -*- coding: utf-8 -*-

import subprocess
from selenium import webdriver
from bs4 import BeautifulSoup
import time
import datetime
from joblib import Parallel, delayed
from file_processing import mkdirs_if_not_exist
#from ProjetDeSemestre.file_processing import mkdirs_if_not_exist
import os
import glob
import multiprocessing
from datetime import date

cpu_count = multiprocessing.cpu_count()


chromeOptions = webdriver.ChromeOptions()
prefs = {"profile.managed_default_content_settings.images": 2}
chromeOptions.add_experimental_option("prefs", prefs)

THIS_DIR = os.path.dirname(__file__)
path_to_chromedriver = '/home/dap/Downloads/ChromeDriver/chromedriver'

print('loaded the chome driver')


def get_all_years(min_year, max_year, till_now = True):
    """Get extensions for queries that allow quarterly queries

    Args:
        min_year:
        max_year:

    Returns:

    """
    today = date.today()
    year = str(today.year)
    month = str(today.month)
    day = str(today.day)
    if(max_year > int(year) or till_now):
        max_year == int(year)
    all_ext = []
    for y in range(min_year, max_year):
        #for q in [1, 4, 7]:
        for q in [1, 3, 5, 7, 9]:
            all_ext.append(
                '%20since%3A' + str(y) + '-0' + str(q) + '-02%20until%3A' + str(y) + '-0' + str(q + 2) + '-01')
        all_ext.append(
            '%20since%3A' + str(y) + '-' + str(11) + '-02%20until%3A' + str(y + 1) + '-0' + str(1) + '-01')
    if till_now:
        if int(year) == max_year:
            for inc in range(1, int(month)-1, 2):
                all_ext.append(
                    '%20since%3A' + year + '-0' + str(inc) + '-02%20until%3A' + year + '-0' +
                    str(inc+2) + '-01')
            if int(month) % 2 == 0:
                all_ext.append(
                    '%20since%3A' + year + '-0' + str(int(month)-1) + '-02%20until%3A' + year + '-0' +
                    month + '-0' + day)
            else:
                all_ext.append(
                    '%20since%3A' + year + '-0' + month + '-02%20until%3A' + year + '-0' +
                    month + '-0' + day)
    return all_ext


def clean_url(string):
    return string.replace('%20', '_').replace('%3A',
                                              '_').replace('&', '_')


# function to handle dynamic page content loading - using Selenium
def scrape_tweets(url, min_year, max_year, high_level_dir='data', attempts=20,max_tweets= 100):
    """Scrape tweets in parallel for the given url by producing multiple urls for each time period

    Args:
        url:
        min_year:
        max_year:
        high_level_dir:
        attempts:

    Returns:

    """
    url = url.replace('&src=typd', '')
    all_years_urls = []
    for ext in get_all_years(min_year, max_year):
        all_years_urls.append(ext)

    page = url.rsplit('/', 2)[-1]
    page = 'search_' + clean_url(page[page.index('search?') + len('search?'):])
    dir_name = page
    base_path = os.path.join(high_level_dir, dir_name)

    if os.path.isdir(base_path):

        print ('folder ' + base_path + ' already exists, so collection is already done for this url')
    else:
        mkdirs_if_not_exist(base_path)

        Parallel(n_jobs=cpu_count)(
            delayed(scrape_url)(url, ext, page, base_path, attempts, max_tweets) for ext in all_years_urls)

    combine_csvs(base_path, dir_name + '.csv')


def scrape_url(url, ext, page, base_path, attempts,max_tweets):
    """Scrape one url

    Args:
        url:
        high_level_dir:
        attempts:

    Returns:

    """
    url = url + ext
    print('processing the url ', url)
    all_links = []
    written_links = []
    prev_html = ''

    now = datetime.datetime.now()
    curr_time = str(now.hour) + '_' + str(now.minute) + '_' + str(now.day) + '_' + str(now.month)
    filename = "{page}_{ext}__{curr_time}.csv".format(page=page, ext=clean_url(ext), curr_time=curr_time)
    outfile = open(os.path.join(base_path, filename), "w")

    browser = webdriver.Chrome(chrome_options=chromeOptions, executable_path=path_to_chromedriver)
    browser.get(url)

    # define initial page height for 'while' loop
    lastHeight = browser.execute_script("return document.body.scrollHeight")

    tweet_count = 0
    reached_max=False
    while True:
        print('scrolling')
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # self.process_html(browser.page_source, outfile)
        ##############################
        ##############################
        #  call dynamic page scroll function here
        page_source = browser.page_source
        newhtml = page_source[len(prev_html):]
        soup = BeautifulSoup(newhtml, "html.parser")
        prev_html = page_source
        for i in soup.find_all('li', {"data-item-type": "tweet"}):
            link = ('https://twitter.com' + i.small.a['href'] if i.small is not None else "")
            all_links.append(link)

        newlinks = set(all_links) - set(written_links)
        print('newlinks: ', newlinks)
        written_links = all_links[:]
        for i in newlinks:
            if tweet_count >= max_tweets:
                reached_max=True
                break
            outfile.write(i + '\n')
            tweet_count += 1
        outfile.flush()
        if reached_max:
            break
        ##############################
        ##############################

        newHeight = browser.execute_script("return document.body.scrollHeight")
        attempt = 0
        while newHeight == lastHeight and attempt < attempts:
            print('sleeping')

            time.sleep(0.5)

            newHeight = browser.execute_script("return document.body.scrollHeight")
            attempt += 1
            print('attempt:', attempt)

        if newHeight == lastHeight:
            break
        else:
            lastHeight = newHeight

    outfile.close()
    browser.quit()


def combine_csvs(base_dir, out_filename):
    """Combine the intermediate data into one csv and remove the intermediate files

    Args:
        base_dir:
        out_filename:

    Returns:

    """
    # command = 'cat ' + base_dir + '/*.csv > ' + base_dir + '/' + out_filename
    # subprocess.Popen(command, shell=True, stdout=subprocess.PIPE).stdout.read()
    #
    # print(command)
    #
    # for filename in glob.glob(base_dir + "/*_since_*.csv"):
    #     os.remove(filename)
    #
    out_filepath=os.path.join(base_dir, out_filename)
    try:
        os.remove(out_filepath)
    except:
        pass
    with open(out_filepath, 'w') as outfile:
        for filename in glob.glob(base_dir + "/*_since_*.csv"):
            print(filename)
            with open(filename) as infile:
                for line in infile:
                    outfile.write(line)


    for filename in glob.glob(base_dir + "/*_since_*.csv"):
        os.remove(filename)

def build_url_for_hashtag(hashtag):
    return 'https://twitter.com/search?l=en&f=tweets&q=%23{}&src=typd'.format(hashtag)

def scrape_tweets_for_hashtags(hashtags, folder = 'data_hate'):
    folder = os.path.join(THIS_DIR, folder)
    retry = True
    i = 0
    for hashtag in hashtags:
        while(retry):
            try:
                scrape_tweets(build_url_for_hashtag(hashtag), 2015, 2017, folder, attempts=20, max_tweets=120)
                retry = False
            except Exception as ex:
                excType = ex.__class__.__name__
                print('exception type', excType)
                print('exception msg', str(ex))
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                if(i < 4):
                    i += 1
                else:
                    retry = False
        retry = True

if __name__ == "__main__":
    #path_to_chromedriver = '/home/dap/Downloads/ChromeDriver/chromedriver'  # TODO: change path as needed

    urls = [
        # 'https://twitter.com/search?f=tweets&q=%23trump lang%3Aen &src=typd'
        #'https://twitter.com/search?l=en&f=tweets&q=%23maga&src=typd'
        #'https://twitter.com/search?f=tweets&q=%23maga&src=typd',
        #'https://twitter.com/search?f=tweets&q=%23resist&src=typd',
        #'https://twitter.com/search?f=tweets&q=%23trump&src=typd',

        #'https://twitter.com/search?f=tweets&q=%23hillary&src=typd'
         'https://twitter.com/search?f=tweets&q=filter%3Areplies%20%20our%20privacy%20policy',
         'https://twitter.com/search?l=en&f=tweets&q=%23trump',
         #'https://twitter.com/search?l=en&f=tweets&q=%23trump%20since%3A2015-09-02%20until%3A2015-011-01',
        # 'https://twitter.com/search?f=tweets&q=filter%3Areplies%20%20our%20privacy%20statement&src=typd',
        # 'https://twitter.com/search?f=tweets&q=filter%3Areplies%20we%20privacy%20policy%20-our&src=typd',
        # 'https://twitter.com/search?f=tweets&q=our%20privacy%20statement%20-policy%20filter%3Areplies&src=typd',
        # 'https://twitter.com/search?f=tweets&q=our%20privacy%20practices%20-policy%20filter%3Areplies&src=typd'
    ]
    for url in urls:
        scrape_tweets(url, 2016, 2017, 'data_hate', attempts=20,max_tweets=375)