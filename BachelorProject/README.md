## HTML Twitter Scraping
Prerequisites:
- Selenium + Chrome Driver. Check this: https://gist.github.com/ziadoz/3e8ab7e944d02fe872c3454d17af31a5
- Specify the path to Chrome Driver in the script `twitter_selenium_scraper.py` by changing this line: 
```
path_to_chromedriver = '/Users/harkous/Downloads/chromedriver
```
- Install the required new python packages (see `requirements.txt` file for the list)

To run the twitter scraper, there are two steps:

- Collect the tweets from the twitter search: This is achieved by running: `twitter_selenium_scraper.py`.
  - You can specify the high level folder, e.g. 'data_hate'. You can set the list of URLs to fetch.
  - Under this folder, you have multiple folders (named like `'search_f=tweets_q=%23trump'`), each for the results of one search (i.e. one URL).
  - The folders have a csv file containing the list of tweet urls found via the search.
- Fetch the info for the collected twitter ids from the twitter api. This is achieved by running: `twitter_html_collector.py`
  - This script goes into the high level folder you specify and fetches the tweets' info for the tweet ids in those tweet urls.
  - It outputs a file called 'tweets.tsv' with the tweets info and places it inside the search folder for each url (e.g. `search_f=tweets_q=%23trump`)
    - This file is simply composed of one json entry per line. Each json is the info for one tweet. So you can parse it one line by one line and read the json into a python dict.
  - It outputs a file called 'all_operations_tweets.tsv' containing all the tweets' info from all the search operations.
