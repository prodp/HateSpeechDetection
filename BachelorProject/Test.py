from ProjetDeSemestre import Snowball_Search
from ProjetDeSemestre import Utilities
from ProjetDeSemestre.HateBase import HateBaseAPI
from ProjetDeSemestre.file_processing import mkdirs_if_not_exist
from ProjetDeSemestre.twitter_html_scraper.twitter_html_collector import TwitterCollector
from ProjetDeSemestre.twitter_html_scraper import twitter_selenium_scraper
import multiprocessing
from joblib import Parallel, delayed
import os
import math

cpu_count = multiprocessing.cpu_count()
THIS_DIR = os.path.dirname(__file__)

def check_hashtags(hashtag_list, hate_words):
    for hashtag_composed in hashtag_list:
        list_hashtag = Utilities.parseText(hashtag_composed)
        for str in hate_words:
            list_mot = Utilities.parseText(str)
            for hashtag in list_hashtag:
                delete_hashtag = True
                if hashtag not in list_mot:
                    delete_hashtag = False
                    break
            if delete_hashtag:
                hashtag_list.remove(hashtag_composed)
                break

def calculate_occurence(texts_hashtags, hate_words):
    hashtags_hate_occurences = {}
    hashtags_total_occurences = {}
    print(len(texts_hashtags))
    for text, hashtags in texts_hashtags:
        hashtag_list = list(map((lambda x: x.lower()), hashtags[:]))
        #check_hashtags(hashtag_list, hate_words)
        for hashtag in hashtag_list:
            total_occurences = hashtags_total_occurences.get(hashtag)
            if total_occurences is None:
                hashtags_total_occurences[hashtag] = 1
            else:
                hashtags_total_occurences[hashtag] = total_occurences + 1
        list_text = Utilities.parseText(text)
        for str in hate_words:
            found_word = True
            list_mot = Utilities.parseText(str)
            for mot in list_mot:
                if mot not in list_text:
                    found_word = False
                    break
            if found_word:
                for hashtag in hashtag_list:
                    occurence = hashtags_hate_occurences.get(hashtag)
                    if occurence is None:
                        hashtags_hate_occurences[hashtag] = 1
                    else:
                        hashtags_hate_occurences[hashtag] = occurence + 1
                break
    return hashtags_total_occurences, hashtags_hate_occurences

def combine_results(results):
    hashtags_hate_occurences = {}
    hashtags_total_occurences = {}
    for result in results:
        local_total_occurence = result[0]
        local_hate_occurence = result[1]
        for hashtag in local_total_occurence:
            occurence = hashtags_total_occurences.get(hashtag)
            if occurence is None:
                hashtags_total_occurences[hashtag] = local_total_occurence[hashtag]
            else:
                hashtags_total_occurences[hashtag] = occurence + local_total_occurence[hashtag]
        for hashtag in local_hate_occurence:
            occurence = hashtags_hate_occurences.get(hashtag)
            if occurence is None:
                hashtags_hate_occurences[hashtag] = local_hate_occurence[hashtag]
            else:
                hashtags_hate_occurences[hashtag] = occurence + local_hate_occurence[hashtag]
    return hashtags_total_occurences, hashtags_hate_occurences

def calculate_occurences(texts_hashtags, hate_words):
    size = math.ceil(len(texts_hashtags) / cpu_count)
    chunks = Utilities.chunk_array(texts_hashtags[:], size)
    result = Parallel(n_jobs=cpu_count)(
        delayed(calculate_occurence)(chunk, hate_words) for chunk in chunks)
    print(len(result))
    print(result)
    hashtags_total_occurences, hashtags_hate_occurences = combine_results(result)
    print(len(hashtags_total_occurences))
    print(len(hashtags_hate_occurences))

if __name__ == "__main__":
    initial_words_religion = ['muslimban', 'resist', 'maga']

    filters_for_hatebase = {'about_religion': '1', 'language': 'eng'}
    path_hatebase = os.path.join(os.path.join(THIS_DIR, 'Data_Hate'), 'hate_words.csv')

    hate_words = Snowball_Search.init_hatebase(filters_for_hatebase, path_hatebase, False)

    twitter_selenium_scraper.scrape_tweets_for_hashtags(initial_words_religion, folder='data_hate')

    texts_hashtags = TwitterCollector.request_hashtags(initial_words_religion, high_level_folder='data_hate')

    calculate_occurences(texts_hashtags, hate_words)