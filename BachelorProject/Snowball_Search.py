import Utilities
from HateBase import HateBaseAPI
from file_processing import mkdirs_if_not_exist
from twitter_html_scraper.twitter_html_collector import TwitterCollector
from twitter_html_scraper import twitter_selenium_scraper
from random import randint
import multiprocessing
from joblib import Parallel, delayed
import os
import math
import sys

# from ProjetDeSemestre import Utilities
# from ProjetDeSemestre.HateBase import HateBaseAPI
# from ProjetDeSemestre.file_processing import mkdirs_if_not_exist
# from ProjetDeSemestre.twitter_html_scraper.twitter_html_collector import TwitterCollector
# from ProjetDeSemestre.twitter_html_scraper import twitter_selenium_scraper

cpu_count = multiprocessing.cpu_count()

THIS_DIR = os.path.dirname(__file__)
snowball_folder = os.path.join(THIS_DIR, 'Data_Religion_EN')

#religion
#REFAIRE QUERY !
initial_words_religion = ['trump', 'londonBridge', 'maga']
new_words = ['isis', 'londonAttack', 'BuildTheWall', 'resist', 'muslimban']
#initial_words_religion = ['muslimban']
filters_for_hatebase = { 'about_religion': '1', 'language' : 'eng' }

#hatewords occurences
global_hatewords_occurences = {}
global_max_hateword = -sys.maxsize
global_min_hateword = sys.maxsize

#'/home/dap/ProjetDeSemestre/Data/hate_words1.csv'
path_hatebase = os.path.join(os.path.join(THIS_DIR, 'Data_Hate'), 'hate_words.csv')

def init_hatebase(filters_for_hateBase=None, path_hatebase=None, hatebase_from_URL=True):
    """
    :param filters_for_hateBase:
    :param path_hatebase:
    :param hatebase_from_URL:
    :return:
    """
    hate_words = []
    if hatebase_from_URL:
        if not filters_for_hatebase == None:
            hate_words = HateBaseAPI.get_vocabulary(filters_for_hateBase, query_type='vocabulary', output='json')
            Utilities.write_to_csv(path_hatebase, hate_words)
    else:
        if not path_hatebase == None:
            hate_words = Utilities.read_from_csv(path_hatebase)
    return hate_words

# def create_hatewords_for_check(hate_words):
#     new_hate_words = []
#     for hate_word in hate_words:
#         new_hate_words.extend(Utilities.parseText(hate_word))
#     return new_hate_words

def create_hatewords_normalized(hate_words):
    new_list = []
    for word in hate_words:
        new_list.append(Utilities.normalizeText(word))
    return new_list

def check_hashtags(hashtag_list, hate_words):
    """
        :param hashtag_list:
        :param hate_words:
        :return: checks if at least one of the hashtags is a hateword
        """
    for hashtag in hashtag_list[:]:
        norm_hashtag = Utilities.normalizeText(hashtag)
        for word in hate_words:
            if word in norm_hashtag:
                hashtag_list.remove(hashtag)
                break


def calculate_occurrences_hasthags(texts_hashtags, hate_words):
    hashtags_hate_occurrences = {}
    hashtags_total_occurrences = {}
    for text, hashtags in texts_hashtags:
        weight = 1
        hashtags = hashtags[:]
        check_hashtags(hashtags, hate_words)
        hashtags = list(map((lambda x: x.lower()), hashtags))
        if len(hashtags) > 0:
            less_hate_occurence = sys.maxsize
            norm_tweet = Utilities.normalizeText(text)
            for word in hate_words:
                if word in norm_tweet:
                    if(global_hatewords_occurences[word] < less_hate_occurence):
                        less_hate_occurence = global_hatewords_occurences[word]
                    #print(word)
                    #print(text)
            if less_hate_occurence < sys.maxsize:
                weight = Utilities.exponential_weight(less_hate_occurence, global_max_hateword,
                                                      global_min_hateword)
            for hashtag in hashtags:
                total_occurrences = hashtags_total_occurrences.get(hashtag)
                if total_occurrences is None:
                    hashtags_total_occurrences[hashtag] = weight
                else:
                    hashtags_total_occurrences[hashtag] = total_occurrences + weight
                if less_hate_occurence < sys.maxsize:
                    occurrence = hashtags_hate_occurrences.get(hashtag)
                    if occurrence is None:
                        hashtags_hate_occurrences[hashtag] = weight
                    else:
                        hashtags_hate_occurrences[hashtag] = occurrence + weight
    return hashtags_total_occurrences, hashtags_hate_occurrences

def combine_results_hashtags(results):
    hashtags_hate_occurrences = {}
    hashtags_total_occurrences = {}
    for result in results:
        local_total_occurrence = result[0]
        local_hate_occurrence = result[1]
        for hashtag in local_total_occurrence:
            occurrence = hashtags_total_occurrences.get(hashtag)
            if occurrence is None:
                hashtags_total_occurrences[hashtag] = local_total_occurrence[hashtag]
            else:
                hashtags_total_occurrences[hashtag] = occurrence + local_total_occurrence[hashtag]
        for hashtag in local_hate_occurrence:
            occurrence = hashtags_hate_occurrences.get(hashtag)
            if occurrence is None:
                hashtags_hate_occurrences[hashtag] = local_hate_occurrence[hashtag]
            else:
                hashtags_hate_occurrences[hashtag] = occurrence + local_hate_occurrence[hashtag]
    return hashtags_total_occurrences, hashtags_hate_occurrences

def calculate_occurrence_hatewords(texts_hashtags, hate_words):
    hatewords_occurences = {}
    for text, hashtags in texts_hashtags:
        hashtags = hashtags[:]
        hashtags = list(map((lambda x: x.lower()), hashtags))
        if len(hashtags) > 0:
            norm_tweet = Utilities.normalizeText(text)
            for word in hate_words:
                if word in norm_tweet:
                    word_occurence = hatewords_occurences.get(word)
                    if word_occurence is None:
                        hatewords_occurences[word] = 1
                    else:
                        hatewords_occurences[word] += 1
    return hatewords_occurences

def combine_results_hatewords(results):
    for result in results:
        local_hatewords_occurence = result
        for word in local_hatewords_occurence:
            global_hatewords_occurences[word] += local_hatewords_occurence[word]
    max = -sys.maxsize
    min = sys.maxsize
    for word in global_hatewords_occurences:
        if(global_hatewords_occurences[word] < min):
            min = global_hatewords_occurences[word]
        if (global_hatewords_occurences[word] > max):
            max = global_hatewords_occurences[word]
    global global_max_hateword
    global global_min_hateword
    global_max_hateword = max
    global_min_hateword = min

def calculate_occurrences(texts_hashtags, hate_words):
    # hate_words_check = create_hatewords_for_check(hate_words)
    size = math.ceil(len(texts_hashtags) / cpu_count)

    chunks = Utilities.chunk_array(texts_hashtags[:], size)
    hatewords_results = Parallel(n_jobs=cpu_count)(
        delayed(calculate_occurrence_hatewords)(chunk, hate_words) for chunk in chunks)
    combine_results_hatewords(hatewords_results)

    chunks = Utilities.chunk_array(texts_hashtags[:], size)
    result = Parallel(n_jobs=cpu_count)(
        delayed(calculate_occurrences_hasthags)(chunk, hate_words) for chunk in chunks)
        #delayed(calculate_occurrence)(chunk, hate_words, hate_words_check) for chunk in chunks)
    hashtags_total_occurrences, hashtags_hate_occurrences = combine_results_hashtags(result)
    return hashtags_total_occurrences, hashtags_hate_occurrences

def calculate_ratio_hashtag(hashtag, hashtags_hate_occurrences, hashtags_total_occurrences):
    total_occurrences = hashtags_total_occurrences.get(hashtag)
    if total_occurrences is None:
        return -1
    hate_occurrences = hashtags_hate_occurrences.get(hashtag)
    if hate_occurrences is None:
        return 0
    return hate_occurrences/total_occurrences

def get_ratios(hashtags_hate_occurrences, hashtags_total_occurrences, min_ratio=0.30):
    dico = {}
    for hashtag in hashtags_total_occurrences:
        ratio = calculate_ratio_hashtag(hashtag, hashtags_hate_occurrences, hashtags_total_occurrences)
        if(ratio > min_ratio):
            dico[hashtag] = ratio
    return dico

def get_best_hashtags(hashtags_ratios, num_ratios=5):
    if len(hashtags_ratios) <= 0:
        return []
    list_sorted_hashtags = [key for (key, value) in
                                 sorted(hashtags_ratios.items(), key=lambda x: x[1], reverse=True)]
    if len(list_sorted_hashtags) <= num_ratios:
        return list_sorted_hashtags
    else:
        return list_sorted_hashtags[:num_ratios]

def snowball_search(hate_words, hashtags, hashtags_ratios, trace_list, level):
    if len(hashtags) == 0:
        return []
    hashtags_hate_occurrences = {}
    hashtags_total_occurrences = {}
    next_hashtags = []

    hashtags = [hashtag.lower() for hashtag in hashtags]

    #récupérer les ids des tweets dans des fichiers
    twitter_selenium_scraper.scrape_tweets_for_hashtags(hashtags, folder='data_hate')

    texts_hashtags = TwitterCollector.request_hashtags(hashtags[:], high_level_folder='data_hate')
    hashtags_total_occurrences, hashtags_hate_occurrences = calculate_occurrences(texts_hashtags, hate_words)
    for hashtag in hashtags:
        ratio = calculate_ratio_hashtag(hashtag, hashtags_hate_occurrences, hashtags_total_occurrences)
        hashtags_ratios[hashtag.lower()] = ratio
        print(hashtag)
        print(ratio)
    #éliminer les hashtags qu'on a déjà vu ou ceux qu'on vient de voir
    hashtag_already_used = hashtags_ratios.keys()
    for hashtag in list(hashtags_total_occurrences.keys())[:]:
        if hashtag in hashtag_already_used:
            if hashtags_total_occurrences.get(hashtag) != None:
                del hashtags_total_occurrences[hashtag]
            if hashtags_hate_occurrences.get(hashtag) != None:
                del hashtags_hate_occurrences[hashtag]
    result = get_ratios(hashtags_hate_occurrences, hashtags_total_occurrences)
    best_hashtags = get_best_hashtags(result)
    if len(best_hashtags) == 0:
        trace_list.append((level, hashtags, []))
    else:
        trace_list.append((level, hashtags, best_hashtags))
        next_hashtags.extend(best_hashtags)
    print(next_hashtags)
    return next_hashtags

def write_results(hashtags_ratios, trace_list, random=6000):
    new_hashtags_ratios = {}
    hashtags = list(hashtags_ratios.keys())[:]
    texts_hashtags = TwitterCollector.request_hashtags(hashtags, high_level_folder='data_hate')
    hashtags_total_occurrences, hashtags_hate_occurrences = calculate_occurrences(texts_hashtags, hate_words)
    for hashtag in hashtags:
        ratio = calculate_ratio_hashtag(hashtag, hashtags_hate_occurrences, hashtags_total_occurrences)
        new_hashtags_ratios[hashtag.lower()] = ratio

    sorted_hashtags = [key for (key, value) in sorted(new_hashtags_ratios.items(), key=lambda x: x[1], reverse=True)]
    sorted_hashtags = sorted_hashtags[:50]

    path_best_hastags = os.path.join(snowball_folder, 'best_hashtags{}.csv'.format(random))
    Utilities.write_to_csv(path_best_hastags, sorted_hashtags[:])

    path_new_hashtags_ratios = os.path.join(snowball_folder, 'new_hashtags_ratios{}.csv'.format(random))
    Utilities.write_to_csv(path_new_hashtags_ratios, list(new_hashtags_ratios.items()))

    path_hashtags_ratios = os.path.join(snowball_folder, 'hashtags_ratios{}.csv'.format(random))
    Utilities.write_to_csv(path_hashtags_ratios, list(hashtags_ratios.items()))

    path_trace_list = os.path.join(snowball_folder, 'trace_list{}.csv'.format(random))
    Utilities.write_to_csv(path_trace_list, trace_list)

    path_global_hatewords_occurences = os.path.join(snowball_folder, 'global_hatewords_occurences{}.csv'.format(random))
    Utilities.write_to_csv(path_global_hatewords_occurences, global_hatewords_occurences.items())

    # Nettoyer le fichier à la fin, je le fais pas ici pour le cas ou je relance le programme avec les mêmes hashtags de départ
    #hashtags_to_clean = set(hashtags_ratios.keys()).difference(sorted_hashtags[:25])
    #TwitterCollector.clean_folder(hashtags_to_clean, 'data_hate')

    for hashtag in sorted_hashtags[:5]:
        ids_texts = TwitterCollector.request_hashtag_for_texts(hashtag, high_level_folder='data_hate')
        path_hashtag = os.path.join(snowball_folder, 'ids_texts_{}_{}.csv'.format(hashtag, random))
        Utilities.write_to_csv(path_hashtag, ids_texts[:300])

if __name__ == "__main__":
    mkdirs_if_not_exist(snowball_folder)
    level = -1
    hashtags_ratios = {}

    hate_words = init_hatebase(filters_for_hatebase, path_hatebase, False)
    #we will use only the normalized hate base
    hate_words = create_hatewords_normalized(hate_words)

    #first iteration for occurences of hatewords at iteration 1
    for word in hate_words:
        global_hatewords_occurences[word] = 0

    try:
        level = 0
        global_nb_tweets = 0
        for i in range(3):
            random = randint(0, 5000)
            trace_list = []
            limit = 7
            if i == 0:
                next_hashtags = snowball_search(hate_words, initial_words_religion, hashtags_ratios,
                                                trace_list, level)
                limit = 3
            else:
                next_hashtags = snowball_search(hate_words, new_words, hashtags_ratios,
                                                trace_list, level)
            level += 1
            j = 1
            while(len(next_hashtags) > 0 and j < limit):
                next_hashtags = snowball_search(hate_words, next_hashtags, hashtags_ratios, trace_list, level)
                level += 1
                j += 1
            write_results(hashtags_ratios, trace_list, random)

            i += 1

    except KeyboardInterrupt:
        write_results(hashtags_ratios, trace_list, random)
    except Exception as ex:
        write_results(hashtags_ratios, trace_list, random)
        excType = ex.__class__.__name__
        print('exception type', excType)
        print('exception msg', str(ex))
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
    except:
        write_results(hashtags_ratios, trace_list, random)