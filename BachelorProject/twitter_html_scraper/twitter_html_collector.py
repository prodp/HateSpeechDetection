import tweepy
import os
import json
import codecs
from tweepy.parsers import JSONParser
from Utilities import chunk_array
#from ProjetDeSemestre.Utilities import chunk_array
from nltk.corpus import stopwords
import numpy as np
import glob
import itertools
import shutil

np.random.seed(59833)  # for reproducibility

THIS_DIR = os.path.dirname(__file__)

class TwitterCollector:
    consumer_key = 'x13i7yWUKnKH1TiHJZgFZbXVn'
    consumer_secret = 'qtYUXkpXcCNFYDVFPztYPZCxN48Rk2v1jXDsWTZ2PeQorefN1j'
    access_key = '837612299250765825-ZKpG2TPOc3F4zzmqmEym7JF0jNNqAka'
    access_secret = '8qSshi1VbfDlmTOn1UV7BgCHyuyhmlU7254MTQJzyqDPg'

    def __init__(self, base_folder=None):
        # Parameter you need to specify

        self.base_folder = base_folder

    def get_id_from_permalink(self, permalink):
        """Given a permalink, gives the tweet id

        :param permalink:
        :return: tweet id
        """
        tweet_id = permalink.split('/')[-1]
        return tweet_id

    def get_parsed_search_data(self, input_file):
        """Get the tweet ids parsed via the html parser, as a csv file

        :param input_file: csv file name produced by the search function
        :return: list of tweet ids
        """

        tweet_ids = []
        with codecs.open(os.path.join(self.base_folder, input_file), 'r', encoding='utf8') as in_file:
            lines = in_file.readlines()
            for line in lines:
                line = line.strip('\n')
                line = line.split(',')
                permalink = line[0]
                tweet_ids.append(str(self.get_id_from_permalink(permalink)))
        return tweet_ids

    def get_tweets_info(self, status_ids, output_file='replies.tsv'):
        """Get the info of the list of tweet ids from twitter api into a tsv file
        The file has one json entry per line

        :param status_ids:
        :param output_file:
        :return:
        """
        print('fetching the tweets via API from the file: ' + output_file)
        auth = tweepy.OAuthHandler(TwitterCollector.consumer_key, TwitterCollector.consumer_secret)
        auth.set_access_token(TwitterCollector.access_key, TwitterCollector.access_secret)
        api = tweepy.API(auth_handler=auth, parser=JSONParser(), wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True)

        l = []
        with codecs.open(os.path.join(self.base_folder, output_file), 'w', encoding='utf8') as outFile:
            chunks = chunk_array(status_ids, 100)
            for chunk in chunks:
                rst = api.statuses_lookup(id_=chunk)
                for tweet in rst:
                    outFile.write(json.dumps(tweet) + "\n")

    @staticmethod
    def fetch_found_tweets(base_folder=None, source_file=None, data_size=-1,
                           fetch_tweets=False, output_file='tweets.tsv'):
        """Combine the separated questions and answers tweets' data into one file (based on a single query)

        Args:
            base_folder: base folder for the search operation (e.g., 'search_f=tweets_q=%23resist')
            source_file: the source file containing the list of tweet ids
            data_size: maximum data to combine
            fetch_tweets: True if we should fetch the tweets from the API again and False if they are already fetched
            output_file: output file name
        Returns:

        """

        t = TwitterCollector(base_folder=base_folder)

        tweet_ids = t.get_parsed_search_data(source_file)

        tweet_ids = tweet_ids[0:data_size] if data_size > 0 else tweet_ids

        if fetch_tweets or not os.path.isfile(os.path.join(base_folder, output_file)):
            # this writes the file into the output file
            t.get_tweets_info(tweet_ids, output_file=output_file)

    @staticmethod
    def combine_all_search_operations(high_level_folder='data', fetch_tweets=False):
        """Combines all the files from different collection steps into one file.
        Also does intermediate combinations and fetches the tweets' info from twitter

        :param high_level_folder:
        :param combine:
        :param fetch_tweets:
        :return:
        """
        for root, subdirs, files in os.walk(high_level_folder):
            for subdir in subdirs:
                # read the combined file
                print('processing ', subdir)
                # find the name the single csv file present in the directory (e.g. search_f=tweets_q=%23resist.csv)
                tweet_ids_file = [x for x in os.listdir(os.path.join(high_level_folder, subdir)) if
                                  x.endswith('.csv')][0]
                TwitterCollector.fetch_found_tweets(base_folder=os.path.join(high_level_folder, subdir),
                                                    source_file=tweet_ids_file,
                                                    data_size=-1, fetch_tweets=fetch_tweets)
        TwitterCollector.combine_operations_csvs(high_level_folder)
    @staticmethod
    def combine_operations_csvs(high_level_folder):
        """Combine the intermediate data into one csv

        Args:
            base_dir:
            out_filename:

        Returns:

        """
        with open(os.path.join(high_level_folder,'all_operations_tweets.tsv'), 'w') as outfile:
            for filename in glob.glob(high_level_folder + "/*/*.tsv"):
                with open(filename) as infile:
                    for line in infile:
                        outfile.write(line)


    @staticmethod
    def get_jsons(base_folder=None, source_file=None):
        jsons = []
        path = os.path.join(base_folder, source_file)
        print(path)
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as data_file:
                for line in data_file:
                    json_object = json.loads(line)
                    jsons.append(json_object)
        return jsons

    @staticmethod
    def get_texts_and_hashtags(base_folder=None, source_file=None):
        texts_hashtags = []
        path = os.path.join(base_folder, source_file)
        print(path)
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as data_file:
                for line in data_file:
                    json_object = json.loads(line)
                    hashtag_list = [hashtag['text'] for hashtag in json_object['entities']['hashtags']]
                    texts_hashtags.extend(list(itertools.product([json_object['text']], hashtag_list)))
        return texts_hashtags
    #     # high_level_folder => ../data_hate, base_folder => ../search_f=tweets_q=..., source_file = tweets.tsv

    @staticmethod
    def get_ids_text(base_folder=None, source_file=None):
        ids_texts = []
        path = os.path.join(base_folder, source_file)
        print(path)
        if os.path.isfile(path):
            with open(path, encoding='utf-8') as data_file:
                for line in data_file:
                    json_object = json.loads(line)
                    ids_texts.append((json_object['id_str'], json_object['text']))
        return ids_texts

    @staticmethod
    def request_hashtags(hashtags, high_level_folder='data_hate'):
        global ids
        global initial_iteration_first
        high_level_folder = os.path.join(THIS_DIR, high_level_folder)
        jsons = []
        for hashtag in hashtags:
            for root, subdirs, files in os.walk(high_level_folder):
                for subdir in subdirs:
                    pos = str(subdir).rfind("%23")
                    if hashtag == str(subdir)[pos + 3:]:
                        print('processing ', subdir)
                        # find the name the single csv file present in the directory (e.g. search_f=tweets_q=%23resist.csv)
                        tweet_ids_file = [x for x in os.listdir(os.path.join(high_level_folder, subdir)) if
                                          x.endswith('.csv')][0]
                        base_folder = os.path.join(high_level_folder, subdir)
                        output_file = "tweets.tsv"
                        try:
                            TwitterCollector.fetch_found_tweets(base_folder=base_folder,
                                                                source_file=tweet_ids_file,
                                                                data_size=-1, output_file=output_file)
                        except Exception as ex:
                            continue
                        jsons.extend(TwitterCollector.get_jsons(base_folder, output_file))
                        break
        ids = set()
        texts_hashtags = []
        for json_object in jsons:
            if json_object['id_str'] not in ids:
                hashtag_list = [hashtag['text'] for hashtag in json_object['entities']['hashtags']]
                texts_hashtags.append((json_object['text'], hashtag_list))
                #texts_hashtags.extend(list(itertools.product([json_object['text']], hashtag_list)))
                ids.add(json_object['id_str'])
        return texts_hashtags

    @staticmethod
    def request_hashtag_for_texts(hashtag, high_level_folder='data_hate'):
        high_level_folder = os.path.join(THIS_DIR, high_level_folder)
        for root, subdirs, files in os.walk(high_level_folder):
            for subdir in subdirs:
                pos = str(subdir).rfind("%23")
                if hashtag == str(subdir)[pos+3:]:
                    print('processing ', subdir)
                    # find the name the single csv file present in the directory (e.g. search_f=tweets_q=%23resist.csv)
                    tweet_ids_file = [x for x in os.listdir(os.path.join(high_level_folder, subdir)) if
                                      x.endswith('.csv')][0]
                    base_folder = os.path.join(high_level_folder, subdir)
                    output_file = "tweets.tsv"
                    TwitterCollector.fetch_found_tweets(base_folder=base_folder,
                                                        source_file=tweet_ids_file,
                                                        data_size=-1, output_file=output_file)

                    return TwitterCollector.get_ids_text(base_folder, output_file)

    @staticmethod
    def clean_folder(hashtags_to_clean, high_level_folder = 'data_hate'):
        high_level_folder = os.path.join(THIS_DIR, high_level_folder)
        for root, subdirs, files in os.walk(high_level_folder):
            for subdir in subdirs:
                for hashtag in hashtags_to_clean:
                    pos = str(subdir).rfind("%23")
                    if hashtag == str(subdir)[pos + 3:]:
                        shutil.rmtree(os.path.join(high_level_folder, subdir))
                        break

if __name__ == "__main__":
    TwitterCollector.combine_all_search_operations(high_level_folder=os.path.join(THIS_DIR, 'data_hate'),
                                                   fetch_tweets=False)
