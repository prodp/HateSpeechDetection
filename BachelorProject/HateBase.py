import urllib.request
import json
import os
import Utilities
from file_processing import mkdirs_if_not_exist

# from ProjetDeSemestre import Utilities
# from ProjetDeSemestre.file_processing import mkdirs_if_not_exist

"""HateBase Wrapper v3.0 for hatebase.org API class

   construct url:
   http://api.hatebase.org/version/key/query-type/output/encoded-filters

   version	v{increment}-{sub-increment}
   key	{32-digit key}
   query type	{query type}	vocabulary; sightings
   output	{output}	xml; json
   (encoded) filters	{key}%3D{value}%7C{key}%3D{value}	(see filter table below)

"""

THIS_DIR = os.path.dirname(__file__)
hate_folder = os.path.join(THIS_DIR, 'Data_Hate')

filters_religion_en = {'about_religion': '1', 'language': 'eng'}
filters_ethnicity_en = {'about_ethnicity': '1', 'language': 'eng'}
filters_nationality_en = {'about_nationality': '1', 'language': 'eng'}
filters_gender_en = {'about_gender': '1', 'language': 'eng'}
filters_sexual_orientation_en = {'about_sexual_orientation': '1', 'language': 'eng'}
filters_disability_en = {'about_disability': '1', 'language': 'eng'}
filters_class_en = {'about_class': '1', 'language': 'eng'}
filters_archaic = {'archaic': '1', 'language': 'eng'}


class HateBaseAPI(object):
    """
            The HateBase Class
    """
    HATEBASE_KEY_1 = '35cf08d1a262aa39c68280a3ea98b8dd'
    HATEBASE_KEY_2 = 'ff8f2949356f8bc8de67131dfdd48768'

    def __init__(self):
        """Initialization of class."""
        self.base_url = "http://api.hatebase.org"
        self.version = "3"
        self.url = ""

    def encoded_filters(self, parameters=dict()):
        """encoded filters and added to the requests code."""
        primary = "%3D"
        secondary = "%7C"
        pair = []

        for key, value in parameters.items():
            pair.append(key + primary + value)

        return self.url + pair[0] + secondary + pair[1]

    def requests(self, filters=dict(), query_type='vocabulary', output='xml'):
        """return url for opening."""
        self.url = self.base_url + "/v" + self.version + "-0/" + self.HATEBASE_KEY_1 + "/" + \
            query_type + "/" + output + "/" + HateBaseAPI().encoded_filters(filters)

        return self.url

    @staticmethod
    def get_vocabulary(filters=dict(), query_type='vocabulary', output='xml'):
        """return a list of hate words from self.url"""
        hatebase = HateBaseAPI()
        hatebase.requests(filters, query_type, output)
        hate_words = []
        sock = urllib.request.urlopen(hatebase.url)
        jdata = json.loads(sock.read().decode('utf-8'))

        for elem in jdata['data']['datapoint']:
            hate_words.append(elem['vocabulary'])
        sock.close

        return hate_words


if __name__ == "__main__":
    mkdirs_if_not_exist(hate_folder)
    path_hate_dico = os.path.join(hate_folder, 'hate_words_Hatebase.csv')
    path_no_swearing = os.path.join(hate_folder, 'NoSwearing.txt')
    hate_words = Utilities.read_from_csv(path_hate_dico)
    new_hate_words = Utilities.read_from_csv(path_no_swearing)[0]
    list_filters = [filters_religion_en, filters_ethnicity_en, filters_nationality_en,
                    filters_gender_en, filters_sexual_orientation_en,
                    filters_disability_en, filters_class_en, filters_archaic]
    for filters in list_filters:
        new_hate_words.extend(HateBaseAPI.get_vocabulary(filters, query_type='vocabulary', output='json'))
    total_hate_words = set(new_hate_words).difference(set(hate_words))
    Utilities.write_to_csv(path_hate_dico, list(total_hate_words))

