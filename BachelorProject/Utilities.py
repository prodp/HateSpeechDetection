import csv
import re
from nltk.tokenize import word_tokenize
import math

def write_to_csv(path, liste):
    """
    :param path:
    :param liste:
    :return:
    """

    with open(path, 'a') as csvfile:
        writer = csv.writer(csvfile, dialect='excel')
        for row in liste:
            if type(row) == type(''):
                writer.writerow([row])
            else:
                writer.writerow(row)

def read_from_csv(path):
    with open(path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        lines = []

        for line in reader:
            if len(line) == 1:
                lines.append(line[0])
            else:
                lines.append(line)

        return lines

def chunk_array(l, n):
    """Yield successive n-sized chunks from l.

    Args:
        l:
        n:

    Returns:

    """
    for i in range(0, len(l), n):
        yield l[i:i + n]

def parseText(text):
    result = []
    tokens = word_tokenize(text)
    for token in tokens:
        l = re.match("[^A-Z]+", token)
        if l != None:
            l = l.group()
            result.append(l)
        l = (re.findall("[A-Z]+[a-z]*", token))
        result.extend([elem.lower() for elem in l])
    return result

# def normalizeText(text):
#     result = []
#     l = (re.findall("\w*", text))
#     result.extend([elem.lower() for elem in l])
#     s = "_"
#     result = s.join(result)
#     return result

def normalizeText(text):
    result = parseText(text)
    s = "_"
    result = s.join(result)
    return "_" + result + "_"

def exponential_weight(less_hate_occurence, max_hateword, min_hateword):
    #valeur entre 1 et 20
    weigth = 2**(6 * (max_hateword-less_hate_occurence) / (max_hateword-min_hateword))
    return math.floor(weigth)

def e_exponential_weight(less_hate_occurence, max_hateword, min_hateword):
    #valeur entre 1 et 20
    weigth = math.exp(3 * (max_hateword-less_hate_occurence) / (max_hateword-min_hateword))
    return math.floor(weigth)

def exponential_weight_normalized(nb_total, less_hate_occurence):
    #valeur entre 1 et 20
    weigth = math.exp(3 * (1-(less_hate_occurence/nb_total)))
    return math.floor(weigth)