import spacy
from spacy.matcher import Matcher
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark import Row
import os
import csv
import time
from math import log
import re
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import utils
import sys
import os


class TextTagger:
    
    special_chars = ['[', '@', '#', '$', '%', '^', '&', '*', '(', ')', '[', ']', '{', '}', ';', ':',
    ',', '/', '<', '>', '\\', '|', '`', '~', ']', '-', '/']

    #The defined filters
    filter1 = [{'POS': 'NOUN'}, {'POS': 'NOUN', 'OP': '+'}]
    filter2 = [{'POS': 'ADJ', 'OP': '+'}, {'POS': 'NOUN', 'OP': '+'}]
    filter3 = [{'POS': 'NOUN', 'OP': '+'}, {'POS': 'ADJ', 'OP': '+'}, {'POS': 'NOUN', 'OP': '*'},
               {'POS': 'ADJ', 'OP': '*'}]
    filter4 = [{'POS': 'NOUN', 'OP': '+'}, {'POS': 'ADV', 'OP': '+'}, {'POS': 'NOUN', 'OP': '*'}]
    filter5 = [{'POS': 'NOUN', 'OP': '+'}, {'POS': 'VERB', 'OP': '+'}, {'POS': 'ADV', 'OP': '*'},
               {'POS': 'ADJ', 'OP': '*'}]
    #The desired threshold

    lemmatizer = WordNetLemmatizer()
    global my_spark_session

    # function to convert nltk tag to wordnet tag
    def nltk_tag_to_wordnet_tag(self, nltk_tag):
        if nltk_tag.startswith('J'):
            return wordnet.ADJ
        elif nltk_tag.startswith('V'):
            return wordnet.VERB
        elif nltk_tag.startswith('N'):
            return wordnet.NOUN
        elif nltk_tag.startswith('R'):
            return wordnet.ADV
        else:
            return None

    def lemmatize_sentence(self, sentence):
        # tokenize the sentence and find the POS tag for each token
        nltk_tagged = nltk.pos_tag(nltk.word_tokenize(sentence))
        # tuple of (token, wordnet_tag)
        wordnet_tagged = map(lambda x: (x[0], self.nltk_tag_to_wordnet_tag(x[1])), nltk_tagged)
        lemmatized_sentence = []
        for word, tag in wordnet_tagged:
            if tag is None:
                # if there is no available tag, append the token as is
                lemmatized_sentence.append(word)
            else:
                # else use the tag to lemmatize the token
                lemmatized_sentence.append(self.lemmatizer.lemmatize(word, tag))
        return " ".join(lemmatized_sentence)

    #Method to initialize or change SparkSession
    def reinitialize_spark_session(self, collection):

        collection = utils.db_conn_str + str(collection)
        self.my_spark_session = SparkSession \
            .builder \
            .master('local') \
            .appName('Automatic domain specific term recognition using machine learning') \
            .config('spark.mongodb.input.uri', collection) \
            .config('spark.mongodb.output.uri', collection) \
            .config('spark.jars.ivy', utils.ivy_repo) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
            .getOrCreate()

    #Method to clear the database to be sure it is a clear run
    def clear_collections(self):
        ent = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        db[utils.col_doc_preprocess].drop()

    def clean_collections(self):
        client = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        collection = db[utils.col_doc_preprocess]
        new_collection = []
        new_collection_dict = {}
        mylist = collection.find()
        for obj in mylist:
            if obj['text'] not in new_collection_dict:
                new_collection.append(obj)
                new_collection_dict[obj['text']] = 1

        collection.drop()

        for obj in new_collection:
            collection.insert_one(obj)


    def read_docs(self):
        self.reinitialize_spark_session(utils.dbname + "." + utils.col_doc_preprocess)

        client = MongoClient.connect(utils.db_conn_str)
        self.clear_collections()
        db = client[dbname]

        nlp = spacy.load('en_core_web_sm')

        filter = self.filter1

       
        no_of_docs = 0

        for subdir, dirs, files in os.walk(path):
            for file in files:
                no_of_docs += 1

        frequency_dicts = dict()
        idf_dict = dict()
       
        for i in range(1, 6):
            print(i)
            matcher = Matcher(nlp.vocab)
            idf_dict = dict()
            frequency_dicts = dict()

            filter = self.filter1

            if i == 1:
                filter = self.filter1

            elif i == 2:
                filter = self.filter2

            elif i == 3:
                filter = self.filter3

            elif i == 4:
                filter = self.filter4

            elif i == 5:
                filter = self.filter5

            matcher.add('Filter' + str(i), None, filter)

            for subdir, dirs, files in os.walk(path):
                for file in files:
                    local_dict = dict()
                    filename = os.path.join(subdir, file)
                    text = open(filename).read()
                    text = re.sub("|".join(self.special_chars), "", text)
                    text = text.lower()
                    text = self.lemmatize_sentence(text)
                    doc = nlp(text)
                    matches = matcher(doc)

                    for match_id, start, end in matches:
                        term = str(doc[start:end]).lower()  # parse through terms
                        noncharacterwords = 0
                        for word in term.split():
                            if (len(word) > 1):
                                noncharacterwords = noncharacterwords + 1

                        if len(term) > 5 and len(term.split()) < 5 and noncharacterwords > 0:

                            if term in frequency_dicts:
                                frequency_dicts[term] += 1
                            else:
                                frequency_dicts[term] = 1

                            if term not in local_dict:
                                local_dict[term] = 1

                    for term in local_dict:
                        if term in idf_dict:
                            idf_dict[term] += 1
                        else:
                            idf_dict[term] = 1

            self.insert_into_db(frequency_dicts, str(i), idf_dict, no_of_docs)

        self.clean_collections()
        return 0

    # Method to insert into the database the new terms and update the count for existing ones

    def insert_into_db(self, frequency_dict, filter_no, idf_dict, no_of_docs):

        sc = self.my_spark_session.sparkContext
        list_of_terms = frequency_dict.items()

        if len(list_of_terms) == 0:
            return -1

        rdd = sc.parallelize(list_of_terms)
        first_terms = rdd.map(lambda x: Row(
            text=x[0],
            noofwords=len(str(x[0]).split()),
            frequency=int(x[1]),
            filter=filter_no,
            idf=idf_dict[x[0]],
            documents=no_of_docs))

        df = self.my_spark_session.createDataFrame(first_terms)
        df = df.select('text', 'noofwords', 'frequency', 'filter', 'idf', 'documents')
        df.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').save()

        return 0


if __name__ == '__main__':
    path = sys.argv[1]
    MyTagger = TextTagger()
    

