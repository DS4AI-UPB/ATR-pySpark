from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark import Row
import utils

class NC_Value:

    def reinitialize_spark_session(self, collection):

        collection = utils.db_conn_str + str(collection)

        self.my_spark_session = SparkSession \
            .builder \
            .master('local') \
            .appName('Detectarea automata a termenilor medicali') \
            .config('spark.mongodb.input.uri', collection) \
            .config('spark.mongodb.output.uri', collection) \
            .config('spark.jars.ivy', utils.ivy_repo) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
            .getOrCreate()

    def clear_collections(self):
        client = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        db[utils.col_nc_value].drop()


    def normalize_results(self):

        client = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        collection = db[utils.col_nc_value]
        new_collection = []
        mylist = collection.find()
        mymax = float('-inf')
        mymin = float('inf')

        for obj in mylist:
            if float(obj['NC_value']) < mymin:
                mymin = float(obj['NC_value'])
            if float(obj['NC_value']) > mymax:
                mymax = float(obj['NC_value'])

        mylist = collection.find()
        for obj in mylist:
            aux = obj
            aux['NC_value'] = (float(aux['NC_value']) - mymin) / (mymax - mymin)
            new_collection.append(aux)

        collection.drop()

        for obj in new_collection:
            collection.insert_one(obj)

    def NC_Value_Method(self):
        self.clear_collections()
        self.reinitialize_spark_session(utils.dbname + "." + utils.col_c_value)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value').filter('C_Value > 0')
        term_list = df.collect()
        context_list = []
        context_words_dict = dict()

        for term in term_list:
            current_term = term.text
            words = current_term.split(' ')
            for word in words:
                if word not in context_list:
                    context_list.append(word)

        self.reinitialize_spark_session(utils.dbname + "." + utils.col_c_value)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value')
        terms = df.collect()

        for term in terms:
            current_term = term.text
            words = current_term.split(' ')
            for word in words:
                if word in context_list:
                    if word in context_words_dict:
                        context_words_dict[word] += 1
                    else:
                        context_words_dict[word] = 1

        weight_dict = dict()
        for context_word in context_words_dict:
            weight_dict[context_word] = context_words_dict[context_word] / len(terms)

        context_value = dict()
        for term in terms:
            current_term = term.text
            words = current_term.split(' ')
            sum = 0
            for word in words:
                if word in weight_dict:
                    sum = sum + weight_dict[word]
            context_value[current_term] = sum


        df = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value')
        rdd = df.rdd
        rdd = rdd.map(lambda x: Row(
            text=x[0],
            frequency=x[1],
            noofwords=x[2],
            filter=x[3],
            nested=x[4],
            C_value=x[5],
            NC_value=0.8 * float(x[5]) + 0.2 * context_value[x[0]]))

        self.reinitialize_spark_session(utils.dbname + "." + utils.col_nc_value)

        df = self.my_spark_session.createDataFrame(rdd)

        df = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value', 'NC_value')
        print("hello")
        df.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').save()

        self.normalize_results()

        return 0

if __name__ == '__main__':
    NCValue = NC_Value()
    NCValue.NC_Value_Method()
    