from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark import Row
from math import log
import utils

class LIDF_Value:

    def reinitialize_spark_session(self, collection):

        collection = utils.db_conn_str + str(collection)

        self.my_spark_session = SparkSession \
            .builder \
            .master('local') \
            .appName('Detectarea automata a termenilor medicali') \
            .config('spark.mongodb.input.uri', collection) \
            .config('spark.mongodb.output.uri', collection) \
            .config('spark.jars.ivy', ivy_repo) \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.1') \
            .getOrCreate()

    def clear_collections(self):
        client = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        db[utils.col_lidf_value].drop()

    def normalize_results(self):

        client = MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        db[utils.col_lidf_value]
        new_collection = []
        mylist = collection.find()
        mymax = float('-inf')
        mymin = float('inf')

        for obj in mylist:
            if float(obj['LIDF']) < mymin:
                mymin = float(obj['LIDF'])
            if float(obj['LIDF']) > mymax:
                mymax = float(obj['LIDF'])

        mylist = collection.find()
        for obj in mylist:
            aux = obj
            aux['LIDF'] = (float(aux['LIDF']) - mymin) / (mymax - mymin)
            new_collection.append(aux)

        collection.drop()

        for obj in new_collection:
            collection.insert_one(obj)

    def LIDF_Method(self, no_of_filters=5):

        self.clear_collections()

        self.reinitialize_spark_session(utils.dbname + '.' + utils.col_doc_preprocess)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()
        filters = dict()
        df = df.select('filter', 'documents').filter("frequency > " + str(utils.threshold))
        filter_sum = 0
        no_of_docs = 0

        for i in range(1, no_of_filters + 1):
            filters[i] = df.filter('filter == ' + str(i)).count()
            filter_sum += filters[i]

        docs = df.collect()

        for entry in docs:
            no_of_docs = int(entry.documents)
            break

        self.reinitialize_spark_session(utils.dbname + "." + utils.col_c_value)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df = df.select("text", "C_value")
        C_value_list = df.collect()
        C_value_dict = dict()

        for term in C_value_list:
            if term.C_value == 0:
                C_value_dict[term.text] = 0
            else:
                C_value_dict[term.text] = float(term.C_value)

        self.reinitialize_spark_session(dbname + '.' + col_doc_preprocess)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()
        df = df.select("text", "filter", "idf", "documents", "frequency", "noofwords").filter("frequency > " + str(self.threshold))
        rdd = df.rdd
        rdd = rdd.map(lambda x: Row(
            text=x[0],
            filter=x[1],
            idf=x[2],
            C_value=C_value_dict[x[0]],
            LIDF=int(filters[int(x[1])]) / filter_sum * C_value_dict[x[0]] * log(no_of_docs /  int(x[2]) ,2),
            noofwords=x[5]))

        self.reinitialize_spark_session(utils.dbname + "." + utils.col_lidf_value)

        df = self.my_spark_session.createDataFrame(rdd)
        df = df.select('text', 'filter', 'idf', 'C_value', 'LIDF', 'noofwords')
        df.write.format('com.mongodb.spark.sql.DefaultSource').mode('overwrite').save()

        self.normalize_results()

        return 0

if __name__ == '__main__':
    LIDFV = LIDF_Value()
    LIDFV.LIDF_Method()