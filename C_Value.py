from pyspark.sql import SparkSession
from pyspark import Row
from math import log
from pymongo import MongoClient
import utils


class C_Value:
    
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
        db[utils.col_c_value].drop()


    def normalize_results(self):

        client =MongoClient.connect(utils.db_conn_str)
        db = client[utils.dbname]
        collection = db[utils.col_c_value]
        new_collection = []
        mylist = collection.find()
        mymax = float('-inf')
        mymin = float('inf')

        for obj in mylist:
            if float(obj['C_value']) < mymin:
                mymin = float(obj['C_value'])
            if float(obj['C_value']) > mymax:
                mymax = float(obj['C_value'])

        mylist = collection.find()
        for obj in mylist:
            aux = obj
            aux['C_value'] = (float(aux['C_value']) - mymin) / (mymax - mymin)
            new_collection.append(aux)

        collection.drop()

        for obj in new_collection:
            collection.insert_one(obj)

    def C_value_Method(self):

        self.clear_collections()

        self.reinitialize_spark_session(dbname + '.' + col_doc_preprocess)

        df = self.my_spark_session.read.format('com.mongodb.spark.sql.DefaultSource').load()

        if df.count() == 0:
            return -1

        df = df.select("text", "frequency", "noofwords", "filter").filter("frequency > " + str(utils.threshold))

        new_rdd = df.rdd

        new_rdd = new_rdd.map(lambda x: Row(
            text=x[0],
            frequency=x[1],
            noofwords=x[2],
            filter=x[3],
            nested='Not defined',
            C_value='Not defined'))

        df = self.my_spark_session.createDataFrame(new_rdd)

        list = new_rdd.collect()

        dict_of_freq = dict()
        dict_of_subterms = dict()
        aux_dict = dict()

        for row in list:
            for check in list:
                if check.text != row.text and row.text in check.text:
                    if row.text in dict_of_subterms:
                        dict_of_subterms[row.text] += 1
                        aux_dict[row.text] += 1
                        dict_of_freq[row.text] += int(check.frequency)
                    else:
                        dict_of_subterms[row.text] = 1
                        aux_dict[row.text] = 1
                        dict_of_freq[row.text] = int(check.frequency)

        for nested in aux_dict:
            for check_nested in aux_dict:
                if check_nested in nested and check_nested != nested:
                    dict_of_subterms.pop(nested, None)

        new_rdd = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value').rdd

        new_rdd = new_rdd.map(lambda x: Row(
            text=x[0],
            frequency=x[1],
            noofwords=x[2],
            filter=x[3],
            nested='true',
            C_value=log(int(x[2]) + 1, 2) * (x[1] - dict_of_freq[x[0]] / dict_of_subterms[x[0]])) \
            if x[0] in dict_of_subterms \
            else Row(
            text=x[0],
            frequency=x[1],
            noofwords=x[2],
            filter=x[3],
            nested='false',
            C_value=log(int(x[2]) + 1, 2) * x[1]))

        self.reinitialize_spark_session(dbanme + '.' + col_c_value)

        df = self.my_spark_session.createDataFrame(new_rdd)
        df = df.select('text', 'frequency', 'noofwords', 'filter', 'nested', 'C_value')
        df.write.format('com.mongodb.spark.sql.DefaultSource').mode('append').save()

        self.normalize_results()

        return 0

if __name__ == '__main__':
    CValue = C_Value()
    CValue.C_value_Method()