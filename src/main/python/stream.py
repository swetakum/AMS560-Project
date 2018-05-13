from pyspark import SparkContext
from pyspark.streaming import StreamingContext, DStream
from pyspark.sql.types import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.session import *
import time

class Reader():
    def __init__(self):
        self.sc = SparkContext('local', 'Stream-SQL')
        self.ssc = StreamingContext(self.sc, batchDuration=3)
        self.spark = SparkSession.builder\
            .getOrCreate()
        self.sc.setLogLevel('ERROR')

    def initStream(self):
        self.readInput()

        self.ssc.start()
        self.ssc.awaitTermination()

    def inputSQLQuery(self, query):
        self.modQuery = ''
        self.dictInnerQuery = {}

        innerFlag = False
        innerCol = ''
        wordList = query.split(' ')
        wordQuery = ''

        for i in range(len(wordList)):
            word = wordList[i]

            # Detect opening '(' of inner query
            if word == '(SELECT':
                innerFlag = True
                innerCol = wordList[i-2]
            
            if innerFlag:
                wordQuery += word + ' '
            else:
                self.modQuery += word + ' '
            
            # Detect closing ')' of table) and not AVG(col)
            if ')' in word and '(' not in word:
                replaceInner = 'Q' + str(len(self.dictInnerQuery))
                self.modQuery += replaceInner + ' '
                key = replaceInner
                value = [wordQuery, innerCol, 0]
                self.dictInnerQuery[key] = value

                innerFlag = False
                wordQuery = ''

    def readInput(self):
        lines = self.ssc.textFileStream('Data/Live')
        
        self.csvSchema = StructType([StructField('col1', IntegerType()),
                        StructField('col2', IntegerType()),
                        StructField('col3', IntegerType())])
        
        self.stateDF = self.spark.createDataFrame(self.sc.emptyRDD(), self.csvSchema)
        # self.stateDF.show()
        self.globalDF = self.spark.createDataFrame(self.sc.emptyRDD(), self.csvSchema)
        
        self.totalTime = 0.0

        def row(inpStr):
            return Row(int(inpStr[0]), int(inpStr[1]), int(inpStr[2]))

        def iterateRDD(rdd):
            start = time.clock()
            data = rdd.map(lambda line: line.split(' ')).map(row)
            df = data.toDF(self.csvSchema)

            if df.count():
                # print(self.stateDF.count())
                curDF = df.union(self.stateDF)
                self.queryRDD(curDF)

                # Append to global DF for batch outputs
                # self.globalDF = df.union(self.globalDF)

                self.outputQuery(curDF)
                self.totalTime += time.clock() - start
                # print(str(round(self.totalTime, 2)) + 's')

        lines.foreachRDD(iterateRDD)

    def queryRDD(self, df):
        # df.show()
        df.createOrReplaceTempView('table')

        for key, value in self.dictInnerQuery.items():
            innerQuery = value[0]
            sqlDF = self.spark.sql(innerQuery)
            sqlRes = sqlDF.first()[0]
            self.dictInnerQuery[key][2] = sqlRes
        
        # df.show()
        b = 14
        addToState = [False for i in range(df.count())]
        for key, value in self.dictInnerQuery.items():
            col = value[1]
            val = value[2]
            # print(col, val, b)
            tupleList = [{col:x[col]} for x in df.rdd.collect()]
            for i in range(len(tupleList)):
                row = tupleList[i]
                if row[col] > val - b and row[col] < val + b:
                    addToState[i] = True
        
        # print(addToState)
        itr = 0
        newRows = []
        newStateDF = self.spark.createDataFrame(self.sc.emptyRDD(), self.csvSchema)
        for row in df.rdd.collect():
            if addToState[itr]:
                newRows.append(row)
            itr += 1
        # print(newRows)
        newStateDF = self.spark.createDataFrame(newRows, self.csvSchema)
        self.stateDF = newStateDF
        # newStateDF.printSchema()
        approxRows = newStateDF.sort('col1', ascending=False).collect()
        approxDF = self.spark.createDataFrame(approxRows, self.csvSchema)
        # approxDF.show()
        self.stateDF = self.spark.createDataFrame(approxDF.head(60), self.csvSchema)
        # self.stateDF.show()

    def outputQuery(self, df):
        curQuery = ' '.join(list(map((lambda word: str(round(self.dictInnerQuery[word][2], 2)) if word in self.dictInnerQuery else word), self.modQuery.split())))
        df.createOrReplaceTempView('table')
        streamOut = self.spark.sql(curQuery).first()[0]
        print(streamOut)

        # self.globalDF.show()
        # self.globalDF.createOrReplaceTempView('table')
        # globalOut = self.spark.sql(curQuery).first()[0]
        # print(type(globalOut))
        # print(streamOut, globalOut)

        
def main():
    query = 'SELECT AVG(col2) FROM table WHERE col2 > (SELECT AVG(col2) FROM table)'
    # query = 'SELECT AVG(col2) FROM table WHERE col2 > (SELECT AVG(col2) FROM table) AND col3 > (SELECT AVG(col3) FROM table)'
    reader = Reader()
    reader.inputSQLQuery(query)
    print(reader.modQuery)
    
    reader.initStream()
    

if __name__ == '__main__':
    main()
