import time
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("University Ranking").getOrCreate()
    sparkContext = spark.sparkContext
    print("Parallel implemenation running with 1 Core")
    df = spark.read.options(header='true', inferschema='true', delimiter=',').csv("data/*.csv")  
    #start time
    time_start = time.time()
    #This is the implementation for getting the columns which hold the subject grades for the students
    df1 = spark.read.options(header='true', inferschema='true', delimiter=',').csv("data/University_.csv")
    #drop thes 2 columns to get only the columns with the subjectcs
    df1 = df1.drop("student_ID")
    df1 = df1.drop("University")
    #Calculating University score
    df = df\
        .transform(lambda dframe: dframe.withColumn('total', sum(dframe[col] for col in df1.columns))) \
        .transform(lambda dframe: dframe.withColumn('GPA', dframe['total'].cast('float') / len(df1.columns))) \
        .transform(lambda dframe: dframe.select('student_ID', 'total', 'GPA', dframe["University"]))\
        .transform(lambda dframe: dframe.groupBy(dframe["University"]).agg(fn.avg('GPA').alias('Score'))) \
        .transform(lambda dframe: dframe.sort(dframe['Score'].desc()))
    
    #calculate the time elapsed
    elapsedTime = str(time.time() - time_start)
    #printing the top 5 universities
    print("The top 5 Universities are ranked as following: ", df.show(5))
    #print time elapsed
    print("The elapsed time for this algorithm is ", elapsedTime)
