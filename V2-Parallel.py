import time
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, DataFrame

def transform(self, f):
    return f(self)
DataFrame.transform = transform
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("University Ranking").getOrCreate()
    sparkContext = spark.sparkContext
    #importing/reading all csv files
    df = spark.read.options(header='true', inferschema='true', delimiter=',').csv("data/*.csv")  # type: DataFrame
    #start time
    time_start = time.time()
    #This is the implementation for getting the columns which hold the subject grades for the students
    df1 = spark.read.options(header='true', inferschema='true', delimiter=',').csv("data/University_.csv")
    #drop thes 2 columns to get only the columns with the subjectcs
    df1 = df1.drop("Student_ID")
    df1 = df1.drop("University")
    print(df1.columns)
    #appending to a list the column names
    names = [[]]
    for name in df1.columns:
        names.append(name)	
    # Calculating the University score based on the students's average
    #summing all gradse the students got for all the subjects
    df = df.transform(lambda dframe: dframe.withColumn('total', sum(dframe[col] for col in names.values)))
    #calculating the student average
    df2 = df.transform(lambda dframe: dframe.withColumn('GPA', dframe['total'].cast('float') / len(df1)))
    #selecting the student attributes and the university
    df3 = df2.transform(lambda dframe: dframe.select('student_ID', 'total', 'GPA', dframe["University"]))
    #grouping all the students and the university and grouping by University name (expensive) ->to get Uni score
    df4 = df3.transform(lambda dframe: dframe.groupBy(dframe["University"]).agg(fn.avg('GPA').alias('Score')))
    df5 = df4.transform(lambda dframe: dframe.sort(dframe['Score'].desc()))#Ranking the universities based on their score
    elapsedTime = str(time.time() - time_start)#calculate the time elapsed
    print("The top 5 Universities are ranked as following: ", df5.show(5))#printing the top 5 universities
    print("The elapsed time for this algorithm is ", elapsedTime)#print time elapsed


