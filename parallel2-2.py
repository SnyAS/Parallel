import time
import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("University Ranking").getOrCreate()
    sparkContext = spark.sparkContext
	
    df = spark.read.options(header='true', inferschema='true', delimiter=',').csv("C:/Users/Swatarianess/Documents/semidas_data/data/*.csv")  # type: DataFrame
    time_start = time.time()
	
	/* This is the implementation for getting the columns which hold the subject grades for the students
	df1 = spark.read.options(header='true', inferschema='true', delimiter=',').csv("C:/Users/Swatarianess/Documents/semidas_data/data/University_.csv")
	df1 = df1.drop("Student_ID")
	df1 = df1.drop("University")
	print(df1.columns)
	#appending to a list the column names
	names = []
	for name in df1.columns:
		names.append(name)
	print(names)
	*/
	
    # Calculating the University score based on the students's average
    df = df\
        .transform(lambda dframe: dframe.withColumn('total', sum(dframe[col] for col in names.values))) \
        .transform(lambda dframe: dframe.withColumn('GPA', dframe['total'].cast('float') / len(df1.columns)) \
        .transform(lambda dframe: dframe.select('student_ID', 'total', 'GPA', dframe["University"]))\
        .transform(lambda dframe: dframe.groupBy(dframe["University"]).agg(fn.avg('GPA').alias('Score'))) \
        .transform(lambda dframe: dframe.sort(dframe['Score'].desc()))
		
	/* broken down
	df2 = df.transform(lambda dframe: dframe.withColumn('GPA', dframe['total'].cast('float') / len(df1)))
	df3 = df2.transform(lambda dframe: dframe.select('student_ID', 'total', 'GPA', dframe["University"]))
	#expensive
	df4 = df3.transform(lambda dframe: dframe.groupBy(dframe["University"]).agg(fn.avg('GPA').alias('Score')))
	df5 = df4.transform(lambda dframe: dframe.sort(dframe['Score'].desc()))
	*/
	
    #calculate the time elapsed
	elapsedTime = str(time.time() - time_start)
	#printing the top 5 universities
	print("The top 5 Universities are ranked as following: ", df2.show(5))
	#print time elapsed
	print("The elapsed time for this algorithm is ", elapsedTime)
