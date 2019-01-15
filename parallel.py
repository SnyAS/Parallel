import pyspark.sql.functions as fn
from pyspark.sql import SparkSession, DataFrame
import datetime

#start time
start_time = datetime.datetime.now()

#list of field to be taken for calculating the student average
field_list = ['Introduction to IT', 'English 1', 'Network Fundamentals (CCNA 1)', 'Programming with Python 1',
              'Precalculus', 'Study Career Coaching 1 (SCC)', 'Calculus 1', 'Programming with Python 2',
              'Project Casual Graphics', 'Logic', 'Research 1', 'English 2', 'Routing Protocols and Concepts (CCNA 2)',
              'DBMS 1', 'Calculus 2', 'Object Oriented Programming 1', 'Project Databases',
              'Study Career Coaching 2 (SCC)', 'DBMS 2', 'Statistics 1', 'Linear Algebra',
              'LAN Switching and Wireless (CCNA 3)', 'English 3', 'Object Oriented Programming 2', 'Probability Theory',
              'Study Career Coaching 3 (SCC)', 'Project Application Development', 'Software Engineering', 'UML',
              'English 4', 'Geometry', 'Accessing the WAN (CCNA 4)', 'Graph Theory', 'Algorithms & Datastructures 1',
              'Research 2', 'Study Career Coaching 4 (SCC)', 'Project Web Science', 'Numerical Analysis',
              'Algorithms & Datastructures 2', 'English 5', 'Internship', 'Research 3', 'Statistics 2',
              'Operations Research', 'Elective/minor', 'Project Engineering Entrepreneurship',
              'Study Career Coaching 5 (SCC)', 'Advanced Data Disclosure', 'Data Mining & Analysis', 'Cryptography',
              'Study Career Coaching 6 (SCC)', 'Research 4', 'Project Big Data', 'Business Intelligence',
              'Distributed Systems and Parallel Computing', 'Emerging Technologies', 'Individual Project (Thesis)']

if __name__ == '__main__':
    #initializing the SparkSession
    spark = SparkSession.builder.master("local[*]").appName("University Ranking").getOrCreate()
    #initializing sparkContext
    sparkContext = spark.sparkContext
    #importing and reading all csv files in the folder
    df = spark.read.options(header='true', inferschema='true', delimiter=',').csv("C:/Users/Home/Desktop/data/*.csv").cache()
    #calculating average for each student using the attributes in the field list
    df = df.withColumn('total', sum(df[col] for col in field_list))
    #type:#DataFrame
    df = df.withColumn('Score', df['total'].cast('float') / 57).select('student_ID', 'Score', df["University"])
    #printing a summary
    df.summary().show()
    # Calculating the University score based on the students's average
    df = df.groupBy(df["University"]).agg(fn.avg('Score').alias('Score'))
    #sort the universities based on score
    df.sort(df['Score'].desc())
    #print the top 5 unies
    df.show(5)
    #calculate the time elapsed
    end_time = datetime.datetime.now()
    elapsed_time = start_time - end_time
    print(elapsed_time)



