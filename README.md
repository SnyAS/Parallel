# Parallel

#Tutorials
https://www.programcreek.com/python/example/98233/pyspark.sql.functions.col



https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html


https://machinelearningnepal.com/2018/01/22/apache-spark-implementation-of-som-batch-algorithm/

- installation
https://changhsinlee.com/install-pyspark-windows-jupyter/

https://stackoverflow.com/questions/3402168/permanently-add-a-directory-to-pythonpath


https://jupyter.readthedocs.io/en/latest/running.html
useful link

-excel
https://www.extendoffice.com/documents/excel/642-excel-generate-random-string.html

sc.parallelize(random_list_of_lists, 10).flatMap(lambda x: x).glom()                                                      .mapPartitions(execute_merge_sort_rdd).reduce(merge)


import findspark
findspark.init()
from time import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from pyspark.sql import SparkSession
import collections
from pyspark.sql.functions import *
from pyspark.ml.linalg import DenseVector
from pyspark.ml.feature import StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import time

sc = SparkContext(master='local[4]')
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
line = sc.textFile('C:/Users/Inholland/Desktop/assignment2_parallel/student_Subject_grade.csv')
type(line)
