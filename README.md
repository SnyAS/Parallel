# Parallel

https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html


https://changhsinlee.com/install-pyspark-windows-jupyter/

https://stackoverflow.com/questions/3402168/permanently-add-a-directory-to-pythonpath


https://jupyter.readthedocs.io/en/latest/running.html
useful link



sc.parallelize(random_list_of_lists, 10).flatMap(lambda x: x).glom()                                                      .mapPartitions(execute_merge_sort_rdd).reduce(merge)
