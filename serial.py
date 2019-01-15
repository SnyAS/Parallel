import time
import operator
import pandas as pd
from os import listdir
from pprint import pprint

#start timer
start_time = time.time()
#dictionary to store the scores for each university
universityScore ={}

#the path containing all the datasets
files_path ='C:/Users/Inholland/Desktop/parallel/data/'
#making a coounter
j=0

#finding all the csv files in a folder
def find_csv_filenames( path_to_dir, suffix=".csv" ):
    filenames = listdir(path_to_dir)
    #returning all the filenames found witht the suffix .csv
    return [filename for filename in filenames if filename.endswith( suffix )]

#extracting all the file names which contain the extension .csv
filenames = find_csv_filenames(files_path)

#The algorithm
for name in filenames:
    #assigning a path to each dataset
    datasetPath = files_path+name
    #reading the dataset based on the semicolon sepparator
    data = pd.read_csv(datasetPath, sep=",")
    columns = []
    j= j+1
    #counting the columns in the dataset and appending it to a list
    for i in data.columns:
        columns.append(i)
    #removing the studentId and University attributes as is not needed for calculating the macro-average
    columns.remove("student_ID")
    columns.remove('University')
    #calculating the macro-average for each student
    data['avg'] = data[columns].mean(axis=1)
    #calculating the score of the university
    score = data['avg'].mean()
    #storing the university name(key) and score(value) in a dictionary
    universityScore[name] = score

#Ranking the universities based on their score
sorted_x = sorted(universityScore.items(), key=operator.itemgetter(1), reverse=True)
#endtime
end_time = time.time()
elapsedTime = end_time - start_time
print("There are ", j, "universities")
#printing the top 5 universities
print("The top 5 Universities are ranked as following: ")
pprint(sorted_x[:5])
#print time elapsed
print("The elapsed time for this algorithm is ", elapsedTime)