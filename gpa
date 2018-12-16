#Prompt user for years
#Input(x)
import random
#generating grades for subjects in each year
from pip._vendor.distlib.compat import raw_input

year_SubjectGrade=[int,[int, int, float]]#[year, [subjectID, credits, grade]]
yearGpa =[int,float]#[year,gpa]
credit =int
#grade_list=[]

def generatingGrade(years):
    #generate grade +credit
    for i in range(years + 1):#for i=1 <= years:
        year_SubjectGrade[i] = [random.randrange(1,10,0.5) for _ in range(1)]
        credit = [random.randrange(1, 30, 1) for _ in range(1)]
        while sum(zip(*year_SubjectGrade)[i,[1]]) > 60:
            credit = [random.randrange(1, 30, 1) for _ in range(1)]
        year_SubjectGrade.append(i, [i+i, credit, [year_SubjectGrade[2]]])

def calculateYearlyGpa(year, subject_grade):
    total_credits=60*years
    #gwa= grade * credit
    for i in range(year + 1, len(subject_grade)):#for i=1, i<= year:
        gwa=(year_SubjectGrade[i,[2]] for grade in year_SubjectGrade[i,[2]])*(year_SubjectGrade[i,[1]] for credit in year_SubjectGrade[i,[1]])
        yearGpa.append([i,sum(gwa)/(total_credits)])
        return yearGpa

def calculateTotalGpa(year_gpa):
    for i in range(len(year_gpa)):
        #of all years
        sum(yearGpa[i,[1]] for _ in yearGpa[i,[1]])/240
        return year_gpa
        
user_years = int(input('Please Enter years to calculate/generate grades for calculating GPA: '))
years = generatingGrade(years=user_years)
#write_to_file(year_SubjectGrade)
yearGPA = calculateYearlyGpa(year=user_years, subject_grade=year_SubjectGrade.value([[2]]))
total_years_gpa=calculateTotalGpa(year_gpa=yearGPA)



#def calculateGpaBasedOnTopiic():
    # classify each subject under a topicex math, programming, projects

'''def write_to_file(grades):
    stemmed_words = open('grades.txt', 'w')
    for j in grades.keys():
        stemmed_words.write(str(j) + '\t\t')  # writing the grade in the file
        stemmed_words.write(str(grades.get(j)) + '\n')
    return
'''
'''    
1.Calculate GPA
a. Use formula sum(grades) /sum(total possible pts)
b. 
2. Sort output by GPA 
'''

