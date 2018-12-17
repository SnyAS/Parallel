import random

#enter years you want to predict and number of students
def generatingGrade(years, student_nr):
    #generate grade +credit
    year_SubjectGrade = []
    for year in range(1, years+1):
        studentID = random.randrange(100,500,10)
        grade = [random.randrange(1,11,1)for studentID in range(student_nr)]
        print("year ",year, "   - > studentID:",studentID, "   - > grade",grade)
        year_SubjectGrade.append((year, [studentID, grade]))
    return year_SubjectGrade
