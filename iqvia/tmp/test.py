# # import pyspark class Row from module sql
# from pyspark.sql import *
# from pyspark.sql import Column
#
#
# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("SparkByExamples.com") \
#       .getOrCreate()
#
#
# class Person:
#   def __init__(self, name, age):
#     self.name = name
#     self.age = age
#
# p1 = Person("John", 36)
#
# department1 = Row(id='123456', name='Computer Science')
#
# # Create Example Data - Departments and Employees
# # Create the Departments
# department1 = Row(id='123456', name='Computer Science')
# x=department1.asDict()
# x['name']='ABC'
#
# Row(x.items())
#
# dic = {'First_name':"Sravan",
#        'Last_name':"Kumar",
#        'address':"hyderabad"}
# row = Row(dic)
#
# department2 = Row(id='789012', name='Mechanical Engineering')
# department3 = Row(id='345678', name='Theater and Drama')
# department4 = Row(id='901234', name='Indoor Recreation')
#
# # Create the Employees
# employee1 = Row(firstName='michael', lastName='armbrust', email='no-reply@berkeley.edu', salary='100000')
# employee10 = Row(firstName='manny', lastName='armbrust', email='no-reply@berkeley.edu', salary='40')
#
# df1 = spark.createDataFrame([employee1])
# df2 = spark.createDataFrame([employee10])
#
# df3=df1.join(df2, on=[df1.lastName==df2.lastName], how='inner')
# df3.select('firstName')
#
# def key_by(row: Row())->Row:
#     (row.id, row)
#
# rdd = df3.rdd.map(lambda r: Row(**key_by(r))))
#
# Employee = Row("firstName", "lastName", "email", "salary")
# employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
# employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
# employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
# employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
# employee5 = Employee('michael', 'jackson', 'no-reply@neverla.nd', 80000)
#
# # Create the DepartmentWithEmployees instances from Departments and Employees
# departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
# departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
# departmentWithEmployees3 = Row(department=department3, employees=[employee5, employee4])
# departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])
#
# print(department1)
# print(employee2)
# print(departmentWithEmployees1.employees[0].email)
#
#
# departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
# df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)
# departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
# df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)
#
#
# # from pyspark.sql.functions import translate
# # x = df1.withColumn('department', translate('department.name', 'Computer Science', 'Science Computer '))
#
#
# unionDF = df1.union(df2)
# unionDF.show(truncate=False)
#
# from pyspark.sql.functions import explode
# Column().getItem()
# tmp = unionDF.select(unionDF.department.getItem('name').alias('n'), explode("employees").alias("e") )
# # tmp = unionDF.select(unionDF.employees)
# # explodeDF = unionDF.select(explode("employees").alias("e"))
#
# flattenDF = tmp.select(tmp.n, tmp.e.firstName, tmp.e.lastName, tmp.e.email, tmp.e.salary)
# flattenDF = tmp.selectExpr("n.name, e.firstName", "e.lastName", "e.email", "e.salary")
#
# flattenDF.show(truncate=False)