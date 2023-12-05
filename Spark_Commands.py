# Databricks notebook source
#Spark Commands

# COMMAND ----------

# DBTITLE 1,Show()
data = [(1,'dgchgdhcgdhsgchdsgcaljcajcjjbjgsyuakjkxn'),
      (2,'alkjxuCXKJbcuGUaljxjbKSLWMXKNbxg'),
      (3,'jaHXUxnbhSHXKSCKLJDICclsnJCJBCcksi'),
      (4,'qwertyuiopzxcvbnmlkjhgfdsaqwertyuio')]

schema=['id','name']

df = spark.createDataFrame(data,schema)

# by default column values are truncated to 20 length and number of rows returned is also 20 by default using the show() function
df.show()

# COMMAND ----------

# truncate=False shows all values in the column
df.show(truncate=False) 

# COMMAND ----------

# column value lengths are truncated till 10 characters
df.show(truncate=10)

# COMMAND ----------

# n stands for number of rows. So, n=2 means only 2 rows will be returned by the show() function
df.show(n=2, truncate=False)

# COMMAND ----------

# vertical=True signifies that the df content will be displayed in a singular column in vertical format
df.show(truncate=False, vertical=True)

# COMMAND ----------

# DBTITLE 1,withColumn()
from pyspark.sql.functions import col, lit

data = [(1,'Anindita','95000'),(2,'Avinash','90000'),(3,'Arnab','90000')]
column = ['id','name','salary']

df = spark.createDataFrame(data, column)

# cast salary column data type from str to int
df1 = df.withColumn(colName='salary', col=col('salary').cast('Integer'))

# multiply salary column with 2
df2 = df1.withColumn('salary', col('salary') * 2)

# add a new column to the dataframe and supply hardcoded values to it
df3 = df2.withColumn('country', lit('USA'))

# add new column duplicating existing data from another column in the dataframe
df4 = df3.withColumn('copiedsalary',col('salary'))

df4.show()
df4.printSchema()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

# DBTITLE 1,withColumnRenamed()
df = df4
# df.show()

df1 = df.withColumnRenamed('copiedsalary','salary_amount')
df1.show()

# COMMAND ----------

# DBTITLE 1,StructType() and StructField()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [(1,'Anindita',25),(2,'Avinash',25),(3,'Arnab',28)]
schema = StructType([\
    StructField('id',IntegerType()),\
    StructField('name',StringType()),\
    StructField('age',IntegerType())\
    ])

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# nested tuples
data = [(1,('Anindita','Karmakar'),25),(2,('Avinash','Kumar'),25),(3,('Arnab','Panda'),28)] 

# containing nested structure

structName = StructType([\
    StructField('firstName',StringType()),\
    StructField('lastName',StringType())\
    ])

schema = StructType([\
    StructField('id',IntegerType()),\
    StructField('name',structName),\
    StructField('age',IntegerType())\
    ])

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()
df.display()

# COMMAND ----------

# DBTITLE 1,ArrayType()
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

data = [('Anindita',[8,8]),('Avinash',[7,5]),('Arnab',[5,5])]
schema = StructType([\
                        StructField('name',StringType()),\
                        StructField('numbers',ArrayType(IntegerType()))\
                            ])

df = spark.createDataFrame(data, schema)
df.show(),
df.printSchema()

# COMMAND ----------

# create additional column 'firstNumber' with values from numbers[0] position

df.withColumn('firstNumber',df.numbers[0]).show()

# COMMAND ----------

from pyspark.sql.functions import col,array

data = [(1,2),(3,4)]
schema = ['num1','num2']

df = spark.createDataFrame(data,schema)
df.show()

# creating num column as array type containing num1 and num2 values in array
df1 = df.withColumn('num', array(col('num1'),col('num2')))
#df1 = df.withColumn('num', array(df.num1,df.num2))
df1.show()

df1.printSchema()

# COMMAND ----------

# DBTITLE 1,explode()
data = [(1,'Anindita',['singing','cooking']),(2,'Avinash',['gym','watching movies'])]
schema = ['id', 'name', 'hobbies']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col

df1 = df.withColumn('hobby',explode(col('hobbies')))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,split()
data = [(1,'Anindita','singing,cooking'),(2,'Avinash','gym,watching movies')]
schema = ['id', 'name', 'hobbies']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import split
df.show(truncate=False)
df1 = df.withColumn('hobbyArray',split('hobbies',','))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,array_contains()
from pyspark.sql.functions import array, col, array_contains

data = [(1,'Anindita',['singing','cooking']),(2,'Avinash',['gym','sex'])]
schema = ['id', 'name', 'hobbies']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)

df1 = df.withColumn('HasHobby', array_contains(col('hobbies'),'sex'))
df1.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import array_contains
help(array_contains)

# COMMAND ----------

# DBTITLE 1,MapType
data = [('Anindita', {'hair':'brown','eye':'brown'}), ('Avinash', {'hair':'black','eye':'brown'})]
schema = ['name','properties']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import MapType, StructType, StructField, StringType

data = [('Anindita', {'hair':'brown','eye':'brown'}), ('Avinash', {'hair':'black','eye':'brown'})]
schema = StructType([\
    StructField('name',StringType()),\
        StructField('properties',MapType(StringType(),StringType()))\
            ])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()
df.display()

# COMMAND ----------

# fetching hair and eye colours to a hair and eye coloumns respectively

df1 = df.withColumn('hair', df.properties['hair'])
df1.show(truncate=False) 

df2 = df1.withColumn('eye', df1.properties.getItem('eye'))
df2.show(truncate=False) 

df2.printSchema()

# COMMAND ----------

from pyspark.sql.types import MapType
help(MapType)

# COMMAND ----------

# DBTITLE 1,explode() on MapType column
from pyspark.sql.types import MapType, StructType, StructField, StringType
from pyspark.sql.functions import explode, col

data = [('Anindita', {'hair':'brown','eye':'brown'}), ('Avinash', {'hair':'black','eye':'brown'})]
schema = StructType([\
    StructField('name',StringType()),\
        StructField('properties',MapType(StringType(),StringType()))\
            ])

df = spark.createDataFrame(data, schema)
df.show(truncate=False)

df1 = df.select('name','properties',explode(col('properties')))
df1.show(truncate=False)

df.printSchema()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,map_keys()
# to get only the keys from the dataframe
from pyspark.sql.functions import map_keys

df.show(truncate=False)

df1 = df.withColumn('keys',map_keys(df.properties))
df1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,map_values()
# to get only the key values from the dataframe
from pyspark.sql.functions import map_values

df.show(truncate=False)

df1 = df.withColumn('values',map_values(df.properties))
df1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Row
from pyspark.sql import Row
#help(Row)

# row = Row('Anindita',85000)
# print(row[0] + ' ' + str(row[1]))

row = Row(name='Anindita',salary=85000)
print(row.name + ' ' + str(row.salary))

# COMMAND ----------

person = Row('name','age','relationship')
person1 = person('Anindita',25,'Mother') #person now acts as a class like Row class
person2 = person('Ritwick',2,'Son')

#person1.name
df = spark.createDataFrame([person1,person2])
df.show()
df.printSchema()

# COMMAND ----------

# creating nested StructType using Row

data = [\
    Row(name='Anindita',details=Row(age=25,gender='Female',relationship='Mother')),\
    Row(name='Ritika',details=Row(age=2,gender='Female',relationship='Daughter')),\
    Row(name='Ritwick',details=Row(age=2,gender='Male',relationship='Son')),\
        ]

df = spark.createDataFrame(data)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Column
from pyspark.sql.functions import lit

col1 = lit('avinash')

print(type(col1))

# COMMAND ----------

data = [('Anindita Karmakar','Wife',26,'Software Engineer'),('Avinash Kumar','Husband',26,'Software Engineer'),('Ritwick K','Son',2,'Child')]

schema = ['name','relationship','age','Occupation']

df = spark.createDataFrame(data,schema)

df.show(truncate=False)
df.printSchema()

# COMMAND ----------

# add new column and provide fixed values to it

df1 = df.withColumn('Country',lit('USA'))

df1.show(truncate=False)
df1.printSchema()


# COMMAND ----------

# extract column values with different methods
from pyspark.sql.functions import col

df1.select(col('relationship')).show()
df1.select(df1.Occupation).show()

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, StructField, IntegerType

data = [('Anindita Karmakar','Wife',26,'Software Engineer',('brown','brown')),('Avinash Kumar','Husband',26,'Software Engineer',('black','brown')),('Ritwick K','Son',2,'Child',('brown','hazel'))]

#schema = ['name','relationship','age','occupation', 'properties']

prop = StructType([\
    StructField('hair',StringType()),\
        StructField('eye',StringType())\
            ])

schema = StructType([\
    StructField('name',StringType()),\
        StructField('relationship',StringType()),\
            StructField('age',IntegerType()),\
                StructField('occupation',StringType()),\
                    StructField('properties',StructType(prop))\
                        ])


df = spark.createDataFrame(data,schema)

df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df.select(col('properties.hair')).show()
df.select(df.properties.eye).show()

# COMMAND ----------

# DBTITLE 1,when()
data = [('Anindita Karmakar','Wife',26,'Software Engineer',82000),('Avinash Kumar','Husband',26,'Software Engineer',80000),('Ritwick K','Son',2,'Child',0)]

schema = ['name','relationship','age','Occupation','salary']

df = spark.createDataFrame(data,schema)

df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when

df.select(\
    df.name,\
    df.salary,\
        when(df.salary==80000, 144000)\
            .when(df.salary==82000, 194000)\
                .otherwise('unknown')\
                    .alias('updated_salary')\
                        )\
                            .show()

# COMMAND ----------

help(when)

# COMMAND ----------

# DBTITLE 1,alias()
df = spark.createDataFrame([\
    {'id':1,'name':'Anindita','salary':85000},\
        {'id':2,'name':'Avinash','salary':75000},\
            {'id':3,'name':'Arnab','salary':80000},\
                {'id':4,'name':'Vikrant','salary':70000}\
                    ])

df.show()

df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.salary.alias('emp_salary')).show()

df.printSchema()

# COMMAND ----------

# DBTITLE 1,asc()
df.show()

df.sort(df.salary.asc()).show()

# COMMAND ----------

# DBTITLE 1,desc()
df.sort(df.salary.desc()).show()

# COMMAND ----------

# DBTITLE 1,cast()
# df.show()
df.printSchema()
df1 = df.select(df.id.cast('Integer'), df.name, df.salary.cast('int'))
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,like()
df.show()

df.filter(df.name.like('V%')).show()

# COMMAND ----------

# DBTITLE 1,filter() and where()
# df.show()
df.filter(df.salary>=80000).show()
df.where(df.salary<80000).show()
df.where((df.salary >= 80000) & (df.id == 1)).show() # both filter() and where() can be used to give more than one condition

# COMMAND ----------

# DBTITLE 1,distinct() & dropDuplicates()
data = [(1,'Anindita','F',96000),(2,'Avinash','M',36000),(3,'Vikrant','M',6000),(3,'Vikrant','M',6000),(4,'Jai','other',36000),]
schema = ['id','name','gender','salary']
df = spark.createDataFrame(data, schema)
df.show(truncate=False)

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

df.dropDuplicates(['gender','salary']).show()

# COMMAND ----------

# DBTITLE 1,sort()
df.show()
df.sort('salary',df.name.desc()).show()

# COMMAND ----------

# DBTITLE 1,orderBy()
df.show()
df.orderBy('salary',df.name.desc()).show()
