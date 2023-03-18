# SparkSQL编程

先初始化pyspark

```python
import pyspark 
from pyspark.sql import SparkSession

#SparkSQL的许多功能封装在SparkSession的方法接口中

spark = SparkSession.builder \
        .appName("test") \
        .config("master","local[4]") \
        .enableHiveSupport() \
        .getOrCreate()
sc = spark.sparkContext
```

## RDD，DataFrame和DataSet对比

RDD、 DataFrame、DataSet都是spark平台下的分布式数据集，都有惰性机制，都会根据内存情况进行自动缓存计算避免移除。

-   **RDD** 一般和MLlib同时使用，不支持sparksql操作。
-   **DataFrame**参照了Pandas的思想，在RDD基础上增加了schma，能够获取列名信息。在Spark2.0中两者得到了统一：DataFrame表示为DataSet\[Row]，即DataSet的子集，两者的API接口完全相同。（Python语言接口支持）
-   **DataSet**在DataFrame基础上进一步增加了数据类型信息，可以在编译时发现类型错误。（只有Scala/Java接口中才支持）

RDD是一种行存储的数据结构，而DataFrame是一种列存储的数据结构。

## 创建DataFrame

### list转换

&#x20;通过**createDataFrame**方法将list转换成pyspark中的DataFrame

```python
values =[('Chen',23,178),('Zhang',23,176)]
df = spark.createDataFrame(values,["name","age","height"])
df.show()
"""
+-----+---+------+
| name|age|height|
+-----+---+------+
| Chen| 23|   178|
|Zhang| 23|   176|
+-----+---+------+
"""
```

### Pandas.DataFrame转换

通过**createDataFrame**方法将Pandas.DataFrame转换成pyspark中的DataFrame

```python
import pandas as pd 

pdf = pd.DataFrame([('Chen',23,178),('Zhang',23,176)],columns = ["name","age","height"])
df = spark.createDataFrame(pdf)
df.show()
"""
+-----+---+------+
| name|age|height|
+-----+---+------+
| Chen| 23|   178|
|Zhang| 23|   176|
+-----+---+------+
"""
```

### DRR转换

可以将RDD用**toDF**方法转换成DataFrame

```python
rdd = sc.parallelize([('Chen',23,178),('Zhang',23,176)])
df = rdd.toDF(["name","age","height"])
df.show()
"""
+-----+---+------+
| name|age|height|
+-----+---+------+
| Chen| 23|   178|
|Zhang| 23|   176|
+-----+---+------+
"""
df.printSchema()
"""
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- height: long (nullable = true)
"""
```

可以通过**createDataFrame**的方法指定rdd和schema创建DataFrame

```python
from pyspark.sql.types import *
from pyspark.sql import Row
from datetime import datetime

schema = StructType([StructField("name", StringType(), nullable = False),
                     StructField("height", IntegerType(), nullable = True),
                     StructField("birthday", DateType(), nullable = True)])
rdd = sc.parallelize([Row("Chen",178,datetime(1999,3,11)),
                      Row("Zhang",176,datetime(1999,6,7))])
df = spark.createDataFrame(rdd, schema)
df.show()
"""
+-----+------+----------+
| name|height|  birthday|
+-----+------+----------+
| Chen|   178|1999-03-11|
|Zhang|   176|1999-06-07|
+-----+------+----------+
"""
```

### 读取文件/数据表

#### 读取json

用spark.read.json方法读取json文件生成DataFrame

```python
#读取json文件生成DataFrame
df = spark.read.json("./input/people.json")
df.show()
"""
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
"""
```

#### 读取csv

header表示是否需要导入表头
inferSchema表示是否需要推导出数据的类型（false默认为string）
delimiter表示指定分隔符进行读取。

```python
df = spark.read\
    .format("com.databricks.spark.csv") \
    .option("header","true") \
    .option("inferSchema","true") \
    .option("delimiter", ",") \
    .load("./input/iris.csv")
df.show(5)
df.printSchema()
"""
+-----------+----------+-----------+----------+-----+
|sepallength|sepalwidth|petallength|petalwidth|label|
+-----------+----------+-----------+----------+-----+
|        5.1|       3.5|        1.4|       0.2|    0|
|        4.9|       3.0|        1.4|       0.2|    0|
|        4.7|       3.2|        1.3|       0.2|    0|
|        4.6|       3.1|        1.5|       0.2|    0|
|        5.0|       3.6|        1.4|       0.2|    0|
+-----------+----------+-----------+----------+-----+
only showing top 5 rows

root
 |-- sepallength: double (nullable = true)
 |-- sepalwidth: double (nullable = true)
 |-- petallength: double (nullable = true)
 |-- petalwidth: double (nullable = true)
 |-- label: integer (nullable = true)
"""
```

#### 读取parquet

```python
df = spark.read.parquet("/home/kesci/input/eat_pyspark9794/data/users.parquet")
df.show()
```

#### 读取hive

```python
spark.sql("CREATE TABLE IF NOT EXISTS boy (name STRING, age INT) USING hive")
spark.sql("LOAD DATA LOCAL INPATH './input/person.txt' INTO TABLE boy")
df = spark.sql("SELECT name, age FROM boy")
df.show()
"""
+-------+----+
|   name| age|
+-------+----+
|Michael|null|
|   Andy|  30|
| Justin|  19|
+-------+----+
"""
```

#### 读取mysql

```python
jbdcDF = spark.read.format("jdbc")\
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("url", "jdbc:mysql://localhost:3306/spark")\
        .option("dbtable", "student")\
        .option("user", "root")\
        .option("password", "123456")\
        .load()
jdbcDF.show()
"""
+---+-----+------+---+
| id| name|gender|age|
+---+-----+------+---+
|  1|Chen |     M| 23|
|  2|Han  |     M| 22|
+---+-----+------+---+
""" 

```

## 保存DataFrame

先创建一个简单的dataframe

```python
df = spark.read.json("./input/people.json")
"""
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
"""
```

可以保存成csv文件，json文件，parquet文件或者保存成hive数据表

#### 保存为csv

```python
df.write.format("csv").option("header","true").save("./out/people.csv")
```

#### 保存为txt

```python
# 先转换成rdd再保存成txt文件
df.rdd.saveAsTextFile("./out/people.txt")

```

#### 保存为json

```python
df.write.json("./out/people.json")
```

#### 保存为parquet

保存成parquet文件压缩格式, 占用存储小, 且是spark内存中存储格式，加载最快

```python
df.write.partitionBy("age").format("parquet").save("./data/people1.parquet")
df.write.parquet("./data/people2.parquet")

# 重新打开两者，注意区别
df = spark.read.parquet("./out/people1.parquet")
df.show()
"""
+-------+----+
|   name| age|
+-------+----+
|Michael|null|
| Justin|  19|
|   Andy|  30|
+-------+----+
"""
df = spark.read.parquet("./out/people2.parquet")
df.show()
"""
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
"""
```

#### 保存为hive数据表

```python
# bucketBy指定分桶的数量和分桶依据的列(可选)
# sortBy根据指定列，在每个分桶中进行排序(可选)
# saveAsTable指定hive表名
df.write.saveAsTable("people")
df.write.bucketBy(1, "name").sortBy("age").saveAsTable("bucketed_sorted_people")

# 重新打开两者，注意区别
df = spark.sql("SELECT age, name FROM people")
df.show()
"""
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
"""
df = spark.sql("SELECT age, name FROM bucketed_sorted_people")
df.show()
"""
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  19| Justin|
|  30|   Andy|
+----+-------+
"""
```

## DataFrame的API交互

先创建一个dataframe

```python
from pyspark.sql import Row
from pyspark.sql.functions import * 

df = spark.createDataFrame(
    [("LiLei",15,"male"),
     ("HanMeiMei",16,"female"),
     ("DaChui",17,"male")],
     ["name","age","gender"])

df.show()
df.printSchema()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+

root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- gender: string (nullable = true)
"""
```

### Action操作

DataFrame的Action操作包括show,count,collect,,describe,take,head,first等操作。

#### show

`show`用来展示数据。 numRows：表示展示的行数（默认展示20行）Truncate：只有两个取值true,false,表示一个字段是否最多显示20个字符，默认为true

```python
df.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+
"""
df.show(2,False)
"""
+---------+---+------+
|name     |age|gender|
+---------+---+------+
|LiLei    |15 |male  |
|HanMeiMei|16 |female|
+---------+---+------+
only showing top 2 rows
"""
```

#### collect

`colletc`获取一个dataframe的里面的数据 形成的是一个数组

```python
df.collect()
"""
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female'),
 Row(name='DaChui', age=17, gender='male')]
"""

```

#### cout

```python
df.count()
"""
3
"""
```

#### first / head / take

```python
df.first()   #取第一行
"""
Row(name='LiLei', age=15, gender='male')
"""

df.take(2)    #获取前n行记录
"""
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female')]
"""

df.head(2)    #获取前n行记录
"""
[Row(name='LiLei', age=15, gender='male'),
 Row(name='HanMeiMei', age=16, gender='female')]
"""
```

### 类RDD操作

**可以把DataFrame当做数据类型为Row的RDD来进行操作，必要时可以将其转换成RDD来操作**。

先创建一个dataframe

```python
# 注意这里需呀在加一个逗号
df = spark.createDataFrame([("Hello World",),("Hello China",),("Hello Spark",)],["value"])
df.show()
"""
+-----------+
|      value|
+-----------+
|Hello World|
|Hello China|
|Hello Spark|
+-----------+
"""
```

#### map

```python
#map操作，需要先转换成rdd
dfmap = df.rdd.map(lambda x:Row(x[0].upper())).toDF(["value"])
dfmap.show()
"""
+-----------+
|      value|
+-----------+
|HELLO WORLD|
|HELLO CHINA|
|HELLO SPARK|
+-----------+
"""
```

#### flatMap

```python
# flatMap，需要先转换成rdd
df_flat = df.rdd.flatMap(lambda x:x[0].split(" ")).map(lambda x:Row(x)).toDF(["value"])
df_flat.show()
"""
+-----+
|value|
+-----+
|Hello|
|World|
|Hello|
|China|
|Hello|
|Spark|
+-----+
"""

# 注意和map的区别
df_map = df.rdd.map(lambda x:x[0].split(" ")).map(lambda x:Row(x)).toDF(["value"])
df_map.show()
"""
+--------------+
|         value|
+--------------+
|[Hello, World]|
|[Hello, China]|
|[Hello, Spark]|
+--------------+
"""
```

#### **fliter**

```python
#filter过滤
df_filter = df.rdd.filter(lambda s:s[0].endswith("Spark")).toDF(["value"])
df_filter.show()
"""
+-----------+
|      value|
+-----------+
|Hello Spark|
+-----------+
"""
```

#### distinct

```python
#distinct
df_distinct = df_flat.distinct()
df_distinct.show() 
"""
+-----+
|value|
+-----+
|World|
|China|
|Hello|
|Spark|
+-----+
"""
```

#### cache 缓存

```python
df.cache()
df.unpersist()
```

#### sample采样

`DataFrame.sample` 的参数：withReplacement是否是有放回抽样，fraction返回的百分比，seed随机种子。

```python
dfsample = df.sample(False,0.5,1)
dfsample.show()
"""
+-----------+
|      value|
+-----------+
|Hello World|
|Hello China|
|Hello Spark|
+-----------+
"""
```

#### intersect / exceptAll

```python
set1 = spark.createDataFrame([['a'],['b'],['c']]).toDF("value")
set2 = spark.createDataFrame([['b'],['c'],['d']]).toDF("value")

dfintersect = set.intersect(set2)
dfintersect.show()
"""
+-----+
|value|
+-----+
|    c|
|    b|
+-----+
"""

dfexcept = set1.exceptAll(set2)
dfexcept.show()
"""
+-----+
|value|
+-----+
|    a|
+-----+
"""
```

### 类Excel操作

**可以对DataFrame进行增加列，删除列，重命名列，排序等操作，去除重复行，去除空行，就跟操作Excel表格一样。**

先创建一个dataframe

```python
df = spark.createDataFrame([
    ("LiLei",15,"male"),
    ("HanMeiMei",16,"female"),
    ("DaChui",17,"male"),
    ("RuHua",16,None)])
    .toDF("name","age","gender")

df.show()
df.printSchema()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+
root
 |-- name: string (nullable = true)
 |-- age: long (nullable = true)
 |-- gender: string (nullable = true)
"""
```

#### withColumn 增加列

```python
dfnew = df.withColumn("birthyear",2020-df["age"])
dfnew.show()
"""
+---------+---+------+---------+
|     name|age|gender|birthyear|
+---------+---+------+---------+
|    LiLei| 15|  male|     2005|
|HanMeiMei| 16|female|     2004|
|   DaChui| 17|  male|     2003|
|    RuHua| 16|  null|     2004|
+---------+---+------+---------+
"""
```

#### setect 选择

```python
dfupdate = dfnew.select("name","age","birthyear","gender")
dfupdate.show()
"""
+---------+---+---------+------+
|     name|age|birthyear|gender|
+---------+---+---------+------+
|    LiLei| 15|     2005|  male|
|HanMeiMei| 16|     2004|female|
|   DaChui| 17|     2003|  male|
|    RuHua| 16|     2004|  null|
+---------+---+---------+------+
"""
```

#### drop 删除列

```python
dfdrop = df.drop("gender")
dfdrop.show() 
"""
+---------+---+
|     name|age|
+---------+---+
|    LiLei| 15|
|HanMeiMei| 16|
|   DaChui| 17|
|    RuHua| 16|
+---------+---+
"""
```

#### withColumnRenamed列重命名

```python
dfrename = df.withColumnRenamed("gender","sex")
dfrename.show()
"""
+---------+---+------+
|     name|age|   sex|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+
"""
```

#### sort / orderby 排序

sort只能选择一个指标

orderby可以同时选择多个指标（默认为升序）

```python
# desc降序，asc升序
dfsorted = df.sort(df["age"].desc())
dfsorted.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|HanMeiMei| 16|female|
|    RuHua| 16|  null|
|    LiLei| 15|  male|
+---------+---+------+
"""

dfordered = df.orderBy(df["age"].desc(),df["name"].desc())
dfordered.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|    RuHua| 16|  null|
|HanMeiMei| 16|female|
|    LiLei| 15|  male|
+---------+---+------+
"""
```

#### nan值处理

```python
# 删除
dfnotnan = df.na.drop()
dfnotnan.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
+---------+---+------+
"""

#填充
df_fill = df.na.fill("female")
df_fill.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|female|
+---------+---+------+
"""
```

#### dropDuplicates 去重

```python
# 默认选择全部字段
dfunique = df2.dropDuplicates()
# 也可以选择指定字段
dfunique_part2 = df.dropDuplicates(["age"])
dfunique_part2.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|   DaChui| 17|  male|
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
+---------+---+------+
"""
```

#### agg聚合操作

```python
dfagg = df.agg({"name":"count","age":"max"})
dfagg.show()
"""
+-----------+--------+
|count(name)|max(age)|
+-----------+--------+
|          4|      17|
+-----------+--------+
"""
```

#### describe汇总信息

```python
df_describe = df.describe()
df_describe.show()
"""
+-------+------+-----------------+------+
|summary|  name|              age|gender|
+-------+------+-----------------+------+
|  count|     4|                4|     3|
|   mean|  null|             16.0|  null|
| stddev|  null|0.816496580927726|  null|
|    min|DaChui|               15|female|
|    max| RuHua|               17|  male|
+-------+------+-----------------+------+
"""
```

#### freqItems 频率筛选

```python
#频率超过0.5的年龄和性别
df_freq = df.stat.freqItems(("age","gender"),0.5)
df_freq.show()
"""
+-------------+----------------+
|age_freqItems|gender_freqItems|
+-------------+----------------+
|         [16]|          [male]|
+-------------+----------------+
"""
```

### 类SQL表操作

类SQL表操作主要包括表查询(select,selectExpr,where),表连接(join,union,unionAll),表分组(groupby,agg,pivot)等操作。

先创建一个dataframe

```python
df = spark.createDataFrame([
    ("LiLei",15,"male"),
    ("HanMeiMei",16,"female"),
    ("DaChui",17,"male"),
    ("RuHua",16,None)],
    ["name","age","gender"])
df.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 15|  male|
|HanMeiMei| 16|female|
|   DaChui| 17|  male|
|    RuHua| 16|  null|
+---------+---+------+
 """
```

#### select

```python
#表查询select
#如果想改表属性名，可以在后面加上 .toDF("name","birth_year")
df2 = df.select("name",2022-df["age"])
df2.show()
"""
+---------+------------+
|     name|(2022 - age)|
+---------+------------+
|    LiLei|        2007|
|HanMeiMei|        2006|
|   DaChui|        2005|
|    RuHua|        2006|
+---------+------------+
"""
```

#### selectExpr

是select的变种，支持SQL表达式，可以使用UDF函数。

```python
#表查询selectExpr,可以使用UDF函数，指定别名等
import datetime
spark.udf.register("getBirthYear",lambda age:datetime.datetime.now().year-age)
dftest = df.selectExpr("name", "getBirthYear(age) as birth_year" , "UPPER(gender) as gender" )
dftest.show()
"""
+---------+----------+------+
|     name|birth_year|gender|
+---------+----------+------+
|    LiLei|      2007|  MALE|
|HanMeiMei|      2006|FEMALE|
|   DaChui|      2005|  MALE|
|    RuHua|      2006|  null|
+---------+----------+------+
"""
```

#### where

```python
#表查询where, 指定SQL中的where字句表达式
df2 = df.where("gender='male' and age>15")
df2.show()
"""
+------+---+------+
|  name|age|gender|
+------+---+------+
|DaChui| 17|  male|
+------+---+------+
"""
```

#### filter

```python
# 两种写法
df1 = df.filter(df["age"]>16)
df1 = df.filter("age>16")
df1.show()
"""
+------+---+------+
|  name|age|gender|
+------+---+------+
|DaChui| 17|  male|
+------+---+------+
"""

# 两种写法
df2 = df.filter(df['gender']=='male')
df2 = df.filter("gender ='male'")
df2.show()
"""
+------+---+------+
|  name|age|gender|
+------+---+------+
| LiLei| 15|  male|
|DaChui| 17|  male|
+------+---+------+
"""
```

#### join

可以指定连接为 **"inner", "left", "right", "outer", "semi", "full", "leftanti", "anti"** 等多种方式（可选参数）

```python
"""
先构建两个表 df(左)           df_score(右)        
+---------+---+------+ +---------+------+-----+ 
|     name|age|gender| |     name|gender|score| 
+---------+---+------+ +---------+------+-----+ 
|    LiLei| 15|  male| |    LiLei|  male|   88| 
|HanMeiMei| 16|female| |HanMeiMei|female|   90| 
|   DaChui| 17|  male| |   DaChui|  male|   50| 
|    RuHua| 16|  null| +---------+------+-----+ 
+---------+---+------+
"""
# 单个字段join(不select的化，光用namejoin会导致有两列gender)
dfjoin = df.join(dfscore.select("name","score"),"name")
dfjoin.show()
"""
+---------+---+------+-----+
|     name|age|gender|score|
+---------+---+------+-----+
|    LiLei| 15|  male|   88|
|HanMeiMei| 16|female|   90|
|   DaChui| 17|  male|   50|
+---------+---+------+-----+
"""

# 多个字段join
dfjoin = df.join(dfscore,["name","gender"])
dfjoin.show()
"""
+---------+------+---+-----+
|     name|gender|age|score|
+---------+------+---+-----+
|HanMeiMei|female| 16|   90|
|   DaChui|  male| 17|   50|
|    LiLei|  male| 15|   88|
+---------+------+---+-----+
"""

# outer-join
dfjoin = df.join(dfscore,["name","gender"],"outer")
dfjoin.show()
"""
+---------+------+---+-----+
|     name|gender|age|score|
+---------+------+---+-----+
|HanMeiMei|female| 16|   90|
|   DaChui|  male| 17|   50|
|    LiLei|  male| 15|   88|
|    RuHua|  null| 16| null|
+---------+------+---+-----+
"""

# 灵活指定链接关系join
dfmark = dfscore.withColumnRenamed("gender","sex")
dfjoin = df.join(dfmark,(df["name"] == dfmark["name"]) & (df["gender"]==dfmark["sex"]),"inner")
dfjoin.show()
"""
+---------+---+------+---------+------+-----+
|     name|age|gender|     name|   sex|score|
+---------+---+------+---------+------+-----+
|HanMeiMei| 16|female|HanMeiMei|female|   90|
|   DaChui| 17|  male|   DaChui|  male|   50|
|    LiLei| 15|  male|    LiLei|  male|   88|
+---------+---+------+---------+------+-----+
"""
```

#### union

```python
dfunion = df_old.union(df_new)
```

#### agg

```python
data = [1,5,10]
dfdata = spark.createDataFrame([(x,) for x in data],["value"])
# 第一种写法
dfagg = dfdata.agg({"value":"avg"})
# 第二种写法
dfagg = dfdata.agg(F.mean("value"))
dfagg.show()
"""
+-----------------+
|       avg(value)|
+-----------------+
|5.333333333333333|
+-----------------+
"""
```

#### groupBy

```python
from pyspark.sql import functions as F 

dfgroup = df.groupBy("gender").agg(F.max("age"))
dfgroup.show()
"""
+------+--------+
|gender|max(age)|
+------+--------+
|  null|      16|
|female|      16|
|  male|      17|
+------+--------+
"""

# agg可以用‘数值函数’也可以用‘expr’，expr可以解析SQL表达式,下面两个输出相同
dfagg = df.groupBy("gender").agg(F.mean("age").alias("mean_age"),
                                 F.collect_list("name").alias("names"))
dfagg = df.groupBy("gender").agg(F.expr("avg(age) as mean_age"),
                                 F.expr("collect_list(name) as names"))
dfagg.show()

# 多个字段group_by
df.groupBy("gender","age").agg(F.collect_list(col("name"))).show()
"""
+------+---+------------------+
|gender|age|collect_list(name)|
+------+---+------------------+
|  male| 15|           [LiLei]|
|  male| 17|          [DaChui]|
|female| 16|       [HanMeiMei]|
|  null| 16|           [RuHua]|
+------+---+------------------+
"""

# pivot 透视
dfstudent = spark.createDataFrame([("LiLei",18,"male",1),
                                ("HanMeiMei",16,"female",1),
                                ("Jim",17,"male",2),("DaChui",20,"male",2)])\
                                .toDF("name","age","gender","class")
dfstudent.show()
dfpivot = dfstudent.groupBy("class").pivot("gender").max("age")
dfpivot.show()
"""
+---------+---+------+-----+
|     name|age|gender|class|
+---------+---+------+-----+
|    LiLei| 18|  male|    1|
|HanMeiMei| 16|female|    1|
|      Jim| 17|  male|    2|
|   DaChui| 20|  male|    2|
+---------+---+------+-----+
+-----+------+----+
|class|female|male|
+-----+------+----+
|    1|    16|  18|
|    2|  null|  20|
+-----+------+----+
"""
```

#### partitionBy

spark2.4中DataFrame没有内置partitionBy（RDD有partitionBy），可通过expr实现

```python
df = spark.createDataFrame([("LiLei",78,"class1"),("HanMeiMei",87,"class2"),
                           ("DaChui",65,"class1"),("RuHua",55,"class2")]) \
                          .toDF("name","score","class")
df.show()
dforder = df.selectExpr("name","score","class",
         "row_number() over (partition by class order by score desc) as order")
dforder.show()
"""
+---------+-----+------+
|     name|score| class|
+---------+-----+------+
|    LiLei|   78|class1|
|HanMeiMei|   87|class1|
|   DaChui|   65|class2|
|    RuHua|   55|class2|
+---------+-----+------+
+---------+-----+------+-----+
|     name|score| class|order|
+---------+-----+------+-----+
|   DaChui|   65|class2|    1|
|    RuHua|   55|class2|    2|
|HanMeiMei|   87|class1|    1|
|    LiLei|   78|class1|    2|
+---------+-----+------+-----+
"""
```

#### explode爆炸函数

```python
# 爆炸函数 exolode
import pyspark.sql.functions as F 
dfstudents = spark.createDataFrame( [("LiLei","Swim|Sing|FootBall"),("Ann","Sing|Dance"),("LiLy","Reading|Sing|Dance")],["name","hobbies"])
dfstudents.show()
# 直接select name,explode(split(hobbies,'\\\\|'))不可以，explode不允许select中出现其他表达式
# explode一行转多行,通常搭配LATERAL VIEW使用，LATERAL VIEW可以生成包含一行或多行的虚拟表
# 如下所示 tmp是虚拟表名，hobby是拆分后字段名
dfstudents.createOrReplaceTempView("students")
dfhobby = spark.sql("""select name,hobby 
                       from students
                       LATERAL VIEW explode(split(hobbies,'\\\\|')) tmp as hobby""") #注意特殊字符作为分隔符要加四个斜杠
dfhobby.show()
"""
+-----+------------------+
| name|           hobbies|
+-----+------------------+
|LiLei|Swim|Sing|FootBall|
|  Ann|        Sing|Dance|
| LiLy|Reading|Sing|Dance|
+-----+------------------+

+-----+--------+
| name|   hobby|
+-----+--------+
|LiLei|    Swim|
|LiLei|    Sing|
|LiLei|FootBall|
|  Ann|    Sing|
|  Ann|   Dance|
| LiLy| Reading|
| LiLy|    Sing|
| LiLy|   Dance|
+-----+--------+
"""
```

> &#x20;使用lateral view注意点：
> 1\.  lateral view的位置是from后where条件前
> 2\.  生成的虚拟表的表名不可省略
> 3\.  from后可带多个lateral view
> 4\.  如果要拆分的字段有null值，需要使用lateral view outer 替代，避免数据缺失

## DataFrame的SQL交互

将DataFrame注册为临时表视图或者全局表视图后，可以使用sql语句对DataFrame进行交互。不仅如此，还可以通过SparkSQL对Hive表直接进行增删改查等操作。

### 注册视图后进行SQL交互

先创建一个dataframe

```python
df = spark.createDataFrame([("LiLei",18,"male"),("HanMeiMei",17,"female"),("Jim",16,"male")],
                              ("name","age","gender"))
df.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 18|  male|
|HanMeiMei| 17|female|
|      Jim| 16|  male|
+---------+---+------+
"""
```

注册为**临时表视图**, 其生命周期和SparkSession相关联。只在当前SparkSession会话中有效，不能跨SparkSession访问

```python
df.createOrReplaceTempView("student")
dfmale = spark.sql("select * from student where gender='male'")
dfmale.show()
"""
+-----+---+------+
| name|age|gender|
+-----+---+------+
|LiLei| 18|  male|
|  Jim| 16|  male|
+-----+---+------+
"""
```

注册为**全局临时表**视图,其生命周期和整个Spark应用程序关联。作用于某个Spark应用程序的所有SparkSession会话。默认保存在系统保留的global\_temp数据库下，所以查询全局临时视图时需要在视图名前面加上`global_temp.`

```python
df.createOrReplaceGlobalTempView("student")
query = """
        select t.gender,collect_list(t.name) as names 
        from global_temp.student t 
        group by t.gender
        """
dfresult = spark.sql(query)
dfresult.show()
"""
+------+------------+
|gender|       names|
+------+------------+
|female| [HanMeiMei]|
|  male|[LiLei, Jim]|
+------+------------+
"""

#可以在新的Session中访问
dfresult = spark.newSession().sql("select * from global_temp.student")
dfresult.show()
"""
+---------+---+------+
|     name|age|gender|
+---------+---+------+
|    LiLei| 18|  male|
|HanMeiMei| 17|female|
|      Jim| 16|  male|
+---------+---+------+
"""
```

### 对Hive表进行增删改查操作

#### 删除Hive表

```python
query = "DROP TABLE IF EXISTS students" 
spark.sql(query) 
```

#### 建立hive分区表

```python
#建立hive分区表
#(注：不可以使用中文字段作为分区字段)
query = """CREATE TABLE IF NOT EXISTS `students`
        (`name` STRING COMMENT '姓名',
         `age` INT COMMENT '年龄')
        PARTITIONED BY 
        (`class` STRING  COMMENT '班级', 
        `gender` STRING  COMMENT '性别')
"""
spark.sql(query) 
```

#### 插入数据

\*\*insert into \*\*：是将数据追加到表的末尾。 **insert overwrite** : 是将重写表中的内容，即将原来的hive表中的数据删除掉，在进行插入数据操作（如果hive 表示分区表的话，insert overwrite 操作只是会[重写](https://so.csdn.net/so/search?q=重写\&spm=1001.2101.3001.7020 "重写")当前分区的数据，是不会重写其他分区的数据的 ）

hive中静态分区和动态分区的区别在于，**静态分区**是指定分区值，**动态区分**是根据值进行自动添加到对应的分区。后者在效率上会比较低，需要启动与分区数相同的数量的reducer。

**动态分区**

```python
# 注意这里有一个设置操作
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
dfstudents = spark.createDataFrame([("LiLei",18,"class1","male"),
                                    ("HanMeimei",17,"class2","female"),
                                    ("DaChui",19,"class2","male"),
                                    ("Lily",17,"class1","female")],
                                    ["name","age","class","gender"])
dfstudents.show()
dfstudents.write.mode("overwrite").format("hive").insertInto("students")
# 如果没有表就得自己建立
#dfstudents.write.mode("overwrite").format("hive")\
#          .partitionBy("class","gender").saveAsTable("students")
          
"""
+---------+---+------+------+
|     name|age| class|gender|
+---------+---+------+------+
|    LiLei| 18|class1|  male|
|HanMeimei| 17|class2|female|
|   DaChui| 19|class2|  male|
|     Lily| 17|class1|female|
+---------+---+------+------+
"""
```

**静态分区**

```python
dfstudents = spark.createDataFrame([("Jim",18,"class3","male"),
                                    ("Tom",19,"class3","male")],
                                    ["name","age","class","gender"])
dfstudents.createOrReplaceTempView("dfclass3")
dfstudents.show()
query = """
INSERT OVERWRITE TABLE `students`
PARTITION(class='class3',gender='male') 
SELECT name,age from dfclass3
""".replace("\n"," ")
spark.sql(query)

"""
+----+---+------+------+
|name|age| class|gender|
+----+---+------+------+
| Jim| 18|class3|  male|
| Tom| 19|class3|  male|
+----+---+------+------+
"""
```

**混合分区**

```python
dfstudents = spark.createDataFrame([("David",18,"class1","male"),
                                    ("Amy",17,"class1","female"),
                                    ("Jerry",19,"class1","male"),
                                    ("Ann",17,"class1","female")],
                                    ["name","age","class","gender"])
dfstudents.createOrReplaceTempView("dfclass1")
dfstudents.show()
query = """
INSERT OVERWRITE TABLE `students`
PARTITION(class='class1',gender) 
SELECT name,age,gender from dfclass1
""".replace("\n"," ")
spark.sql(query)

"""
+-----+---+------+------+
| name|age| class|gender|
+-----+---+------+------+
|David| 18|class1|  male|
|  Amy| 17|class1|female|
|Jerry| 19|class1|  male|
|  Ann| 17|class1|female|
+-----+---+------+------+
"""
```

#### 读取

```python
dfdata = spark.sql("select * from students")
dfdata.show()
"""
+---------+---+------+------+
|     name|age| class|gender|
+---------+---+------+------+
|      Amy| 17|class1|female|
|      Ann| 17|class1|female|
|    David| 18|class1|  male|
|    Jerry| 19|class1|  male|
|HanMeimei| 17|class2|female|
|   DaChui| 19|class2|  male|
|      Jim| 18|class3|  male|
|      Tom| 19|class3|  male|
+---------+---+------+------+
"""
```

> 这里需要特别注意 ：一共插入10条数据在表中，但是用的是“insert overwrite”模式，其中写入混合分区时，'David', 'Jerry' 是 {class1,male} 覆盖掉了 'Lilei',  'Amy','Ann'是{class1, female}覆盖掉了'Lily'。所以一共只有8条数据
