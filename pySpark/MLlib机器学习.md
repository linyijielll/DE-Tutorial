# MLlib机器学习

先初始化pyspark

```python
import pyspark 
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel 

#SparkSQL的许多功能封装在SparkSession的方法接口中

spark = SparkSession.builder \
        .appName("dbscan") \
        .config("master","local[4]") \
        .enableHiveSupport() \
        .getOrCreate()

sc = spark.sparkContext
```



## 基础概念

pyspark.ml包含基于DataFrame的机器学习算法API，可以用来构建机器学习工作流Pipeline

1.  **DataFrame** : MLlib中数据的存储形式，其列可以存储特征向量，标签，以及原始的文本，图像。
2.  **Transformer(转换器)**：具有transform方法。通过附加一个或多个列将一个DataFrame转换成另外一个DataFrame。

$$
DF \rightarrow Transformer.transform() \rightarrow DF
$$


3.  **Estimator(估计器)**：具有fit方法。它接受一个DataFrame数据作为输入后经过训练，产生一个Model(Model也是一种Transformer)作为输出。 

$$
DF \rightarrow Estimator.fit ()\rightarrow Model
$$
    
$$
DF \rightarrow Model.transform ()\rightarrow DF
$$  

4.  **Pipeline(流水线/管道)**：具有setStages方法。顺序将多个Transformer和1个Estimator串联起来，得到一个流水线模型。



## 特征工程

spark的特征处理功能主要在 pyspark.ml.feature 模块中，包含

1.  特征提取：Tf-idf, Word2Vec, CountVectorizer, FeatureHasher
2.  特征转换：OneHotEncoder, Normalizer, Imputer(缺失值填充), StandardScaler, MinMaxScaler, Tokenizer(构建词典),StopWordsRemover, SQLTransformer, Bucketizer, Interaction(交叉项), Binarizer(二值化), n-gram,……
3.  特征选择：VectorSlicer(向量切片), RFormula, ChiSqSelector(卡方检验)
4.  LSH(局部敏感哈希)转换：LSH广泛用于海量数据中求最邻近，聚类等算法。

### CountVectorizer

CountVectorizer提取文本中的词频特征

```python
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel

df = spark.createDataFrame([
  (0, ["a", "b", "c"]),
  (1, ["a", "b", "b", "c", "a"])],["id","raw"])

cv = CountVectorizer().setInputCol("raw").setOutputCol("vectors")
model = cv.fit(df)
model.transform(df).show(truncate=False)

"""
+---+---------------+-------------------------+
|id |raw            |vectors                  |
+---+---------------+-------------------------+
|0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
|1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
+---+---------------+-------------------------+
"""
```

### Word2Vec

Word2Vec使用浅层神经网络提取文本中词的相似语义信息

```python
from pyspark.ml.feature import Word2Vec

df = spark.createDataFrame([
    ("I love spark".split(" "), ),
    ("Spark and Hive are good".split(" "), ),
    ("Hive is good".split(" "),)], ["text"])

word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="vector")
model = word2Vec.fit(df)
res = model.transform(df)

for row in res.collect():
    text,vector = row
    print("text: {t} \n vector:{v}\n".format(t=text,v=vector))   

"""
text: ['I', 'love', 'spark'] 
 vector:[0.11634243776400884,-0.0057766701793298125,0.09596821169058481]

text: ['Spark', 'and', 'Hive', 'are', 'good'] 
 vector:[0.008357611298561097,-0.010436675976961851,-0.05672093303874135]

text: ['Hive', 'is', 'good'] 
 vector:[-0.05636680747071902,-0.0940300424893697,0.0042815486279626684]
"""
```

### OneHotEncoder

OneHotEncoder可以将类别特征转换成OneHot编码

与 `scikit-learn` 的OneHotEncoder不同, `spark`中的OneHotEncoder最后一个类别默认是不包含进去的(可以通过dropLast参数进行修改，默认是True)。例如有五个值，第四个表示为\[0.0, 0.0, 0.0, 0.0]

```python
from pyspark.ml.feature import OneHotEncoder

df = spark.createDataFrame([
    (1,"hadoop",3.0),(2,"spark",4.0),(3,"flink",0.0),(4,"kafka",1.0),
    (5,"java",2.0),(6,"flink",0.0),(7,"kafka",1.0),(8,"python",5.0)], 
    ["id","key", "value"])

ohe = OneHotEncoder().setInputCol("value").setOutputCol("vectors")
model = ohe.fit(df)
model.transform(df).show(truncate=False)

"""
+---+------+-----+-------------+
|id |key   |value|vectors      |
+---+------+-----+-------------+
|1  |hadoop|3.0  |(5,[3],[1.0])|
|2  |spark |4.0  |(5,[4],[1.0])|
|3  |flink |0.0  |(5,[0],[1.0])|
|4  |kafka |1.0  |(5,[1],[1.0])|
|5  |java  |2.0  |(5,[2],[1.0])|
|6  |flink |0.0  |(5,[0],[1.0])|
|7  |kafka |1.0  |(5,[1],[1.0])|
|8  |python|5.0  |(5,[],[])    |
+---+------+-----+-------------+
注：这个示例中一共有6个值，one-hot表示为5，python为[0.0, 0.0, 0.0, 0.0, 0.0]表达为(5,[],[])
"""
```

### MinMaxScaler

MinMaxScaler将每个特征单独重新缩放到一个公共范围 \[min, max]，默认\[0.0,1.0]

```python
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (0, Vectors.dense([1.0, 5.0]),),
    (1, Vectors.dense([2.0, 0.0]),),
    (2, Vectors.dense([3.0, 10.0]),)], ["id", "features"])

mmScaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
model = mmScaler.fit(df)
model.transform(df).show(truncate=False)

"""
+---+----------+--------------+
|id |features  |scaledFeatures|
+---+----------+--------------+
|0  |[1.0,5.0] |[0.0,0.5]     |
|1  |[2.0,0.0] |[0.5,0.0]     |
|2  |[3.0,10.0]|[1.0,1.0]     |
+---+----------+--------------+
"""
```

### MaxAbsScaler

MaxAbsScaler通过除每个特征中的最大绝对值，将每个特征单独重新缩放到范围 \[-1.0, 1.0]

```python
from pyspark.ml.feature import MaxAbsScaler
from pyspark.ml.linalg import Vectors

df = spark.createDataFrame([
    (0, Vectors.dense([-1.0, 5.0]),),
    (1, Vectors.dense([2.0, 0.0]),),
    (2, Vectors.dense([10.0, 10.0]),)], ["id", "features"])

maScaler = MaxAbsScaler(inputCol="features", outputCol="scaledFeatures")
model = maScaler.fit(df)
model.transform(df).show(truncate=False)

"""
+---+-----------+--------------+
|id |features   |scaledFeatures|
+---+-----------+--------------+
|0  |[-1.0,5.0] |[-0.1,0.5]    |
|1  |[2.0,0.0]  |[0.2,0.0]     |
|2  |[10.0,10.0]|[1.0,1.0]     |
+---+-----------+--------------+
"""
```

### Imputer

Imputer可以填充缺失值，使用缺失值所在列的平均值或中值(默认使用平均值，可以用*strategy=*'mean'/'median'来设置)

```python
from pyspark.ml.feature import Imputer

df = spark.createDataFrame([
    (1.0, float("nan")), (2.0, float("nan")), 
    (float("nan"), 3.0),(4.0, 4.0), (5.0, 5.0)], ["a", "b"])

imputer = Imputer().setInputCols(["a", "b"]).setOutputCols(["out_a", "out_b"])
model = imputer.fit(df)
model.transform(df).show(truncate=False)

"""
+---+---+-----+-----+
|a  |b  |out_a|out_b|
+---+---+-----+-----+
|1.0|NaN|1.0  |4.0  |
|2.0|NaN|2.0  |4.0  |
|NaN|3.0|3.0  |3.0  |
|4.0|4.0|4.0  |4.0  |
|5.0|5.0|5.0  |5.0  |
+---+---+-----+-----+
"""
```



## 分类模型

Mllib支持常见的机器学习分类模型：逻辑回归，SoftMax回归，决策树，随机森林，梯度提升树，线性支持向量机，朴素贝叶斯，One-Vs-Rest，以及多层感知机模型。下面以**决策树**为例，其他模型API基本相同，可以查阅官方文档。

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 载入数据
dfdata = spark.read.format("libsvm").load("./data_libsvm.txt")
(dftrain, dftest) = dfdata.randomSplit([0.7, 0.3])

# 特征工程
# 对label进行序号标注，将字符串换成整数序号
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(dfdata)
# features如果取值可能行不超过maxCategories就会被自动识别为类别类型，并转换成以0开始的类别索引
featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(dfdata)

# 定义模型
dt = DecisionTreeClassifier(featuresCol="indexedFeatures",labelCol="indexedLabel")
# 构建流水线
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])
# 训练
model = pipeline.fit(dftrain)
# 预测
dfpredictions = model.transform(dftest)
dfpredictions.show(5)

# 打印Decision Tree
treeModel = model.stages[2]
print(treeModel)

# 评估模型误差
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction",labelCol="indexedLabel", metricName="accuracy")
accuracy = evaluator.evaluate(dfpredictions)
print("accuracy:{v}%".format(v=accuracy*100))

"""
+-----+--------------------+------------+--------------------+-------------+-----------+----------+
|label|            features|indexedLabel|     indexedFeatures|rawPrediction|probability|prediction|
+-----+--------------------+------------+--------------------+-------------+-----------+----------+
|  0.0|(692,[95,96,97,12...|         1.0|(692,[95,96,97,12...|   [0.0,29.0]|  [0.0,1.0]|       1.0|
|  0.0|(692,[100,101,102...|         1.0|(692,[100,101,102...|   [0.0,29.0]|  [0.0,1.0]|       1.0|
|  0.0|(692,[122,123,148...|         1.0|(692,[122,123,148...|   [0.0,29.0]|  [0.0,1.0]|       1.0|
|  1.0|(692,[158,159,160...|         0.0|(692,[158,159,160...|   [45.0,0.0]|  [1.0,0.0]|       0.0|
|  1.0|(692,[158,159,160...|         0.0|(692,[158,159,160...|   [45.0,0.0]|  [1.0,0.0]|       0.0|
+-----+--------------------+------------+--------------------+-------------+-----------+----------+

DecisionTreeClassificationModel (uid=DecisionTreeClassifier_dc5e898ce1ad) of depth 2 with 5 nodes

accuracy:87.5%
"""

```

其中对于模型输出

1.  rawPrediction是模型原始预测，其含义因算法而异
2.  probability 是预测为各个类别的概率
3.  prediction 是最终预测结果



## 回归模型

Mllib支持常见的回归模型：线性回归，广义线性回归，决策树回归，随机森林回归，梯度提升树回归，生存回归，保序回归。下面以**决策树回归**为例，其他模型API基本相同，可以查阅官方文档。

```python
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

# 载入处理数据
dfdata = spark.read.format("libsvm").load("./data_libsvm2.txt")
(dftrain, dftest) = dfdata.randomSplit([0.7, 0.3])
featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(dfdata)

# 定义模型
dt = DecisionTreeRegressor(featuresCol="indexedFeatures")
# 构建流水线
pipeline = Pipeline(stages=[featureIndexer, dt])
# 训练
model = pipeline.fit(dftrain)
# 预测
dfpredictions = model.transform(dftest)
dfpredictions.show(5)

# 打印模型
treeModel = model.stages[1]
print(treeModel)

# 评估模型
evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(dfpredictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

"""
+-------------------+--------------------+--------------------+--------------------+
|              label|            features|     indexedFeatures|          prediction|
+-------------------+--------------------+--------------------+--------------------+
|-26.805483428483072|(10,[0,1,2,3,4,5,...|(10,[0,1,2,3,4,5,...|-0.36833092206684886|
| -23.51088409032297|(10,[0,1,2,3,4,5,...|(10,[0,1,2,3,4,5,...|  -2.631117296441115|
|-22.949825936196074|(10,[0,1,2,3,4,5,...|(10,[0,1,2,3,4,5,...|-0.36833092206684886|
|-22.837460416919342|(10,[0,1,2,3,4,5,...|(10,[0,1,2,3,4,5,...|   1.103692852732295|
|-17.494200356883344|(10,[0,1,2,3,4,5,...|(10,[0,1,2,3,4,5,...|   1.103692852732295|
+-------------------+--------------------+--------------------+--------------------+
only showing top 5 rows

DecisionTreeRegressionModel (uid=DecisionTreeRegressor_756878e67c6e) of depth 5 with 57 nodes

Root Mean Squared Error (RMSE) on test data = 11.3015
"""
```



## 聚类模型

Mllib支持的聚类模型主要有K均值聚类K-Means，高斯混合模型GMM，隐含狄利克雷分布LDA模型等。

### K-Means

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# 载入数据
dfdata = spark.read.format("libsvm").load("./data_libsvm3.txt")

# 定义模型
kmeans = KMeans().setK(2).setSeed(1)
# 训练
model = kmeans.fit(dfdata)
# 预测
dfpredictions = model.transform(dfdata)
dfpredictions.show(truncate=False)

# 评估模型
cost = model.computeCost(dfdata)
print("Within Set Sum of Squared Errors = " + str(cost))
evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(dfpredictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# 打印中心点
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers : print(center)

"""
+-----+-------------------------+----------+
|label|features                 |prediction|
+-----+-------------------------+----------+
|0.0  |(3,[],[])                |0         |
|1.0  |(3,[0,1,2],[0.1,0.1,0.1])|0         |
|2.0  |(3,[0,1,2],[0.2,0.2,0.2])|0         |
|3.0  |(3,[0,1,2],[9.0,9.0,9.0])|1         |
|4.0  |(3,[0,1,2],[9.1,9.1,9.1])|1         |
|5.0  |(3,[0,1,2],[9.2,9.2,9.2])|1         |
+-----+-------------------------+----------+

Within Set Sum of Squared Errors = 0.11999999999994547
Silhouette with squared euclidean distance = 0.9997530305375207
Cluster Centers: 
[0.1 0.1 0.1]
[9.1 9.1 9.1]
"""
```

### 高斯混合模型

```python
from pyspark.ml.clustering import GaussianMixture

# 载入数据
dfdata = spark.read.format("libsvm").load("./data_libsvm3.txt")
# 定义模型
gmm = GaussianMixture().setK(2).setSeed(1)
# 训练
model = gmm.fit(dfdata)
# 预测
dfpredictions = model.transform(dfdata)
dfpredictions.show(truncate=False)

print("Gaussians shown as a DataFrame: ")
model.gaussiansDF.show()


"""
+-----+-------------------------+----------+-------------------------------------------+
|label|features                 |prediction|probability                                |
+-----+-------------------------+----------+-------------------------------------------+
|0.0  |(3,[],[])                |0         |[0.9999999999999979,2.093996169658831E-15] |
|1.0  |(3,[0,1,2],[0.1,0.1,0.1])|0         |[0.999999999999999,9.891337521299578E-16]  |
|2.0  |(3,[0,1,2],[0.2,0.2,0.2])|0         |[0.9999999999999979,2.093996169657856E-15] |
|3.0  |(3,[0,1,2],[9.0,9.0,9.0])|1         |[2.0939961696573796E-15,0.9999999999999979]|
|4.0  |(3,[0,1,2],[9.1,9.1,9.1])|1         |[9.89133752130383E-16,0.999999999999999]   |
|5.0  |(3,[0,1,2],[9.2,9.2,9.2])|1         |[2.0939961696583838E-15,0.9999999999999979]|
+-----+-------------------------+----------+-------------------------------------------+

Gaussians shown as a DataFrame: 
+--------------------+--------------------+
|                mean|                 cov|
+--------------------+--------------------+
|[0.10000000000001...|0.006666666666806...|
|[9.09999999999998...|0.006666666666812...|
+--------------------+--------------------+
"""
```

## 降维模型

Mllib中支持的降维模型只有主成分分析PCA算法。这个模型在pyspark.ml.feature中，通常作为特征预处理的一种技巧使用。

```python
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors

data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
        (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
        (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
dfdata = spark.createDataFrame(data, ["features"])
dfdata.show(truncate=False)

pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
model = pca.fit(dfdata)
dfresult = model.transform(dfdata).select("pcaFeatures")
dfresult.show(truncate=False)

"""
+---------------------+
|features             |
+---------------------+
|(5,[1,3],[1.0,7.0])  |
|[2.0,0.0,3.0,4.0,5.0]|
|[4.0,0.0,0.0,6.0,7.0]|
+---------------------+

+-----------------------------------------------------------+
|pcaFeatures                                                |
+-----------------------------------------------------------+
|[1.6485728230883807,-4.013282700516296,-5.524543751369388] |
|[-4.645104331781534,-1.1167972663619026,-5.524543751369387]|
|[-6.428880535676489,-5.337951427775355,-5.524543751369389] |
+-----------------------------------------------------------+
"""
```

## 模型优化

Mllib支持网格搜索方法进行超参数调优，相关函数在spark.ml.tunning模块中。  网格搜索有两种模式，一种是交叉验证(cross-validation)，另一种是留出法(hold-out)。 &#x20;

### 交叉验证

交叉验证模式使用的是K-fold交叉验证，将数据随机等分划分成K份，每次将一份作为验证集，其余作为训练集，根据K次验证集的平均结果来决定超参选取，计算成本较高，但是结果更加可靠。

```python
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# 训练数据
dfdata = spark.createDataFrame([
    (0, "a b c d e spark", 1.0),(1, "b d", 0.0),(2, "spark f g h", 1.0),(3, "hadoop mapreduce", 0.0),
    (4, "b spark who", 1.0),(5, "g d a y", 0.0),(6, "spark fly", 1.0),(7, "was mapreduce", 0.0),
    (8, "e spark program", 1.0),(9, "a e c l", 0.0),(10, "spark compile", 1.0),(11, "hadoop software", 0.0)
], ["id", "text", "label"])
# 测试数据
test = spark.createDataFrame([
    (0, "spark i j k"),(1, "l m n"),(2, "mapreduce spark"),(3, "apache hadoop")
], ["id", "text"])

# 构建流水线 [Tokenizer,hashingTF,LogisticRegression]
tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features")
lr = LogisticRegression(maxIter=10)
pipeline = Pipeline(stages=[tokenizer, hashingTF, lr])

# 创建网格
# hashingTF.numFeatures有3个可选值,lr.regParam有2个可选值,共有2*3=6个点需要搜索
paramGrid = ParamGridBuilder() \
    .addGrid(hashingTF.numFeatures, [10, 100, 1000]) \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .build()

# 创建交叉验证
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=5) 

# fit后会输出最优的模型
cvModel = crossval.fit(dfdata)

# 使用最优模型进行预测
prediction = cvModel.transform(test)
prediction.select("id", "text","words","features", "probability", "prediction").show(truncate=False)

"""
+---+---------------+------------------+-------------------------------------+-----------------------------------------+----------+
|id |text           |words             |features                             |probability                              |prediction|
+---+---------------+------------------+-------------------------------------+-----------------------------------------+----------+
|0  |spark i j k    |[spark, i, j, k]  |(100,[41,56,60,86],[1.0,1.0,1.0,1.0])|[0.3406838021397036,0.6593161978602964]  |1.0       |
|1  |l m n          |[l, m, n]         |(100,[40,75,90],[1.0,1.0,1.0])       |[0.9431747331746291,0.056825266825370924]|0.0       |
|2  |mapreduce spark|[mapreduce, spark]|(100,[50,86],[1.0,1.0])              |[0.34485023165234385,0.6551497683476561] |1.0       |
|3  |apache hadoop  |[apache, hadoop]  |(100,[83,85],[1.0,1.0])              |[0.956285034364316,0.043714965635684]    |0.0       |
+---+---------------+------------------+-------------------------------------+-----------------------------------------+----------+
"""
```

### 留出法

留出法只用将数据随机划分成训练集和验证集，仅根据验证集的单次结果决定超参选取，往往需要多次取平均，结果没有交叉验证可靠，但计算成本较低。

```python
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# 训练数据/测试数据
dfdata = spark.read.format("libsvm").load("./data_libsvm3.txt")
dftrain, dftest = dfdata.randomSplit([0.9, 0.1], seed=1)

# 构建模型
lr = LinearRegression(maxIter=10)

# 构建网格作为超参数搜索空间
paramGrid = ParamGridBuilder()\
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.fitIntercept, [False, True])\
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])\
    .build()

# 创建留出法超参调优器
tvs = TrainValidationSplit(estimator=lr,
                           estimatorParamMaps=paramGrid,
                           evaluator=RegressionEvaluator(),
                           trainRatio=0.8)

# fit后会输出最优的模型
model = tvs.fit(dftrain)

# 使用最优模型进行预测
prediction = model.transform(dftest)
```

## 实用工具

pyspark.ml.linalg模块提供了线性代数向量和矩阵对象。

pyspark.ml.stat模块提供了数理统计诸如卡方检验，相关性分析等功能。

### 矩阵和向量

```python
from pyspark.ml.linalg import DenseVector,SparseVector,DenseMatrix,SparseMatrix,Vectors,Matrices

# 稠密向量
dense_vec = DenseVector([1, 0, 0, 2.0, 0])
print("dense_vec: ", dense_vec)
print("dense_vec.numNonzeros: ", dense_vec.numNonzeros())
"""
dense_vec:  [1.0,0.0,0.0,2.0,0.0]
dense_vec.numNonzeros:  2
toArray: array([1., 0., 0., 2., 0.])
"""

# 稀疏向量
# 参数分别为: 维度,非零索引,非零元素值
sparse_vec = SparseVector(5, [0,3],[1.0,2.0])  
print("sparse_vec: ", sparse_vec)
print("sparse_vec.numNonzeros: ", sparse_vec.numNonzeros())
print("toArray: ",sparse_vec.toArray())
"""
sparse_vec:  (5,[0,3],[1.0,2.0])
sparse_vec.numNonzeros:  2
toArray: array([1., 0., 0., 2., 0.])
"""

# 稠密矩阵
# 参数分别为: 行数,列数,元素,是否转置(默认False)
dense_matrix = DenseMatrix(3, 2, [1, 3, 5, 2, 4, 6])
"""
dense_matrix:
  DenseMatrix([[1., 2.],
               [3., 4.],
               [5., 6.]])
toArray:
  [[1. 2.]
   [3. 4.]
   [5. 6.]]
"""

# 稀疏矩阵
# 参数分别为: 行数,列数,colPtrs,rowIndices,values,是否转置(默认False)
# colPtrs：元素个数为列数+1，第一个值固定0，后面为截止当列非0值累计个数
# rowIndices ：每个非0值的行号
# values: 按照列排列的非0值
sparse_matrix = SparseMatrix(3, 3, [0, 2, 3, 6],[0, 2, 1, 0, 1, 2], [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
print("sparse_matrix:\n",sparse_matrix)
print("toArray:\n", sparse_matrix.toArray())
"""
sparse_matrix:
    3 X 3 CSCMatrix
    (0,0) 1.0
    (2,0) 2.0
    (1,1) 3.0
    (0,2) 4.0
    (1,2) 5.0
    (2,2) 6.0
toArray:
   [[1. 0. 4.]
    [0. 3. 5.]
    [2. 0. 6.]]
"""

# Vectors有dense(),norm(),sparse(),squared_distance(),zeros()
# dense()
vec1 = Vectors.dense([1, 2, 3])
# norm() 求范数 [第一范数，绝对值相加] [第二范数，平方根] 
vec2 = Vectors.norm(vec1,2)
# sparse下面三种表达方式值相同
vec3 = Vectors.sparse(4, {1: 1.0, 3: 5.5})
vec3 = Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
vec3 = Vectors.sparse(4, [1, 3], [1.0, 5.5])
# squared_distance():平方距离
a = Vectors.sparse(4, [(0, 1), (3, 4)])
b = Vectors.dense([2, 5, 4, 1])
res = a.squared_distance(b)
# zeros()
vec4 = Vectors.zeros(3)

# Matrices有两个方法dense()和sparse()分别创建稠密矩阵和稀疏举证
matrix1 = Matrices.dense(3, 2, [1,3,5,2,4,6])
matrix2 = Matrices.sparse(3, 3, [0,2,3,6],[0,2,1,0,1,2], [1,2,3,4,5,6])

```

### **数理统计**

```python
#相关性分析
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])
r1 = Correlation.corr(df, "features").head()
print("Pearson correlation matrix:\n" + str(r1[0]))
r2 = Correlation.corr(df, "features", "spearman").head()
print("Spearman correlation matrix:\n" + str(r2[0]))
"""
Pearson correlation matrix:
DenseMatrix([[1.        , 0.05564149,        nan, 0.40047142],
             [0.05564149, 1.        ,        nan, 0.91359586],
             [       nan,        nan, 1.        ,        nan],
             [0.40047142, 0.91359586,        nan, 1.        ]])
Spearman correlation matrix:
DenseMatrix([[1.        , 0.10540926,        nan, 0.4       ],
             [0.10540926, 1.        ,        nan, 0.9486833 ],
             [       nan,        nan, 1.        ,        nan],
             [0.4       , 0.9486833 ,        nan, 1.        ]])
"""


#卡方检验
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest

data = [(0.0, Vectors.dense(0.5, 10.0)),
        (0.0, Vectors.dense(1.5, 20.0)),
        (1.0, Vectors.dense(1.5, 30.0)),
        (0.0, Vectors.dense(3.5, 30.0)),
        (0.0, Vectors.dense(3.5, 40.0)),
        (1.0, Vectors.dense(3.5, 40.0))]
df = spark.createDataFrame(data, ["label", "features"])
r = ChiSquareTest.test(df, "features", "label").head()
print("pValues: " + str(r.pValues))
print("degreesOfFreedom: " + str(r.degreesOfFreedom))
print("statistics: " + str(r.statistics))
"""
pValues: [0.6872892787909721,0.6822703303362126]
degreesOfFreedom: [2, 3]
statistics: [0.75,1.5]
"""
```

## 模型保存载入

```python
model.write().overwrite().save("./data/mymodel.model")
model_loaded = PipelineModel.load("./data/mymodel.model")
```

