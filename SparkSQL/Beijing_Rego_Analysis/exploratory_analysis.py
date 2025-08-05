from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession

# Set spark parameters as conf
sc_conf = SparkConf()
# sc_conf.setMaster()
# sc_conf.setAppName('my-app')
sc_conf.set('spark.executor.memory', '2g') 
sc_conf.set('spark.driver.memory', '4g') 
sc_conf.set("spark.executor.cores", '2') 
sc_conf.set('spark.cores.max', 20)

# Configure conf
sc = SparkContext(conf=sc_conf)

# Data path 
rootPath = '../RawData'
hdfs_path_apply = rootPath + '/apply'
hdfs_path_lucky = rootPath + '/lucky'

# Open spark session
spark = SparkSession(sc)

# Grab dataframe by API
applyNumbersDF = spark.read.parquet(hdfs_path_apply)
# applyNumbersDF.show()

luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
# luckyDogsDF.show()

# Filter dataframe after 2016-01 and select carNum column
filteredLuckyDogs = luckyDogsDF.filter(luckyDogsDF['batchNum'] >= '201601').select('carNum')

# Inner join two dataframe on carNum column
jointDF = applyNumbersDF.join(filteredLuckyDogs, 'carNum', 'inner')
# If OOM need to increase spark.driver.memory
# jointDF.show()

# import function
from pyspark.sql import functions as f

# Count appearance number by batchNum and carNum
multipliers = jointDF.groupBy(['batchNum','carNum']).agg(f.count('batchNum').alias("multiplier"))
# multipliers.show()

# Select the largest appearance number according to carNum in different batch
uniqueMultipliers = multipliers.groupBy('carNum').agg(f.max('multiplier').alias('multiplier'))
# uniqueMultipliers.show()

# Count the number of carNum in each batch
result = uniqueMultipliers.groupBy('multiplier').agg(f.count('carNum').alias('cnt')).orderBy('multiplier')
result2 = result.collect()

import matplotlib.pyplot as plt
plt.bar([i['multiplier'] for i in result2], [i['cnt'] for i in result2])





