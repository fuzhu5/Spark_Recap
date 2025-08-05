/*
  The file is conducting the explorayory analysis of the dataset.
*/
import org.apache.spark.sql.DataFrame

// File root path
val rootPath: String = _

// Applicants Data 
val hdfs_path_apply: String = s"${rootPath}/apply"

// Grab file by read API 
val applyNumbersDF: DataFrame = spark.read.parquet(hdfs_path_apply)
 
// Lucky dog path
val hdfs_path_lucky: String = s"${rootPath}/lucky"

// Grab file by read API
val luckyDogsDF: DataFrame = spark.read.parquet(hdfs_path_lucky)
 
// Filter by year (keep record after 2016-01) and only select "carNum" feature
val filteredLuckyDogs: DataFrame = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
 
// Join two df by "carNum" feature
val jointDF: DataFrame = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")
 
// Group by batchNum and carNum, count appearance number
val multipliers: DataFrame = jointDF.groupBy(col("batchNum"),col("carNum"))
.agg(count(lit(1)).alias("multiplier"))
 
// Group by "carNum", keep the maximum appearance number
val uniqueMultipliers: DataFrame = multipliers.groupBy("carNum")
.agg(max("multiplier").alias("multiplier"))
 
// Group by appearance number, count how many people
val result: DataFrame = uniqueMultipliers.groupBy("multiplier")
.agg(count(lit(1)).alias("cnt"))
.orderBy("multiplier")

// collect result
result.collect
