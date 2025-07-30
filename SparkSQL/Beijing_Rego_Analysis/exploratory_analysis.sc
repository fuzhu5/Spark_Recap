/*
  The file is conducting the explorayory analysis of the dataset.
*/

import org.apache.spark.sql.DataFrame

// Path for file
val rootPath: String = _

// EOI Data Path
val hdfs_pth_apply: String = s"${rootPath}/apply"

// Read API
val applyNumbersDF: DataFrame = spark.read.parquet(hdfs_pth_apply)

// show
applyNumbersDF.show

// Lodge Data Path
val hdfs_path_lucky: String = s"${rootPath}/lucky"

// Read API
val luckyDogsDF: DataFrame = spark.read.parquet(hdfs_path_lucky)

// Show
luckyDogsDF.show

// Filter the data by 2016
val filteredLuckyDogs: DataFrame = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
 
// Join two dataframe by carNum
val jointDF: DataFrame = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")
