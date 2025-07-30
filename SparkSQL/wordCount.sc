/*
  The code is to counting how many times a word appeared in wiki of spark.
  The code will generate the top-5 appeared words.
*/

import org.apache.spark.rdd.RDD

val rootPath: String = _
val file: String = s"${rootPath}/wikiOfSpark.txt"

// Read file content
val lineRDD: RDD[String] = spark.sparkContext.textFile(file)

// Split by empty space
val wordRDD: RDD[String] = lineRDD.flatMap(line => line.split(" "))

// Filter empty item
val cleanWordRDD: RDD[String] = wordRDD.filter(word => !word.equals(""))

// Change to key, value format
val kvRDD: RDD[(String, Int)] = cleanWordRDD.map(word => (word, 1))

// Start Counting
val wordCounts: RDD[(String, Int)] = kvRDD.reduceByKey((x, y) = x + y)

// Print the top-5 appeared words
wordCounts.map{case (k, v) => (v, k)}.sortByKey(false).take(5)
