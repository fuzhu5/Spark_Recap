"""
  The file generates the top-5 appeared words in the wiki of spark.
"""

from pyspark import SparkContext

# read file
textFile = SparkContext().textFile("./wikiOfSpark.txt")

# counting
wordCount = (
  textFile.flatMap(lambda line: line.split(" "))
  .filter(lambda word: word != " ")
  .map(lambda word: (word, 1))
  .reduceByKey(lambda x, y: x + y)
  .sortBy(lambda x: x[1], False)
  .take(5)
)

print(wordCount)
