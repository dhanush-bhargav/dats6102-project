import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

session = SparkSession.builder.master("local").appName("project-sample").getOrCreate()

dataSchema = StructType().add("title", "string").add("rank", "integer").add("date", "string").add("artist", "string").add("url", "string").add("region", "string").add("chart", "string").add("trend", "string").add("streams", "integer")

data = session.read.csv("hdfs://localhost:9000/user/input/charts.csv", schema=dataSchema)

data.createOrReplaceTempView("charts_data")

result = session.sql("SELECT * FROM charts_data")
result.show()