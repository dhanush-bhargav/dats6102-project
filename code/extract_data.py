import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

#Create a Spark Session on local machine with name 'project-sample'. If it already exists, get the session.
session = SparkSession.builder.master("local").appName("randomname").config("spark.executor.memory", "6g").getOrCreate()

#Define the schema before importing data from Hadoop. Schema contains name of each column and data type.
dataSchema = StructType().add("title", "string").add("rank", "integer").add("date", "string").add("artist", "string").add("url", "string").add("region", "string").add("chart", "string").add("trend", "string").add("streams", "integer")

#Read CSV file from HDFS running on localhost using the previously defined schema. `header` indicates that the file contains a header row which should be omitted before reading.
data = session.read.csv("hdfs://localhost:9000/user/input/charts.csv", schema=dataSchema, header=True)

#Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
data.createOrReplaceTempView("charts_data")

# #Run a simple SQL query on the table charts_data. The result is stored in a DataFrame which can then be shown, counted etc.
# result = session.sql("SELECT * FROM charts_data")
# result.show()

# #Get distinct charts for which data is present in this dataset
# result = session.sql("SELECT chart, COUNT(chart) FROM charts_data GROUP BY chart")
# result.show()

#Top200 chart has the most number of entries so we're only interested in that entries with chart=top200
result = session.sql("SELECT * FROM charts_data WHERE chart='top200'")
result.write.csv("hdfs://localhost:9000/user/input/top200.csv")