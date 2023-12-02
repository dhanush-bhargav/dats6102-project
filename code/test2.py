from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
import pandas as pd

# Assuming you already have a SparkSession created
spark = SparkSession.builder.appName("example").getOrCreate()

# Your DataFrame
data = {
    'artist': ['Ed Sheeran', 'Ed Sheeran', 'Ed Sheeran', 'Coez', 'Karel Gott', 'Beéle', 'Jenni Vartiainen', 'Beéle', 'Ed Sheeran', 'Ed Sheeran', 'Coez', 'Coez', 'Karel Gott', 'Karel Gott', 'Beéle', 'Beéle', 'Jenni Vartiainen', 'Jenni Vartiainen'],
    'rank_cat': ['top200', 'top200', 'top50', 'top100', 'top200', 'top50', 'top50', 'top50', 'top10', 'top20', 'top10', 'top20', 'top10', 'top20', 'top10', 'top20', 'top10', 'top20'],
    'total_streams': [6863056312, 58115173, 58115173, 58115173, 549206, 26677129, 6966862, 6966862, 527896, 685267, 948706, 446031, 207812, 146723, 642731, 750539, 871075, 134822],
    'num_rank_cat': [174677, 1104, 1104, 1104, 153, 1300, 274, 1300, 762, 231, 827, 722, 940, 401, 398, 438, 398, 832]
}

df = spark.createDataFrame(pd.DataFrame(data))

# Register DataFrame as a temporary table
df.createOrReplaceTempView("my_table")

# Perform the SQL query
result_df = spark.sql("""
    SELECT
        artist,
        first(total_streams) as total_streams,
        SUM(CASE WHEN rank_cat = 'top10' THEN num_rank_cat ELSE 0 END) AS top10,
        SUM(CASE WHEN rank_cat = 'top20' THEN num_rank_cat ELSE 0 END) AS top20, 
        SUM(CASE WHEN rank_cat = 'top50' THEN num_rank_cat ELSE 0 END) AS top50,                      
        SUM(CASE WHEN rank_cat = 'top100' THEN num_rank_cat ELSE 0 END) AS top100,
        SUM(CASE WHEN rank_cat = 'top200' THEN num_rank_cat ELSE 0 END) AS top200
        
    FROM
        my_table
    GROUP BY
        artist
""")

# Show the result
result_df.show()
