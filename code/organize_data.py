from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, when

def clean_data(session):

    #Define the schema before importing data from Hadoop. Schema contains name of each column and data type.
    dataSchema = StructType().add("title", "string").add("rank", "integer").add("date", "string").add("artist", "string").add("region", "string").add("streams", "integer")

    #Read CSV file from HDFS running on localhost using the previously defined schema. `header` indicates that the file contains a header row which should be omitted before reading.
    data = session.read.csv("hdfs://localhost:9000/user/input/top200_raw.csv", schema=dataSchema, header=True)

    #Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
    data.createOrReplaceTempView("charts_data")

    #Check for missing values in any of the columns
    missing_val = session.sql("SELECT * FROM charts_data WHERE title IS NULL OR rank IS NULL OR date IS NULL OR artist IS NULL OR region IS NULL OR streams IS NULL")
    print(missing_val.count())

    return data

def categorize_ranks(session, data):

    #Create a new column that categorizes the songs into top10, top20, top50, top100, and top200.
    rank_categories = data.withColumn("rank_cat", when((data.rank <= 10), lit("top10"))
                                      .when((data.rank <= 20), lit("top20"))
                                      .when((data.rank <= 50), lit("top50"))
                                      .when((data.rank <= 100), lit("top100"))
                                      .otherwise(lit("top200")))
    
    rank_categories.write.csv("hdfs://localhost:9000/user/input/top200_cat.csv", header="true", mode="overwrite")

    return rank_categories

def create_artist_leaderboard(session, data):
    #Creating a temporary table view to run SQL queries
    data.createOrReplaceTempView("cat_charts_data")

    artist_leaderboard = session.sql("SELECT artist, rank_cat, SUM(streams) AS total_streams, COUNT(*) num_rank_cat FROM cat_charts_data GROUP BY artist, rank_cat")
    artist_leaderboard.show()
    artist_leaderboard.write.csv("hdfs://localhost:9000/user/input/artists.csv", header="true", mode="overwrite")


if __name__=="__main__":
    data = clean_data()
    data = categorize_ranks(data)
    