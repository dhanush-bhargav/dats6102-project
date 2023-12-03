from pyspark.sql.functions import lit, when

def clean_data(session, data):

    #Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
    data.createOrReplaceTempView("charts_data")

    #Check for missing values in any of the columns
    missing_val = session.sql("SELECT * FROM charts_data WHERE title IS NULL OR rank IS NULL OR date IS NULL OR artist IS NULL OR region IS NULL OR streams IS NULL")
    print(f"Number of missing values in dataset {missing_val.count()}")

    session.catalog.dropTempView('charts_data') #Dropping the temp table as we no longer need it.

    return data

def categorize_ranks(session, data):

    #Create a new column that categorizes the songs into top10, top20, top50, top100, and top200.
    rank_categories = data.withColumn("rank_cat", when((data.rank <= 10), lit("top10"))
                                      .when((data.rank <= 20), lit("top20"))
                                      .when((data.rank <= 50), lit("top50"))
                                      .when((data.rank <= 100), lit("top100"))
                                      .otherwise(lit("top200")))
    
    #Write back the categorized files to Hadoop DFS
    rank_categories.write.csv("hdfs://localhost:9000/user/input/top200_cat.csv", header="true", mode="overwrite")

    return rank_categories

def create_artist_leaderboard(session, data):
    #Creating a temporary table view to run SQL queries
    data.createOrReplaceTempView("cat_charts_data")

    #We are now interested in artists from some specific regions and their ranking stats.
    artist_leaderboard = session.sql("SELECT artist, rank_cat, SUM(streams) AS total_streams, COUNT(*) AS num_rank_cat FROM cat_charts_data WHERE region IN ('Canada', 'United States', 'India', 'Mexico', 'Australia', 'United Kingdom') GROUP BY artist, rank_cat")
    
    #Write the artists leaderboard file back to Hadoop DFS
    artist_leaderboard.write.csv("hdfs://localhost:9000/user/input/artists.csv", header="true", mode="overwrite")

    session.catalog.dropTempView('cat_charts_data') #Dropping the temp table as we no longer need it.

    return artist_leaderboard


if __name__=="__main__":
    data = clean_data()
    data = categorize_ranks(data)
    