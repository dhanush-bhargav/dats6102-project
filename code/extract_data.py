from pyspark.sql.types import StructType

def extract_data(session):

    #Define the schema before importing data from Hadoop. Schema contains name of each column and data type.
    dataSchema = StructType().add("title", "string").add("rank", "integer").add("date", "string").add("artist", "string").add("url", "string").add("region", "string").add("chart", "string").add("trend", "string").add("streams", "integer")

    # -- Reading Raw Data -- 

    #Read CSV file from HDFS running on localhost using the previously defined schema. `header` indicates that the file contains a header row which should be omitted before reading.
    data = session.read.csv("hdfs://localhost:9000/user/input/charts.csv", schema=dataSchema, header=True)

    #Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
    data.createOrReplaceTempView("charts_data")


    # Top200 chart has the most number of entries so we're only interested in that entries with chart=top200.
    # We are only interested in the columns title, rank, date, artist, region, and streams so we will select only those columns.

    result = session.sql("SELECT title, rank, date, artist, region, streams FROM charts_data WHERE chart='top200'")
    result.write.csv("hdfs://localhost:9000/user/input/top200_raw.csv", header="true", mode="overwrite")
    
def arstistSession(session):

    '''
    This function creates a schema for the top200_cat.csv, 
    also returns the artists.csv data frame.  

    '''
    dataSchema_ = StructType().add("artist", "string").add("rank_cat","string").add("total_streams","long").add("num_rank_cat","integer")

    artists = session.read.csv("hdfs://localhost:9000/user/input/artists.csv", schema=dataSchema_, header=True)

    #Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
    artists.createOrReplaceTempView("Artists")

    return artists

def Top200CatSession(session):

    '''
    This function creates a schema for the top200_cat.csv, 
    also returns the top200_cat.csv data frame.  

    '''

    dataSchema_200 = StructType().add("title","string").add("rank","integer").add("date","string").add("artist", "string").add("region","string").add("Streams","integer").add("rank_cat","string")

    top200Category = session.read.csv("hdfs://localhost:9000/user/input/top200_cat.csv", schema=dataSchema_200, header=True)

    #Create a temporary local table view of the DataFrame with name as `charts_data`. We can now run SQL queries on this table.
    top200Category.createOrReplaceTempView("top200_Category")

    return top200Category


if __name__=="__main__":
    extract_data()
