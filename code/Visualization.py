#%%
import code_runner as cr
import createSession as cs
from pyspark.sql import SparkSession as spark
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE,SMOTENC
from imblearn.pipeline import make_pipeline
from pyspark.ml.feature import VectorAssembler


class Visualization:

    def __init__(self) -> None:
        pass

    def getData(self):
        self.artistDF = cr.readArtistDF()
        self.top200DF = cr.readTop200Cat()
        return self.artistDF, self.top200DF

    def countPlot(self):
        self._artist = self.artistDF.toPandas()
        sns.countplot(data=self._artist, x="rank_cat")
        plt.title('Count Plot')
        plt.xlabel('Rank Category')
        plt.ylabel('Count')
        plt.show()
    
    def histPlot(self):
        sns.histplot(data=self._artist, x = "num_rank_cat")
        plt.title('Hist Plot')
        plt.xlabel('No.of Times Ranked')
        plt.ylabel('Count')
        plt.show()

    def transformingDataSql(self):

        # Register DataFrame as a temporary table
        self.artistDF.createOrReplaceTempView("my_table")

        session = cr.transformData()
        # Perform the SQL query
        result_df = session.sql("""
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
        return result_df.show()

        
        
        

if __name__ == "__main__":
    data = Visualization()
    data.getData()
    #data.countPlot()
    data.transformingDataSql()
    
