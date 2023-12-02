#%%
import code_runner as cr
import createSession as cs
from pyspark.sql import SparkSession as spark
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE,SMOTENC
from imblearn.pipeline import make_pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

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
    
    def transformingDataSql(self):

        # Register DataFrame as a temporary table
        self.artistDF.createOrReplaceTempView("my_table")

        session = cr.transformData()
        # Perform the SQL query
        self.result_df = session.sql("""
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
        return self.result_df.show()

    def duplicateRecords(self):
        duplicateRecords = self.result_df.exceptAll(self.result_df.dropDuplicates(['artist']))
        return duplicateRecords.count()
    
    def kMeansClustering(self):
        
        numerical_columns = self.result_df.columns[1:]
        # numerical_columns = ["artist","total_streams","top10","top20", "top50", "top100", "top200"]

        # Display the first few rows of the DataFrame with numerical columns
        self.result_df.select(numerical_columns).show(5)
        # Assemble features into a single vector column
        assembler = VectorAssembler(inputCols=numerical_columns, outputCol="features")
        assembled_df = assembler.transform(self.result_df)
        
        k_cluster = 3 
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k_cluster, seed=123)

        kmeans_model = kmeans.fit(assembled_df)

        # Get cluster assignments for each data point
        clusteringModel = kmeans_model.transform(assembled_df)

        #Checking the counts of the clusters:
        print(clusteringModel.groupBy("cluster").count().show())

        evaluator = ClusteringEvaluator(predictionCol="cluster") 
        silhouette_score = evaluator.evaluate(clusteringModel)
        print(f"Silhouette Score: {silhouette_score}")

        self.result_df.show()

if __name__ == "__main__":
    data = Visualization()
    data.getData()
    data.countPlot()
    data.transformingDataSql()
    #data.duplicateRecords()
    data.kMeansClustering()
    

# %%
