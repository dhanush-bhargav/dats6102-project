import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

class Modelling:

    def __init__(self, session, data):
        self.session = session
        self.artistDF = data

    # def getData(self):
    #     self.artistDF = cr.readArtistDF()
    #     self.top200DF = cr.readTop200Cat()
    #     return self.artistDF, self.top200DF

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

        # Perform the SQL query
        self.result_df = self.session.sql("""
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
        

    # def duplicateRecords(self):
    #     duplicateRecords = self.result_df.exceptAll(self.result_df.dropDuplicates(['artist']))
    #     return duplicateRecords.count()
    
    def kMeansClustering(self):
        
        numerical_columns = self.result_df.columns[1:]

        # Assemble features into a single vector column
        assembler = VectorAssembler(inputCols=numerical_columns, outputCol="features")
        assembled_df = assembler.transform(self.result_df)
        
        k_cluster = 3 
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k_cluster, seed=123)

        kmeans_model = kmeans.fit(assembled_df.select('features'))

        # Get cluster assignments for each data point
        clusteringModel = kmeans_model.transform(assembled_df)

        #Checking the counts of the clusters:
        print(clusteringModel.groupBy("cluster").count().show())

        evaluator = ClusteringEvaluator(predictionCol="cluster") 
        silhouette_score = evaluator.evaluate(clusteringModel)
        print(f"Silhouette Score: {silhouette_score}")

        clusteringModel = clusteringModel.drop('features')
        clusteringModel.write.csv("hdfs://localhost:9000/user/input/results.csv", header='true', mode='overwrite')

# if __name__ == "__main__":
#     data = Modelling()
#     data.getData()
#     data.countPlot()
#     data.transformingDataSql()
#     #data.duplicateRecords()
#     data.kMeansClustering()
    