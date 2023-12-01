import code_runner as cr
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import make_pipeline
from pyspark.ml.feature import VectorAssembler


class Visualization:

    def __init__(self) -> None:
        pass

    def getData(self):
        self.artistDF = cr.readArtistDF()
        self.top200DF = cr.readTop200Cat()
        return self.artistDF, self.top200DF

    def plot(self):
        self.data = self.artistDF.toPandas()
        sns.countplot(data=self.data, x="rank_cat")
        plt.title('Count Plot')
        plt.xlabel('Rank Category')
        plt.ylabel('Count')
        plt.show()

if __name__ == "__main__":
    data = Visualization()
    data.getData()
    data.plot()

    # Uncomment the following lines if you want to perform SMOTE analysis
    # balanced_features, balanced_labels = data.balancingData()
