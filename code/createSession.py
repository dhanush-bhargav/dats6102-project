from pyspark.sql import SparkSession

def getOrCreateSession(name):
    #Create a Spark Session on local machine with name 'project-sample'. If it already exists, get the session.
    session = SparkSession.builder.master("local").appName(name).config("spark.executor.memory", "6g").getOrCreate()
    
    return session

