# Exploring PySpark MLLib and Hadoop
## Introduction

[Apache Hadoop](https://hadoop.apache.org/) is an open source framework for distributed processing of large datasets. It is scalable and robust finding usage in Big Data environments.

Within Hadoop, the [HDFS](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) is used to store, access and maintain large data accross hardware.

[Apache Spark](https://spark.apache.org/) is a data science engine which can execute data engineering, data science, or machine learning algorithms on distributed or single node systems. It has support for multiple languages inclduing SQL, Python, and Scala.

## Objective
The objective of this project is to explore the functionality of Hadoop, how to read and manipulate data from a HDFS using Spark, and exploring the machine learning algorithms offered by Spark in [MLLib](https://spark.apache.org/mllib/).

Due to constraints, we are only able to run this environment on our local machines. Hence we can only explore the functionality on a single node.

## Instructions to contributors

1. `master` is the primary branch for this repository. Make sure to pull and push changes to it.
2. To make any changes, create a new branch from the updated `master` branch, commit your changes to that branch and then submit a pull request to merge the branch back to `master`.
3. DO NOT FORK. DO NOT commit directly to `master`.

## Setting up the environment
1. Start Hadoop DFS and Yarn using the commands `start-dfs.sh` and `start-yarn.sh` commands respectively. (These commands might vary in your system)
2. Create new directory using the command `hadoop fs -mkdir /user/input`.
3. Download the dataset from this [link](https://www.kaggle.com/datasets/dhruvildave/spotify-charts/). 
4. Use the command `hadoop fs -put path-to-dowloaded file /user/input`.
5. Now run `hadoop fs -ls /user/input` and you should be able to see the file is now in the HDFS.