# Instructions for running this project

## Install dependencies
This project needs three major components to run:
1. Python
2. Hadoop DFS
3. PySpark

### Installing Python
This project was written with Python 3.8.10 but a newer version should also work.
1. Follow the instructions on this [link](https://www.digitalocean.com/community/tutorials/install-python-windows-10) to download and install Python (version 3.8.10 or later).
2. Check if Python is installed by running the below command in your terminal:
```
    python --version
```

### Installing Hadoop
For this project we will be running Hadoop locally in a single node configuration. Before we install Hadoop, we need a Java Virtual Machine which is available in JDK or JRE.
1. First download and install the latest version of [Java](https://www.oracle.com/java/technologies/downloads/). If you already have Java, skip this step.
2. Download the Hadoop file `hadoop-3.3.6.tar.gz` from [here](https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/) and extract it in a convenient location.
3. Navigate to the `bin` folder of the JDK install location and copy the path. Example: "C:\Program Files\Java\jdk1.8.0_241\bin". Open the Edit System Environment window and create a New environment variable called "JAVA_HOME" and paste the path as its value.
4. Navigate to the `bin` folder in the extracted hadoop folder. Example: "C:\hadoop\bin". Create a new system environment variable "HADOOP_HOME" and paste the copied path as its value.
5. Edit the System environment variable "PATH" and add both the above paths to it.
6. Run the following commands in the terminal to verify proper setup:
```
    java --version
    hadoop
```

### Preparing to Start Hadoop
We need to set up some files in Hadoop before running the node. All the files can be found in the "etc/hadoop" folder in the hadoop directory.
1. Open the file `core-site.xml` in a text editor and add the following lines:
```
    <configuration>
        <property>
                <name>fs.default.name</name>
                <value>hdfs://0.0.0.0:9000</value>
        </property>
    </configuration>

```
Save and close.

2. Open the file `hdfs-site.xml` in a text editor and add the following lines:
```
    <configuration>
        <property>

            <name>dfs.replication</name>

            <value>1</value>

        </property>
    </configuration>
```
Save and close.

3. Run the following command in your terminal:
```
    hadoop namenode -format
```

### Starting and checking HDFS
1. Run the following command in your terminal:
```
    start-dfs.sh
```
2. Open http://localhost:9870 which will show you the Hadoop Dashboard.
3. In the terminal type the following command while DFS is running:
```
    hadoop fs -mkdir /user/input
```
4. Now Hadoop DFS and the file structure required for this project is set up.

### Installing PySpark
1. Run the command:
```
    pip install pyspark
```
This will install pyspark to your system and its now ready to run this project.

## Add files
The main data file required for this project is the `charts.csv` file present in the `data` folder.
1. Navigate to the `data` folder and open terminal there.
2. Run the following commands:
```
    start-dfs.sh
    hadoop fs -put charts.csv /user/input
```
3. Now the data file for this project is in Hadoop.

## Running the code

### Additional dependencies
1. The project also requires seaborn to be installed for running. Use the following command:
```
    pip install seaborn
```

### Running the main file

1. Ensure hdfs is running. If not, run the command:
```
    start-dfs.sh
```
from the terminal.

2. From the root directory run the command:
```
    python code/code_runner.py
```
to execute the code.

3. If any plot windows pop up during execution, please close them to continue execution. Execution will be halted as long as the window is open.

## Post note about running Hadoop on Windows.
All the members of this team have Mac or Linux machines which means we did not run any of the software or code on Windows. Although the instructions provided above will help install and run the software on Windows, it is possible that you may run into problems.

Installation for Linux or Mac is similar to Windows. Please refer this [link](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html) for instructions in case this project does not run in Windows and you would like to try it on a Mac or Linux machine.