import createSession
import organize_data
import extract_data
from modelling import Modelling

# Create a Spark Session with the name stored in SESSION_NAME

SESSION_NAME = "new_session_1"
session = createSession.getOrCreateSession(SESSION_NAME)

# Read data stored in csv file from Hadoop DFS, extract only the required data write it back into Hadoop as a CSV file.
data = extract_data.extract_data(session)

# Check if there are any missing values in the extracted data set and clean it (there's no missing values in our dataset so we have not cleaned)
data = organize_data.clean_data(session, data)

# Categorize songs based on their position in the ranking charts.
data = organize_data.categorize_ranks(session, data)

# Creates an artist leaderboard based on their appearances in the ranking categories.
artist_data = organize_data.create_artist_leaderboard(session, data)

del data #Cleaning up some space in the RAM

# Create an instance of the Modelling object
model = Modelling(session, artist_data)

# Visualizing the data
model.countPlot()

# Transform the data before running the model
model.transformingDataSql()

# Perform K-Means clustering on the data
model.kMeansClustering()

