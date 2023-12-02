#%%
import createSession
import organize_data
import extract_data
import Visualization as vz

#%% 
SESSION_NAME = "new_session_1"

session = createSession.getOrCreateSession(SESSION_NAME)


#%%
def Extract_data():
    '''
    The reason to use this funciton is beacause without function 
    every time I call/ whenever I call either readArtistDF/readTop200Cat

    data = organize_data.clean_data(session)
    data = organize_data.categorize_ranks(session, data)
    data = organize_data.create_artist_leaderboard(session, data)

    Above commands will get executed and will be writing csv again to
    the hadoop. 
    '''

    extract_data.extract_data(session)
    data = organize_data.clean_data(session)
    data = organize_data.categorize_ranks(session, data)
    data = organize_data.create_artist_leaderboard(session, data)

#%%
def readArtistDF():
    # Reading the newly written CSV's
    artistDF = extract_data.arstistSession(session)
    return artistDF

#%%
def readTop200Cat():
    rankCategoryDF = extract_data.Top200CatSession(session)
    return rankCategoryDF

#%%
def transformData():
    return session

# %%

if __name__ == "__main__":
    #%%
    Extract_data()
    #%%
    transformData()
# %%
