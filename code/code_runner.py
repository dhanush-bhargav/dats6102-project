import createSession
import organize_data
import extract_data

SESSION_NAME = "new_session_1"

session = createSession.getOrCreateSession(SESSION_NAME)

extract_data.extract_data(session)
data = organize_data.clean_data(session)
data = organize_data.categorize_ranks(session, data)
data = organize_data.create_artist_leaderboard(session, data)