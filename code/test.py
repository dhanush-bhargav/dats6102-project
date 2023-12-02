# import pandas as pd

# # Your updated DataFrame
# data = {'artist': ['Ed Sheeran', 'Ed Sheeran','Ed Sheeran', 'Coez', 'Karel Gott', 'Beéle', 'Jenni Vartiainen','Beéle'],
#         'rank_cat': ['top200', 'top200', 'top50','top100', 'top200', 'top50', 'top50','top50'],
#         'total_streams': [6863056312, 58115173, 58115173,58115173, 549206, 26677129, 6966862,6966862],
#         'num_rank_cat': [174677, 1104,1104 ,1104, 153, 1300, 274, 1300]}

# df = pd.DataFrame(data)


# # Group by artist and rank_cat, then aggregate num_rank_cat
# grouped_df = df.groupby(['artist', 'rank_cat'])['num_rank_cat'].sum().reset_index()

# # Pivot the DataFrame to have rank_cat categories as columns
# pivot_df = grouped_df.pivot_table(index='artist', columns='rank_cat', values='num_rank_cat', aggfunc='sum', fill_value=0)

# # Convert the list of unique rank categories to strings (optional)
# pivot_df.reset_index(inplace=True)
# pivot_df.columns.name = None


# print(type(pivot_df))
# # # Pivot the DataFrame
# # pivot_df = grouped_df.pivot_table(index='artist', columns='rank_cat', values='num_rank_cat', aggfunc='8', fill_value=0)

# # # Reset the index to make 'artist' a regular column
# # pivot_df.reset_index(inplace=True)

# # # Display the result
# # print((pivot_df))




import createSession as cs

print(cs.getOrCreateSession("session_v"))