import pandas as pd
import json

def change_to_df(filpath,splitter,columns) :

    df = pd.read_csv(filpath, sep=splitter, header=None, names=columns)
    return df

user_rating = ['userId', 'movieId', 'rating', 'timestamp']
movies_review = ["movieId","movie_title","release_date","error","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"]

user_df = change_to_df('ml-100K/u_data.txt','\t',user_rating)
item_df = change_to_df('ml-100K/u_item.txt','|',movies_review)
# drop column :
item_df = item_df.drop('error', axis=1)

########################################################################
#############  Create Structure Json Api And Save'it ##########
########################################################################

result = []
for _, entry in user_df.iterrows():
    user_id = str(entry['userId'])
    movie_id = str(entry['movieId'])
    rating = str(entry['rating'])
    timestamp = str(entry['timestamp'])

    # Check if movie_id exists in item_df
    movie_data = item_df[item_df['movieId'] == int(movie_id)]
    if not movie_data.empty:
        movie_data = movie_data.iloc[0]
        title = movie_data['movie_title']
        genres = [genre for genre, value in movie_data.items() if value == 1 and genre != 'movieId']

        fishies_entry = {
            'userId': user_id,
            'movie': {
                'movieId': movie_id,
                'title': title,
                'genres': genres
            },
            'rating': rating,
            'timestamp': timestamp
        }

        result.append(fishies_entry)

# Save this to a JSON file :
with open('movies_rating.json', 'w') as f:
    json.dump(result, f)