import pandas as pd
import csv,json

def change_to_csv_file(Filename,splitter,columns):

    # read lines from file u_data.txt and save in csv file :
    with open(f'ml-100K/{Filename}.txt', 'r') as file:

        # create csv with column :
        with open(f'{Filename}.csv', 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(columns)

        lines = file.readlines()
        for line in lines:
            line = line.strip().split(splitter)
            line = [value.replace(',', '') for value in line if value != '']
            # print(line)
            # save to  csv :
            with open(f'{Filename}.csv', 'a') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(line)


user_rating = ['userId', 'movieId', 'rating', 'timestamp']
movies_review = ["movieId","movie_title","release_date","IMDb_URL","unknown","Action","Adventure","Animation","Children's","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western"]

change_to_csv_file('u_data','\t',user_rating)
change_to_csv_file('u_item','|',movies_review)


########################################################################
#############  Create Structure Json Api And Save'it ##########
########################################################################

csv1_df = pd.read_csv('u_data.csv')
csv2_df = pd.read_csv('u_item.csv')

# Merge data into the desired structure : 
result = []
for _, entry in csv1_df.iterrows():
    user_id = str(entry['userId'])
    movie_id = str(entry['movieId'])
    rating = str(entry['rating'])
    timestamp = str(entry['timestamp'])

    # Check if movie_id exists in csv2_df
    movie_data = csv2_df[csv2_df['movieId'] == int(movie_id)]
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

# Save this to a JSON file
with open('movies_rating.json', 'w') as f:
    json.dump(result, f)
