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