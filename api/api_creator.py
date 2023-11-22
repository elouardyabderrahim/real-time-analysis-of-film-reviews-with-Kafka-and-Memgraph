import json
from flask import Flask, jsonify, abort

app = Flask(__name__)

# Read JSON data from file :
with open('movies_rating.json') as f:
    json_data = json.load(f)

@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(json_data)

@app.route('/api/moviesId', methods=['GET'])
def get_all_movie_ids():
    unique_movie_ids = set()
    for item in json_data:
        movie_id = item["movie"].get("movieId")
        if movie_id:
            unique_movie_ids.add(movie_id)
    
    return jsonify(list(unique_movie_ids))


@app.route('/api/usersId', methods=['GET'])
def get_all_user_ids():
    unique_user_ids = set()
    for item in json_data:
        user_id = item.get("userId")
        if user_id:
            unique_user_ids.add(user_id)
    
    return jsonify(list(unique_user_ids))


if __name__ == '__main__':
    app.run(debug=True)