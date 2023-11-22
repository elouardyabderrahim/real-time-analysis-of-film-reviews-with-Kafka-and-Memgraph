import json
from flask import Flask, jsonify, abort

app = Flask(__name__)

# Read JSON data from file :
with open('movies_rating.json') as f:
    json_data = json.load(f)

@app.route('/api/movies', methods=['GET'])
def get_movies():
    return jsonify(json_data)

if __name__ == '__main__':
    app.run(debug=True)