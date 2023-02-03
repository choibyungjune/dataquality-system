from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import glob
BOOKS = [
    {
        'title': 'On the Road',
        'author': 'Jack Kerouac',
        'read': True
    },
    {
        'title': 'Harry Potter and the Philosopher\'s Stone',
        'author': 'J. K. Rowling',
        'read': False
    },
    {
        'title': 'Green Eggs and Ham',
        'author': 'Dr. Seuss',
        'read': True
    }
]

# configuration
DEBUG = True

# instantiate the app
app = Flask(__name__)
app.config.from_object(__name__)
UPLOAD_FOLDER = './uploads'
csvfiles = []
for file in glob.glob(os.path.join(UPLOAD_FOLDER, "*.csv")):
    csvfiles.append(file)

# enable CORS
CORS(app, resources={r'/*': {'origins': '*'}})


# sanity check route
@app.route('/ping', methods=['GET'])
def ping_pong():
    return jsonify('pong!')

@app.route('/login', methods=['POST'])
def login():
  print(request.data)

  email = request.json.get('email')
  password = request.json.get('password')
  usertype = request.json.get('usertype')
  # Perform authentication and return a response
  print(email, password, usertype)

  return jsonify({'message': 'Login successful'})

@app.route('/dbconnect', methods=['POST'])
def dbconnect():
  print(request.data)

  user = request.json.get('user')
  password = request.json.get('password')
  host = request.json.get('host')
  port = request.json.get('port')
  # Perform authentication and return a response
  print(user, password, host, port)

  return jsonify({'message': 'Login successful'})

@app.route('/csvupload', methods=['POST'])
def upload():
    file = request.files['file'] #
    filename = file.filename
    #print(file)
    #print(filename)
    file.save(os.path.join(UPLOAD_FOLDER, filename))

    df = pd.read_csv(os.path.join(UPLOAD_FOLDER, filename))

    print(df.head())
    # return df.to_json()
    return df.head(5).to_json()

@app.route('/columnname', methods=['POST'])
def column_name_inspect():

    print(csvfiles)

    # return df.to_json()
    return jsonify({'message': 'operation success'})

@app.route('/books', methods=['GET', 'POST'])
def all_books():
    response_object = {'status': 'success'}
    if request.method == 'POST':
        post_data = request.get_json()
        BOOKS.append({
            'title': post_data.get('title'),
            'author': post_data.get('author'),
            'read': post_data.get('read')
        })
        response_object['message'] = 'Book added!'
    else:
        response_object['books'] = BOOKS
    return jsonify(response_object)

if __name__ == '__main__':
  app.run(debug=True)