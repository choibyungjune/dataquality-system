from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import glob
from csv_column_name import check_col_type, change_column
from csv_value import check_word, change_word, change_word_null
import json
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

@app.route('/file-name', methods=['GET'])
def file_name():
    file_names = []
    for file in glob.glob("./uploads/*.csv"):
        file_names.append(file)
    print(file_names)
    json_file_name = json.dumps(file_names)

    return jsonify(json_file_name)


@app.route('/columnname', methods=['GET'])
def column_name_inspect():
    df = pd.read_csv('./uploads/sample_column_name.csv')
    df = check_col_type(df)
    print(df)

    # Check the column names
    column_names = df.columns
    print(column_names)
    return jsonify(column_names.tolist())

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