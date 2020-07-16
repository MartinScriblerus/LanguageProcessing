import os
import requests
import operator
import re
import nltk
import json
from rq import Queue
from rq.job import Job
from worker import conn
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from stop_words import stops
from collections import Counter
from bs4 import BeautifulSoup
from sqlalchemy.dialects.postgresql import JSON
from psycopg2.extensions import register_adapter

register_adapter(dict, JSON)

app = Flask(__name__)
app.config.from_object(os.environ.get('APP_SETTINGS'))
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db = SQLAlchemy(app)

q = Queue(connection=conn)

class Result(db.Model):
    __tablename__ = 'results'

    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String())
    result_all = db.Column(JSON)
    result_no_stop_words = db.Column(JSON)

    def __init__(self, url, result_all, result_no_stop_words):
        
        self.url = url
        self.result_all = result_all
        self.result_no_stop_words = result_no_stop_words

    def __repr__(self):
        return '<id {}>'.format(self.id)

print(Result)

def count_and_save_words(url):
    
    errors = []

    try:
        r = requests.get(url)
    except:
        errors.append(
            "Unable to get URL. Please make sure it's valid and try again."
        )
        return {"error": errors}

    # text processing
    raw = BeautifulSoup(r.text, features="html.parser").get_text()
    nltk.data.path.append('./nltk_data/')  # set the path
    tokens = nltk.word_tokenize(raw)
    text = nltk.Text(tokens)

    # remove punctuation, count raw words
    nonPunct = re.compile('.*[A-Za-z].*')
    raw_words = [w for w in text if nonPunct.match(w)]
    raw_word_count = Counter(raw_words)
    print(raw_words)
    # stop words
    no_stop_words = [w for w in raw_words if w.lower() not in stops]
    no_stop_words_count = Counter(no_stop_words)
    # print("HERE IS DB JUST PRIOR TO INIT SESSION", db)
    # save the results
    try:
        result = Result(
            url=url,
            result_all=raw_word_count,
            result_no_stop_words=no_stop_words_count
        )
        print(result)
        # print("HERE IS DB JUST PRIOR TO INIT SESSION", db)
        db.session.add(result)
        db.session.commit()
        print(db)
        return result.id
    except:
        errors.append("Unable to add item to database.")
        return {"error": errors}





@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
#   results = {}
#     if request.method == "POST":
#         # this import solves a rq bug which currently exists
#         from app import count_and_save_words
def get_counts():
    # get url
    data = json.loads(request.data.decode())
    # get url that the person has entered
    url = data['url']
    # if 'http://' not in url[:7]:
    #     url = 'http://' + url
    if not url[:8].startswith(('https://', 'http://')):
        url = 'https://' + url
    job = q.enqueue_call(
        func=count_and_save_words, args=(url,), result_ttl=5000
    )
    # return created job id
    return job.get_id()

@app.route("/results/<job_key>", methods=['GET'])
def get_results(job_key):

    job = Job.fetch(job_key, connection=conn)
    

    if job.is_finished:
      
        result = Result.query.filter_by(id=job.result).first()
        results = sorted(
            result.result_no_stop_words.items(),
            key=operator.itemgetter(1),
            reverse=True
        )[:10]
       
        return jsonify(results)
       
    else:
        return "Nay!", 202



if __name__ == '__main__':
    app.run()