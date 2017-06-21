from __future__ import print_function # In python 27
from app import app
from flask import render_template
import redis
import json

# redis connection
redis_server = "localhost"
redis_db = redis.StrictRedis(redis_server, port=6379, db=0)

@app.route('/')
@app.route('/index')
def index():
    user = { 'nickname': 'Wang Han' } # fake user
    mylist = [1,2,3,4]
    return render_template("index.html", title = 'Home', user = user, mylist = mylist)

@app.route('/email')
def email():
 return render_template("base.html")

@app.route('/realtime')
def realtime():
    return render_template("realtime.html")

@app.route('/centers')
def centers():
    centers = json.dumps(getCenters())
    return render_template("centers.html",centers=centers)
    # return render_template("centers.html",redis_db=redis_db)

def getCenters():
    centers = {}
    for i in range(5):
        k = "key-"+str(i)
        print(redis_db.get(k))
        centers[k] = parse_center(redis_db.get(k))
    return centers

@app.route('/_realtimecenter')
def realtimecenter():
    print('realtimecenter is called')
    centers = json.dumps(getCenters())
    return centers

def parse_center(center_str):
    center_str = center_str.replace("'",'')
    center_str = center_str.replace('[','')
    center_str = center_str.replace(']','')
    center_list = center_str.split(',')
    center_list = map(lambda x:float(x),center_list)
    return center_list