#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#dependencies
import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import time
from sqlalchemy import create_engine, Column, Integer, String, Float, func
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from flask import Flask, jsonify
import os


# In[ ]:


#path of sqlite file goes here in relation to current file
database_path = 'merit.sqlite'

#connect to sqllite db
engine = create_engine(f"sqlite:///{database_path}")


# In[ ]:


#reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Merit = Base.classes.merit


# In[ ]:


#set variable 'app' to run Flask
app = Flask(__name__)


# In[ ]:


@app.route("/")
def welcome():
    description1 = '''
    Available Routes:<br/>
    /api/v0.1/number_received/[uid]</br>
    /api/v0.1/received_recent/<uid></br>
    /api/v0.1/number_sent/[uid]</br>
    /api/v0.1/between/[uid_from]/[uid_to]</br>
    /api/v0.1/received/[uid]</br>
    /api/v0.1/sent/[uid]</br>
    </br></br>
    /api/v0.1/number_received/[uid]</br>
    replace [uid] with uid of forum member, must be an integer</br>
    returns:</br> Total Received Merit:  the total merit the uid has received</br>
    uid: the uid you are querying</br></br>
    /api/v0.1/received_recent/[uid]</br>
    replace [uid] with uid of forum member, must be an integer</br>
    returns:</br> Recent Received Merit:  the total merit the uid has received in the last 120 days</br>
    uid: the uid you are querying</br></br>    
    /api/v0.1/number_sent/[uid]</br>
    replace [uid] with uid of forum member, must be an integer</br>
    returns:</br> Total Sent Merit:  the total merit the uid has sent</br>
    uid: the uid you are querying</br></br>
    /api/v0.1/between/[uid_from]/[uid_to]</br>
    replace [uid_from] and [uid_to] with uids of forum members, must be  integers</br>
    Queries merit transactions from [uid_from] to [uid_to]</br>
    returns:</br>Total Received Merit: the total merit sent to [uid_to] from [uid_from]</br>
    Sent from: [uid_from]</br>
    Sent to: [uid_to]</br>
    Transactions: List of dictionaries of merit transactions that contain the following information:</br>
    time: time of merit transaction in the following format: yyyy-mm-dd hh:mm:ss</br>
    Month: Month of merit transaction</br>
    Day of Week: the day of the week of the merit transaction
    number of merit: the number of merit that was sent from [uid_from] to [uid_to] in the merit transaction</br>
    Post: Post ID that received merit </br></br>
    /api/v0.1/received/[uid]</br>
    replace [uid] with uid of forum member, must be an integer</br>
    returns:<br/>
    Total Received Merit:  the total merit the uid has received</br>
    Sent to: the uid you are querying</br>
    Transactions: List of dictionaries of merit transactions that contain the following information:</br>
    time: time of merit transaction in the following format: yyyy-mm-dd hh:mm:ss</br>
    Month: Month of merit transaction</br>
    Day of Week: the day of the week of the merit transaction
    number of merit: the number of merit that was sent to [uid] in the merit transaction</br>
    Post: Post ID that received merit </br>
    Sent from: the uid that sent the merit transaction</br></br>
    /api/v0.1/sent/[uid]</br>
    replace [uid] with uid of forum member, must be an integer</br>
    returns:<br/>
    Total Sent Merit:  the total merit the uid has sent</br>
    Sent to: the uid you are querying</br>
    Transactions: List of dictionaries of merit transactions that contain the following information:</br>
    time: time of merit transaction in the following format: yyyy-mm-dd hh:mm:ss</br>
    Month: Month of merit transaction</br>
    Day of Week: the day of the week of the merit transaction
    number of merit: the number of merit that was sent from [uid_from] in the merit transaction</br>
    Post: Post ID that received merit </br>
    Sent to: the uid that merit was sent to in the subject transaction</br></br>
    
    '''
    


    return (description1)


# In[ ]:


@app.route('/api/v0.1/number_received/<uid>')
def number_received(uid):
    try:
        #confirm the input was an integer
        uid1 = int(uid)
        #connect to sqllite DB
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_to == uid1).all()
        session.close()
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_received = {}
        merit_received['Total Received Merit'] = response
        merit_received['uid'] = uid1
        response_list.append(merit_received)
        return jsonify(response_list) #jsonify
    except ValueError:
        value1 = {'Error': f'{uid} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        no_merit = {"Total_Received_Merit":0, "uid":uid1}
        return jsonify(no_merit)


# In[ ]:


@app.route('/api/v0.1/number_sent/<uid>')
def number_sent(uid):
    try:
        #confirm the input was an integer
        uid1 = int(uid)
        #connect to sqllite DB
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_from == uid1).all()
        session.close()
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_sent = {}
        merit_sent['Total_Sent_Merit'] = response
        merit_sent['uid'] = uid1
        response_list.append(merit_sent)
        return jsonify(response_list)
    except ValueError:
        value1 = {'Error': f'{uid} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        no_merit = {"Total_Sent_Merit":0, "uid":uid1}
        return jsonify(no_merit)


# In[ ]:


@app.route('/api/v0.1/between/<fromm>/<to>')
def between(fromm, to):
    try:
        #confirm the input was an integer
        from1 = int(fromm)
        to1 = int(to)
        #connect to sqllite DB
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_from == from1).filter(Merit.uid_to == to1).all()
        response2 = session.query(Merit.number_of_merit, Merit.message_id, Merit.time).filter(Merit.uid_from == from1).filter(Merit.uid_to == to).all()
        session.close()##pick up coding here
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_sent = {}
        merit_sent['Total_Received_Merit'] = response
        merit_sent['Sent_from'] = from1
        merit_sent['Sent_to'] = to1
        
        response3 = []
        for merit_number, message_id, time in response2:
            response2_dict = {}
            time1 = datetime.utcfromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
            time_day = datetime.utcfromtimestamp(int(time)).strftime('%A') #day of week
            time_month = datetime.utcfromtimestamp(int(time)).strftime('%B') #month
            response2_dict['time'] = time1 
            response2_dict['Month'] = time_month
            response2_dict['Day_of_Week'] = time_day
            response2_dict['number_of_merit'] = merit_number
            response2_dict['Post'] = message_id
            response3.append(response2_dict)
        merit_sent['Transactions'] = response3
        response_list.append(merit_sent) #this might need to be moved to the end
        return jsonify(response_list)
    except ValueError:
        value1 = {'Error': f'{fromm} or {to} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        no_merit = {"Total_Received Merit":0, "Sent_from":from1, 'Sent_to': to1,
                    'Transactions':[{'time': '2009-01-08 08:21:00','Month': 
                                     'January','Day of Week': 'Thursday','number of merit': 0,
                                     'Post': '9999999.msg999999999'}] }
        return jsonify(no_merit)
    


# In[ ]:


@app.route('/api/v0.1/received/<to>')
def transactions_received(to):
    try:
        #confirm the input was an integer
        to1 = int(to)
        #connect to sqllite DB
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_to == to1).all()
        response2 = session.query(Merit.uid_from, Merit.number_of_merit, Merit.message_id, Merit.time).filter(Merit.uid_to == to).all()
        session.close()##pick up coding here
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_sent = {}
        merit_sent['Total_Received_Merit'] = response
        merit_sent['Sent_to'] = to1
        
        response3 = []
        for received_from, merit_number, message_id, time in response2:
            response2_dict = {}
            time1 = datetime.utcfromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
            time_day = datetime.utcfromtimestamp(int(time)).strftime('%A') #day of week
            time_month = datetime.utcfromtimestamp(int(time)).strftime('%B') #month
            response2_dict['time'] = time1 
            response2_dict['Month'] = time_month
            response2_dict['Day_of_Week'] = time_day
            response2_dict['number_of_merit'] = merit_number
            response2_dict['Post'] = message_id
            response2_dict['Sent_from'] = received_from
            response3.append(response2_dict)
        merit_sent['Transactions'] = response3
        response_list.append(merit_sent) #this might need to be moved to the end
        return jsonify(response_list)
    except ValueError:
        value1 = {'Error': f'{to} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        no_merit = {"Total_Received_Merit":0, 'Sent to': to1,
                    'Transactions':[{'time': '2009-01-08 08:21:00','Month': 
                                     'January','Day_of_Week': 'Thursday','number_of_merit': 0,
                                     'Post': '9999999.msg999999999'}] }
        return jsonify(no_merit)
    


# In[ ]:


@app.route('/api/v0.1/sent/<fromm>')
def transactions_sent(fromm):
    try:
        #confirm the input was an integer
        from1 = int(fromm)
        #connect to sqllite DB
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_from == from1).all()
        response2 = session.query(Merit.uid_to, Merit.number_of_merit, Merit.message_id, Merit.time).filter(Merit.uid_from == from1).all()
        session.close()##pick up coding here
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_sent = {}
        merit_sent['Total_Sent_Merit'] = response
        merit_sent['Sent_from'] = from1
        
        response3 = []
        for sent_to, merit_number, message_id, time in response2:
            response2_dict = {}
            time1 = datetime.utcfromtimestamp(int(time)).strftime('%Y-%m-%d %H:%M:%S')
            time_day = datetime.utcfromtimestamp(int(time)).strftime('%A') #day of week
            time_month = datetime.utcfromtimestamp(int(time)).strftime('%B') #month
            response2_dict['time'] = time1 
            response2_dict['Month'] = time_month
            response2_dict['Day of Week'] = time_day
            response2_dict['number of merit'] = merit_number
            response2_dict['Post'] = message_id
            response2_dict['sent to'] = sent_to
            response3.append(response2_dict)
        merit_sent['Transactions'] = response3
        response_list.append(merit_sent) #this might need to be moved to the end
        return jsonify(response_list)
    except ValueError:
        value1 = {'Error': f'{fromm} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        #todo - fix dictionary to match normal function output
        no_merit = {"Total_Received_Merit":0, "Sent_from":from1, 'Sent_to': 2,
                    'Transactions':[{'time': '2009-01-08 08:21:00','Month': 
                                     'January','Day of Week': 'Thursday','number_of_merit': 0,
                                     'Post': '9999999.msg999999999'}] }
        return jsonify(no_merit)
    


# In[ ]:


@app.route('/api/v0.1/received_recent/<uid>')
def recent_merit(uid):
    try:
        #confirm the input was an integer
        uid1 = int(uid)
        #connect to sqllite DB
        early_date = datetime.today() + dt.timedelta(days=-120)
        start_date = dt.date.strftime(early_date, '%Y-%m-%d')
        start_date_epoch = int(time.mktime(time.strptime(start_date, '%Y-%m-%d')))
        session = Session(engine)
        response = session.query(func.sum(Merit.number_of_merit)).filter(Merit.uid_to == uid1).filter(Merit.time >= start_date_epoch).all()
        session.close()
        response = list(np.ravel(response))
        response = int(response[0])
        response_list = []
        merit_received = {}
        merit_received['Recent_Received_Merit'] = response
        merit_received['uid'] = uid1
        response_list.append(merit_received)
        return jsonify(response_list) #jsonify
    except ValueError:
        value1 = {'Error': f'{uid} is not an Integer. Please reformat into an Integer and try again'}
        return jsonify(value1)
    except TypeError:
        no_merit = {"Recent_Received_Merit":0, "uid":uid1}
        return jsonify(no_merit)


# In[ ]:


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
#uncomment these lines in production -- do not use in jupyter notebook


# In[ ]:




