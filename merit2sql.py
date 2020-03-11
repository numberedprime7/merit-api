#dependencies
import pandas as pd
import numpy as np
#from datetime import datetime #this library is not required
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

#path of sqlite file goes here in relation to current file
database_path = 'merit.sqlite'

#create DataFrame from mert transaction history file on loyce.club
data = pd.read_csv('http://loyce.club/Merit/merit.all.txt', sep='	', header=None)
#rename columns
data = data.rename(columns={0: 'time', 1: 'number_of_merit', 2:'message_id', 3:'UID_from', 4: 'UID_to'})



#connect to sqllite db
engine = create_engine(f"sqlite:///{database_path}")
session = Session(engine)



#use default declarative base function as variable 'Base'
Base = declarative_base()

#define table schema
class Merit(Base):
    __tablename__ = 'merit'
    id = Column(Integer, primary_key=True)#this is a column with a unique value for each transaction
    time = Column(Integer) #unix time
    number_of_merit = Column(Integer)
    message_id = Column(String(25))
    uid_from = Column(Integer)
    uid_to = Column(Integer)


#create table in sqllite file
Base.metadata.create_all(engine)


#set first value for id as 1
id1 = 1
#loop through DataFrame to add each row in the DataFrame to the SQLlite DB.
for x in np.arange(len(data)):
    session.add(Merit(id=id1, time=int(data['time'][x]), number_of_merit=int(data['number_of_merit'][x]),
                      message_id=data['message_id'][x], uid_from=int(data['UID_from'][x]), uid_to=int(data['UID_to'][x])))
    id1 = id1 + 1 #after the row is added, the id1 variable value will be increased by one
    #will commit rows in batches of 100
    if len(session.new) > 100:
        session.commit()
session.commit() #commit last batch of rows


#check to make sure all rows were successfully imported
if len(session.query(Merit.id).all()) == len(data):
    print(f'All the data from the DataFrame was successfully imported into a SQL file found at {database_path}')
else:
    print(f'There was a problem importing all the merit transactions and {len(data) - len(session.query(Merit.id).all())} were not imported. Troubleshooting is required')
