'''
Created on 18.09.2014

@author: colinos
'''
import psycopg2 as pg
import pandas.io.sql as psql
import numpy as np
import pandas as pd
import re
import sys
from time import sleep

def connect_db():
    global db_connection
    db_connection = pg.connect("dbname=ebayPredictor user=postgres password=quejodes")

def disconnect_db():
    global db_connection,db_cursor
    db_connection.close()
    db_connection=None  


def get_all_items(category):
    global db_connection
    query="SELECT itemId FROM itemids where categoryid={}".format(category)
    print query
    return psql.read_sql(query, db_connection)
    
def get_all_attributes():
    global db_connection
    query="SELECT * FROM itemAttributes"
    return psql.read_sql(query, db_connection)

def get_all_states():
    global db_connection
    query="SELECT * FROM itemState"
    return psql.read_sql(query, db_connection)

def get_item_attributes(category):
    global db_connection
    
    items=get_all_items(category)
    attributes=get_all_attributes()
    
    attributes=pd.merge(left=items,right=attributes,left_on='itemid',right_on='itemid')
    
    #print items.head()
    attributes.to_csv('attributes.csv',encoding='utf-8')

def dump_tables():
    global db_connection
    query="SELECT * FROM itemIds"
    item_ids=psql.read_sql(query, db_connection)
    item_ids.to_csv('item_ids.csv',encoding='utf-8',index=False,sep='|')
    query="SELECT * FROM itemAttributeIds"
    item_attribute_ids=psql.read_sql(query, db_connection)
    item_attribute_ids.to_csv('item_attribute_ids.csv',encoding='utf-8',index=False,sep='|')
    query="SELECT * FROM itemState"
    item_state=psql.read_sql(query, db_connection)
    item_state.to_csv('item_state.csv',encoding='utf-8',index=False,sep='|')
    query="SELECT * FROM itemAttributes"
    item_attributes=psql.read_sql(query, db_connection)
    item_attributes.to_csv('item_attributes.csv',encoding='utf-8',index=False,sep='|')
    query="SELECT * FROM itemData"
    item_data=psql.read_sql(query, db_connection)
    item_data.to_csv('item_data.csv',encoding='utf-8',index=False,sep='|')
    
def remove_backslashes():
    query="SELECT * FROM itemData"
    item_data=psql.read_sql(query, db_connection)
    db_cursor = db_connection.cursor()
    pattern=re.compile('[\W_]+', re.UNICODE)
    
    for (index,row) in item_data.iterrows():
        #print row['title']
        if('\\' in row['title']):
            title=row['title']
            new_title=title.replace('\\','')
            print title
        #for word in row['title']:
        #    
        #   if(re.match(pattern, word)):
        #      print word
            
        query='UPDATE itemData set title=\'{}\' where itemid={}'.format(new_title,row['itemid'])
        db_cursor.execute(query)
    
    db_connection.commit()
    #for item in item_data

def get_item_states(category):
    global db_connection
    
    items=get_all_items(category)
    states=get_all_states()
    
    states=pd.merge(left=items,right=states,left_on='itemid',right_on='itemid')
    
    #print items.head()
    states.to_csv('states.csv',encoding='utf-8')


def write_test():
    for i in range(10):
        print i
        sleep(1)
    

def exception_test():
    global error
    error=False
    a=[1,2,3]
    try:
        b=a[5]
    except IndexError as e:
        print e
        error=True
    return error
if __name__ == '__main__':
    error=None
    db_connection=None
    connect_db()
    func = globals().get(sys.argv[1])
    #if func is None:
    #    _usage_and_exit()

    func(*sys.argv[2:])
    
    disconnect_db()
    
    pass